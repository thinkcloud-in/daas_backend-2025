import logging
import os
import uuid

from fastapi import HTTPException
from sqlalchemy.orm import Session
from temporalio.common import SearchAttributeKey, SearchAttributePair, TypedSearchAttributes

_LXC_BRIDGE   = os.getenv("LXC_BRIDGE",   "vmbr0")
_LXC_SSH_USER = os.getenv("LXC_SSH_USER", "root")
_LXC_SSH_PASS = os.getenv("LXC_SSH_PASS", "")

from models.lxc_restore_model import LXCRestoreJob, LXCRestoreCreate
from models.library_model import LibraryItem
from models.IPs_model import IPEntry, IPSModel
from models.models import Cluster
from service.temporalResource.workers.workers_lxc_restore import TASK_QUEUE
from service.temporalResource.workflows.workflows_lxc_restore import LXCRestoreWorkflow
from utils.temporal_client import TemporalClientManager
from utils import response_format

logger = logging.getLogger(__name__)

_ENTITY_KEY   = SearchAttributeKey.for_keyword("Entity")
_ACTION_KEY   = SearchAttributeKey.for_keyword("Action")
_USERNAME_KEY = SearchAttributeKey.for_keyword("UserName")


def _make_search_attrs(entity: str, action: str, username: str = "system") -> TypedSearchAttributes:
    return TypedSearchAttributes([
        SearchAttributePair(_ENTITY_KEY,   entity),
        SearchAttributePair(_ACTION_KEY,   action),
        SearchAttributePair(_USERNAME_KEY, username),
    ])


def _job_to_dict(r: LXCRestoreJob) -> dict:
    return {
        "id":               r.id,
        "name":             r.name,
        "cluster_id":       r.cluster_id,
        "ip_pool_id":       r.ip_pool_id,
        "library_item_id":  r.library_item_id,
        "node":             r.node,
        "vmid":             r.vmid,
        "ip_address":       r.ip_address,
        "storage":          r.storage,
        "status":           r.status,
        "workflow_id":      r.workflow_id,
        "created_at":       str(r.created_at) if r.created_at else None,
        "updated_at":       str(r.updated_at) if r.updated_at else None,
    }


async def create_lxc_restore_job(data: LXCRestoreCreate, db: Session):
    try:
        # Validate cluster
        cluster = db.query(Cluster).filter(Cluster.name == data.cluster).first()
        if not cluster:
            raise HTTPException(status_code=404, detail=f"Cluster '{data.cluster}' not found")

        # Validate library item (LXC backup on K8s PV)
        item = db.query(LibraryItem).filter(LibraryItem.id == data.template_id).first()
        if not item:
            raise HTTPException(status_code=404, detail=f"Library item {data.template_id} not found")
        if item.status != "ready":
            raise HTTPException(
                status_code=409,
                detail=f"Library item '{item.name}' is not ready yet (status={item.status}). Wait for upload to complete.",
            )

        # Resolve IP pool and reserve one IP
        pool = db.query(IPSModel).filter(IPSModel.Pool_name == data.ip_pool).first()
        if not pool:
            raise HTTPException(status_code=404, detail=f"IP pool '{data.ip_pool}' not found")

        ip_entry = (
            db.query(IPEntry)
            .filter(IPEntry.pool_id == pool.id, IPEntry.status == "unused")
            .order_by(IPEntry.id.asc())
            .first()
        )
        if not ip_entry:
            raise HTTPException(status_code=400, detail=f"No available IPs in pool '{data.ip_pool}'")

        ip_entry.status = "used"
        db.flush()

        # Create job record — shows in list immediately at status=provisioning
        record = LXCRestoreJob(
            name=data.name,
            cluster_id=cluster.id,
            ip_pool_id=pool.id,
            library_item_id=data.template_id,
            ip_address=ip_entry.ip,
            storage=data.storage or "local-lvm",
            status="provisioning",
        )
        db.add(record)
        db.commit()
        db.refresh(record)

        # Build workflow payload — bridge/ssh from env, not from user input
        workflow_payload = {
            "job_id":          record.id,
            "cluster_id":      cluster.id,
            "library_item_id": data.template_id,
            "ip_address":      ip_entry.ip,
            "ip_pool_id":      pool.id,
            "name":            data.name,
            "storage":         data.storage or "local-lvm",
            "bridge":          _LXC_BRIDGE,
            "ssh_user":        _LXC_SSH_USER,
            "ssh_pass":        _LXC_SSH_PASS,
        }

        client  = await TemporalClientManager.get_temporal_client()
        wf_id   = f"lxc-restore-{record.id}-{uuid.uuid4().hex[:8]}"
        await client.start_workflow(
            LXCRestoreWorkflow.run,
            workflow_payload,
            id=wf_id,
            task_queue=TASK_QUEUE,
            search_attributes=_make_search_attrs(data.name, "LXC-Restore"),
        )

        record.workflow_id = wf_id
        db.commit()

        return response_format.success_response(
            201, "LXC restore job started",
            {**_job_to_dict(record), "template_name": item.name},
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"create_lxc_restore_job error: {e}", exc_info=True)
        return response_format.error_response(500, "Failed to create LXC restore job", str(e))


def list_lxc_restore_jobs(db: Session, page: int = 1, page_size: int = 10):
    try:
        page      = max(1, page)
        page_size = max(1, min(page_size, 100))
        offset    = (page - 1) * page_size

        total   = db.query(LXCRestoreJob).count()
        records = (
            db.query(LXCRestoreJob)
            .order_by(LXCRestoreJob.created_at.desc())
            .offset(offset).limit(page_size).all()
        )

        # Enrich with library item name
        lib_ids  = list({r.library_item_id for r in records if r.library_item_id})
        lib_map  = {}
        if lib_ids:
            items   = db.query(LibraryItem).filter(LibraryItem.id.in_(lib_ids)).all()
            lib_map = {i.id: i.name for i in items}

        data = [
            {**_job_to_dict(r), "template_name": lib_map.get(r.library_item_id)}
            for r in records
        ]

        total_pages = (total + page_size - 1) // page_size
        return response_format.success_response(200, "LXC restore jobs fetched", {
            "items": data,
            "pagination": {
                "page":        page,
                "page_size":   page_size,
                "total":       total,
                "total_pages": total_pages,
                "has_next":    page < total_pages,
                "has_prev":    page > 1,
            },
        })
    except Exception as e:
        return response_format.error_response(500, "Failed to list LXC restore jobs", str(e))


def get_lxc_restore_job(job_id: int, db: Session):
    try:
        record = db.query(LXCRestoreJob).filter(LXCRestoreJob.id == job_id).first()
        if not record:
            raise HTTPException(status_code=404, detail=f"LXC restore job {job_id} not found")

        item = db.query(LibraryItem).filter(LibraryItem.id == record.library_item_id).first()
        return response_format.success_response(200, "LXC restore job fetched", {
            **_job_to_dict(record),
            "template_name": item.name if item else None,
        })
    except HTTPException:
        raise
    except Exception as e:
        return response_format.error_response(500, "Failed to get LXC restore job", str(e))

import logging

import requests
from fastapi import HTTPException
from sqlalchemy.orm import Session

from models.lxc_restore_model import LXCRestoreJob
from models.library_model import LibraryItem
from models.models import Cluster
from service.clusterService import get_api_token, getting_Proxmox_host
from utils import response_format

logger = logging.getLogger(__name__)

# Library types that represent a Harbor deployment
_HARBOR_TYPES = {"harbor_template", "lxc_backup"}


def _job_to_dict(r: LXCRestoreJob, lib_item: LibraryItem | None = None) -> dict:
    return {
        "id":              r.id,
        "name":            r.name,
        "cluster_id":      r.cluster_id,
        "library_item_id": r.library_item_id,
        "template_name":   lib_item.name if lib_item else None,
        "template_type":   lib_item.type if lib_item else None,
        "node":            r.node,
        "vmid":            r.vmid,
        "ip_address":      r.ip_address,
        "deploy_status":   r.status,
        "container_state": r.container_state or "unknown",
        "workflow_id":     r.workflow_id,
        "created_at":      str(r.created_at) if r.created_at else None,
        "updated_at":      str(r.updated_at) if r.updated_at else None,
    }


def _harbor_item_map(db: Session) -> dict[int, LibraryItem]:
    items = db.query(LibraryItem).filter(LibraryItem.type.in_(_HARBOR_TYPES)).all()
    return {i.id: i for i in items}


def list_harbor_deployments(db: Session, page: int = 1, page_size: int = 10):
    try:
        page      = max(1, page)
        page_size = max(1, min(page_size, 100))
        offset    = (page - 1) * page_size

        lib_map = _harbor_item_map(db)
        if not lib_map:
            return response_format.success_response(200, "No harbor deployments found", {
                "items": [],
                "pagination": {"page": 1, "page_size": page_size, "total": 0,
                               "total_pages": 0, "has_next": False, "has_prev": False},
            })

        harbor_ids = list(lib_map.keys())
        total      = db.query(LXCRestoreJob).filter(LXCRestoreJob.library_item_id.in_(harbor_ids)).count()
        records    = (
            db.query(LXCRestoreJob)
            .filter(LXCRestoreJob.library_item_id.in_(harbor_ids))
            .order_by(LXCRestoreJob.created_at.desc())
            .offset(offset).limit(page_size).all()
        )

        data        = [_job_to_dict(r, lib_map.get(r.library_item_id)) for r in records]
        total_pages = (total + page_size - 1) // page_size
        return response_format.success_response(200, "Harbor deployments fetched", {
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
        logger.error(f"list_harbor_deployments error: {e}", exc_info=True)
        return response_format.error_response(500, "Failed to list harbor deployments", str(e))


def get_harbor_deployment(job_id: int, db: Session):
    try:
        record = db.query(LXCRestoreJob).filter(LXCRestoreJob.id == job_id).first()
        if not record:
            raise HTTPException(status_code=404, detail=f"Harbor deployment {job_id} not found")

        lib_item = db.query(LibraryItem).filter(LibraryItem.id == record.library_item_id).first()
        return response_format.success_response(200, "Harbor deployment fetched",
                                                _job_to_dict(record, lib_item))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_harbor_deployment error: {e}", exc_info=True)
        return response_format.error_response(500, "Failed to get harbor deployment", str(e))


def sync_harbor_state(job_id: int, db: Session):
    """
    Proxmox API se real container state query karo aur DB update karo.
    Useful jab Proxmox pe manually kuch kiya ho aur DB out-of-sync ho.
    """
    try:
        record = db.query(LXCRestoreJob).filter(LXCRestoreJob.id == job_id).first()
        if not record:
            raise HTTPException(status_code=404, detail=f"Harbor deployment {job_id} not found")
        if not record.vmid or not record.node:
            raise HTTPException(status_code=400,
                                detail="VMID / node not set — deployment may still be in progress")

        cluster = db.query(Cluster).filter(Cluster.id == record.cluster_id).first()
        if not cluster:
            raise HTTPException(status_code=404, detail="Cluster not found")

        api_token    = get_api_token(db, cluster.name)
        headers      = {"Authorization": f"PVEAPIToken={api_token}"}
        proxmox_host = getting_Proxmox_host(cluster)

        resp = requests.get(
            f"{proxmox_host}/api2/json/nodes/{record.node}/lxc/{record.vmid}/status/current",
            headers=headers, verify=False, timeout=10,
        )
        resp.raise_for_status()
        proxmox_state = resp.json().get("data", {}).get("status", "unknown")

        record.container_state = proxmox_state
        db.commit()

        logger.info(f"[Harbor] sync job={job_id} vmid={record.vmid} state={proxmox_state}")
        return response_format.success_response(200, "Container state synced from Proxmox", {
            "job_id":          job_id,
            "vmid":            record.vmid,
            "node":            record.node,
            "container_state": proxmox_state,
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"sync_harbor_state error: {e}", exc_info=True)
        return response_format.error_response(500, "Failed to sync container state", str(e))


def harbor_container_action(job_id: int, action: str, db: Session):
    """
    Container ko Proxmox pe start/stop/shutdown karo aur DB update karo.
    action: "start" | "stop" | "shutdown"
    """
    if action not in ("start", "stop", "shutdown"):
        raise HTTPException(status_code=400, detail="action must be one of: start, stop, shutdown")

    try:
        record = db.query(LXCRestoreJob).filter(LXCRestoreJob.id == job_id).first()
        if not record:
            raise HTTPException(status_code=404, detail=f"Harbor deployment {job_id} not found")
        if not record.vmid or not record.node:
            raise HTTPException(status_code=400,
                                detail="VMID / node not set — deployment may still be in progress")

        cluster = db.query(Cluster).filter(Cluster.id == record.cluster_id).first()
        if not cluster:
            raise HTTPException(status_code=404, detail="Cluster not found")

        api_token    = get_api_token(db, cluster.name)
        headers      = {"Authorization": f"PVEAPIToken={api_token}"}
        proxmox_host = getting_Proxmox_host(cluster)

        resp = requests.post(
            f"{proxmox_host}/api2/json/nodes/{record.node}/lxc/{record.vmid}/status/{action}",
            headers=headers, verify=False, timeout=30,
        )
        resp.raise_for_status()

        new_state = "running" if action == "start" else "stopped"
        record.container_state = new_state
        db.commit()

        logger.info(f"[Harbor] action={action} job={job_id} vmid={record.vmid} → {new_state}")
        return response_format.success_response(200, f"Container {action} triggered", {
            "job_id":          job_id,
            "vmid":            record.vmid,
            "action":          action,
            "container_state": new_state,
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"harbor_container_action error: {e}", exc_info=True)
        return response_format.error_response(500, f"Failed to {action} container", str(e))

import asyncio
import logging
import os
import pytz
from fastapi import HTTPException
from sqlalchemy.orm import Session
from temporalio.api.enums.v1 import EventType
from temporalio.client import WorkflowExecutionStatus

from models.llm_inference_v2_model import LLMInferenceJob, LLMInferenceJobCreate, LLMInferenceJobUpdate, PoolActionRequest
from models.IPs_model import IPEntry, IPSModel
from models.models import Cluster, Machine
from utils.temporal_client import TemporalClientManager
from service.temporalResource.workers.workers_llm_inference_v2 import TASK_QUEUE
from service.temporalResource.workflows.workflows_llm_inference_v2 import CreateMultiNodeLLMWorkflow, DeleteLLMPoolWorkflow
from utils import response_format

_IST = pytz.timezone("Asia/Kolkata")
_TIME_FMT = "%Y-%m-%d %H:%M:%S"

_ACTIVITY_DISPLAY = {
    # LLM inference
    "clone_and_configure_vm_activity":    "VM Clone & Configure",
    "update_llm_inference_job_activity":  "Update Job Status",
    "install_ray_vllm_activity":          "Install Ray + vLLM",
    "add_affinity_rule_activity":         "Set Affinity Rules",
    "configure_ray_activity":             "Configure Ray",
    "launch_vllm_from_template_activity": "Launch vLLM",
    "restore_llm_services_activity":      "Restore LLM Services",
    "vm_power_action_activity":           "VM Power Action",
    "delete_llm_pool_activity":           "Delete VMs & Cleanup",
    # Library
    "transfer_file_activity":             "File Transfer to PV",
    "transfer_file_to_harbor_activity":   "File Transfer to Harbor (SFTP)",
    "finalize_library_record_activity":   "Finalize Record",
    "mark_upload_failed_activity":        "Cleanup Failed Upload",
    "delete_library_file_activity":       "Delete File from Storage",
    "update_library_metadata_activity":   "Update Library Metadata",
    # LXC Restore
    "upload_lxc_to_proxmox_activity":    "Upload LXC Backup to Proxmox",
    "restore_lxc_container_activity":    "Restore & Start LXC Container",
    "setup_lxc_container_activity":      "Configure SSH + iptables",
    "update_lxc_job_status_activity":    "Update Job Status",
}


def _fmt_time(ts):
    try:
        return ts.astimezone(_IST).strftime(_TIME_FMT)
    except Exception:
        return None


async def _fetch_steps(handle) -> list:
    """Parse Temporal workflow history into ordered activity step list."""
    history = await handle.fetch_history()
    step_map = {}
    step_order = []

    for event in history.events:
        et = event.event_type

        if et == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
            attrs = event.activity_task_scheduled_event_attributes
            name  = attrs.activity_type.name
            step_map[event.event_id] = {
                "activity_name": name,
                "display_name":  _ACTIVITY_DISPLAY.get(name, name),
                "status":        "pending",
                "scheduled_at":  _fmt_time(event.event_time),
                "started_at":    None,
                "completed_at":  None,
                "error":         None,
            }
            step_order.append(event.event_id)

        elif et == EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED:
            sid = event.activity_task_started_event_attributes.scheduled_event_id
            if sid in step_map:
                step_map[sid]["status"]     = "running"
                step_map[sid]["started_at"] = _fmt_time(event.event_time)

        elif et == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
            sid = event.activity_task_completed_event_attributes.scheduled_event_id
            if sid in step_map:
                step_map[sid]["status"]       = "completed"
                step_map[sid]["completed_at"] = _fmt_time(event.event_time)

        elif et == EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED:
            attrs = event.activity_task_failed_event_attributes
            sid   = attrs.scheduled_event_id
            if sid in step_map:
                step_map[sid]["status"]       = "failed"
                step_map[sid]["completed_at"] = _fmt_time(event.event_time)
                step_map[sid]["error"]        = getattr(attrs.failure, "message", str(attrs.failure)) if attrs.failure else "Unknown"

        elif et == EventType.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
            sid = event.activity_task_timed_out_event_attributes.scheduled_event_id
            if sid in step_map:
                step_map[sid]["status"]       = "timed_out"
                step_map[sid]["completed_at"] = _fmt_time(event.event_time)
                step_map[sid]["error"]        = "Activity timed out"

    return [step_map[eid] for eid in step_order]

logger = logging.getLogger(__name__)

_SSH_USER = os.getenv("LLM_VM_SSH_USER", "root")
_SSH_PASS  = os.getenv("LLM_VM_SSH_PASS", "Teamw0rk@1")


async def create_llm_inference_job(data: LLMInferenceJobCreate, db: Session):
    try:
        if len(data.nodes) == 0:
            raise HTTPException(status_code=400, detail="At least one node is required")
        if len(data.ipPools) == 0:
            raise HTTPException(status_code=400, detail="At least one IP pool is required")
        if not data.template or not data.template.strip():
            raise HTTPException(status_code=400, detail="Template VM is required. Provide a valid Proxmox template VMID or name.")

        # ── Prevent duplicate job name ────────────────────────────────────────
        existing = db.query(LLMInferenceJob).filter(LLMInferenceJob.name == data.poolName).first()
        if existing:
            raise HTTPException(status_code=409, detail=f"LLM inference job '{data.poolName}' already exists")

        # ── Resolve clusterName → Cluster ─────────────────────────────────────
        cluster = db.query(Cluster).filter(Cluster.name == data.clusterName).first()
        if not cluster:
            raise HTTPException(status_code=404, detail=f"Cluster '{data.clusterName}' not found")

        # ── Resolve ipPools → IPSModel list ───────────────────────────────────
        ip_pool_objects = []
        for pool_name in data.ipPools:
            pool = db.query(IPSModel).filter(IPSModel.Pool_name == pool_name).first()
            if not pool:
                raise HTTPException(status_code=404, detail=f"IP pool '{pool_name}' not found")
            ip_pool_objects.append(pool)

        ip_pool_ids = [p.id for p in ip_pool_objects]

        # ── Reserve one IP per node from any available pool ───────────────────
        reserved_ips = []
        for i, node_cfg in enumerate(data.nodes):
            ip_entry = None
            for pool in ip_pool_objects:
                ip_entry = (
                    db.query(IPEntry)
                    .filter(IPEntry.pool_id == pool.id, IPEntry.status == "unused")
                    .order_by(IPEntry.id.asc())
                    .first()
                )
                if ip_entry:
                    break
            if not ip_entry:
                db.rollback()
                raise HTTPException(
                    status_code=400,
                    detail=f"Not enough available IPs across pools {data.ipPools} (failed at node index {i})"
                )
            ip_entry.status = "used"
            reserved_ips.append({"ip": ip_entry.ip, "pool_id": ip_entry.pool_id})

        db.flush()

        # ── Persist job record ────────────────────────────────────────────────
        record = LLMInferenceJob(
            name=data.poolName,
            cluster_id=cluster.id,
            ip_pool_ids=ip_pool_ids,
            template=data.template,
            nodes=[n.dict() for n in data.nodes],
            machine_name=data.machine_name,
            pool_os_type=data.poolOSType,
            storage=data.storage or "local-lvm",
            model=data.model,
            status="provisioning",
        )
        db.add(record)
        db.commit()
        db.refresh(record)

        # ── Build workflow payload ────────────────────────────────────────────
        workflow_payload = {
            "job_id":       record.id,
            "name":         data.poolName,
            "cluster_id":   cluster.id,
            "ip_pool_ids":  ip_pool_ids,
            "template":     data.template,
            "nodes":        [n.dict() for n in data.nodes],
            "reserved_ips": reserved_ips,
            "storage":      data.storage or "local-lvm",
            "machine_name":  data.machine_name or data.poolName,
            "name_template": data.machine_name or None,
            "model":         data.model or "",
            "model_path":    data.model_path or "/vllm_data/hf_cache",
            "ssh_user":      data.ssh_user or _SSH_USER,
            "ssh_pass":      data.ssh_pass or _SSH_PASS,
        }

        # ── Start Temporal workflow ───────────────────────────────────────────
        client = await TemporalClientManager.get_temporal_client()
        handle = await client.start_workflow(
            CreateMultiNodeLLMWorkflow.run,
            workflow_payload,
            id=f"llm-inference-v2-{record.id}-{__import__('uuid').uuid4().hex[:8]}",
            task_queue=TASK_QUEUE,
            search_attributes={
                "Entity":   [data.poolName],
                "Action":   ["Private-LLM-Create"],
                "UserName": ["system"],
            },
        )

        record.workflow_id = handle.id
        db.commit()

        return response_format.success_response(
            201, "LLM inference job provisioning started",
            {"id": record.id, "workflow_id": handle.id, "reserved_ips": [r["ip"] for r in reserved_ips]}
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"create_llm_inference_job error: {e}", exc_info=True)
        return response_format.error_response(500, "Failed to create LLM inference job", str(e))


def _resolve_names(db: Session, cluster_id: int, ip_pool_ids: list) -> tuple:
    """Return (cluster_name, ip_pool_names) resolved from DB."""
    cluster     = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    cluster_name = cluster.name if cluster else None

    ip_pool_names: list = []
    if ip_pool_ids:
        from models.IPs_model import IPSModel
        pools = db.query(IPSModel).filter(IPSModel.id.in_(ip_pool_ids)).all()
        pool_map = {p.id: p.Pool_name for p in pools}
        ip_pool_names = [pool_map.get(pid) for pid in ip_pool_ids]

    return cluster_name, ip_pool_names


def list_llm_inference_jobs(db: Session, page: int = 1, page_size: int = 10):
    try:
        page      = max(1, page)
        page_size = max(1, min(page_size, 100))
        offset    = (page - 1) * page_size

        total   = db.query(LLMInferenceJob).count()
        records = (
            db.query(LLMInferenceJob)
            .order_by(LLMInferenceJob.created_at.desc())
            .offset(offset)
            .limit(page_size)
            .all()
        )

        # Bulk-resolve cluster names
        cluster_ids = list({r.cluster_id for r in records if r.cluster_id})
        cluster_map: dict = {}
        if cluster_ids:
            rows = db.query(Cluster).filter(Cluster.id.in_(cluster_ids)).all()
            cluster_map = {c.id: c.name for c in rows}

        # Bulk-resolve IP pool names
        from models.IPs_model import IPSModel
        all_pool_ids = list({pid for r in records for pid in (r.ip_pool_ids or [])})
        pool_map: dict = {}
        if all_pool_ids:
            pools = db.query(IPSModel).filter(IPSModel.id.in_(all_pool_ids)).all()
            pool_map = {p.id: p.Pool_name for p in pools}

        data = [
            {
                "id":             r.id,
                "name":           r.name,
                "cluster_id":     r.cluster_id,
                "cluster_name":   cluster_map.get(r.cluster_id),
                "ip_pool_ids":    r.ip_pool_ids,
                "ip_pool_names":  [pool_map.get(pid) for pid in (r.ip_pool_ids or [])],
                "template":       r.template,
                "nodes":          r.nodes,
                "machine_name":   r.machine_name,
                "pool_os_type":   r.pool_os_type,
                "storage":        r.storage,
                "vmids":          r.vmids,
                "ip_addresses":   r.ip_addresses,
                "head_ip":        r.head_ip,
                "endpoint_url":   r.endpoint_url,
                "status":         r.status,
                "workflow_id":    r.workflow_id,
                "created_at":     str(r.created_at),
            }
            for r in records
        ]

        total_pages = (total + page_size - 1) // page_size
        return response_format.success_response(200, "LLM inference jobs fetched", {
            "items":       data,
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
        return response_format.error_response(500, "Failed to list LLM inference jobs", str(e))


def get_llm_inference_job(job_id: int, db: Session):
    try:
        record = db.query(LLMInferenceJob).filter(LLMInferenceJob.id == job_id).first()
        if not record:
            raise HTTPException(status_code=404, detail="LLM inference job not found")

        # Build machines list — enrich from Machine table where available
        vmids        = record.vmids        or []
        ip_addresses = record.ip_addresses or []
        nodes        = record.nodes        or []

        machine_map = {}
        if vmids:
            rows = db.query(Machine).filter(Machine.vm_id.in_([str(v) for v in vmids])).all()
            machine_map = {m.vm_id: m for m in rows}

        machines = []
        for i, vmid in enumerate(vmids):
            m          = machine_map.get(str(vmid))
            ip_address = ip_addresses[i] if i < len(ip_addresses) else None
            machines.append({
                "vm_id":      vmid,
                "name":       m.name     if m else None,
                "ip_address": ip_address,
                "hostname":   m.hostname if m else ip_address,
                "protocol":   m.protocol if m else "ssh",
                "port":       m.port     if m else 22,
                "username":   m.username if m else None,
                "status":     m.status   if m else None,
                "node":       nodes[i]["node"] if i < len(nodes) else None,
                "gpu":        nodes[i]["gpu"]  if i < len(nodes) else [],
                "role":       "head" if ip_address == record.head_ip else "worker",
            })

        cluster_name, ip_pool_names = _resolve_names(db, record.cluster_id, record.ip_pool_ids or [])

        return response_format.success_response(200, "Fetched", {
            "id":             record.id,
            "name":           record.name,
            "cluster_id":     record.cluster_id,
            "cluster_name":   cluster_name,
            "ip_pool_ids":    record.ip_pool_ids,
            "ip_pool_names":  ip_pool_names,
            "template":       record.template,
            "nodes":          record.nodes,
            "machine_name":   record.machine_name,
            "pool_os_type":   record.pool_os_type,
            "storage":        record.storage,
            "model":          record.model,
            "vmids":          record.vmids,
            "ip_addresses":   record.ip_addresses,
            "head_ip":        record.head_ip,
            "endpoint_url":   record.endpoint_url,
            "status":         record.status,
            "workflow_id":    record.workflow_id,
            "created_at":     str(record.created_at),
            "updated_at":     str(record.updated_at),
            "machines":       machines,
        })
    except HTTPException:
        raise
    except Exception as e:
        return response_format.error_response(500, "Failed to get LLM inference job", str(e))


def update_llm_inference_job(job_id: int, data: LLMInferenceJobUpdate, db: Session):
    try:
        record = db.query(LLMInferenceJob).filter(LLMInferenceJob.id == job_id).first()
        if not record:
            raise HTTPException(status_code=404, detail="LLM inference job not found")
        if data.status is not None:
            record.status = data.status
        db.commit()
        return response_format.success_response(200, "Updated", {"id": record.id})
    except HTTPException:
        raise
    except Exception as e:
        return response_format.error_response(500, "Failed to update LLM inference job", str(e))


async def delete_llm_inference_job(job_id: int, db: Session):
    try:
        record = db.query(LLMInferenceJob).filter(LLMInferenceJob.id == job_id).first()
        if not record:
            raise HTTPException(status_code=404, detail="LLM inference job not found")

        workflow_payload = {
            "job_id":       job_id,
            "vmids":        record.vmids or [],
            "nodes":        record.nodes or [],
            "cluster_id":   record.cluster_id,
            "ip_addresses": record.ip_addresses or [],
        }

        client = await TemporalClientManager.get_temporal_client()
        handle = await client.start_workflow(
            DeleteLLMPoolWorkflow.run,
            workflow_payload,
            id=f"llm-delete-{job_id}-{__import__('uuid').uuid4().hex[:8]}",
            task_queue=TASK_QUEUE,
            search_attributes={
                "Entity":   [record.name],
                "Action":   ["Private-LLM-Delete"],
                "UserName": ["system"],
            },
        )

        return response_format.success_response(200, "Delete started", {
            "job_id":      job_id,
            "workflow_id": handle.id,
            "status":      "deleting",
        })
    except HTTPException:
        raise
    except Exception as e:
        return response_format.error_response(500, "Failed to start delete workflow", str(e))


_POOL_ACTION_PENDING_STATUS = {
    "start":    "starting",
    "restart":  "restarting",
    "stop":     "stopping",
    "shutdown": "stopping",
}


async def pool_vm_action(job_id: int, data: PoolActionRequest, db: Session):
    try:
        record = db.query(LLMInferenceJob).filter(LLMInferenceJob.id == job_id).first()
        if not record:
            raise HTTPException(status_code=404, detail="LLM inference job not found")

        vmids = record.vmids or []
        nodes = record.nodes or []
        if not vmids:
            raise HTTPException(status_code=400, detail="No VMs found in this pool")

        # Mark pool as in-progress immediately
        record.status = _POOL_ACTION_PENDING_STATUS[data.action]
        db.commit()

        n_nodes         = len(nodes)
        n_gpus_per_node = len(nodes[0]["gpu"]) if nodes and nodes[0].get("gpu") else 1

        workflow_payload = {
            "job_id":                 job_id,
            "action":                 data.action,
            "vmids":                  vmids,
            "nodes":                  nodes,
            "cluster_id":             record.cluster_id,
            "head_ip":                record.head_ip,
            "ip_addresses":           record.ip_addresses or [],
            "ssh_user":               _SSH_USER,
            "ssh_pass":               _SSH_PASS,
            "tensor_parallel_size":   n_nodes,
            "pipeline_parallel_size": n_gpus_per_node,
        }

        client = await TemporalClientManager.get_temporal_client()
        handle = await client.start_workflow(
            "PoolVMActionWorkflow",
            workflow_payload,
            id=f"pool-action-{job_id}-{data.action}-{__import__('uuid').uuid4().hex[:8]}",
            task_queue=TASK_QUEUE,
            search_attributes={
                "Entity":   [record.name],
                "Action":   [f"Private-LLM-{data.action.capitalize()}"],
                "UserName": ["system"],
            },
        )

        # Store latest workflow_id for tracking
        record.workflow_id = handle.id
        db.commit()

        return response_format.success_response(200, f"Pool {data.action} started", {
            "job_id":      job_id,
            "action":      data.action,
            "workflow_id": handle.id,
            "status":      record.status,
        })

    except HTTPException:
        raise
    except Exception as e:
        return response_format.error_response(500, f"Failed to start pool {data.action}", str(e))

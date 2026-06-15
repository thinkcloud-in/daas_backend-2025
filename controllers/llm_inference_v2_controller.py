import logging
import os
from fastapi import HTTPException
from sqlalchemy.orm import Session

from models.llm_inference_v2_model import LLMInferenceJob, LLMInferenceJobCreate, LLMInferenceJobUpdate
from models.IPs_model import IPEntry, IPSModel
from models.models import Cluster
from utils.temporal_client import TemporalClientManager
from service.temporalResource.workers.workers_llm_inference_v2 import TASK_QUEUE
from service.temporalResource.workflows.workflows_llm_inference_v2 import CreateMultiNodeLLMWorkflow
from utils import response_format

logger = logging.getLogger(__name__)

_SSH_USER = os.getenv("LLM_VM_SSH_USER", "root")
_SSH_PASS  = os.getenv("LLM_VM_SSH_PASS", "Teamw0rk@1")


async def create_llm_inference_job(data: LLMInferenceJobCreate, db: Session):
    try:
        if len(data.nodes) == 0:
            raise HTTPException(status_code=400, detail="At least one node is required")
        if len(data.ipPools) == 0:
            raise HTTPException(status_code=400, detail="At least one IP pool is required")

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
            "machine_name": data.machine_name or data.poolName,
            "ssh_user":     _SSH_USER,
            "ssh_pass":     _SSH_PASS,
        }

        # ── Start Temporal workflow ───────────────────────────────────────────
        client = await TemporalClientManager.get_temporal_client()
        handle = await client.start_workflow(
            CreateMultiNodeLLMWorkflow.run,
            workflow_payload,
            id=f"llm-inference-v2-{record.id}-{__import__('uuid').uuid4().hex[:8]}",
            task_queue=TASK_QUEUE,
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


def list_llm_inference_jobs(db: Session):
    try:
        records = db.query(LLMInferenceJob).order_by(LLMInferenceJob.created_at.desc()).all()
        data = [
            {
                "id": r.id,
                "name": r.name,
                "cluster_id": r.cluster_id,
                "ip_pool_ids": r.ip_pool_ids,
                "template": r.template,
                "nodes": r.nodes,
                "machine_name": r.machine_name,
                "pool_os_type": r.pool_os_type,
                "storage": r.storage,
                "vmids": r.vmids,
                "ip_addresses": r.ip_addresses,
                "head_ip": r.head_ip,
                "endpoint_url": r.endpoint_url,
                "status": r.status,
                "workflow_id": r.workflow_id,
                "created_at": str(r.created_at),
            }
            for r in records
        ]
        return response_format.success_response(200, "LLM inference jobs fetched", data)
    except Exception as e:
        return response_format.error_response(500, "Failed to list LLM inference jobs", str(e))


def get_llm_inference_job(job_id: int, db: Session):
    try:
        record = db.query(LLMInferenceJob).filter(LLMInferenceJob.id == job_id).first()
        if not record:
            raise HTTPException(status_code=404, detail="LLM inference job not found")
        return response_format.success_response(200, "Fetched", {
            "id": record.id,
            "name": record.name,
            "cluster_id": record.cluster_id,
            "ip_pool_ids": record.ip_pool_ids,
            "template": record.template,
            "nodes": record.nodes,
            "machine_name": record.machine_name,
            "pool_os_type": record.pool_os_type,
            "storage": record.storage,
            "vmids": record.vmids,
            "ip_addresses": record.ip_addresses,
            "head_ip": record.head_ip,
            "endpoint_url": record.endpoint_url,
            "status": record.status,
            "workflow_id": record.workflow_id,
            "created_at": str(record.created_at),
            "updated_at": str(record.updated_at),
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


def delete_llm_inference_job(job_id: int, db: Session):
    try:
        import requests as req
        from service.clusterService import get_api_token, getting_Proxmox_host

        record = db.query(LLMInferenceJob).filter(LLMInferenceJob.id == job_id).first()
        if not record:
            raise HTTPException(status_code=404, detail="LLM inference job not found")

        # ── Delete VMs from Proxmox ───────────────────────────────────────────
        if record.vmids:
            cluster = db.query(Cluster).filter(Cluster.id == record.cluster_id).first()
            if cluster:
                api_token    = get_api_token(db, cluster.name)
                headers      = {"Authorization": f"PVEAPIToken={api_token}"}
                PROXMOX_HOST = getting_Proxmox_host(cluster)
                nodes        = record.nodes or []

                for i, vmid in enumerate(record.vmids):
                    node = nodes[i]["node"] if i < len(nodes) else None
                    if not node:
                        continue

                    # 1. Remove from HA resources
                    req.delete(
                        f"{PROXMOX_HOST}/api2/json/cluster/ha/resources/vm%3A{vmid}",
                        headers=headers, verify=False, timeout=15
                    )

                    # 2. Remove HA group
                    req.delete(
                        f"{PROXMOX_HOST}/api2/json/cluster/ha/groups/llm-{vmid}",
                        headers=headers, verify=False, timeout=15
                    )

                    # 3. Remove GPU (clear hostpci config)
                    req.put(
                        f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/config",
                        headers=headers,
                        data={"delete": "hostpci0,hostpci1,hostpci2,hostpci3"},
                        verify=False, timeout=15
                    )

                    # 4. Stop VM
                    req.post(
                        f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/status/stop",
                        headers=headers, verify=False, timeout=30
                    )

                    # 5. Wait for VM to stop (max 60s)
                    import time
                    for _ in range(12):
                        time.sleep(5)
                        status_resp = req.get(
                            f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/status/current",
                            headers=headers, verify=False, timeout=10
                        )
                        if status_resp.ok:
                            if status_resp.json().get("data", {}).get("status") == "stopped":
                                break

                    # 6. Delete VM permanently with disk cleanup
                    req.delete(
                        f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}",
                        headers=headers,
                        params={"purge": 1, "destroy-unreferenced-disks": 1},
                        verify=False, timeout=60
                    )

        # ── Release IPs back to unused ────────────────────────────────────────
        if record.ip_addresses:
            for ip in record.ip_addresses:
                entry = db.query(IPEntry).filter(IPEntry.ip == ip).first()
                if entry:
                    entry.status = "unused"
                    entry.vm_id  = None

        db.delete(record)
        db.commit()
        return response_format.success_response(200, "Deleted", {"id": job_id})
    except HTTPException:
        raise
    except Exception as e:
        return response_format.error_response(500, "Failed to delete LLM inference job", str(e))

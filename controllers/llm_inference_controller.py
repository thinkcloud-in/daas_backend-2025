import logging
import os
from fastapi import HTTPException
from sqlalchemy.orm import Session

from models.llm_inference_model import LLMInference, LLMInferenceCreate, LLMInferenceUpdate
from models.IPs_model import IPEntry
from utils.temporal_client import TemporalClientManager
from service.temporalResource.workers.workers_llm_inference import TASK_QUEUE
from service.temporalResource.workflows.workflows_llm_inference import CreateLLMInferenceWorkflow
from utils import response_format

logger = logging.getLogger(__name__)

_DEVRAQ_RPM_URL = os.getenv("DEVRAQ_RPM_URL", "")
_SSH_USER = os.getenv("LLM_VM_SSH_USER", "root")
_SSH_PASS = os.getenv("LLM_VM_SSH_PASS", "Teamw0rk@1")
_PVE_SSH_USER = os.getenv("PVE_SSH_USER", "root")
_PVE_SSH_PASS = os.getenv("PVE_SSH_PASS", "Teamw0rk@1")
_HF_TOKEN = os.getenv("HF_TOKEN", "")


async def create_llm_inference(data: LLMInferenceCreate, db: Session):
    try:
        existing = db.query(LLMInference).filter(LLMInference.name == data.name).first()
        if existing:
            raise HTTPException(status_code=409, detail=f"LLM inference '{data.name}' already exists")

        # ── Reserve next unused IP from pool — activity resolves full details ──
        from models.IPs_model import IPSModel

        ip_pool = db.query(IPSModel).filter(IPSModel.id == int(data.ip_pool_id)).first()
        if not ip_pool:
            raise HTTPException(status_code=404, detail="IP pool not found")

        ip_entry = (
            db.query(IPEntry)
            .filter(IPEntry.pool_id == int(data.ip_pool_id), IPEntry.status == "unused")
            .first()
        )
        if not ip_entry:
            raise HTTPException(status_code=400, detail="No available IPs in the selected IP pool")

        ip_entry.status = "reserved"
        db.flush()

        record = LLMInference(
            name=data.name,
            ip_pool_id=data.ip_pool_id,
            cluster_id=data.cluster_id,
            node=data.node,
            gpu=data.gpu,
            base_os=data.base_os,
            cpu=data.cpu,
            ram=data.ram,
            datastore=data.datastore,
            os_disk_size=data.os_disk_size,
            data_disk_size=data.data_disk_size,
            model=data.model,
            status="provisioning",
        )
        db.add(record)
        db.commit()
        db.refresh(record)

        workflow_payload = {
            "inference_id": record.id,
            "name": data.name,
            "cluster_id": data.cluster_id,
            "node": data.node,
            "gpu": data.gpu,
            "base_os": data.base_os,
            "cpu": data.cpu,
            "ram": data.ram,
            "datastore": data.datastore,
            "os_disk_size": data.os_disk_size,
            "data_disk_size": data.data_disk_size,
            "model": data.model,
            # Pass only IDs — activity resolves ip/gateway/subnet/dns from these
            "ip_pool_id": data.ip_pool_id,
            "reserved_ip": ip_entry.ip,
            "ssh_user": _SSH_USER,
            "ssh_pass": _SSH_PASS,
            "pve_ssh_user": _PVE_SSH_USER,
            "pve_ssh_pass": _PVE_SSH_PASS,
            "devraq_rpm_url": _DEVRAQ_RPM_URL,
            "hf_token": _HF_TOKEN,
            "gpu_count": 1,
        }

        client = await TemporalClientManager.get_temporal_client()
        handle = await client.start_workflow(
            CreateLLMInferenceWorkflow.run,
            workflow_payload,
            id=f"llm-inference-{record.id}",
            task_queue=TASK_QUEUE,
        )

        record.workflow_id = handle.id
        db.commit()

        return response_format.success_response(201, "LLM inference provisioning started", {"id": record.id, "workflow_id": handle.id})

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"create_llm_inference error: {e}", exc_info=True)
        return response_format.error_response(500, "Failed to create LLM inference", str(e))


def list_llm_inferences(db: Session):
    try:
        records = db.query(LLMInference).order_by(LLMInference.created_at.desc()).all()
        data = [
            {
                "id": r.id,
                "name": r.name,
                "cluster_id": r.cluster_id,
                "node": r.node,
                "gpu": r.gpu,
                "model": r.model,
                "vmid": r.vmid,
                "ip_address": r.ip_address,
                "endpoint_url": r.endpoint_url,
                "status": r.status,
                "workflow_id": r.workflow_id,
                "created_at": str(r.created_at),
            }
            for r in records
        ]
        return response_format.success_response(200, "LLM inferences fetched", data)
    except Exception as e:
        return response_format.error_response(500, "Failed to list LLM inferences", str(e))


def get_llm_inference(inference_id: int, db: Session):
    try:
        record = db.query(LLMInference).filter(LLMInference.id == inference_id).first()
        if not record:
            raise HTTPException(status_code=404, detail="LLM inference not found")
        return response_format.success_response(200, "Fetched", {
            "id": record.id,
            "name": record.name,
            "cluster_id": record.cluster_id,
            "node": record.node,
            "gpu": record.gpu,
            "model": record.model,
            "vmid": record.vmid,
            "ip_address": record.ip_address,
            "endpoint_url": record.endpoint_url,
            "status": record.status,
            "workflow_id": record.workflow_id,
            "cpu": record.cpu,
            "ram": record.ram,
            "datastore": record.datastore,
            "os_disk_size": record.os_disk_size,
            "data_disk_size": record.data_disk_size,
            "created_at": str(record.created_at),
            "updated_at": str(record.updated_at),
        })
    except HTTPException:
        raise
    except Exception as e:
        return response_format.error_response(500, "Failed to get LLM inference", str(e))


def update_llm_inference(inference_id: int, data: LLMInferenceUpdate, db: Session):
    try:
        record = db.query(LLMInference).filter(LLMInference.id == inference_id).first()
        if not record:
            raise HTTPException(status_code=404, detail="LLM inference not found")
        record.model = data.model
        db.commit()
        return response_format.success_response(200, "LLM inference model updated successfully", {"id": record.id, "model": record.model})
    except HTTPException:
        raise
    except Exception as e:
        return response_format.error_response(500, "Failed to update LLM inference", str(e))


def delete_llm_inference(inference_id: int, db: Session):
    try:
        record = db.query(LLMInference).filter(LLMInference.id == inference_id).first()
        if not record:
            raise HTTPException(status_code=404, detail="LLM inference not found")
        db.delete(record)
        db.commit()
        return response_format.success_response(200, "LLM inference deleted successfully", {"id": inference_id})
    except HTTPException:
        raise
    except Exception as e:
        return response_format.error_response(500, "Failed to delete LLM inference", str(e))

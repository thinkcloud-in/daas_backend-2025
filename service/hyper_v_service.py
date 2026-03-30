from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
import httpx
import os
from datetime import datetime
from service.gucamoleService import connectionWithClient
# from service.temporalResource.workflows import workflows_hyper_v  # Move to inside functions
import logging
import uuid
from db_configuration.config import get_db
from models.models import Machine, Pool
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

HYPER_V_AGENT_URL = os.getenv('HYPER_V_AGENT_URL')
def unique_id():
    unique_id = datetime.now()
    return f"{unique_id.hour }:{unique_id.minute}:{unique_id.second}"

async def get_vms():
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/get_vms"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(url)
        data = response.json()
        return data['data']

async def clone_vm_for_single_node(request) -> dict:
    req_dict = jsonable_encoder(request)
    workflow_id = f"clone_vm_hyperv-{uuid.uuid4().hex}"

    client = await connectionWithClient()
    if client is None:
        logger.error("Temporal client connection failed")
        raise HTTPException(status_code=500, detail="Temporal client connection failed")

    try:
        from service.temporalResource.workflows import workflows_hyper_v
        logger.info("Starting workflow %s with payload keys: %s", list(req_dict.keys()))
        handle = await client.start_workflow(
            workflows_hyper_v.CloneVMHyperVWorkflow.run,
            args=[req_dict],
            id=workflow_id,
            task_queue="hyperv-task-queue",
        )
    except Exception as e:
        logger.exception("Failed to start workflow %s: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed to start workflow: {str(e)}")

    result =  await handle.result()
    return result

async def generate_mac_activity() -> str:
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/generate_mac_add"
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(url)
        data = resp.json()
        return data["data"]["MAC_Add"]

async def create_iso_activity(payload: dict) -> str:
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/create_iso"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, json=payload)
        data = resp.json()
        return data["data"]["iso_path"]

async def get_switches_activity() -> str:
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/get_switches"
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url)
        data = resp.json()
        return data["data"]["Name"]

async def clone_vm_activity(payload: dict) -> dict:
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/clone_vm_for_single_node"
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(url, json=payload)
        return resp.json()["data"]

async def attach_iso_activity(payload: dict) -> dict:
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/attach_iso"
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(url, json=payload)
        return resp.json()["data"]

async def get_vm_info(vm_id):
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/get_vm_info/{vm_id}"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(url)
        data = response.json()
        
        agent_data = data.get('data', '')
        # If not found by ID, we might need a fallback or better error reporting
        if isinstance(agent_data, str) and ("ObjectNotFound" in agent_data or "unable to find" in agent_data.lower()):
            # Potentially the ID changed or is misformatted. 
            # In a real scenario, we might want to search by VM Name here if we had it.
            return {"error": f"VM with ID {vm_id} not found on Hyper-V host.", "agent_error": agent_data}
            
        return agent_data
    
async def get_switches():
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/get_switches"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(url)
        data = response.json()
        return data['data']

async def delete_vm(vm_id: str) -> dict:
    # workflow_id = f"delete_vm_hyperv-{uuid.uuid4().hex}"

    client = await connectionWithClient()
    if client is None:
        logger.error("Temporal client connection failed")
        raise HTTPException(status_code=500, detail="Temporal client connection failed")

    request = {"vm_id": vm_id}

    try:
        from service.temporalResource.workflows import workflows_hyper_v
        logger.info("Starting workflow %s with payload keys: %s", request)
        handle = await client.start_workflow(
            workflows_hyper_v.DeleteVMHyperVWorkflow.run,
            args=[request],
            # id=workflow_id,
            task_queue="hyperv-task-queue",
        )
    except Exception as e:
        logger.exception("Failed to start delete VM workflow")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start delete workflow: {str(e)}"
        )

    result = await handle.result()
    return result
async def delete_hyperv_vm(vm_id):
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/delete_vm/{vm_id}"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.delete(url)
        data = response.json()
        return data['data']

async def get_status(vm_id: str) -> dict:
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/get_status/{vm_id}"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get("data", {})
    
async def handle_action(request) -> dict:
    # workflow_id = f"hyperv-handle-action-{uuid.uuid4().hex}"
    client = await connectionWithClient()

    payload = request.dict() if hasattr(request, "dict") else request

    if client is None:
        logger.info("Starting workflow %s with payload keys: %s", payload)
        raise HTTPException(status_code=500, detail="Temporal client connection failed")

    try:
        from service.temporalResource.workflows import workflows_hyper_v
        handle = await client.start_workflow(
            workflows_hyper_v.HandleActionHyperVWorkflow.run,
            args=[payload],
            # id=workflow_id,
            task_queue="hyperv-task-queue",
        )
    except Exception as e:
        logger.exception("Failed to start handle_action workflow")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start workflow: {str(e)}"
        )

    result = await handle.result()
    return result
        
async def delete_disk(disk_path: str) -> dict:
    workflow_id = f"delete_hyperv_disk-{uuid.uuid4().hex}"

    client = await connectionWithClient()
    if client is None:
        raise HTTPException(status_code=500, detail="Temporal client connection failed")

    request = {"disk_path": disk_path}

    try:
        from service.temporalResource.workflows import workflows_hyper_v
        handle = await client.start_workflow(
            workflows_hyper_v.DeleteHyperVDiskWorkflow.run,
            args=[request],
            id=workflow_id,
            task_queue="hyperv-task-queue",
        )
    except Exception as e:
        logger.exception("Failed to start delete disk workflow")
        raise HTTPException(status_code=500, detail=f"Failed to start workflow: {str(e)}")

    result = await handle.result()
    return result

async def vm_rebuild(request):
    from db_configuration.config import SessionLocal
    from models.models import Machine
    
    payload = request.dict() if hasattr(request, "dict") else request
    vm_id = payload.get("vm_id")
    
    db = SessionLocal()
    try:
        machine = db.query(Machine).filter(Machine.vm_id == str(vm_id)).first()
        if not machine:
            return {"status": "error", "error": f"Machine with vm_id {vm_id} not found in DB."}
        
        uniqueId = unique_id()
        client = await connectionWithClient()
        workflow_id = f"vmrebuild_hyperv-{uniqueId}-{uuid.uuid4().hex[:4]}"
        
        from service.temporalResource.workflows import workflows_hyper_v
        handle = await client.start_workflow(
            workflows_hyper_v.VmRebuildHyperVWorkflow.run,
            args=[payload],
            id=workflow_id,
            task_queue="hyperv-task-queue",
        )
        result = await handle.result()
        return result
    except Exception as e:
        logger.error(f"Error in Hyper-V vm_rebuild: {e}")
        return {"status": "error", "error": str(e)}
    finally:
        db.close()

async def pool_rebuild(pool_id: str, db):
    uniqueId = unique_id()
    client = await connectionWithClient()
    if client is None:
        return {"status": "error", "error": "Temporal client connection failed"}
        
    workflow_id = f"poolrebuild_hyperv-{pool_id}-{uniqueId}"
    
    from service.temporalResource.workflows import workflows_hyper_v
    try:
        handle = await client.start_workflow(
            workflows_hyper_v.HyperVPoolRebuildWorkflow.run,
            args=[{"pool_id": pool_id}],
            id=workflow_id,
            task_queue="hyperv-task-queue",
        )
        return {"workflow_id": workflow_id, "status": "success", "msg": "Hyper-V Pool Rebuild started."}
    except Exception as e:
        logger.error(f"Failed to start pool rebuild workflow: {e}")
        return {"status": "error", "error": str(e)}
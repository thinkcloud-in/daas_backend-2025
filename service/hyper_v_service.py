from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
import httpx
import os
from datetime import datetime
from service.gucamoleService import connectionWithClient
from service.temporalResource.workflows import workflows_hyper_v
import logging
import uuid
# from models.hyper_v_model import Hyper_V

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

async def clone_vm_for_single_node(request, db=None) -> dict:
    req_dict = jsonable_encoder(request)
    workflow_id = f"clone_vm_hyperv-{uuid.uuid4().hex}"

    client = await connectionWithClient()
    if client is None:
        logger.error("Temporal client connection failed")
        raise HTTPException(status_code=500, detail="Temporal client connection failed")

    try:
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
        return data['data']
    
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

# async def get_status(vm_id):
#     url = f"{HYPER_V_AGENT_URL}v1/hyper-v/get_status/{vm_id}"
#     async with httpx.AsyncClient(timeout=20.0) as client:
#         response = await client.get(url)
#         data = response.json()
#         return data['data']

async def get_status(vm_id: str) -> dict:
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/get_status/{vm_id}"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get("data", {})
    
# async def handle_action(request, db):
#     url = f"{HYPER_V_AGENT_URL}v1/hyper-v/handle_action"
#     async with httpx.AsyncClient(timeout=20.0) as client:
#         response = await client.post(url, json=request.dict())
#         data = response.json()
#         if data.get('code') == 200:
#             return data['data']
#         else:
#             return data.get('msg', 'Unknown error occurred')
async def handle_action(request) -> dict:
    # workflow_id = f"hyperv-handle-action-{uuid.uuid4().hex}"
    client = await connectionWithClient()

    payload = request.dict() if hasattr(request, "dict") else request

    if client is None:
        logger.info("Starting workflow %s with payload keys: %s", payload)
        raise HTTPException(status_code=500, detail="Temporal client connection failed")

    try:
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
        
# async def delete_hyperv_disk(disk_path):
#     url = f"{HYPER_V_AGENT_URL}v1/hyper-v/delete_disk?disk_path={disk_path}"
#     async with httpx.AsyncClient(timeout=20.0) as client:
#         response = await client.delete(url)
#         data = response.json()
#         if data.get('code') == 200:
#             return data['data']
#         else:
#             return data.get('msg', 'Unknown error occurred')
async def delete_hyperv_disk(disk_path: str) -> dict:
    # workflow_id = f"delete_hyperv_disk-{uuid.uuid4().hex}"

    client = await connectionWithClient()
    if client is None:
        raise HTTPException(status_code=500, detail="Temporal client connection failed")

    request = {"disk_path": disk_path}

    try:
        handle = await client.start_workflow(
            workflows_hyper_v.DeleteHyperVDiskWorkflow.run,
            args=[request],
            # id=workflow_id,
            task_queue="hyperv-task-queue",
        )
    except Exception as e:
        logger.exception("Failed to start delete disk workflow")
        raise HTTPException(status_code=500, detail=f"Failed to start workflow: {str(e)}")

    result = await handle.result()
    return result
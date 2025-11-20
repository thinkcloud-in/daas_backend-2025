
import httpx
import os
from datetime import datetime
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from models.models import Machine, Pool
from service.proxmoxService import generate_machine_name
import asyncio
from service.gucamoleService import connectionWithClient
from service.temporalResource.workflows import workflows_hyper_v
from service.temporalResource.workers import worker_hyper_v
import logging
import traceback
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
    

_worker_task = None

async def ensure_hyperv_worker_running():
    global _worker_task
    if _worker_task is None or _worker_task.done():
        logging.info("Starting clone VM Single Node HyperV worker task...")
        _worker_task = asyncio.create_task(worker_hyper_v.cloneVm_SingleNode_HyperV_worker())
        # give it a moment to try connecting; worker will log success/failure
        await asyncio.sleep(0.1)
    else:
        logging.debug("Hyper-V worker task already running.")

async def clone_vm_for_single_node(clone_payload: dict, db):
    # ensure worker is running (non-blocking)
    try:
        await ensure_hyperv_worker_running()
    except Exception as e:
        logging.error("Failed to ensure worker running: %s\n%s", e, traceback.format_exc())

    uniqueId = unique_id()
    client = await connectionWithClient()
    workflow_id = f"clone_vm_for_single_node-{uniqueId}"
    logging.info("Starting workflow with ID clone_vm_for_single_node : %s", workflow_id)
    try:
        clone_payload["workflowId"] = workflow_id
        handle = await client.start_workflow(
            workflows_hyper_v.CloneVMSingleNodeHyperVWorkflow.run,
            args=[clone_payload, db],
            id=workflow_id,
            task_queue="clone_vm_for_single_node-task-queue",
        )
        # If you want to wait for completion keep this; otherwise return the handle/run_id
        result = await handle.result()
        if isinstance(result, dict) and "error" in result:
            return result
        return result
    except Exception as e:
        logging.error("start_workflow failed: %s\n%s", e, traceback.format_exc())
        return {"error": str(e)}

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
    
async def delete_vm(vm_id):
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/delete_vm/{vm_id}"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.delete(url)
        data = response.json()
        return data['data']
    
async def get_status(vm_id):
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/get_status/{vm_id}"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(url)
        data = response.json()
        return data['data']
    
async def handle_action(request, db):
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/handle_action"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.post(url, json=request.dict())
        data = response.json()
        if data.get('code') == 200:
            return data['data']
        else:
            return data.get('msg', 'Unknown error occurred')
        
async def delete_disk(request, db):
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/delete_disk?disk_path={request.disk_path}"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.delete(url)
        data = response.json()
        if data.get('code') == 200:
            return data['data']
        else:
            return data.get('msg', 'Unknown error occurred')
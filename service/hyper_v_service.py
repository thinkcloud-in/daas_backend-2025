
import uuid
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
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

 
from service.temporalResource.workers import worker_hyper_v


async def clone_vm_for_single_node(request, db=None) -> dict:
    req_dict = jsonable_encoder(request)
    workflow_id = f"clone_vm_hyperv-{uuid.uuid4().hex}"

    client = await connectionWithClient()
    if client is None:
        logger.error("Temporal client connection failed")
        raise HTTPException(status_code=500, detail="Temporal client connection failed")

    try:
        logger.info("Starting workflow %s with payload keys: %s", workflow_id, list(req_dict.keys()))
        handle = await client.start_workflow(
            workflows_hyper_v.CloneVMHyperVWorkflow.run,
            args=[req_dict],
            id=workflow_id,
            task_queue="clonevmhyperv-task-queue",
        )
    except Exception as e:
        logger.exception("Failed to start workflow %s: %s", workflow_id, e)
        raise HTTPException(status_code=500, detail=f"Failed to start workflow: {str(e)}")

    result =  await handle.result()
    return result 

      
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
    
async def delete_hyperv_vm(vm_id):
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
        
async def delete_hyperv_disk(disk_path):
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/delete_disk?disk_path={disk_path}"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.delete(url)
        data = response.json()
        if data.get('code') == 200:
            return data['data']
        else:
            return data.get('msg', 'Unknown error occurred')
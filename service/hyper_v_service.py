
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

 
from service.temporalResource.workers import worker_hyper_v
from service.temporalResource.workflows import workflows_hyper_v

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
    print("Workflow result:", result)
    return result 

    
# async def clone_vm_for_single_node(request,db) -> dict:
#     req_dict = jsonable_encoder(request)
#     uniqueId = unique_id()
#     client = await connectionWithClient()
#     # pool_name = pool_data.get("pool_name", "UnknownPool")
#     # userName = pool_data.get('email', "UnknownUser")
#     workflow_id = f"clone_vm_hyperv-{uniqueId}"
#     handle = await client.start_workflow(
#         workflows_hyper_v.CloneVMHyperVWorkflow.run,
#         args=[req_dict],
#         id=workflow_id,
#         task_queue= "clonevmhyperv-task-queue",
#     )
#     result =  await handle.result()
#     return result 

# async def clone_vm_for_single_node(request, db):
#     url = f"{HYPER_V_AGENT_URL}v1/hyper-v/clone_vm_for_single_node"
 
#     template = request.get("template_vm_id", {})
 
#     vhdPath = template.get("vhdPath")
#     PvhdPath = template.get("PvhdPath")
#     generation = template.get("generation")
#     memory = template.get("memory")
#     switch = template.get("switch")
#     number_of_vms = request.get("count")
   
#     base_vm_name = request.get("name_template", "cloned_vm")
#     # Only fetch existing VM names from Hyper-V
#     try:
#         hyperv_vms = await get_vms()
#         hyperv_names = []
#         for vm in hyperv_vms:
#             name = vm.get("VMName") or vm.get("Name")
#             if name:
#                 hyperv_names.append(name)
#     except Exception:
#         hyperv_names = []
#     new_names = generate_machine_name(base_vm_name, hyperv_names, number_of_vms)
#     if not new_names:
#         return {"error": "No unique VM names available for cloning."}
 
#     result_vms = []
#     for vm_name in new_names:
#         payload = {
#             "vm_name": vm_name,
#             "memory": memory,
#             "vhdPath": vhdPath,
#             "switch": switch,
#             "generation": generation,
#             "PvhdPath": PvhdPath,
#         }
#         print("Payload for clone_vm_for_single_node:", payload)
#         async with httpx.AsyncClient(timeout=120.0) as client:
#             response = await client.post(url, json=payload)
#         data = response.json()
#         # print("Agent Response:", data)
#         if data.get('code') != 200:
#             continue
#         # The agent response may have 'data' as a dict with 'VM' key, or as a list of dicts
#         if isinstance(data.get('data'), dict) and "VM" in data.get('data'):
#             vm_info = data['data']['VM']
#             result_vms.append({
#                 "name": vm_info.get("Name") or vm_info.get("VMName") or vm_name,
#                 "vmid": vm_info.get("Id") or vm_info.get("VMId")
#             })
#         elif isinstance(data.get('data'), list):
#             for vm_item in data.get('data'):
#                 if isinstance(vm_item, dict) and "VM" in vm_item:
#                     vm_info = vm_item['VM']
#                     result_vms.append({
#                         "name": vm_info.get("Name") or vm_info.get("VMName") or vm_name,
#                         "vmid": vm_info.get("Id") or vm_info.get("VMId")
#                     })
#         else:
#             # Fallback: just append the name
#             result_vms.append({"name": vm_name})
#     return {
#         "machines_created": len(result_vms),
#         "created_names": new_names,
#         "vms": result_vms,
#     }
 
        
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
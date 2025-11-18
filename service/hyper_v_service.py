
import httpx
import os
from datetime import datetime
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from models.models import Machine, Pool
from service.proxmoxService import generate_machine_name
# from models.hyper_v_model import Hyper_V

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

# async def clone_vm_for_single_node(request, db):
#     url = f"{HYPER_V_AGENT_URL}v1/hyper-v/clone_vm_for_single_node"
#     async with httpx.AsyncClient(timeout=20.0) as client:
#         response = await client.post(url, json=request.dict())
#         data = response.json()

#         if data.get('code') == 200:
#             return data['data']
#         else:
#             return data.get('msg', 'Unknown error occurred')

# async def clone_vm_for_single_node(request, db:AsyncSession):
#     url = f"{HYPER_V_AGENT_URL}v1/hyper-v/clone_vm_for_single_node"
#     number_of_vms = getattr(request, "number_of_vms", 1)
#     base_vm_name = request.vm_name
 
#     vm_clone_list = []
#     for i in range(1, number_of_vms + 1):
#         suffix = f"_{i:03d}"
#         vm_name = f"{base_vm_name}{suffix}" if number_of_vms > 1 else base_vm_name
 
#         vm_clone_list.append({
#             "vm_name": vm_name,
#             "memory": request.memory,
#             "vhdPath": request.vhdPath,
#             "switch": request.switch,
#             "generation": request.generation,
#             "PvhdPath": request.PvhdPath,
#         })
 
#     async with httpx.AsyncClient(timeout=120.0) as client:
#         response = await client.post(url, json=vm_clone_list)
 
#     data = response.json()
 
#     if data.get('code') != 200:
#         raise RuntimeError(data.get('msg', 'Unknown error occurred'))
 
#     vm_array = data.get('data', [])
#     if not vm_array:
#         raise RuntimeError("No VM data returned from Hyper-V")
 
#     pool_template_vm_json = []
 
#     new_pool = Pool(
#         pool_failover_only=request.failover_only,
#         pool_guacd_hostname=request.guacd_hostname,
#         pool_guacd_encryption=request.guacd_encryption,
#         pool_template_vm_id=[]
#     )
#     db.add(new_pool)
#     await db.commit()
#     await db.refresh(new_pool)
 
#     for vm_item in vm_array:
#         vm_data = vm_item.get('VM', {})
#         vhd_data = vm_item.get('VHD', {})
 
#         pool_template_vm_json.append({
#             "vm": {
#                 "id": vm_data.get("VMId"),
#                 "name": vm_data.get("VMName"),
#                 "file_location": vm_data.get("CheckpointFileLocation"),
#             },
#             "vhd": {
#                 "path": vhd_data.get("Path"),
#                 "format": vhd_data.get("VhdFormat"),
#                 "type": vhd_data.get("VhdType"),
#                 "parent_path": vhd_data.get("ParentPath"),
#                 "disk_identifier": vhd_data.get("DiskIdentifier"),
#             },
#         })
 
#         new_machine = Machine(
#             vm_id=str(vm_data.get("VMId")),
#             name=str(vm_data.get("VMName")),
#             failover_only=request.failover_only,
#             guacd_hostname=request.guacd_hostname,
#             guacd_encryption=request.guacd_encryption,
#             pool_id=new_pool.id
#         )
#         db.add(new_machine)
 
#     new_pool.pool_template_vm_id = pool_template_vm_json
 
#     db.add(new_pool)
#     await db.commit()
#     await db.refresh(new_pool)
 
#     return {
#         "pool_id": new_pool.id,
#         "machines_created": len(new_pool.pool_template_vm_id),
#     }
 
async def clone_vm_for_single_node(request, db):
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/clone_vm_for_single_node"
    number_of_vms = getattr(request, "count", 1)
    base_vm_name = request.vm_name

    # Only fetch existing VM names from Hyper-V
    try:
        hyperv_vms = await get_vms()
        # hyperv_names = [vm.get("VMName") for vm in hyperv_vms if vm.get("VMName")]
        hyperv_names = []
        for vm in hyperv_vms:
            name = vm.get("VMName") or vm.get("Name")
            if name:
                hyperv_names.append(name)
    except Exception:
        hyperv_names = []

    # Generate unique VM names using only Hyper-V names
    new_names = generate_machine_name(base_vm_name, hyperv_names, number_of_vms)

    if not new_names:
        return {"error": "No unique VM names available for cloning."}

    result_vms = []
    for vm_name in new_names:
        payload = {
            "vm_name": vm_name,
            "memory": request.memory,
            "vhdPath": request.vhdPath,
            "switch": request.switch,
            "generation": request.generation,
            "PvhdPath": request.PvhdPath,
        }
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(url, json=payload)
        data = response.json()
        print("Agent Response:", data)
        if data.get('code') != 200:
            continue
        # The agent response may have 'data' as a dict with 'VM' key, or as a list of dicts
        if isinstance(data.get('data'), dict) and "VM" in data.get('data'):
            vm_info = data['data']['VM']
            result_vms.append({
                "name": vm_info.get("Name") or vm_info.get("VMName") or vm_name,
                "vmid": vm_info.get("Id") or vm_info.get("VMId")
            })
        elif isinstance(data.get('data'), list):
            for vm_item in data.get('data'):
                if isinstance(vm_item, dict) and "VM" in vm_item:
                    vm_info = vm_item['VM']
                    result_vms.append({
                        "name": vm_info.get("Name") or vm_info.get("VMName") or vm_name,
                        "vmid": vm_info.get("Id") or vm_info.get("VMId")
                    })
        else:
            # Fallback: just append the name
            result_vms.append({"name": vm_name})
    return {
        "machines_created": len(result_vms),
        "created_names": new_names,
        "vms": result_vms,
    }
        
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
import httpx
import os
from datetime import datetime
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from models.models import Machine
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

async def clone_vm_for_single_node(request, db):
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/clone_vm_for_single_node"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.post(url, json=request.dict())
        data = response.json()
        
        if data.get('code') == 200:
            # vm_data = data['data']['VM']
            # vhd_data = data['data']['VHD']

            # new_vm = Machine(
            #     vm_id=str(vm_data.get('VMId')),
            #     name=str(vm_data.get('VMName')),
            #     failover_only=request.failover_only,
            #     guacd_hostname=request.guacd_hostname,
            #     guacd_encryption=request.guacd_encryption,
            # )
            # new_vm = Hyper_V(
                # vm_id=str(vm_data.get('VMId')),
                # vm_name=str(vm_data.get('VMName')),
                # is_deleted=str(vm_data.get('IsDeleted', False)),
                # file_location=str(vm_data.get('CheckpointFileLocation')),
                # state=str(vm_data.get('State')),
                # vhd_path=str(vhd_data.get('Path')),
                # vhd_format=str(vhd_data.get('VhdFormat')),
                # vhd_type=str(vhd_data.get('VhdType')),
                # vhd_file_size=str(vhd_data.get('FileSize')),
                # vhd_size=str(vhd_data.get('Size')),
                # vhd_minimum_size=str(vhd_data.get('MinimumSize')),
                # vhd_logical_sector_size=str(vhd_data.get('LogicalSectorSize')),
                # vhd_physical_sector_size=str(vhd_data.get('PhysicalSectorSize')),
                # vhd_block_size=str(vhd_data.get('BlockSize')),
                # vhd_parent_path=str(vhd_data.get('ParentPath')),
                # vhd_disk_identifier=str(vhd_data.get('DiskIdentifier')),
            # )
            # db.add(new_vm)
            # await db.commit()
            # await db.refresh(new_vm)
            # return {"vm_id": new_vm.vm_id, "vm_name": new_vm.vm_name}
            return data['data']
        else:
            return data.get('msg', 'Unknown error occurred')
        
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
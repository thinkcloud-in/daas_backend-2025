import httpx
import os
from datetime import datetime

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

async def clone_vm_for_single_node(request):
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/clone_vm_for_single_node"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.post(url, json=request.dict())
        data = response.json()
        if data.get('code') == 200:
            return data.get('data')
        else:
            return data.get('msg', 'Unknown error occurred')
import httpx
import os
from datetime import datetime

from utils import response_format

HYPER_V_AGENT_URL = os.getenv('HYPER_V_AGENT_URL')
def unique_id():
    unique_id = datetime.now()
    return f"{unique_id.hour }:{unique_id.minute}:{unique_id.second}"

async def get_vms_from_hyperv():
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/get_vms/"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = (await client.get(url)).json()
        return response['data']
    
async def clone_vms():
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/clone_vms/"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = (await client.post(url)).json()
        if response['code'] == 200:
            return response['data']
        else:
            return response['msg']
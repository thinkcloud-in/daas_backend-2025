import asyncio
from datetime import datetime
import subprocess
from fastapi import HTTPException
from typing import Optional
from pydantic import BaseModel
import re
import subprocess
import json
from typing import Optional
import json
import os



class CreateNamespaceRequest(BaseModel):
    namespace: str
    retention_days: int
    description: Optional[str] = ""

class UpdateRetentionRequest(BaseModel):
    namespace: str
    retention_days: int
    email:str

from temporalio.client import Client
from service.temporalResource.workflows import workflows_retentionPeriod
from service.temporalResource.workers import workers_retentionPeriod

def unique_id():
    unique_id = datetime.now()
    return f"{unique_id.hour }:{unique_id.minute}:{unique_id.second}"

async def connectionWithTemporal():
    temporal_address = os.getenv('TEMPORAL_SERVER')
    
    try:
        client = await Client.connect(temporal_address)
        
        return client
    except Exception as e:
        
        return None
    



def parse_ttl_to_seconds(ttl: str) -> int:

    if ttl.endswith("s") and ttl[:-1].isdigit():
        return int(ttl[:-1])
    
    match = re.match(r"(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?", ttl)
    if match:
        d, h, m, s = match.groups()
        d = int(d) if d else 0
        h = int(h) if h else 0
        m = int(m) if m else 0
        s = int(s) if s else 0
        return d * 86400 + h * 3600 + m * 60 + s

    return 0

def format_retention_as_days(ttl: str) -> str:
    
    seconds = parse_ttl_to_seconds(ttl)
    days = seconds // 86400
    return f"{days} day{'s' if days != 1 else ''}"

async def get_current_retention_days(namespace: str) -> Optional[int]:
   
    try:
        result = subprocess.run(
            [
                "temporal",
                "operator", "namespace", "describe",
                "--namespace", namespace,
                "--output", "json"
            ],
            capture_output=True,
            text=True,
            check=True
        )
        namespace_info = json.loads(result.stdout)

        ttl = namespace_info["config"].get("workflowExecutionRetentionTtl", "0s")
        return parse_ttl_to_seconds(ttl) // 86400  # return days
    except subprocess.CalledProcessError as e:
        
        return None





async def list_namespaces():
    uniqueId = unique_id()
    client = await connectionWithTemporal()
    if client is None:
        return 


    try:
        asyncio.create_task(workers_retentionPeriod.get_namespaces_worker())

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    handle = await client.start_workflow(
        workflows_retentionPeriod.GetNamespacesWorkflow.run,
        id=f"list-namespaces-workflow-{uniqueId}",
        task_queue="namespace-tasks"
    )
    namespaces = await handle.result()
    return namespaces

async def update_namespace_retention(request: UpdateRetentionRequest):
    uniqueId = unique_id()
    client = await connectionWithTemporal()
    if client is None:
        
        return 

    current_retention = await get_current_retention_days(request.namespace)
    action_message = f"Retention-Updation ({current_retention}d - {request.retention_days}d)"

    try:
        
        asyncio.create_task(workers_retentionPeriod.update_retentionPeriod_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    handle = await client.start_workflow(
        workflows_retentionPeriod.UpdateRetentionWorkflow.run,
        args=[request.namespace, request.retention_days],
        id=f"update-retention-{request.namespace}-{uniqueId}",
        task_queue="update-Retention-tasks",
        search_attributes={
            "Entity": [request.namespace],
            "Action": [action_message],
            "UserName": [request.email]
        },
    )
    namespaces = await handle.result()
    return namespaces

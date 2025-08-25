import asyncio
from datetime import datetime
import subprocess
from fastapi import HTTPException, logger
from typing import Dict, Optional
from pydantic import BaseModel
import re
import subprocess
import json
from typing import Optional
import json
import os
# from dotenv import load_dotenv

# load_dotenv()


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
    # logger.info(f"Generated unique ID - {unique_id}")
    return f"{unique_id.hour }:{unique_id.minute}:{unique_id.second}"

async def connectionWithTemporal():
    temporal_address = os.getenv('TEMPORAL_SERVER')  # Fetch the address
    print(f'Connecting to Temporal server at address: {temporal_address}')
    try:
        client = await Client.connect(temporal_address)
        print(f'Connected to Temporal server at: {temporal_address}')
        return client
    except Exception as e:
        print(f"Connection Refused to Temporal server at {temporal_address}: {e}")
        return None
    



def parse_ttl_to_seconds(ttl: str) -> int:
    """
    Parses TTL strings like '2678400s' or '3d12h0m0s' into total seconds.
    """
    if ttl.endswith("s") and ttl[:-1].isdigit():
        return int(ttl[:-1])
    
    # Match format like "3d12h0m0s", "72h", "45m", etc.
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
        # print("namespace_info---------", namespace_info)

        # Extract and parse TTL
        ttl = namespace_info["config"].get("workflowExecutionRetentionTtl", "0s")
        return parse_ttl_to_seconds(ttl) // 86400  # return days
    except subprocess.CalledProcessError as e:
        print(f"Failed to fetch current retention: {e.stderr.strip()}")
        return None





async def list_namespaces():
    uniqueId = unique_id()
    client = await connectionWithTemporal()
    if client is None:
        print("Could not connect to Temporal server, terminating.")
        return 

    print("list-namespaces-workflow about to start-------")

    try:
        print("worker about to start in list namespaces...")
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
        print("Could not connect to Temporal server, terminating.")
        return 

    current_retention = await get_current_retention_days(request.namespace)
    action_message = f"Retention-Updation ({current_retention}d - {request.retention_days}d)"

    try:
        print("worker about to start in update retention...")
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

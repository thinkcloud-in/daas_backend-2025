from typing import Any, Dict
from temporalio import activity
import httpx
import os


TEMPORAL_SERVER_ADDRESS = os.getenv('TEMPORAL_SERVER_ADDRESS')

@activity.defn
async def list_namespaces_activity() -> Dict[str, Any]:
    
    url = f"{TEMPORAL_SERVER_ADDRESS}/temporal/retention"
    headers = {'Content-Type': 'application/json'}

    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        
        return response.json()

@activity.defn
async def update_retention_activity(namespace: str, retention_days: int) -> Dict[str, Any]:
    url = f"{TEMPORAL_SERVER_ADDRESS}/temporal/retention"
    payload = {
        "days": retention_days,
        "namespace": namespace
    }
    headers = {'Content-Type': 'application/json'}

    async with httpx.AsyncClient() as client:
        response = await client.post(url, headers=headers, json=payload)
        response_data = response.json()
        
        return {
            "newRetentionPeriod": response_data.get("newRetentionPeriod"),
            "message": response_data.get("message"),
            "namespaceName": response_data.get("namespaceName")
        }
    

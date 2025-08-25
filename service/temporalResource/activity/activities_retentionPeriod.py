# workflows.py
from typing import Any, Dict
import requests
from temporalio import activity
import httpx
import json
import os
# from dotenv import load_dotenv

# load_dotenv()

TEMPORAL_SERVER_ADDRESS = os.getenv('TEMPORAL_SERVER_ADDRESS')

@activity.defn
async def list_namespaces_activity() -> Dict[str, Any]:
    print("list_namespaces_activity starting----------")
    url = f"{TEMPORAL_SERVER_ADDRESS}/temporal/retention"
    headers = {'Content-Type': 'application/json'}

    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        print("response----------", response.json())
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
        print("response update---", response_data)
        return {
            "newRetentionPeriod": response_data.get("newRetentionPeriod"),
            "message": response_data.get("message"),
            "namespaceName": response_data.get("namespaceName")
        }
    

# @activity.defn
# async def list_namespaces_activity() -> Dict[str, Any]:
#     print("list_namespaces_activity starting----------")
#     url = f"{TEMPORAL_SERVER_ADDRESS}/temporal/retention"
 
    
#     headers = {
#     'Content-Type': 'application/json'
#     }
    
#     response = requests.request("GET", url, headers=headers)
#     print("response----------",response.json())
#     return response.json()
   

# @activity.defn
# async def update_retention_activity(namespace: str, retention_days: int) -> Dict[str, Any]:
     
#     url = f"{TEMPORAL_SERVER_ADDRESS}/temporal/retention"
    
#     payload = json.dumps({
#     "days": retention_days,
#     "namespace": namespace
#     })
#     headers = {
#     'Content-Type': 'application/json'
#     }
    
#     response = requests.request("POST", url, headers=headers, data=payload)
#     response_data = response.json()
 
#     print("response update---",response.json())
#     return {
#         "newRetentionPeriod": response_data.get("newRetentionPeriod"),
#         "message": response_data.get("message"),
#         "namespaceName": response_data.get("namespaceName")
#     }
  
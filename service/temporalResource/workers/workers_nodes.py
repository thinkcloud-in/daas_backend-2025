from fastapi import HTTPException
from temporalio import worker
from service.temporalResource.workflows import workflows_nodes
from service.temporalResource.activity import activities_nodes
from temporalio.worker import Worker
from temporalio.client import Client
import os

async def connectionWithTemporal():
    print('Connecting to Temporal server...')
    try:
        client = await Client.connect(os.getenv('TEMPORAL_SERVER'))  
        print('Connected to Temporal server.')
        return client
    except Exception as e:
        print(f"Connection Refused to Temporal server: {e}")
        return None
    
 
async def get_nodes_proxmox_worker():
    client = await connectionWithTemporal()
    if client is None:
        print("Could not connect to Temporal server, terminating.")
        return 

    worker = Worker(
        client,
        task_queue="getnodes-task-queue",
        workflows=[workflows_nodes.GetNodesProxmoxWorkflow],
        activities=[activities_nodes.get_all_nodes_activity],
    )
    try:
        print('Worker starting for retrieve the pool data...')
        await worker.run()
    except Exception as e:
        print(f"Error in Temporal worker: {e}")
        print('Worker stopped...')
        raise HTTPException(status_code=500, detail=f"Error in Temporal worker: {e}")
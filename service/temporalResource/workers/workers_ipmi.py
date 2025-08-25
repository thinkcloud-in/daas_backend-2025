from temporalio.client import Client
from temporalio.worker import Worker
import os
from service.temporalResource.activity import activities_ipmi
from service.temporalResource.workflows import workflows_ipmi
from fastapi import HTTPException
async def connectionWithTemporal():
    print('Connecting to Temporal server...')
    try:
        client = await Client.connect(os.getenv('TEMPORAL_SERVER'))  
        print('Connected to Temporal server.')
        return client
    except Exception as e:
        print(f"Connection Refused to Temporal server: {e}")
        return None
   
# worker function for create ipmi
 
async def create_ipmi_worker():
    client = await connectionWithTemporal()
    if client is None:
        print("Could not connect to Temporal server, terminating.")
        return
 
    worker = Worker(
        client,
        task_queue="Createipmi-task-queue",
        workflows=[workflows_ipmi.IpmiCreationWorkflow],
        activities=[activities_ipmi.ipmi_activity],
    )
    try:
        print('Worker starting for creating the IPMI...')
        await worker.run()
    except Exception as e:
        print(f"Error in Temporal worker: {e}")
        print('Worker stopped...')
        raise HTTPException(status_code=500, detail=f"Error in Temporal worker: {e}")
    
async def update_ipmi_worker():
    client = await connectionWithTemporal()
    if client is None:
        print("Could not connect to Temporal server, terminating.")
        return
 
    worker = Worker(
        client,
        task_queue="Updateipmi-task-queue",
        workflows=[workflows_ipmi.IpmiUpdateWorkflow],
        activities=[activities_ipmi.update_ipmi_activity],
    )
    try:
        print('Worker starting for updating the IPMI...')
        await worker.run()
    except Exception as e:
        print(f"Error in Temporal worker: {e}")
        print('Worker stopped...')
        raise HTTPException(status_code=500, detail=f"Error in Temporal worker: {e}")
    
async def delete_ipmi_worker():
    client = await connectionWithTemporal()
    if client is None:
        print("Could not connect to Temporal server, terminating.")
        return
 
    worker = Worker(
        client,
        task_queue="Deleteipmi-task-queue",
        workflows=[workflows_ipmi.IpmiDeletionWorkflow],
        activities=[activities_ipmi.delete_ipmi_activity],
    )
    try:
        print('Worker starting for deleting the IPMI...')
        await worker.run()
    except Exception as e:
        print(f"Error in Temporal worker: {e}")
        print('Worker stopped...')
        raise HTTPException(status_code=500, detail=f"Error in Temporal worker: {e}")
 
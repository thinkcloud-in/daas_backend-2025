from temporalio.client import Client
from temporalio.worker import Worker
import os
from service.temporalResource.activity import activities_ipmi
from service.temporalResource.workflows import workflows_ipmi
from utils.temporal_client import TemporalClientManager
from fastapi import HTTPException
   
# worker function for create ipmi
 
async def create_ipmi_worker():
    client = await TemporalClientManager.get_temporal_client()
    if client is None:

        return None

    worker = Worker(
        client,
        task_queue="Createipmi-task-queue",
        workflows=[workflows_ipmi.IpmiCreationWorkflow],
        activities=[activities_ipmi.ipmi_activity],
    )
    try:
        await worker.run()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in Temporal worker: {e}")
    
async def update_ipmi_worker():
    client = await TemporalClientManager.get_temporal_client()
    if client is None:
        
        return None
 
    worker = Worker(
        client,
        task_queue="Updateipmi-task-queue",
        workflows=[workflows_ipmi.IpmiUpdateWorkflow],
        activities=[activities_ipmi.update_ipmi_activity],
    )
    try:
        
        await worker.run()
    except Exception as e:
        
        
        raise HTTPException(status_code=500, detail=f"Error in Temporal worker: {e}")
    
async def delete_ipmi_worker():
    client = await TemporalClientManager.get_temporal_client()
    if client is None:

        return None

    worker = Worker(
        client,
        task_queue="Deleteipmi-task-queue",
        workflows=[workflows_ipmi.IpmiDeletionWorkflow],
        activities=[activities_ipmi.delete_ipmi_activity],
    )
    try:
        
        await worker.run()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in Temporal worker: {e}")


async def run_all_ipmi_workers():
    """Starts all IPMI-related workers concurrently in the same event loop."""
    import asyncio
    
    tasks = [
        create_ipmi_worker(),
        update_ipmi_worker(),
        delete_ipmi_worker()
    ]
    
    print("Starting all IPMI workers...")
    await asyncio.gather(*tasks)
 
import os
from temporalio.client import Client
from temporalio.worker import Worker
from service.temporalResource.workflows import workflows_retentionPeriod
from service.temporalResource.activity import activities_retentionPeriod
from utils.temporal_client import TemporalClientManager
from dotenv import load_dotenv

load_dotenv()


async def get_namespaces_worker():
    client = await TemporalClientManager.get_temporal_client()
    if client is None:

        return None

    worker = Worker(
        client,
        task_queue="namespace-tasks",
        workflows=[workflows_retentionPeriod.GetNamespacesWorkflow], 
        activities=[activities_retentionPeriod.list_namespaces_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e


async def update_retentionPeriod_worker():
    client = await TemporalClientManager.get_temporal_client()
    if client is None:

        return None

    
    worker = Worker(
        client,
        task_queue="update-Retention-tasks",
        workflows=[workflows_retentionPeriod.UpdateRetentionWorkflow], 
        activities=[activities_retentionPeriod.update_retention_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e


async def run_all_retention_workers():
    """Starts all retention-related workers concurrently in the same event loop."""
    import asyncio
    
    tasks = [
        get_namespaces_worker(),
        update_retentionPeriod_worker()
    ]
    
    print("Starting all retention workers...")
    await asyncio.gather(*tasks)

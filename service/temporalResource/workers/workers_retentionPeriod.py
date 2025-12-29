import os
from temporalio.client import Client
from temporalio.worker import Worker
from service.temporalResource.workflows import workflows_retentionPeriod
from service.temporalResource.activity import activities_retentionPeriod
# from dotenv import load_dotenv

# load_dotenv()

async def connectionWithTemporal():
    
    try:
        client = await Client.connect(os.getenv('TEMPORAL_SERVER'))  
        
        return client
    except Exception as e:
        
        raise e

async def get_namespaces_worker():
    client = await connectionWithTemporal()
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
    client = await connectionWithTemporal()
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

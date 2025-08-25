# worker.py
import asyncio
import os
from temporalio.client import Client
from temporalio.worker import Worker
# from service.temporalResource.workflows.workflows_retentionPeriod  import (
#     GetNamespacesWorkflow,
#     UpdateRetentionWorkflow,
    
# )
# from service.temporalResource.activity.activities_retentionPeriod import (
#     list_namespaces_activity,
#     update_retention_activity,
# )

from service.temporalResource.workflows import workflows_retentionPeriod
from service.temporalResource.activity import activities_retentionPeriod

# async def main():
#     client = await Client.connect("172.16.0.108:7233")  # Or from .env
#     worker = Worker(
#         client,
#         task_queue="namespace-tasks",
#         workflows=[GetNamespacesWorkflow, UpdateRetentionWorkflow],
#         activities=[list_namespaces_activity, update_retention_activity],
#     )
#     print("Worker started.")
#     await worker.run()

from dotenv import load_dotenv

load_dotenv()

async def connectionWithTemporal():
    print('Connecting to Temporal server...')
    try:
        client = await Client.connect(os.getenv('TEMPORAL_SERVER'))  
        print('Connected to Temporal server in namespace---.',client)
        return client
    except Exception as e:
        print(f"Connection Refused to Temporal server: {e}")
        return None

async def get_namespaces_worker():
    client = await connectionWithTemporal()
    if client is None:
        print("Could not connect to Temporal server, terminating.")
        return 

    print('Worker starting ----"list-namespaces-workflow about to started =====-------")...')
    worker = Worker(
        client,
        task_queue="namespace-tasks",
        workflows=[workflows_retentionPeriod.GetNamespacesWorkflow], 
        activities=[activities_retentionPeriod.list_namespaces_activity],
    )
    print('Worker started ----"list-namespaces-workflow about to started =====-------")...')
    try:
        await worker.run()
        print("Temporal worker started. Waiting for tasks...")
    except Exception as e:
        print(f"Error in worker: {e}")


async def update_retentionPeriod_worker():
    client = await connectionWithTemporal()
    if client is None:
        print("Could not connect to Temporal server, terminating.")
        return 

    print('Worker starting ----"list-namespaces-workflow about to started =====-------")...')
    worker = Worker(
        client,
        task_queue="update-Retention-tasks",
        workflows=[workflows_retentionPeriod.UpdateRetentionWorkflow], 
        activities=[activities_retentionPeriod.update_retention_activity],
    )
    print('Worker started ----"list-namespaces-workflow about to started =====-------")...')
    try:
        await worker.run()
        print("Temporal worker started. Waiting for tasks...")
    except Exception as e:
        print(f"Error in worker: {e}")

# if __name__ == "__main__":
#     asyncio.run(main())

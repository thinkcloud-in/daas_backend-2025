from temporalio.client import Client
from temporalio.worker import Worker
import os
from service.temporalResource.activity import activities_pool
from service.temporalResource.workflows import workflows_pool
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
    

# worker function for create pool
async def create_pool_worker():
    client = await connectionWithTemporal()
    if client is None:
        print("Could not connect to Temporal server, terminating.")
        return 

    worker = Worker(
        client,
        task_queue="Createpool-task-queue",
        workflows=[workflows_pool.PoolCreationWorkflow],
        activities=[activities_pool.create_pool_activity],
    )
    try:
        print('Worker starting for creating the pool...')
        await worker.run()
    except Exception as e:
        print(f"Error in Temporal worker: {e}")
        print('Worker stopped...')
        raise RuntimeError(str(e))
    

 # worker function for update pool   
async def update_pool_worker():
    client = await connectionWithTemporal()
    if client is None:
        print("Could not connect to Temporal server, terminating.")
        return 

    worker = Worker(
        client,
        task_queue="Updatepool-task-queue",
        workflows=[workflows_pool.PoolUpdateWorkflow],
        activities=[activities_pool.update_pool_activity],
    )
    try:
        print('Worker starting for update the pool...')
        await worker.run()
    except Exception as e:
        print(f"Error in Temporal worker: {e}")
        print('Worker stopped...')
        raise RuntimeError(str(e))


# worker function for delete pool
async def delete_pool_worker():
    client = await connectionWithTemporal()
    if client is None:
        print("Could not connect to Temporal server, terminating.")
        return 

    worker = Worker(
        client,
        task_queue="Deletepool-task-queue",
        workflows=[workflows_pool.PoolDeletionWorkflow],
        activities=[activities_pool.delete_pool_activity],
    )
    try:
        print('Worker starting for deleting the pool..')
        await worker.run()
    except Exception as e:
        print(f"Error in Temporal worker: {e}")
        print('Worker stopped...')
        raise RuntimeError(str(e))


# worker function for retrieve pool data   
async def retrieve_pool_data_worker():
    client = await connectionWithTemporal()
    if client is None:
        print("Could not connect to Temporal server, terminating.")
        return 

    worker = Worker(
        client,
        task_queue="Retrievepooldata-task-queue",
        workflows=[workflows_pool.RetrievePoolDataWorkflow],
        activities=[activities_pool.retrieve_pool_data_activity],
    )
    try:
        print('Worker starting for retrieve the pool data...')
        await worker.run()
    except Exception as e:
        print(f"Error in Temporal worker: {e}")
        print('Worker stopped...')
        raise RuntimeError(str(e))
    
async def get_all_pool_names_worker():
    client = await connectionWithTemporal()
    if client is None:
        print("Could not connect to Temporal server, terminating.")
        return 

    worker = Worker(
        client,
        task_queue="Getallpoolsnames-task-queue",
        workflows=[workflows_pool.GetListofPoolNamesWorkflow],
        activities=[activities_pool.list_all_pool_names_activity],
    )
    try:
        print('Worker starting for retrieve all the pools...')
        await worker.run()
    except Exception as e:
        print(f"Error in Temporal worker: {e}")
        print('Worker stopped...')
        raise HTTPException(status_code=500, detail=f"Error in Temporal worker: {e}")
    

async def get_all_pools_worker():
    client = await connectionWithTemporal()
    if client is None:
        print("Could not connect to Temporal server, terminating.")
        return 

    worker = Worker(
        client,
        task_queue="Getallpools-task-queue",
        workflows=[workflows_pool.get_all_pools_workflow],
        activities=[activities_pool.get_all_pools_activity],
    )
    try:
        print('Worker starting for retrieve all the pools...')
        await worker.run()
    except Exception as e:
        print(f"Error in Temporal worker: {e}")
        print('Worker stopped...')
        raise HTTPException(status_code=500, detail=f"Error in Temporal worker: {e}")
    

async def get_pool_details_ID_worker():
    client = await connectionWithTemporal()
    if client is None:
        print("Could not connect to Temporal server, terminating.")
        return 

    worker = Worker(
        client,
        task_queue="GetpooldetailsID-task-queue",
        workflows=[workflows_pool.get_pool_details_ID_workflow],
        activities=[activities_pool.get_pool_details_id_activity],
    )
    try:
        print('Worker starting for retrieve pool details...')
        await worker.run()
    except Exception as e:
        # print(f"Error in Temporal worker: {e}")
        print('Worker stopped...')
        raise HTTPException(status_code=500, detail=f"Error in Temporal worker: {e}")

    



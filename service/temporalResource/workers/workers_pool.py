from temporalio.worker import Worker
from service.temporalResource.activity import activities_pool
from service.temporalResource.workflows import workflows_pool
from utils.temporal_client import TemporalClientManager
from fastapi import HTTPException



async def create_pool_worker():
    client = await TemporalClientManager.get_temporal_client()
    if client is None:
        return None

    worker = Worker(
        client,
        task_queue="Createpool-task-queue",
        workflows=[workflows_pool.PoolCreationWorkflow],
        activities=[activities_pool.create_pool_activity],
    )
    try:
        
        await worker.run()
    except Exception as e:    
        raise RuntimeError(str(e))
    

 
async def update_pool_worker():
    client = await TemporalClientManager.get_temporal_client()
    if client is None:

        return None

    worker = Worker(
        client,
        task_queue="Updatepool-task-queue",
        workflows=[workflows_pool.PoolUpdateWorkflow],
        activities=[activities_pool.update_pool_activity],
    )
    try:
        
        await worker.run()
    except Exception as e:
        raise RuntimeError(str(e))



async def delete_pool_worker():
    client = await TemporalClientManager.get_temporal_client()
    if client is None:

        return None

    worker = Worker(
        client,
        task_queue="Deletepool-task-queue",
        workflows=[workflows_pool.PoolDeletionWorkflow],
        activities=[activities_pool.delete_pool_activity],
    )
    try:
        
        await worker.run()
    except Exception as e: 
        raise RuntimeError(str(e))



async def retrieve_pool_data_worker():
    client = await TemporalClientManager.get_temporal_client()
    if client is None:
        
        return None

    worker = Worker(
        client,
        task_queue="Retrievepooldata-task-queue",
        workflows=[workflows_pool.RetrievePoolDataWorkflow],
        activities=[activities_pool.retrieve_pool_data_activity],
    )
    try:
        
        await worker.run()
    except Exception as e:   
        raise RuntimeError(str(e))
    
async def get_all_pool_names_worker():
    client = await TemporalClientManager.get_temporal_client()
    if client is None:
        
        return None

    worker = Worker(
        client,
        task_queue="Getallpoolsnames-task-queue",
        workflows=[workflows_pool.GetListofPoolNamesWorkflow],
        activities=[activities_pool.list_all_pool_names_activity],
    )
    try:
        
        await worker.run()
    except Exception as e:   
        raise HTTPException(status_code=500, detail=f"Error in Temporal worker: {e}")
    

async def get_all_pools_worker():
    client = await TemporalClientManager.get_temporal_client()
    if client is None:
        
        return None

    worker = Worker(
        client,
        task_queue="Getallpools-task-queue",
        workflows=[workflows_pool.get_all_pools_workflow],
        activities=[activities_pool.get_all_pools_activity],
    )
    try:
        
        await worker.run()
    except Exception as e:     
        raise HTTPException(status_code=500, detail=f"Error in Temporal worker: {e}")
    

async def get_pool_details_ID_worker():
    client = await TemporalClientManager.get_temporal_client()
    if client is None:
        
        return None

    worker = Worker(
        client,
        task_queue="GetpooldetailsID-task-queue",
        workflows=[workflows_pool.get_pool_details_ID_workflow],
        activities=[activities_pool.get_pool_details_id_activity],
    )
    try:
        
        await worker.run()
    except Exception as e: 
        raise HTTPException(status_code=500, detail=f"Error in Temporal worker: {e}")

async def domain_join_worker():
    client = await TemporalClientManager.get_temporal_client()
    if client is None:
        return None

    worker = Worker(
        client,
        task_queue="domain-join-task-queue",
        workflows=[workflows_pool.DomainJoinWorkflow],
        activities=[activities_pool.domain_join_activity],
    )
    try:
        await worker.run()
    except Exception as e: 
        raise HTTPException(status_code=500, detail=f"Error in Temporal worker: {e}")


async def run_all_pool_workers():
    """Starts all pool-related workers concurrently in the same event loop."""
    import asyncio
    
    tasks = [
        create_pool_worker(),
        update_pool_worker(),
        delete_pool_worker(),
        retrieve_pool_data_worker(),
        get_all_pool_names_worker(),
        get_all_pools_worker(),
        get_pool_details_ID_worker(),
        domain_join_worker()
    ]
    
    print("Starting all pool workers...")
    await asyncio.gather(*tasks)

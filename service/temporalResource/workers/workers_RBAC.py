import os
from temporalio.worker import Worker
from temporalio.client import Client
from service.temporalResource.activity import activities_RBAC
from service.temporalResource.workflows import workflows_RBAC
from utils.temporal_client import TemporalClientManager
from dotenv import load_dotenv

load_dotenv()



async def get_client_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="get_client_taskqueue",  
        workflows=[workflows_RBAC.GetClientWorkflow], 
        activities=[activities_RBAC.get_client_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        


async def get_client_roles_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="get_client_roles_taskqueue",  
        workflows=[workflows_RBAC.GetClientRolesWorkflow], 
        activities=[activities_RBAC.get_client_roles_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        

async def creating_role_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="creating_role_taskqueue",  
        workflows=[workflows_RBAC.CreatingRoleWorkflow], 
        activities=[activities_RBAC.creating_role_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e


async def deleting_role_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="deleting_role_taskqueue",  
        workflows=[workflows_RBAC.DeletingRoleWorkflow], 
        activities=[activities_RBAC.deleting_role_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        


async def updating_role_component_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="updating_role_component_taskqueue",  
        workflows=[workflows_RBAC.UpdateRoleComponentWorkflow], 
        activities=[activities_RBAC.updating_role_component_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        

async def getting_role_component_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="getting_role_component_taskqueue",  
        workflows=[workflows_RBAC.GetRoleComponentWorkflow], 
        activities=[activities_RBAC.getting_role_components_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        


async def assign_user_role_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="assign_user_role_taskqueue",  
        workflows=[workflows_RBAC.AssignUserRoleworkflow], 
        activities=[activities_RBAC.assign_user_role_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        


async def get_user_permissions_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="get_user_permissions_taskqueue",  
        workflows=[workflows_RBAC.GetUserPermissionsWorkflow], 
        activities=[activities_RBAC.get_user_permissions_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        

async def delete_role_from_user_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="delete_role_from_user_taskqueue",  
        workflows=[workflows_RBAC.DeleteRoleFromUserWorkflow], 
        activities=[activities_RBAC.delete_role_from_user_activity],
    )
    
    try:
        await worker.run()
    except Exception as e:
        raise e

async def run_all_rbac_workers():
    """Starts all RBAC-related workers concurrently in the same event loop."""
    import asyncio
    
    tasks = [
        get_client_worker(),
        get_client_roles_worker(),
        creating_role_worker(),
        deleting_role_worker(),
        updating_role_component_worker(),
        getting_role_component_worker(),
        assign_user_role_worker(),
        get_user_permissions_worker(),
        delete_role_from_user_worker()
    ]
    
    print("Starting all RBAC workers...")
    await asyncio.gather(*tasks)
        
import os
from temporalio.worker import Worker
from temporalio.client import Client
from service.temporalResource.activity import activities_machine
from service.temporalResource.workflows import workflows_machine
# from dotenv import load_dotenv

# load_dotenv()

async def connectionWithTemporal():
    
    try:
        client = await Client.connect(os.getenv('TEMPORAL_SERVER'))  
        
        return client
    except Exception as e:
        
        raise e

async def create_machine_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="create-machine-task-queue",  
        workflows=[workflows_machine.CreateMachineWorkflow], 
        activities=[activities_machine.create_machine_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        


async def delete_machine_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="delete-machine-task-queue",  
        workflows=[workflows_machine.DeleteMachineWorkflow], 
        activities=[activities_machine.delete_machine_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e

async def update_machine_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="update-machine-task-queue",  
        workflows=[workflows_machine.UpdateMachineWorkflow], 
        activities=[activities_machine.update_machine_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e



async def get_all_machines_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="get-all-machines-task-queue",  
        workflows=[workflows_machine.GetAllMachinesWorkflow], 
        activities=[activities_machine.get_all_machines_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        


async def update_is_custom_machine_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="update-is-custom-task-queue",  
        workflows=[workflows_machine.UpdateIsCustomMachineWorkflow], 
        activities=[activities_machine.update_is_custom_machine_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        

async def add_user_to_machine_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="add-user-to-machine-task-queue",  
        workflows=[workflows_machine.AddUserToMachineWorkflow], 
        activities=[activities_machine.add_user_to_machine_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        


async def delete_user_from_machine_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="delete-user-from-machine-task-queue",  
        workflows=[workflows_machine.DeleteUserFromMachineWorkflow], 
        activities=[activities_machine.delete_user_from_machine_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        

async def list_all_machine_in_pool_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="list-all-machine-in-pool-task-queue",  
        workflows=[workflows_machine.GetAllMachineInPoolWorkflow], 
        activities=[activities_machine.list_all_machine_in_pool_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        


async def list_all_asigned_users_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="list-all-asigned_users-task-queue",  
        workflows=[workflows_machine.ListOfAsignedUsersWorkflow], 
        activities=[activities_machine.list_of_asigned_users_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        


async def get_machine_details_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="get-machine-details-task-queue",  
        workflows=[workflows_machine.GetMachineDetailsWorkflow], 
        activities=[activities_machine.get_machine_details_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        
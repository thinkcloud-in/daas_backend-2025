import asyncio
from datetime import datetime
import logging
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session # type: ignore
from models.models import Pool, CreatePoolBase, IsCustomeValue, UpdatePoolBase, CreateMachineBase,Pool, Machine,CreateClusterBase,Cluster,UpdateClusterBase,UpdateMachineBase
from fastapi import HTTPException,Request
from service.gucamoleService import connectionWithClient
from typing import List

from .temporalResource.workflows import workflows_machine
from .temporalResource.workers import workers_machine

from service.temporalResource.workers import  workers_pool
from service.temporalResource.workflows import  workflows_pool
# from Backend_temporal_2025.service.temporalResource import get_status_recent_work
from models import task_models
from typing import Optional

from service.temporalResource.workflows import workflows_cluster
from temporalio.client import WorkflowExecutionStatus



def unique_id():
    unique_id = datetime.now()
    logger.info(f"Generated unique ID - {unique_id}")
    return f"{unique_id.hour }:{unique_id.minute}:{unique_id.second}"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("create_machine_activity")

#Function to create pool
# async def create_pool(pool_data: dict,db) -> dict:
#     uniqueId = unique_id()
#     client = await connectionWithClient()
#     workflow_id = f"Creating-pool-{uniqueId}"
#     # print(f"pool_data type: {type(pool_data)}") 

#     print("Creating pool with temporal server...")
#     try:
#         print("worker about to start...")
#         asyncio.create_task(workers_pool.create_pool_worker())
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
#     handle = await client.start_workflow(
#         workflows_pool.PoolCreationWorkflow.run,
#         pool_data,
#         id=workflow_id,
#         task_queue="Createpool-task-queue",

#     )
#     # result =  await handle.result()
   
#     # return result

#     return {
#         "message": "Pool creation started",
#         "workflow_id": workflow_id,
#         "check_status_at": f"/workflow-status/{workflow_id}",
#         "status":workflow_status.get_workflow_status
#     }


async def create_pool(pool_data: dict, db) -> dict:
    uniqueId = unique_id()
    client = await connectionWithClient()

    # body = await request.body()
    # print("🔍 Raw Request Body:", body)
   
    # print("🔍 Received pool_data:", pool_data)  # ✅ Add this log to check incoming data

    pool_name = pool_data.get("pool_name", "UnknownPool")
    userName = pool_data.get('email', "UnknownUser")
   
    workflow_id = f"{pool_name} Creating-{uniqueId}"
    print(pool_name,userName,workflow_id)

    try:
        print("worker about to start...")
        asyncio.create_task(workers_pool.create_pool_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # pool_data["PoolAction"] = "Created"  
    
    handle = await client.start_workflow(
        workflows_pool.PoolCreationWorkflow.run,
        pool_data,
        id=workflow_id,
        task_queue= "Createpool-task-queue",
        search_attributes={
            "Entity": [pool_name],  # Store Pool Name for easier filtering
            "Action": ["Pool-Creation"],
            "UserName": [userName]
        },
    )
    result =  await handle.result()
    # if isinstance(result, dict):
    #     result["workflow_id"] = workflow_id
    # else:
    #     # If result is not a dict, create a new dict with result and workflow_id
    #     result = {"result": result, "workflow_id": workflow_id}
   
    return result

    # Return workflow_id along with result
    # return {"workflow_id": workflow_id, "result": result}
    # Return basic info immediately
    # Don't include the status yet - we'll let the route handle that
    # return {
    #     # "message": result,
    #     "workflow_id": workflow_id,
    #     "check_status_at": f"/workflow-status/{workflow_id}"
    # }
    
# async def get_recent_tasks_status(workflow_id: str) -> dict:
#     # from workflow_status import get_workflow_status
    
#     try:
#         status = await get_status_recent_work.get_workflow_status(workflow_id)
#         return status
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to get pool status: {str(e)}")
#Function to update pool    
async def update_pool(pool_id:int,email: Optional[str], pool_data: dict,db)->dict:
    print(f"pool_id type: {type(pool_id)}") 
    uniqueId = unique_id()
    client = await connectionWithClient()
    # print("Updating pool with temporal server...")
    pool_name = pool_data.get("pool_name", "UnknownPool")
    userName = pool_data.get("email", "UnknownUser")
    print("userName in update pool---------", userName)

    workflow_id = f"{pool_name} Updating-{uniqueId}"


    try:
        print("worker about to start...")
        asyncio.create_task(workers_pool.update_pool_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_pool.PoolUpdateWorkflow.run,
        args=[pool_id,pool_data],
        id=workflow_id,
        task_queue="Updatepool-task-queue",
        search_attributes={
            "Entity": [pool_name],  # Store Pool Name for easier filtering
            "Action": ["Pool-Updation"],
            "UserName": [userName]
        },
    )
    result =  await handle.result()
    return result
#Function to create a machine




# async def create_machine(machine_data: "CreateMachineBase"):
#     uniqueId = unique_id()
#     client = await connectionWithClient()
#     machine_name = machine_data.name
#     userName = machine_data.email
 
#     # Generate the create-machine workflow ID
#     create_machine_wfid = f"{machine_name}create-{uniqueId}"
 
#     # Merge IDs: always start with the create-machine ID
#     workflowId_list = [create_machine_wfid]
 
#     # Merge in clone_workflow_id if present
#     clone_wfid = getattr(machine_data, "clone_workflow_id", None)
#     if clone_wfid:
#         if isinstance(clone_wfid, list):
#             workflowId_list.extend(clone_wfid)
#         else:
#             workflowId_list.append(str(clone_wfid))
 
#     # Optionally, merge any already-passed workflowId (avoid duplicates)
#     existing_wfids = getattr(machine_data, "workflowId", None)
#     if existing_wfids:
#         if isinstance(existing_wfids, list):
#             for wf in existing_wfids:
#                 if wf not in workflowId_list:
#                     workflowId_list.append(wf)
#         elif existing_wfids not in workflowId_list:
#             workflowId_list.append(str(existing_wfids))
 
#     print("Final merged workflowId_list:", workflowId_list)
 
#     # ---- NEW: Initialize workflow_status for all workflow IDs ----
#     workflow_status_map = {wfid: {"status": "running", "error": None} for wfid in workflowId_list}
 
#     try:
#         # Start the worker if needed
#         asyncio.create_task(workers_machine.create_machine_worker())
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error while starting worker task: {str(e)}")
 
#     try:
#         # Prepare data for the workflow/activity
#         machine_data_dict = machine_data.dict() if hasattr(machine_data, "dict") else dict(machine_data)
#         machine_data_dict["workflowId"] = workflowId_list
#         machine_data_dict["workflow_status"] = workflow_status_map  # <-- <--- THIS LINE ADDED
#         machine_data_dict.pop("clone_workflow_id", None)  # Remove helper field before storing
 
#         handle = await client.start_workflow(
#             workflows_machine.CreateMachineWorkflow.run,
#             machine_data_dict,
#             id=create_machine_wfid,
#             task_queue="create-machine-task-queue",
#             search_attributes={
#                 "Entity": [machine_name],
#                 "Action": ["Machine-Creation"],
#                 "UserName": [userName]
#             }
#         )
#         result = await handle.result()
#         return result
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"An error occurred during machine creation: {str(e)}")
from db_configuration.config import SessionLocal, get_db
db=SessionLocal() 
 
async def create_machine(machine_data: CreateMachineBase):
    logger.info(f"Received machine_data for creation: {machine_data}")
    uniqueId = unique_id()
    client = await connectionWithClient()
    machine_name = machine_data.name
    userName = machine_data.email
    Pool_data = db.query(Pool).filter(Pool.id == machine_data.pool_id).first()

    create_machine_wfid = f"{machine_name}create-{uniqueId}"
    workflowId_list = [create_machine_wfid]

    # If Automated, merge in other workflow IDs
    if Pool_data.pool_type == "Automated":
        logger.info("Pool type is Automated, processing workflow IDs.")
        clone_wfid = getattr(machine_data, "clone_workflow_id", None)
        if clone_wfid:
            if isinstance(clone_wfid, list):
                workflowId_list.extend(clone_wfid)
            else:
                workflowId_list.append(str(clone_wfid))
        existing_wfids = getattr(machine_data, "workflowId", None)
        if existing_wfids:
            if isinstance(existing_wfids, list):
                for wf in existing_wfids:
                    if wf not in workflowId_list:
                        workflowId_list.append(wf)
            elif existing_wfids not in workflowId_list:
                workflowId_list.append(str(existing_wfids))
        workflow_status_map = {wfid: {"status": "running", "error": None} for wfid in workflowId_list}
        
    else:
        logger.info("Pool type is Manual, skipping additional workflow IDs.")
        workflow_status_map = None
    try:
        asyncio.create_task(workers_machine.create_machine_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error while starting worker task: {str(e)}")
    # Prepare data for workflow/activity
    machine_data_dict = machine_data.dict() if hasattr(machine_data, "dict") else dict(machine_data)
    machine_data_dict["workflowId"] = workflowId_list
    machine_data_dict.pop("clone_workflow_id", None)
    if workflow_status_map is not None:
        machine_data_dict["workflow_status"] = workflow_status_map

    try:
        logger.info(f"Starting workflow for machine creation: {machine_data_dict}")
        handle = await client.start_workflow(
            workflows_machine.CreateMachineWorkflow.run,
            machine_data_dict,
            id=create_machine_wfid,
            task_queue="create-machine-task-queue",
            search_attributes={
                "Entity": [machine_name],
                "Action": ["Machine-Creation"],
                "UserName": [userName]
            }
        )   
        result = await handle.result()
        logger.info(f"Machine creation workflow completed with result")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred during machine creation: {str(e)}")

# async def create_machine(machine_data: "CreateMachineBase"):
#     uniqueId = unique_id()
#     client = await connectionWithClient()
#     machine_name = machine_data.name
#     userName = machine_data.email

#     # Generate the create-machine workflow ID
#     create_machine_wfid = f"{machine_name}create-{uniqueId}"

#     # Merge IDs: always start with the create-machine ID
#     workflowId_list = [create_machine_wfid]

#     # Merge in clone_workflow_id if present
#     clone_wfid = getattr(machine_data, "clone_workflow_id", None)
#     if clone_wfid:
#         if isinstance(clone_wfid, list):
#             workflowId_list.extend(clone_wfid)
#         else:
#             workflowId_list.append(str(clone_wfid))

#     # Optionally, merge any already-passed workflowId (avoid duplicates)
#     existing_wfids = getattr(machine_data, "workflowId", None)
#     if existing_wfids:
#         if isinstance(existing_wfids, list):
#             for wf in existing_wfids:
#                 if wf not in workflowId_list:
#                     workflowId_list.append(wf)
#         elif existing_wfids not in workflowId_list:
#             workflowId_list.append(str(existing_wfids))

#     print("Final merged workflowId_list:", workflowId_list)

#     try:
#         # Start the worker if needed
#         asyncio.create_task(workers_machine.create_machine_worker())
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error while starting worker task: {str(e)}")

#     try:
#         # Prepare data for the workflow/activity
#         machine_data_dict = machine_data.dict() if hasattr(machine_data, "dict") else dict(machine_data)
#         machine_data_dict["workflowId"] = workflowId_list
#         machine_data_dict.pop("clone_workflow_id", None)  # Remove helper field before storing

#         handle = await client.start_workflow(
#             workflows_machine.CreateMachineWorkflow.run,
#             machine_data_dict,
#             id=create_machine_wfid,
#             task_queue="create-machine-task-queue",
#             search_attributes={
#                 "Entity": [machine_name],
#                 "Action": ["Machine-Creation"],
#                 "UserName": [userName]
#             }
#         )
#         result = await handle.result()
#         return result
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"An error occurred during machine creation: {str(e)}")


#Function to delete a machine
async def delete_machine(machine_identifier: str, email: Optional[str], db: Session):
    uniqueId = unique_id()
    client = await connectionWithClient()

    # Query the database to get machine details
    machine = db.query(Machine).filter(Machine.identifier == machine_identifier).first()
    
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")

    machine_name = machine.name  # Assuming the table has a 'name' column
    userName = email
    # userName = machine.email
    # print("Machine Name:", machine_name)

    # userName = pool_data.get('email')

    try:
        asyncio.create_task(workers_machine.delete_machine_worker())
        logger.info(f"Started worker task for deleting machine with unique ID {uniqueId}.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_machine.DeleteMachineWorkflow.run,
        machine_identifier,
        id=f"delete-machines-{uniqueId}",
        task_queue="delete-machine-task-queue",
        search_attributes={
            "Entity": [machine_name],  
            "Action": ["Machine-Deletion"],
            "UserName": [userName]
        },
    )
    result = await handle.result()
    # logger.info(f"Workflow completed. Result: {result}")

    # print('-------result---------',handle)
    # print(result)
    return result


async def delete_pool(pool_id: int, email: Optional[str], db: Session) -> dict:
    print(f"pool_id type: {type(pool_id)}") 
    uniqueId = unique_id()
    client = await connectionWithClient()

    # Fetch the pool from the database
    pool = db.query(Pool).filter(Pool.id == pool_id).first()
    if pool is None:
        raise HTTPException(status_code=404, detail="Pool not found")

    pool_name = pool.pool_name
    userName = email if email else "Unknown User"  # Use email from frontend
    # userName = email
    print("userEmil in pool deletion----",userName)

    workflow_id = f"{pool_name}-deleting-{uniqueId}"

    try:
        print("worker about to start...")
        asyncio.create_task(workers_pool.delete_pool_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    handle = await client.start_workflow(
        workflows_pool.PoolDeletionWorkflow.run,
        pool_id,
        id=workflow_id,
        task_queue="Deletepool-task-queue",
        search_attributes={
            "Entity": [pool_name],  
            "Action": ["Pool-Deletion"],
            "UserName": [userName]  # Now uses the frontend email
        },
    )
    result = await handle.result()
    return result


# async def assign_machine(usernames=['user1@testingad.local', 'arpit@testingad.local']):
#     for username in usernames:
#             assign_connection_to_user(username, '24')
            
#     return assign_connection_to_user(['user1@testingad.local', 'arpit@testingad.local'], '24')
     
#Function to assign user to the machine               
async def add_user_to_machine( machine_identifier: str, username: str):
    uniqueId = unique_id()
    client = await connectionWithClient()
    try:
        asyncio.create_task(workers_machine.add_user_to_machine_worker())
        logger.info(f"Started worker task for assigning user to machine with unique ID {uniqueId}.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_machine.AddUserToMachineWorkflow.run,
        args = [machine_identifier, username],
        id=f"add-user-to-machine-{uniqueId}",
        task_queue="add-user-to-machine-task-queue",
    )
    result = await handle.result()
    # logger.info(f"Workflow completed. Result: {result}")
    # print('-------result---------',handle)
    # print(result)
    return result



#remove assigned user from a machine 
async def delete_user_from_machine(machine_identifier: str, user_id: str):
    uniqueId = unique_id()
    client = await connectionWithClient()
    try:
        asyncio.create_task(workers_machine.delete_user_from_machine_worker())
        logger.info(f"Started worker task for removing user from machine with unique ID {uniqueId}.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_machine.DeleteUserFromMachineWorkflow.run,
        args = [machine_identifier, user_id],
        id=f"delete-user-from-machine-{uniqueId}",
        task_queue="delete-user-from-machine-task-queue",
    )
    result = await handle.result()
    # logger.info(f"Workflow completed. Result: {result}")
    # print('-------result---------',handle)
    # print(result)
    return result

#Function to update machine    
async def update_machine(machine_identifier: str, machine_data: UpdateMachineBase):
    uniqueId = unique_id()
    client = await connectionWithClient()
    machine_name = machine_data.name
    userName = machine_data.email
    print("machine_name update_machine",machine_name)
    print("userName update_machine",userName)
    workflow_id = f"{machine_name} update-machine-{uniqueId}"
    try:
        asyncio.create_task(workers_machine.update_machine_worker())
        logger.info(f"Started worker task for updating machine with unique ID {uniqueId}.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_machine.UpdateMachineWorkflow.run,
        args = [machine_identifier,machine_data.dict()],
        id=workflow_id,
        task_queue="update-machine-task-queue",
        search_attributes={
            "Entity": [machine_name],  
            "Action": ["Machine-Updation"],
            "UserName": [userName]
        },
    )
    result = await handle.result()
    # logger.info(f"Workflow completed. Result: {result}")
    # print('-------result---------',handle)
    # print(result)
    return result  

async def get_machines():
    uniqueId = unique_id()
    client = await connectionWithClient()
    try:
        asyncio.create_task(workers_machine.get_all_machines_worker())
        logger.info(f"Started worker task for getting all machines with unique ID {uniqueId}.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_machine.GetAllMachinesWorkflow.run,
        id=f"get-all-machines-{uniqueId}",
        task_queue="get-all-machines-task-queue"
    )
    result = await handle.result()
    # logger.info(f"Workflow completed. Result: {result}")
    return result

async def update_is_custom_machine(machine_identifier: str, machine_details: IsCustomeValue):
    
    uniqueId = unique_id()
    client = await connectionWithClient()
    try:
        asyncio.create_task(workers_machine.update_is_custom_machine_worker())
        logger.info(f"Started worker task for updating custom machine with unique ID {uniqueId}.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_machine.UpdateIsCustomMachineWorkflow.run,
        args = [machine_identifier, machine_details.dict()],
        id=f"update-is-custom-{uniqueId}",
        task_queue="update-is-custom-task-queue"
    )
    result = await handle.result()
    # logger.info(f"Workflow completed. Result: {result}")
    return result


async def list_of_all_machine_in_pool(pool_id : str):
    uniqueId = unique_id()
    client = await connectionWithClient()
    try:
        asyncio.create_task(workers_machine.list_all_machine_in_pool_worker())
        logger.info(f"Started worker task for listing all machines in pool with unique ID {uniqueId}.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_machine.GetAllMachineInPoolWorkflow.run,
        pool_id,
        id=f"list-all-machine-in-pool-{uniqueId}",
        task_queue="list-all-machine-in-pool-task-queue"
    )
    result = await handle.result()
    # logger.info(f"Workflow completed. Result: {result}")
    return result

#list all the assigned users to a machine
async def list_assigned_users( machine_id: str):
    uniqueId = unique_id()
    client = await connectionWithClient()
    try:
        asyncio.create_task(workers_machine.list_all_asigned_users_worker())
        logger.info(f"Started worker task for listing all assigned users to machine with unique ID {uniqueId}.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_machine.ListOfAsignedUsersWorkflow.run,
        machine_id,
        id=f"list-all-asigned_users-{uniqueId}",
        task_queue="list-all-asigned_users-task-queue"
    )
    result = await handle.result()
    # logger.info(f"Workflow completed. Result: {result}")
    return result

#get pool details based on pool id 6
async def get_pool_details(pool_id:int, db: Session):
    uniqueId = unique_id()
    client = await connectionWithClient()
    print("Getting pool details based on ID with temporal server...")
    try:
        print("worker about to start...")
        asyncio.create_task(workers_pool.get_pool_details_ID_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_pool.get_pool_details_ID_workflow.run,
        pool_id,
        id=f"Get-pool-details_ID-{uniqueId}",
        task_queue="GetpooldetailsID-task-queue",
    )
    result =  await handle.result()
    return result

async def retrive_pool_data(pool_name:str,db:Session):

    uniqueId = unique_id()
    client = await connectionWithClient()
    print("Retrieving pool data with temporal server...")
    try:
        print("worker about to start...")
        asyncio.create_task(workers_pool.retrieve_pool_data_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_pool.RetrievePoolDataWorkflow.run,
        pool_name,
        id=f"Retrieving-pool-data-{uniqueId}",
        task_queue="Retrievepooldata-task-queue",
    )
    result =  await handle.result()
    return result

async def get_all_pool_names(db: Session):
    uniqueId = unique_id()
    client = await connectionWithClient()
    print("Getting all pool names with temporal server...")
    try:
        print("worker about to start...")
        asyncio.create_task(workers_pool.get_all_pool_names_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_pool.GetListofPoolNamesWorkflow.run,
        id=f"Get-pool-names-{uniqueId}",
        task_queue="Getallpoolsnames-task-queue",
    )
    result =  await handle.result()
    print("pools",result)
    return result


async def get_all_pools(db: Session):
    uniqueId = unique_id()
    client = await connectionWithClient()
    print("Getting all pools with temporal server...")
    workflow_id = f"Get-all-pools-{uniqueId}"
    try:
        print("worker about to start...")
        asyncio.create_task(workers_pool.get_all_pools_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_pool.get_all_pools_workflow.run,
        id=workflow_id,
        task_queue="Getallpools-task-queue",
    )
    result =  await handle.result()
    return result

    # return {
    #     "message": result,
    #     "workflow_id": workflow_id,
    #     "check_status_at": f"/workflow-status/{workflow_id}"
    # }

#get machine details based on machine id
async def get_machine_details( machine_id: str):
    uniqueId = unique_id()
    client = await connectionWithClient()
    try:
        asyncio.create_task(workers_machine.get_machine_details_worker())
        logger.info(f"Started worker task for getting machine details with unique ID {uniqueId}.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_machine.GetMachineDetailsWorkflow.run,
        machine_id,
        id=f"get-machine-details-{uniqueId}",
        task_queue="get-machine-details-task-queue"
    )
    result = await handle.result()
    # logger.info(f"Workflow completed. Result: {result}")
    return result
    


async def create_cluster(cluster_data: CreateClusterBase,db):
    uniqueId = unique_id()
    client = await connectionWithClient()
    cluster_name = cluster_data['name']
    userName = cluster_data.get('email', "UnknownUser")

 
    handle = await client.start_workflow(
        workflows_cluster.CreateClusterWorkflow.run,
        cluster_data,
        id=f'cluster-creation-{uniqueId}',
        task_queue="cluster-task-queue",
        # id_reuse_policy=temporalio.common.WorkflowIDReusePolicy.REJECT_DUPLICATE,
        search_attributes={
            "Entity": [cluster_name],  
            "Action": ["Cluster-Creation"],
            "UserName": [userName]
        },
    )
    print("Workflow started! Workflow ID:", handle.id)
    result = await handle.result()
    print("Workflow result:", result)
    return result

async def delete_cluster( db: Session, cluster_id: str, email: Optional[str] = None, ):
    uniqueId = unique_id()
    client = await connectionWithClient()

    # Optionally fetch the cluster name from DB for search attributes
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    cluster_name = cluster.name if cluster else f"Cluster-{cluster_id}"
    userName = email if email else "Unknown User"  # Use email from frontend
 
    handle = await client.start_workflow(
        workflows_cluster.DeleteClusterWorkflow.run,
        cluster_id,
        id=f'cluster-deletion-{uniqueId}',
        task_queue="cluster-task-queue",
        search_attributes={
            "Entity": [str(cluster_name)],
            "Action": ["Cluster-Deletion"],
            "UserName": [userName]
        },
    )
    print("Workflow started! Workflow ID:", handle.id)
    result = await handle.result()
    print("Workflow result:", result)
    return result
    
async def update_cluster(db, cluster_id: str, cluster_data: UpdateClusterBase):
    uniqueId = unique_id()
    client = await connectionWithClient()
    
    # Convert Pydantic model to dict for serialization
    cluster_data_dict = jsonable_encoder(cluster_data)
    cluster_name = cluster_data_dict.get('name')
    userName = cluster_data_dict.get('email', "UnknownUser")
    # try:
    #     print("worker about to start...")
    #     asyncio.create_task(workers_cluster.update_cluster_worker())
    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=str(e))

    handle = await client.start_workflow(
        workflows_cluster.UpdateClusterWorkflow.run,
        args=[cluster_data_dict,cluster_id],
        id=f"cluster-update-{uniqueId}",
        task_queue="cluster-task-queue",
        search_attributes={
            "Entity": [str(cluster_name)],
            "Action": ["Cluster-Updation"],
            "UserName": [userName]
        },
    )
    result = await handle.result()
    return result
 
 # get pool details based on pool id 6
async def get_cluster_details(db: Session, cluster_id: str):
    try:
        # Query the database to get the pool by ID
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster:
            raise HTTPException(status_code=404, detail="Cluster not found")
        # Return the pool details
        return cluster
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

def create_task_details(task: task_models.Task_config, db: Session):
    try:
        # Create and add the new pool
        task = task_models.Task_DB(**task.dict())
        db.add(task)
        db.commit()
        
        db.refresh(task)
        task_json = jsonable_encoder(task)
        
        return {"msg": "Task Added Successfully", "task": task_json}
 
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Task Adding Process failed: {str(e)}")
    finally:
        db.close()
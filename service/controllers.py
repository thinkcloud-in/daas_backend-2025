import asyncio
from datetime import datetime
import logging
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session
from models.models import Pool, IsCustomeValue, CreateMachineBase,Pool, Machine,CreateClusterBase,Cluster,UpdateClusterBase,UpdateMachineBase
from fastapi import HTTPException
from service.clusterService import get_api_token
from service.gucamoleService import connectionWithClient
from service.proxmoxService import is_valid_ip
from utils import response_format
from .temporalResource.workflows import workflows_machine
from .temporalResource.workers import workers_machine
from service.temporalResource.workers import  workers_pool
from service.temporalResource.workflows import  workflows_pool
from models import task_models
from typing import Optional
from service.temporalResource.workflows import workflows_cluster
from db_configuration.config import SessionLocal
from utils.session_manager import SessionManager




def unique_id():
    unique_id = datetime.now()
    logger.info(f"Generated unique ID - {unique_id}")
    return f"{unique_id.hour }:{unique_id.minute}:{unique_id.second}"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("create_machine_activity")


async def get_proxmox_storages(payload, db):
    try:
        cluster_data = await get_cluster_details(db, payload.cluster_id)
        api_token = get_api_token(db, cluster_data.name)
        headers = {
            "Authorization": f"PVEAPIToken={api_token}",
            "Content-Type": "application/json"
        }
        
        if isinstance(cluster_data.ip, str):
            ip_list = [ip.strip() for ip in cluster_data.ip.split(",") if is_valid_ip(ip.strip())]
        else:
            ip_list = [ip for ip in cluster_data.ip if is_valid_ip(ip)]
    
        if not ip_list:
            raise RuntimeError("No valid IPs found for cluster.")
        
        nodes = list({
            n.strip()
            for n in (payload.nodes or [])
            if n and n.strip()
        })

        if not nodes:
            raise HTTPException(status_code=400, detail="No nodes provided")

        result = {}

        for ip in ip_list:
            PROXMOX_BASE_URL = f"https://{ip}:{cluster_data.port}"

            for node in nodes:
                url = f"{PROXMOX_BASE_URL}/api2/json/nodes/{node}/storage"

                session = SessionManager.get_session()
                response = session.get(
                    url,
                    headers=headers,
                    verify=False,
                    timeout=30
                )

                if response.status_code != 200:
                    raise HTTPException(
                        status_code=response.status_code,
                        detail=f"Failed to fetch storages for node {node} on {ip}"
                    )

                data = response.json().get("data", [])
                for storage in data:
                    if not storage or not isinstance(storage, dict):
                        continue
                    storage_name = storage.get("storage")
                    if not storage_name:
                        continue
                    if storage_name not in result:
                        result[storage_name] = storage

        return list(result.values())
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def create_pool(pool_data: dict, db) -> dict:
    uniqueId = unique_id()
    client = await connectionWithClient()
    pool_name = pool_data.get("pool_name", "UnknownPool")
    userName = pool_data.get('email', "UnknownUser")
    workflow_id = f"{pool_name} Creating-{uniqueId}"
    
    handle = await client.start_workflow(
        workflows_pool.PoolCreationWorkflow.run,
        pool_data,
        id=workflow_id,
        task_queue= "Createpool-task-queue",
        search_attributes={
            "Entity": [pool_name],
            "Action": ["Pool-Creation"],
            "UserName": [userName]
        },
    )
    # result =  await handle.result()
    # if isinstance(result, dict) and result.get("status") == "error":
    #     raise HTTPException(
    #         status_code=400,
    #         detail={ 
    #             "error_type": result.get("error_type"),
    #             "error": result.get("error")
    #         }
    #     )
    return {
        "status": "success",
        "msg": "Pool creation workflow started",
        "workflow_id": handle.id,
        "run_id": handle.run_id
    } 

async def update_pool(pool_id:int,email: Optional[str], pool_data: dict,db)->dict:

    uniqueId = unique_id()
    client = await connectionWithClient()
    pool_name = pool_data.get("pool_name", "UnknownPool")
    userName = pool_data.get("email", "UnknownUser")
    workflow_id = f"{pool_name} Updating-{uniqueId}"
    handle = await client.start_workflow(
        workflows_pool.PoolUpdateWorkflow.run,
        args=[pool_id,pool_data],
        id=workflow_id,
        task_queue="Updatepool-task-queue",
        search_attributes={
            "Entity": [pool_name],
            "Action": ["Pool-Updation"],
            "UserName": [userName]
        },
    )
    result =  await handle.result()
    return result


 
async def create_machine(machine_data: CreateMachineBase, db: Session = None):
    logger.info(f"Received machine_data for creation: {machine_data}")
    own_db = False
    if db is None:
        db = SessionLocal()
        own_db = True
    try:
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
            # result = await handle.result()
            # logger.info(f"Machine creation workflow completed with result")
            # return result
            return {
                "status": "success",
                "msg": "Machine creation workflow started",
                "workflow_id": handle.id,
                "run_id": handle.run_id
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"An error occurred during machine creation: {str(e)}")
    finally:
        if own_db:
            db.close()



async def delete_machine(machine_identifier: str, email: Optional[str], db: Session):
    uniqueId = unique_id()
    client = await connectionWithClient()

    machine = db.query(Machine).filter(Machine.identifier == machine_identifier).first()
    
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")

    machine_name = machine.name  # Assuming the table has a 'name' column
    userName = email

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
    return result


async def delete_pool(pool_id: int, email: Optional[str], db: Session) -> dict:
    uniqueId = unique_id()
    client = await connectionWithClient()

    pool = db.query(Pool).filter(Pool.id == pool_id).first()
    if pool is None:
        raise HTTPException(status_code=404, detail="Pool not found")

    pool_name = pool.pool_name
    userName = email if email else "Unknown User"  # Use email from frontend

    workflow_id = f"{pool_name}-deleting-{uniqueId}"


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
        
async def add_user_to_machine( machine_identifier: str, username: str):
    uniqueId = unique_id()
    client = await connectionWithClient()
    handle = await client.start_workflow(
        workflows_machine.AddUserToMachineWorkflow.run,
        args = [machine_identifier, username],
        id=f"add-user-to-machine-{uniqueId}",
        task_queue="add-user-to-machine-task-queue",
    )
    result = await handle.result()
    return result
#remove assigned user from a machine 
async def delete_user_from_machine(machine_identifier: str, user_id: str):
    uniqueId = unique_id()
    client = await connectionWithClient()
    handle = await client.start_workflow(
        workflows_machine.DeleteUserFromMachineWorkflow.run,
        args = [machine_identifier, user_id],
        id=f"delete-user-from-machine-{uniqueId}",
        task_queue="delete-user-from-machine-task-queue",
    )
    result = await handle.result()
    if(result['msg'] != "404"):
        return result


async def update_machine(machine_identifier: str, machine_data: UpdateMachineBase):
    uniqueId = unique_id()
    client = await connectionWithClient()
    machine_name = machine_data.name
    userName = machine_data.email
    workflow_id = f"{machine_name} update-machine-{uniqueId}"
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
    return result  

async def get_machines():
    uniqueId = unique_id()
    client = await connectionWithClient()
    handle = await client.start_workflow(
        workflows_machine.GetAllMachinesWorkflow.run,
        id=f"get-all-machines-{uniqueId}",
        task_queue="get-all-machines-task-queue"
    )
    result = await handle.result()
    return result

async def update_is_custom_machine(machine_identifier: str, machine_details: IsCustomeValue):
    
    uniqueId = unique_id()
    client = await connectionWithClient()
    handle = await client.start_workflow(
        workflows_machine.UpdateIsCustomMachineWorkflow.run,
        args = [machine_identifier, machine_details.dict()],
        id=f"update-is-custom-{uniqueId}",
        task_queue="update-is-custom-task-queue"
    )
    result = await handle.result()
    return result


async def list_of_all_machine_in_pool(pool_id : str):
    uniqueId = unique_id()
    client = await connectionWithClient()
    handle = await client.start_workflow(
        workflows_machine.GetAllMachineInPoolWorkflow.run,
        pool_id,
        id=f"list-all-machine-in-pool-{uniqueId}",
        task_queue="list-all-machine-in-pool-task-queue"
    )
    result = await handle.result()
    return result

#list all the assigned users to a machine
async def list_assigned_users( machine_id: str):
    uniqueId = unique_id()
    client = await connectionWithClient()
    handle = await client.start_workflow(
        workflows_machine.ListOfAsignedUsersWorkflow.run,
        machine_id,
        id=f"list-all-asigned_users-{uniqueId}",
        task_queue="list-all-asigned_users-task-queue"
    )
    result = await handle.result()
    return result

async def get_pool_details(pool_id:int):
    uniqueId = unique_id()
    client = await connectionWithClient()
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
    handle = await client.start_workflow(
        workflows_pool.GetListofPoolNamesWorkflow.run,
        id=f"Get-pool-names-{uniqueId}",
        task_queue="Getallpoolsnames-task-queue",
    )
    result =  await handle.result()
    return result


async def get_all_pools():
    uniqueId = unique_id()
    client = await connectionWithClient()
    workflow_id = f"Get-all-pools-{uniqueId}"
    handle = await client.start_workflow(
        workflows_pool.get_all_pools_workflow.run,
        id=workflow_id,
        task_queue="Getallpools-task-queue",
    )
    result =  await handle.result()
    return result

async def get_machine_details( machine_id: str):
    uniqueId = unique_id()
    client = await connectionWithClient()
    handle = await client.start_workflow(
        workflows_machine.GetMachineDetailsWorkflow.run,
        machine_id,
        id=f"get-machine-details-{uniqueId}",
        task_queue="get-machine-details-task-queue"
    )
    result = await handle.result()
    return result
    


async def create_cluster(cluster_data: CreateClusterBase):
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
        }
    )
    result = await handle.result()
    return result

async def delete_cluster( db: Session, cluster_id: str, email: Optional[str] = None, ):
    uniqueId = unique_id()
    client = await connectionWithClient()

    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    cluster_name = cluster.name if cluster else f"Cluster-{cluster_id}"
    userName = email if email else "Unknown User"
 
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
    result = await handle.result()
    return result
    
async def update_cluster(db, cluster_id: str, cluster_data: UpdateClusterBase):
    uniqueId = unique_id()
    client = await connectionWithClient()
    
    cluster_data_dict = jsonable_encoder(cluster_data)
    cluster_name = cluster_data_dict.get('name')
    userName = cluster_data_dict.get('email', "UnknownUser")


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
 
async def get_cluster_details(db: Session, cluster_id: str):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found-")
    return cluster


def create_task_details(task: task_models.Task_config, db: Session):
    try:
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
import requests
import asyncio
import logging
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session
from models.models import Pool, IsCustomeValue, CreateMachineBase,Pool, Machine,CreateClusterBase,Cluster,UpdateClusterBase,UpdateMachineBase
from fastapi import HTTPException
from service.clusterService import get_api_token
from service.proxmoxService import is_valid_ip
from utils.temporal_client import TemporalClientManager
from .temporalResource.workflows import workflows_machine
from service.temporalResource.workflows import  workflows_pool
from models import task_models
from typing import Optional
from service.temporalResource.workflows import workflows_cluster
from db_configuration.config import SessionLocal
import uuid


def unique_id():
    u_id = uuid.uuid4().hex[:8]
    logger.info(f"Generated unique ID - {u_id}")
    return u_id

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

                response = requests.get(
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
    client = await TemporalClientManager.get_temporal_client()
    pool_name = pool_data.get("pool_name", "UnknownPool")
    userName = pool_data.get('email', "UnknownUser")
    pool_ad_domain = pool_data.get('pool_ad_domain', "UnknownDomain")
    pool_ad_password = pool_data.get('pool_ad_password', "UnknownPassword")
    pool_ad_username = pool_data.get('pool_ad_username', "UnknownUsername")
    pool_ad_path = pool_data.get('pool_ad_path', "")
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
    result =  await handle.result()
    
    if isinstance(result, dict) and result.get("status") == "error":
        raise HTTPException(
            status_code=400,
            detail={ 
                "error_type": result.get("error_type"),
                "error": result.get("error")
            }
        )

    if isinstance(result, dict) and "pool" in result:
        pool_id = result["pool"]["id"]
        if pool_ad_domain != "UnknownDomain":
            await client.start_workflow(
                workflows_pool.DomainJoinWorkflow.run,
                args=[pool_id, pool_ad_domain, pool_ad_password, pool_ad_username, pool_ad_path],
                id=f"{pool_name} DomainJoin-{uniqueId}",
                task_queue="domain-join-task-queue",
                search_attributes={
                    "Entity": [pool_name],
                    "Action": ["Domain-Join"],
                    "UserName": [userName]
                },
            )
    return result 

async def update_pool(pool_id:int,email: Optional[str], pool_data: dict,db)->dict:
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    pool_name = pool_data.get("pool_name", "UnknownPool")
    userName = pool_data.get("email", "UnknownUser")
    pool_ad_domain = pool_data.get('pool_ad_domain', "UnknownDomain")
    pool_ad_password = pool_data.get('pool_ad_password', "UnknownPassword")
    pool_ad_username = pool_data.get('pool_ad_username', "UnknownUsername")
    pool_ad_path = pool_data.get('pool_ad_path', "")
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
    if isinstance(result, dict) and "pool" in result:
        pool_id = result["pool"]["id"]
        if pool_ad_domain != "UnknownDomain":
            await client.start_workflow(
                workflows_pool.DomainJoinWorkflow.run,
                args=[pool_id, pool_ad_domain, pool_ad_password, pool_ad_username, pool_ad_path],
                id=f"{pool_name} DomainJoin-{uniqueId}",
                task_queue="domain-join-task-queue",
                search_attributes={
                    "Entity": [pool_name],
                    "Action": ["Domain-Join"],
                    "UserName": [userName]
                },
            )

    return result


 
async def create_machine(machine_data: CreateMachineBase, db: Session = None):
    logger.info(f"Received machine_data for creation: {machine_data}")
    own_db = False
    if db is None:
        db = SessionLocal()
        own_db = True
    try:
        uniqueId = unique_id()
        client = await TemporalClientManager.get_temporal_client()
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
            result = await handle.result()
            logger.info(f"Machine creation workflow completed with result")
            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"An error occurred during machine creation: {str(e)}")
    finally:
        if own_db:
            db.close()



async def delete_machine(machine_identifier: str, email: Optional[str], db: Session):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()

    machine = db.query(Machine).filter(Machine.identifier == machine_identifier).first()
    
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")

    machine_name = machine.name  # Assuming the table has a 'name' column

    machine_name = machine.name  # Assuming the table has a 'name' column
    handle = await client.start_workflow(
        workflows_machine.DeleteMachineWorkflow.run,
        machine_identifier,
        id=f"delete-machines-{uniqueId}",
        task_queue="delete-machine-task-queue",
        search_attributes={
            "Entity": [machine_name],  
            "Action": ["Machine-Deletion"],
            "UserName": [email]
        },
    )
    result = await handle.result()
    return result


async def delete_pool(pool_id: int, email: Optional[str], db: Session) -> dict:
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()

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
    client = await TemporalClientManager.get_temporal_client()
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
    client = await TemporalClientManager.get_temporal_client()
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
    client = await TemporalClientManager.get_temporal_client()
    machine_name = machine_data.name
    userName = machine_data.email
    workflow_id = f"{machine_name} update-machine-{uniqueId}"
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

async def get_machines(db: Session = None):
    """Directly query the database for all machines (Bypasses Temporal for responsiveness)."""
    own_db = False
    if db is None:
        db = SessionLocal()
        own_db = True
    try:
        machines = db.query(Machine).all()
        return jsonable_encoder(machines)
    finally:
        if own_db:
            db.close()

async def update_is_custom_machine(machine_identifier: str, machine_details: IsCustomeValue):
    
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    handle = await client.start_workflow(
        workflows_machine.UpdateIsCustomMachineWorkflow.run,
        args = [machine_identifier, machine_details.dict()],
        id=f"update-is-custom-{uniqueId}",
        task_queue="update-is-custom-task-queue"
    )
    result = await handle.result()
    return result


async def list_of_all_machine_in_pool(pool_id: str, db: Session = None):
    """Directly query the database for machines in a specific pool (Bypasses Temporal for responsiveness)."""
    own_db = False
    if db is None:
        db = SessionLocal()
        own_db = True
    try:
        machines = db.query(Machine).filter(Machine.pool_id == pool_id).all()
        return jsonable_encoder(machines)
    finally:
        if own_db:
            db.close()

#list all the assigned users to a machine
async def list_assigned_users(machine_id: str, db: Session = None):
    """Directly query the database for users assigned to a machine (Bypasses Temporal for responsiveness)."""
    own_db = False
    if db is None:
        db = SessionLocal()
        own_db = True
    try:
        machine = db.query(Machine).filter(Machine.id == machine_id).first()
        if not machine:
            # Fallback to identifier if id not found
            machine = db.query(Machine).filter(Machine.identifier == machine_id).first()
        return machine.users_assigned if machine else []
    finally:
        if own_db:
            db.close()

async def get_pool_details(pool_id: int, db: Session = None):
    """Directly query the database for pool details (Bypasses Temporal for responsiveness)."""
    own_db = False
    if db is None:
        db = SessionLocal()
        own_db = True
    try:
        pool = db.query(Pool).filter(Pool.id == pool_id).first()
        pool_json = jsonable_encoder(pool)
        if pool_json:
            cluster_id_raw = pool_json.get("cluster_id")
            if cluster_id_raw:
                c_id = str(cluster_id_raw).split("_")[-1] if "_" in str(cluster_id_raw) else cluster_id_raw
                try:
                    c_id_int = int(c_id)
                    cluster = db.query(Cluster).filter(Cluster.id == c_id_int).first()
                    if cluster:
                        pool_json["cluster"] = cluster.name
                    else:
                        pool_json["cluster"] = "NA"
                except (ValueError, TypeError):
                    pool_json["cluster"] = "NA"
            else:
                pool_json["cluster"] = "NA"
                
            return {"msg": "Pool Retrived Successfully", "pool": pool_json}
        else:
            return {"msg": "Pool not found"}
    finally:
        if own_db:
            db.close()

async def retrive_pool_data(pool_name: str, db: Session = None):
    """Directly query the database for pool data by name (Bypasses Temporal for responsiveness)."""
    own_db = False
    if db is None:
        db = SessionLocal()
        own_db = True
    try:
        pool = db.query(Pool).filter(Pool.pool_name == pool_name).first()
        pool_json = jsonable_encoder(pool)
        
        if pool_json:
            cluster_id_raw = pool_json.get("cluster_id")
            if cluster_id_raw:
                c_id = str(cluster_id_raw).split("_")[-1] if "_" in str(cluster_id_raw) else cluster_id_raw
                try:
                    c_id_int = int(c_id)
                    cluster = db.query(Cluster).filter(Cluster.id == c_id_int).first()
                    if cluster:
                        pool_json["cluster"] = cluster.name
                    else:
                        pool_json["cluster"] = "NA"
                except (ValueError, TypeError):
                    pool_json["cluster"] = "NA"
            else:
                pool_json["cluster"] = "NA"
                
            return {"msg": f"{pool_name} Pool found ", "pool": pool_json}
        else:
            return {"msg": f"{pool_name} Pool not found"}
    finally:
        if own_db:
            db.close()

async def get_all_pool_names(db: Session = None):
    """Directly query the database for pool names (Bypasses Temporal for responsiveness)."""
    own_db = False
    if db is None:
        db = SessionLocal()
        own_db = True
    try:
        pools = db.query(Pool).all()
        pool_names = [pool_item.pool_name for pool_item in pools]
        return {"msg": "listed all the Pool names successfully", "pool_names": pool_names}
    finally:
        if own_db:
            db.close()

async def get_all_pools(db: Session = None):
    """Directly query the database for all pools (Bypasses Temporal for responsiveness)."""
    own_db = False
    if db is None:
        db = SessionLocal()
        own_db = True
    try:
        pools = db.query(Pool).all()
        pools_json = jsonable_encoder(pools)
        
        # Fetch cluster names for each pool
        for pool_data in pools_json:
            cluster_id_raw = pool_data.get("cluster_id")
            if cluster_id_raw:
                # Handle the case where cluster_id is formatted as "poolid_clusterid"
                c_id = str(cluster_id_raw).split("_")[-1] if "_" in str(cluster_id_raw) else cluster_id_raw
                try:
                    c_id_int = int(c_id)
                    cluster = db.query(Cluster).filter(Cluster.id == c_id_int).first()
                    if cluster:
                        pool_data["cluster"] = cluster.name
                    else:
                        pool_data["cluster"] = "NA"
                except (ValueError, TypeError):
                    pool_data["cluster"] = "NA"
            else:
                pool_data["cluster"] = "NA"
                
        return {"msg": "listed all the Pools successfully", "pools": pools_json}
    finally:
        if own_db:
            db.close()

async def get_machine_details(machine_id: str, db: Session = None):
    """Directly query the database for machine details (Bypasses Temporal for responsiveness)."""
    own_db = False
    if db is None:
        db = SessionLocal()
        own_db = True
    try:
        from models.models import Machine
        machine = db.query(Machine).filter(Machine.identifier == machine_id).first()
        if not machine:
            # Try by vm_id if identifier fails
            machine = db.query(Machine).filter(Machine.vm_id == machine_id).first()
            
        return jsonable_encoder(machine) if machine else {"msg": "Machine not found"}
    finally:
        if own_db:
            db.close()
    


async def create_cluster(cluster_data: CreateClusterBase):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
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
    client = await TemporalClientManager.get_temporal_client()

    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    
    if cluster:
        connected_pools = False
        all_pools = db.query(Pool).all()
        for p in all_pools:
            c_val = str(p.cluster_id) if p.cluster_id else ""
            if c_val == str(cluster_id) or c_val.endswith(f"_{cluster_id}"):
                connected_pools = True
                break
        
        if connected_pools:
            raise HTTPException(
                status_code=400, 
                detail="Cannot delete cluster: there are active Pools and Machines connected to it. Please delete the pools first."
            )

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
    client = await TemporalClientManager.get_temporal_client()
    
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
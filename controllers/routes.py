from utils.temporal_client import TemporalClientManager
import json
from fastapi import APIRouter, HTTPException, Depends, Request 
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session
from db_configuration.config import get_db
from keycloak_configration import keycloak_config as key_config
from models import models
from typing import Any, List
from service import controllers as controller
from fastapi import  HTTPException
from temporalio.client import Client
import os
import pytz
from models.API_Response_model import APIResponse
from utils import response_format

router = APIRouter(prefix="/v1")

TEMPORAL_SERVER = os.getenv("TEMPORAL_SERVER")

@router.post('/get_proxmox_storages', response_model=APIResponse)
async def get_proxmox_storages(payload: models.NodeRequest, db: Session = Depends(get_db)):
    res = await controller.get_proxmox_storages(payload, db)
    return response_format.success_response(200, "Proxmox Storages retrieved successfully.", res)

@router.post('/create_pool', response_model=APIResponse)
async def create_pool_endpoint(pool_data: models.CreatePoolBase, db: Session = Depends(get_db)):
    pool_data_dict = pool_data.dict()
    res = await controller.create_pool(pool_data_dict,db)
    return response_format.success_response(200, "Pool created successfully.", res)

@router.get('/retrive_pool/{pool_name}', response_model=APIResponse)
async def retrive_machihe(pool_name:str,db: Session = Depends(get_db)):
    res = await controller.retrive_pool_data(pool_name ,db)
    return response_format.success_response(200, "Pool retrieved successfully.", res)

@router.put('/vdi_pools/update_pool/{pool_id}', response_model=APIResponse)
async def update_pool_route(pool_id: str, request: Request, db: Session = Depends(get_db)):
    pool_id_int = int(pool_id)
 
    body = await request.json()
    email = body.get("email", "Unknown User")
    pool_data = body
    result = await controller.update_pool(pool_id_int, email, pool_data, db)
    return response_format.success_response (200, "Pool updated successfully.", result)


@router.delete('/delete_pool/{pool_id}', response_model=APIResponse)
async def delete_pool_route(pool_id: str, request: Request, db: Session = Depends(get_db)):
    pool_id_int = int(pool_id)
    raw_body = await request.body()
    if not raw_body:
        raise HTTPException(status_code=400, detail="Empty request body")
    body = json.loads(raw_body)
    email = body.get("email")

    result = await controller.delete_pool(pool_id_int, email, db)
    return response_format.success_response(200, "Pool deleted successfully.", result)



@router.get('/pools_names', response_model=APIResponse)
async def list_pools_names(db: Session = Depends(get_db)):
    res = await controller.get_all_pool_names(db)
    return response_format.success_response(200, "Pool names retrieved successfully.", res)

# Route to list all the pools
@router.get('/vdi_pools/pools', response_model=APIResponse)
async def list_pools():
    pools = await controller.get_all_pools()
    return response_format.success_response(200, "Pools retrieved successfully.", pools.get("pools", []))

#get pool details based on id
@router.get("/vdi_pools/pool/{pool_id}", response_model=APIResponse)
async def get_pool_details_route(pool_id: int):
    data = await controller.get_pool_details(pool_id)
    return response_format.success_response(200, "Pool details retrieved successfully.", data)


@router.post('/vdi_pools/create_machine',response_model=APIResponse)
async def create_machine_endpoint(machine_data: models.CreateMachineBase):
    data = await controller.create_machine( machine_data)
    return response_format.success_response(200, "Machine created successfully.", data)

@router.delete("/vdi_pools/delete_machine/{machine_id}", response_model=APIResponse)
async def delete_machine_by_id(machine_id: str, email: str, db: Session = Depends(get_db)):
    if not email:
        raise HTTPException(status_code=400, detail="Email is required")

    data = await controller.delete_machine(machine_id, email, db)
    return response_format.success_response(200, "Machine deleted successfully.", data)

#Route to update a machine/connection
@router.put('/vdi_pools/update_machine/{machine_identifier}', response_model=APIResponse)
async def update_machine_endpoint(machine_identifier: str, machine_data: models.UpdateMachineBase): 
    data = await controller.update_machine( machine_identifier,machine_data)
    return response_format.success_response(200, "Machine updated successfully.", data)


#Route to list all the machines available
@router.get("/machines", response_model=APIResponse)
async def list_machines():
    res = await controller.get_machines()
    return response_format.success_response(200, "Machines retrieved successfully.", res)


@router.put("/machines/{machine_identifier}/is_custom_machine/", response_model=APIResponse)
async def update_machine_isCustom_field(machine_identifier: str, machine_data: models.IsCustomeValue):
    res = await controller.update_is_custom_machine( machine_identifier, machine_data)
    return response_format.success_response(200, "Machine is_custom field updated successfully.", res)


#route to assign user to a machine
@router.post("/add_user_to_machine/{machine_id}/{user_id}", response_model=APIResponse)
async def add_user(machine_id: str, user_id: str):
    res = await controller.add_user_to_machine( machine_id, user_id)
    return response_format.success_response(200, "User added to machine successfully.", res)


#delete user from the machine
@router.delete("/delete_user_from_machine/{machine_id}/{user_id}", response_model=APIResponse)
async def delete_user(machine_id: str, user_id: str):
    res = await controller.delete_user_from_machine( machine_id, user_id)
    return response_format.success_response(200, "User deleted from machine successfully.", res)


#get all the machines available in a pool
@router.get("/pool/machines/{pool_id}", response_model=APIResponse)
async def list_machines_in_pool(pool_id: str):
    data = await controller.list_of_all_machine_in_pool(pool_id)
    return response_format.success_response(200, "Successfully retrieved machines in pool", data)



#get all the users assigned to a machine
@router.get("/machine/users/{machine_id}", response_model=APIResponse)
async def list_assigned_users_route(machine_id: str):
    data = await controller.list_assigned_users( machine_id)
    return response_format.success_response(200, "Successfully retrieved assigned users", data)


#get machine details by id
@router.get("/machine/{machine_id}", response_model=APIResponse)
async def get_machine_details_route(machine_id: str):
    data = await controller.get_machine_details( machine_id)
    return response_format.success_response(200, "Successfully retrieved machine details", data)


#----------------------------- done ------------------------------------

# Create cluster
@router.post('/create_cluster', response_model=APIResponse)
async def create_cluster_endpoint(cluster_data: models.CreateClusterBase):
    cluster_data_dict = cluster_data.dict()
    res = await controller.create_cluster(cluster_data_dict)
    return response_format.success_response(200, "Cluster created successfully.", res)

#Route to list all the clusters
@router.get('/cluster/clusters', response_model=APIResponse[Any])
async def list_clusters(db: Session = Depends(get_db)):
    clusters = db.query(models.Cluster).all()
    clusters_json = jsonable_encoder(clusters)
    return response_format.success_response(200, "Clusters retrieved successfully.", clusters_json)

#get pool details based on id
@router.get("/cluster/{cluster_id}", response_model=APIResponse)
async def get_cluster_details_endpoint(cluster_id: str, db: Session = Depends(get_db)):
    res = await controller.get_cluster_details(db, cluster_id)
    return response_format.success_response(200, "Cluster details retrieved successfully.", jsonable_encoder(res))


@router.delete('/cluster/delete_cluster/{cluster_id}', response_model=APIResponse)
async def delete_cluster_route(cluster_id: str, request: Request, db: Session = Depends(get_db)):
    # cluster_id_int = int(cluster_id)
    raw_body = await request.body()
    if not raw_body:
        raise HTTPException(status_code=400, detail="Empty request body")
    body = json.loads(raw_body)
    email = body.get("email")
    res = await controller.delete_cluster(db, cluster_id, email)
    return response_format.success_response(200, "Cluster deleted successfully.", res)

#Route to update a cluster
@router.put('/cluster/update_cluster/{cluster_id}', response_model=APIResponse)
async def update_cluster_endpoint(cluster_id: str, cluster_data: models.UpdateClusterBase, db: Session = Depends(get_db)):
    res = await controller.update_cluster(db, cluster_id, cluster_data)
    return response_format.success_response(200, "Cluster updated successfully.", res)

# Ldap connection in keycloak
@router.post('/domain/ad_ldap_connection' ,response_model=APIResponse)
async def ldap_Configuration(ldap_data:models.LDAPCredential):
    res =  await key_config.configuration_ad(ldap_data)
    if isinstance(res, dict) and res.get("msg") == "Error occurred":
        return response_format.error_response(500, res.get("error", "LDAP configuration failed"), res)
    return response_format.success_response(200, "LDAP configuration successful.", res)

#Route to list all the LDAPs configured
@router.get('/ldaps', response_model=APIResponse)
async def get_LDAPs_from_keycloak_endpoint():
    res = await key_config.get_LDAPs_from_keycloak()
    if isinstance(res, dict) and res.get("msg") == "Error occurred":
        return response_format.error_response(500, res.get("error", "Failed to retrieve LDAP configurations"), res)
    return response_format.success_response(200, "LDAP configurations retrieved successfully.", res)

#Route to test LDAP connection
@router.post('/domain/test_ldap_connection', response_model=APIResponse)
async def test_ldap_connection_endpoint(ldap_data:models.LDAP_test_connection_model):
    res = await key_config.test_ldap_connection(ldap_data)
    return res #--------------don't change this format it may break the workflow and response format

#Route to test LDAP authentication
@router.post('/domain/test_ldap_authentication', response_model=APIResponse)
async def test_ldap_authentication_endpoint(ldap_data:models.LDAP_test_connection_model):
    res = await key_config.test_ldap_authentication(ldap_data)
    return res #--------------don't change this format it may break the workflow and response format

# Route to delete LDAP configuration
@router.delete('/domain/delete_ldap_configuration/{ldap_id}', response_model=APIResponse)
async def delete_ldap_config_endpoint(ldap_id: str):
    res = await key_config.delete_ldap_config(ldap_id)
    if isinstance(res, dict) and res.get("msg") == "Error occurred":
        return response_format.error_response(500, res.get("error", "Failed to delete LDAP configuration"), res)
    return response_format.success_response(200, "LDAP configuration deleted successfully.", res)

# get LDAP details by id
@router.get('/domain/get_ldap_by_id/{ldap_id}', response_model=APIResponse)
async def get_LDAP_by_id_endpoint(ldap_id: str):
    data = await key_config.get_LDAP_by_id(ldap_id)
    if isinstance(data, dict) and data.get("msg") == "Error occurred":
        return response_format.error_response(500, data.get("error", "Failed to retrieve LDAP configuration"), data)
    return response_format.success_response(200, "LDAP configuration retrieved successfully.", data)

# update_ldap_config
@router.put('/domain/update_ldap_config/{ldap_id}')
async def update_ldap_config_endpoint(ldap_data: models.LDAPCredential,ldap_id: str):
    ldap_data_dict = ldap_data.dict()
    data = await key_config.update_ldap_config(ldap_data_dict,ldap_id)
    if isinstance(data, dict) and data.get("msg") == "Error occurred":
         return response_format.error_response(500, data.get("error", "Failed to update LDAP configuration"), data)
    return response_format.success_response(200, "LDAP configuration updated successfully.", data)

@router.get('/domain/sync_users/{ldap_id}', response_model=APIResponse)
async def sync_ad_user_from_keycloak(ldap_id: str):
    res = await key_config.sync_user_from_keycloak(ldap_id)
    return response_format.success_response(200, "User synced successfully.", res)

@router.get('/domain/sync_changed_users/{ldap_id}', response_model=APIResponse)
async def sync_changed_users_from_keycloak_endpoint(ldap_id: str):
    res = await key_config.sync_changed_users_from_keycloak(ldap_id)
    return response_format.success_response(200, "Changed users synced successfully.", res)

@router.get('/domain/unlink_users/{ldap_id}', response_model=APIResponse)
async def unlink_users_from_keycloak_endpoint(ldap_id: str):
    res = await key_config.unlink_users_from_keycloak(ldap_id)
    return response_format.success_response(200, "Users unlinked successfully.", res)

@router.get('/domain/remove_imported_users/{ldap_id}', response_model=APIResponse)
async def remove_imported_users_from_keycloak_endpoint(ldap_id: str):
    res = await key_config.remove_imported_users_from_keycloak(ldap_id)
    return response_format.success_response(200, "Imported users removed successfully.", res)

@router.put('/totp/enable-disable-totp-browser/{value}')
def enable_browser_authflow(value):
    if value == 'true':
        value=True
    else:
        value=False
    return key_config.set_otp_for_browser_auth(value)

@router.put('/totp/enable-disable-guac/{value}')
def enable_browser_guacamole_authflow(value):
    if value == 'true':
        value=True
    else:
        value=False
    return  key_config.set_otp_for_guacamole_browser(value)

@router.get('/totp/get-enable-disable-totp-browser')
def  get_enable_browser_authflow():
    return  key_config.get_Auth_flow_Value_browser()

@router.get('/totp/get-enable-disable-guac')
def get_enable_browser_guacamole_authflow():
    return  key_config.get_Auth_flow_Value_guacamole_browser()


@router.get("/workflows")
async def list_workflows():
    """Fetch and return active workflows from Temporal."""
    client = await TemporalClientManager.get_temporal_client()
    time_format = "%Y-%m-%dT%H:%M:%S.%fZ"  
    
    # Set IST timezone (Indian Standard Time)
    import pytz
    IST_TZ = pytz.timezone("Asia/Kolkata")


    workflows = []
    # Using a Temporal Query to filter workflows on the server side.
    # This significantly reduces the load on the persistence layer by not fetching 
    # utility workflows that don't have the 'Entity' attribute.
    query = "Entity != ''"
    
    try:
        async for wf in client.list_workflows(query=query):
            # Convert UTC to IST (Indian Standard Time)
            start_time_ist = wf.start_time.astimezone(IST_TZ).strftime(time_format)[:-3]
            close_time_ist = wf.close_time.astimezone(IST_TZ).strftime(time_format)[:-3] if wf.close_time else "Ongoing"

            execution_time = None
            if wf.close_time:
                delta_ms = (wf.close_time - wf.start_time).total_seconds() * 1000
                if delta_ms < 1000:
                    execution_time = f"{int(delta_ms)}ms"
                else:
                    execution_time = f"{delta_ms / 1000:.2f}s"
            else:
                execution_time = None

            entity = wf.search_attributes.get("Entity", ["UnknownEntity"])[0]
            action = wf.search_attributes.get("Action", ["UnknownAction"])[0]
            UserName = wf.search_attributes.get("UserName", ["UnknownUserName"])[0]

            if entity == "UnknownEntity":
                continue

            workflows.append({
                "workflow_id": wf.id,
                "run_id": wf.run_id,
                "workflow_type": wf.workflow_type,
                "task_name": entity,
                "action": action,
                "start_time": start_time_ist,
                "close_time": close_time_ist,
                "execution_time": execution_time,
                "status": wf.status.name,
                "UserName": UserName
            })
    except Exception as e:
        print(f"Error listing workflows with query: {e}")
        # Fallback to listing all but with a strict limit to avoid hitting QPS again
        async for wf in client.list_workflows():
            if len(workflows) >= 50:
                break
                
            entity = wf.search_attributes.get("Entity", ["UnknownEntity"])[0]
            if entity == "UnknownEntity":
                continue
                
            start_time_ist = wf.start_time.astimezone(IST_TZ).strftime(time_format)[:-3]
            close_time_ist = wf.close_time.astimezone(IST_TZ).strftime(time_format)[:-3] if wf.close_time else "Ongoing"
            
            workflows.append({
                "workflow_id": wf.id,
                "run_id": wf.run_id,
                "workflow_type": wf.workflow_type,
                "task_name": entity,
                "action": wf.search_attributes.get("Action", ["UnknownAction"])[0],
                "start_time": start_time_ist,
                "close_time": close_time_ist,
                "status": wf.status.name,
                "UserName": wf.search_attributes.get("UserName", ["UnknownUserName"])[0]
            })
    
    return response_format.success_response(200, "Workflows retrieved successfully.", workflows)




from service.pollingStatus import get_workflow_failure_message_simple
@router.get("/workflow-status/{workflow_id}")
async def workflow_status(workflow_id: str):
    return await get_workflow_failure_message_simple(workflow_id)


@router.post("/pools/update-status")
def update_pool_status(
    pool_ids: List[int],
    status: str,
    db: Session = Depends(get_db)
):
    if status not in ["enabled", "disabled"]:
        raise HTTPException(status_code=400, detail="Invalid status value")
    pools = db.query(models.Pool).filter(models.Pool.id.in_(pool_ids)).all()
    if not pools:
        raise HTTPException(status_code=404, detail="Pools not found")
    for pool in pools:
        pool.pool_status = status
    db.commit()
    return {"message": "Status updated", "updated_pools": [pool.id for pool in pools]}

@router.post('/add-tasks-data')
async def add_data(task,db: Session = Depends(get_db)):
    return controller.create_task_details(task,db)
 
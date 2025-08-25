from datetime import datetime
import json
from fastapi import APIRouter, HTTPException, Depends, Query, Request # type: ignore
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session # type: ignore
from db_configuration.config import SessionLocal
from keycloak_configration import keycloak_config as key_config
from models import models,task_models
from typing import Optional, Union,List
from service import controllers as controller
from service.temporalResource import get_status_recent_work
from fastapi import FastAPI, HTTPException
from temporalio.client import Client
from service.temporalResource.activity import activities_pool
from service.pollingStatus import update_workflow_status

import os
import pytz

router = APIRouter()

# Function to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

#Route to create a pool
# class ClonePayload(BaseModel):
#     cluster_id: str
#     node: List[str]
#     template_vm_id: str
#     name_template: str
#     ip_pool_names: List[str] = []
#     count: int
# class CreatePoolRequest(BaseModel):
#     pool_data: models.CreatePoolBase
#     clone_payload: Optional[activities_pool.ClonePayload] = None


TEMPORAL_SERVER = os.getenv("TEMPORAL_SERVER")

#-----------------------------------for poojitha ---------------------------------
 
#Route to create a pool
@router.post( '/create_pool' )
async def create_pool_endpoint(pool_data: models.CreatePoolBase, db: Session = Depends(get_db)):
    pool_data_dict = pool_data.dict()
    # print(f"pool_data type in routes: {type(pool_data_dict)}")
    return await controller.create_pool(pool_data_dict,db)
# @router.post('/create_pool')
# async def create_pool_endpoint(request:activities_pool.CreatePoolRequest, db: Session = Depends(get_db)):
#     combined_payload = {**request.pool_data.dict()}
#     if request.clone_payload:
#         combined_payload.update(request.clone_payload.dict())
 
#     return await controller.create_pool(combined_payload, db)

@router.get('/retrive_pool/{pool_name}')
async def retrive_machihe(pool_name:str,db: Session = Depends(get_db)):
    print(f'pool_name type: {type(pool_name)}')
    return await controller.retrive_pool_data(pool_name ,db)


@router.put('/update_pool/{pool_id}')
async def update_pool_route(pool_id: str, request: Request, db: Session = Depends(get_db)):
    pool_id_int = int(pool_id)
 
    try:
        body = await request.json()
        email = body.get("email", "Unknown User")
        pool_data = body
    except Exception as e:
        return JSONResponse(status_code=400, content={"msg": f"Invalid request body: {str(e)}", "error": True})
 
    try:
        result = await controller.update_pool(pool_id_int, email, pool_data, db)
        return result
    except HTTPException as e:
        # Always return the error detail as 'msg'
        return JSONResponse(status_code=e.status_code, content={"msg": str(e.detail), "error": True})
    except Exception as e:
        # Other errors
        return JSONResponse(status_code=500, content={"msg": f"Pool update failed: {str(e)}", "error": True})


@router.delete('/delete_pool/{pool_id}')
async def delete_pool_route(pool_id: str, request: Request, db: Session = Depends(get_db)):
    pool_id_int = int(pool_id)
    # email = request.headers.get("email", None)

    raw_body = await request.body()  # Get raw request body
    print("Received raw body:", raw_body.decode("utf-8"))  # Debugging

    if not raw_body:
        raise HTTPException(status_code=400, detail="Empty request body")

    body = json.loads(raw_body)  # Parse JSON manually
    email = body.get("email")

    return await controller.delete_pool(pool_id_int, email, db)


@router.delete("/delete_machine/{machine_id}")
async def delete_machine_by_id(machine_id: str, request: Request, db: Session = Depends(get_db)):
    try:
        raw_body = await request.body()  # Get raw request body
        print("Received raw body:", raw_body.decode("utf-8"))  # Debugging

        if not raw_body:
            raise HTTPException(status_code=400, detail="Empty request body")

        body = json.loads(raw_body)  # Parse JSON manually
        email = body.get("email")

        if not email:
            raise HTTPException(status_code=400, detail="Email is required")
    
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON format")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error processing request: {str(e)}")

    return await controller.delete_machine(machine_id, email, db)

#Route to list all the pool names
@router.get('/pools_names')
async def list_pools_names(db: Session = Depends(get_db)):
    return await controller.get_all_pool_names(db)

# Route to list all the pools
@router.get('/pools')
async def list_pools(db: Session = Depends(get_db)):
    # pools=db.query(Pool).all()
    return await controller.get_all_pools(db)

#get pool details based on id
@router.get("/pool/{pool_id}")
async def get_pool_details_route(pool_id: int, db: Session = Depends(get_db)):
    print(f"in pool details---POOL_ID Type: {type(pool_id)}")
    return await controller.get_pool_details(pool_id,db)

#Route to create a machine

# @router.get("/machines/{machine_id}/refresh-workflow-status")
# async def refresh_workflow_status(machine_id: int, db: Session = Depends(get_db)):
#     machine = db.query(models.Machine).filter(models.Machine.id == machine_id).first()
#     if not machine or not machine.workflowId:
#         raise HTTPException(status_code=404, detail="Machine not found or missing workflowId")
#     # Update status in DB and return it
#     status = await update_machine_workflow_status(db, machine.workflowId)
#     return {"machine_id": machine.id, "workflow_status": status}



# @router.post("/machines/{machine_id}/workflow_status/fetch")
# async def api_update_status_from_temporal(
#     machine_id: int,
#     wfid: str,
#     db: Session = Depends(get_db)
# ):
#     try:
#         result = await update_status_from_temporal(db, machine_id, wfid)
#         return result
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
    
# @router.get("/machines/{machine_id}/workflow_status")
# def get_machine_workflow_status(
#     machine_id: int,
#     db: Session = Depends(get_db)
# ):
#     """
#     Get workflow status mapping and overall machine status from the database.
#     """
#     machine = db.query(models.Machine).filter(models.Machine.id == machine_id).first()
#     if not machine:
#         raise HTTPException(status_code=404, detail=f"Machine {machine_id} not found")
#     return {
#         "workflow_status": machine.workflow_status,
#         "machine_status": machine.status,
#         "error_message": machine.error_message
#     }
 
@router.post('/create_machine')
async def create_machine_endpoint(machine_data: models.CreateMachineBase):
    # print(f"machine data {machine_data}")
    return await controller.create_machine( machine_data)

#Route to delete a machine
# @router.delete("/delete_machine/{machine_id}")
# async def delete_machine_by_id(machine_id: str, db: Session = Depends(get_db)):
#     return await controller.delete_machine( machine_id,db)

@router.delete("/delete_machine/{machine_id}")
async def delete_machine_by_id(machine_id: str, request: Request, db: Session = Depends(get_db)):
    try:
        body = await request.json()  # Extract JSON body
        email = body.get("email")  # Get email from request body

        if not email:
            raise HTTPException(status_code=400, detail="Email is required")
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An error occurred: {str(e)}")

    return await controller.delete_machine(machine_id, email, db)

#Route to update a machine/connection
@router.put('/update_machine/{machine_identifier}')
async def update_machine_endpoint(machine_identifier: str, machine_data: models.UpdateMachineBase): 
    return await controller.update_machine( machine_identifier,machine_data)


#Route to list all the machines available
@router.get("/machines")
async def list_machines():
    # machines = db.query(Machine).all()
    # return machines
    return await controller.get_machines()


@router.put("/machines/{machine_identifier}/is_custom_machine/")
async def update_machine_isCustom_field(machine_identifier: str, machine_data: models.IsCustomeValue):
    # print(machine_identifier, machine_data)
    return await controller.update_is_custom_machine( machine_identifier, machine_data)


#route to assign user to a machine
@router.put("/add_user_to_machine/{machine_id}/{user_id}")
async def add_user(machine_id: str, user_id: str):
    # print("add user",machine_id,user_id)
    return await controller.add_user_to_machine( machine_id, user_id)


#delete user from the machine
@router.delete("/delete_user_from_machine/{machine_id}/{user_id}")
async def delete_user(machine_id: str, user_id: str):
    # print("Deleting user", user_id,machine_id)
    return await controller.delete_user_from_machine( machine_id, user_id)


#get all the machines available in a pool
@router.get("/pool/machines/{pool_id}")
async def list_machines_in_pool(pool_id: str):
    try:
        # Query the database to get machines in the specified pool
        # machines = db.query(Machine).filter(Machine.pool_id == pool_id).all()
        # return machines
        return await controller.list_of_all_machine_in_pool(pool_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


#get all the users assigned to a machine
@router.get("/machine/users/{machine_id}")
async def list_assigned_users_route(machine_id: str):
    return await controller.list_assigned_users( machine_id)


#get machine details by id
@router.get("/machine/{machine_id}")
async def get_machine_details_route(machine_id: str):
    return await controller.get_machine_details( machine_id)

# @router.get('/asignment')
# async def get_machine_details():
#       return await assign_machine()  

#----------------------------- done ------------------------------------

# Create cluster
@router.post('/create_cluster')
async def create_cluster_endpoint(pool_data: models.CreateClusterBase, db: Session = Depends(get_db)):
    # pool_data_dict = pool_data.dict()
    print(f"Received cluster data: {pool_data}")
    pool_data_dict = pool_data.dict()
    print(type(pool_data_dict))
    return await controller.create_cluster(pool_data_dict,db)

#Route to list all the clusters
@router.get('/clusters')
async def list_pools(db: Session = Depends(get_db)):
    clusters=db.query(models.Cluster).all()
    return clusters

#get pool details based on id
@router.get("/cluster/{cluster_id}")
async def get_cluster_details_endpoint(cluster_id: str, db: Session = Depends(get_db)):
    return await controller.get_cluster_details(db, cluster_id)

# Route to delete a cluster
# @router.delete('/delete_cluster/{cluster_id}')
# async def delete_cluster_route(cluster_id: str, db: Session = Depends(get_db)):
#     return await controller.delete_cluster(db, cluster_id)


@router.delete('/delete_cluster/{cluster_id}')
async def delete_cluster_route(cluster_id: str, request: Request, db: Session = Depends(get_db)):
    # cluster_id_int = int(cluster_id)
    

    raw_body = await request.body()
    print("Received raw body:", raw_body.decode("utf-8"))

    if not raw_body:
        raise HTTPException(status_code=400, detail="Empty request body")

    try:
        body = json.loads(raw_body)
        email = body.get("email")
    except Exception as e:
        raise HTTPException(status_code=400, detail="Malformed JSON body")

    return await controller.delete_cluster(db, cluster_id, email)

#Route to update a cluster
@router.put('/update_cluster/{cluster_id}')
async def update_cluster_endpoint(cluster_id: str, cluster_data: models.UpdateClusterBase, db: Session = Depends(get_db)):
    print("update cluster")
    return await controller.update_cluster(db, cluster_id, cluster_data)

# Ldap connection in keycloak
@router.post('/ad_ldap_connection')
async def ldap_Configuration(ldap_data:models.LDAPCredential):
    # print(f'ldap-data-----{type(ldap_data)}')
    # ldap_data_dict = ldap_data.dict()
    # print(f'ldap-data-dict-----{type(ldap_data_dict)}')
    return await key_config.configuration_ad(ldap_data)

#Route to list all the LDAPs configured
@router.get('/ldaps')
async def get_LDAPs_from_keycloak_endpoint():
    res = key_config.get_LDAPs_from_keycloak()
    return await res

#Route to test LDAP connection
@router.post('/test_ldap_connection')
async def test_ldap_connection_endpoind(ldap_data:models.LDAP_test_connection_model):
    # print(ldap_data)
    return await key_config.test_ldap_connection(ldap_data)

#Route to test LDAP authenticaion
@router.post('/test_ldap_authenticaion')
async def test_ldap_authenticaion_endpoind(ldap_data:models.LDAP_test_connection_model):
    print(ldap_data)
    return await key_config.test_ldap_authentication(ldap_data)

# http://172.16.0.102:8443/admin/realms/gucamole/components/12ab6ce9-fb33-4351-8330-1ad765f8e265

# Route to delete LDAP configuration
@router.delete('/delete_ldap_configuration/{ldap_id}')
async def delete_ldap_config_endpoint(ldap_id: str):
    print(ldap_id)
    print(f'ldap_id type---{type(ldap_id)}')
    return await key_config.delete_ldap_config(ldap_id)

# get LDAP details by id
@router.get('/get_ldap_by_id/{ldap_id}')
async def get_LDAP_by_id_endpoint(ldap_id: str):
    return await key_config.get_LDAP_by_id(ldap_id)

# update_ldap_config
@router.put('/update_ldap_config/{ldap_id}')
async def update_ldap_config_endpoint(ldap_data: models.LDAPCredential,ldap_id: str):
    ldap_data_dict = ldap_data.dict()
    # print(f'ldap_data type: {type(ldap_data_dict)}')
    # print(ldap_data)
    return await key_config.update_ldap_config(ldap_data_dict,ldap_id)

@router.get('/sync_users/{ldap_id}')
async def sync_ad_user_from_keycloak(ldap_id: str):
    return await key_config.sync_user_from_keycloak(ldap_id)

@router.get('/sync_changed_users/{ldap_id}')
async def sync_changed_users_from_keycloak_endpoint(ldap_id: str):
    return await key_config.sync_changed_users_from_keycloak(ldap_id)

@router.get('/unlink_users/{ldap_id}')
async def unlink_users_from_keycloak_endpoint(ldap_id: str):
    return await key_config.unlink_users_from_keycloak(ldap_id)

@router.get('/remove_imported_users/{ldap_id}')
async def remove_imported_users_from_keycloak_endpoint(ldap_id: str):
    return await key_config.remove_imported_users_from_keycloak(ldap_id)

# routes for  enable or disable totp 
@router.put('/enable-disable-totp-browser/{value}')
def enable_browser_authflow(value):
    print(value)
    if value == 'true':
        value=True
    else:
        value=False
    # print(ldap_data)
    return key_config.set_otp_for_browser_auth(value)

@router.put('/enable-disable-guac/{value}')
def enable_browser_guacamole_authflow(value):
    print(value)
    if value == 'true':
        value=True
    else:
        value=False
    # print(ldap_data)
    return  key_config.set_otp_for_guacamole_browser(value)

@router.get('/get-enable-disable-totp-browser')
def  get_enable_browser_authflow():
    # print(ldap_data)
    return  key_config.get_Auth_flow_Value_browser()

@router.get('/get-enable-disable-guac')
def get_enable_browser_guacamole_authflow():
    # print(ldap_data)
    return  key_config.get_Auth_flow_Value_guacamole_browser()



# @router.get('/get-status-recent-task/{workflow_id}')
# async def get_status(workflow_id:str):
#     return await get_status_recent_work.get_workflow_status(workflow_id)

# @router.post('/add-tasks-data')
# async def add_data(task:task_models.Task_config ,db: Session = Depends(get_db)):
#     return controller.create_task_details(task,db)







# TEMPORAL_URL = "http://172.16.0.108:8080/api/v1/namespaces/default/workflows"

# @router.get("/workflows")
# async def get_workflows():
#     async with httpx.AsyncClient() as client:
#         try:
#             response = await client.get(TEMPORAL_URL)
#             response.raise_for_status()  # Raise error for bad status codes
#             data = response.json()
#             return {"workflows": data.get("executions", [])}
#         except httpx.HTTPStatusError as e:
#             return {"error": f"HTTP error: {e}"}
#         except httpx.RequestError as e:
#             return {"error": f"Request failed: {e}"}

 
  # Update if different

async def get_temporal_client():
    """Establish connection with the Temporal server."""
    try:
        client = await Client.connect(TEMPORAL_SERVER)
        return client
    except Exception as e:
        print(f"Connection Refused to Temporal server: {e}")
        raise HTTPException(status_code=503, detail=f"Cannot connect to Temporal server: {str(e)}")

# Set IST timezone (Indian Standard Time)
IST_TZ = pytz.timezone("Asia/Kolkata")

@router.get("/workflows")
async def list_workflows():
    """Fetch and return active workflows from Temporal."""
    client = await get_temporal_client()
    time_format = "%Y-%m-%dT%H:%M:%S.%fZ"  


    workflows = []
    async for wf in client.list_workflows():

        # print("Full Search Attributes:", wf.search_attributes)

        # Convert UTC to IST (Indian Standard Time)
        start_time_ist = wf.start_time.astimezone(IST_TZ).strftime(time_format)[:-3]
        close_time_ist = wf.close_time.astimezone(IST_TZ).strftime(time_format)[:-3] if wf.close_time else "Ongoing"

        execution_time = None
        if wf.close_time:
            execution_time = f"{round((wf.close_time - wf.start_time).total_seconds() * 1000)}ms"

        entity = wf.search_attributes.get("Entity", ["UnknownEntity"])[0]
        action = wf.search_attributes.get("Action", ["UnknownAction"])[0]
        UserName = wf.search_attributes.get("UserName", ["UnknownUserName"])[0]
        
      
        
        if entity == "UnknownEntity":
            continue 


        workflows.append({
            "workflow_id": wf.id,
            "run_id": wf.run_id,
            "workflow_type": wf.workflow_type,
            "task_name": entity,  # Add task_name in Backend
            "action":action,
            "start_time": start_time_ist,  
            "close_time": close_time_ist,  
            "execution_time": execution_time,
            "status": wf.status.name,
            "UserName":UserName
        })
    
    return {"workflows": workflows}



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
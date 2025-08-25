 
from typing import List
from fastapi import BackgroundTasks, Depends, APIRouter, HTTPException, Request
from fastapi.params import Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import requests
from sqlalchemy.orm import Session
 
from db_configuration.config import get_db
import service.proxmoxService as service
from service.proxmoxService import get_cluster_nodes, update_cluster_nodes,migrate_bucket_all_data,update_metric_server_token
from service.controllers import get_cluster_details
from service.clusterService import get_influxdb_metric_server,create_and_get_metric_server,delete_influxdb_metric_server,can_delete_metric_server,get_metric_server_from_db
from models.models import Cluster, Machine,Pool
from service.clusterService import save_metric_server_to_db
from models.proxmox_model import MetricServer
from models.proxmox_model import MigrateRequest, VMPowerRequest
from temporalio.client import Client
from service.gucamoleService import connectionWithClient
from service.proxmoxService import reboot_vm_proxmox, shutdown_vm_proxmox, start_vm_proxmox, stop_vm_proxmox, vm_rebuild,get_all_vm_details

import os
proxmox_router = APIRouter()
class NodesRequest(BaseModel):
    nodes: List[str]
 
class NameRequest(BaseModel):
    template: str
 
 
class CloneRequest(BaseModel):
    node: List[str]
    template_vm_id: int
    name_template: str  
    count: int
    cluster_id: str
    # storage: str = "local-lvm"
 
 
 
@proxmox_router.get("/get_all_cluster_vms")
def get_nodes(db: Session = Depends(get_db)):
    cluster_vms = service.get_all_cluster_vms(db)    
    return cluster_vms
 
@proxmox_router.get("/get_templates")
async def get_templates_for_nodes(db: Session = Depends(get_db),cluster_id: str = None):
    cluster_data = await get_cluster_details(db, cluster_id)
        # If no cluster_id is provided, get all templates
    templates = service.get_templates(db,cluster_data)
    return templates
 
@proxmox_router.post("/clone-vms")
async def clone_vms_endpoint(payload: CloneRequest, db: Session = Depends(get_db)):
    # try:
    cluster_id=payload.cluster_id
    cluster_data = await get_cluster_details(db, cluster_id)
    result = service.clone_vm(
        db=db,
        nodes=payload.node,
        template_vm_id=payload.template_vm_id,
        name_template=payload.name_template,
        count=payload.count,
        cluster_data=cluster_data
    )
    return result
 
 
 
@proxmox_router.post("/generate-name")
def generate_name(request: NameRequest,existing_names):
    try:
        name = service.generate_machine_name(request.template)
        return {"name": name}
    except ValueError as e:
        return {"error": str(e)}
   
 
@proxmox_router.put("/update-nodes")
def update_nodes_route(db: Session = Depends(get_db)):
    updated_clusters = update_cluster_nodes(db)
    return {"updated_clusters": updated_clusters}

@proxmox_router.get('/get-cluster-nodes')
async def get_cluster_nodes_endpoint(cluster_id: str, db: Session = Depends(get_db)):
    cluster_data = await get_cluster_details(db, cluster_id)
    if not cluster_data:
        raise HTTPException(status_code=404, detail="Cluster not found")
    return get_cluster_nodes(cluster_data)





from fastapi import Query

@proxmox_router.get('/get_influxdb_metric_server')
async def get_influxdb_metric_server_endpoint(
    cluster_id: str,
    monitoring: bool = Query(False),
    db: Session = Depends(get_db)
):
    cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster_data:
        raise HTTPException(status_code=404, detail="Cluster not found")
    influxdb_metric_server = get_influxdb_metric_server(cluster_data)
    # Find current value to preserve it
    ms = db.query(MetricServer).filter(MetricServer.cluster_id == cluster_id).first()
    current_is_custom = ms.is_custom_integration if ms else False
    # Only update relevant fields, always pass current is_custom_integration
    if influxdb_metric_server and not influxdb_metric_server.get("error") and monitoring:
        save_metric_server_to_db(
            db,
            cluster_data.id,
            influxdb_metric_server,
            monitoring=True,
            is_custom_integration=current_is_custom  # preserve flag!
        )
    return {"influxdb_metric_server": influxdb_metric_server}
@proxmox_router.get('/edit/get_influxdb_metric_server')
async def get_influxdb_metric_server_endpoint(
    cluster_id: int,
    db: Session = Depends(get_db)
):
    cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster_data:
        raise HTTPException(status_code=404, detail="Cluster not found")

    ms = get_metric_server_from_db(cluster_id)
    if ms:
        influxdb_metric_server = {
            "type": ms.type,
            "server": ms.server,
            "port": ms.port,
            "proto": ms.proto,
            "organization": ms.organization,
            "bucket": ms.bucket,
            "token": ms.token,
            "disabled": ms.disabled,
            "monitoring": ms.monitoring,
            "is_custom_integration": ms.is_custom_integration,
        }
        return {"influxdb_metric_server": influxdb_metric_server}

    return {"influxdb_metric_server": {"error": "No metric server integration found for this cluster."}}
@proxmox_router.post('/add_influxdb_metric_server')
async def add_influxdb_metric_server_endpoint(cluster_id: str, request: Request, db: Session = Depends(get_db)):
    cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster_data:
        raise HTTPException(status_code=404, detail="Cluster not found")
    try:
        body = await request.json()
        monitoring = body.get("monitoring", True)
        is_custom_integration = body.get("is_custom_integration", True)
        metric_info = create_and_get_metric_server(cluster_data)
        if not metric_info.get("error"):
            save_metric_server_to_db(
                db,
                cluster_data.id,
                metric_info,
                monitoring=monitoring,
                is_custom_integration=is_custom_integration
            )
        return {"message": "InfluxDB metric server added successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




@proxmox_router.delete('/delete_influxdb_metric_server')
async def delete_influxdb_metric_server_endpoint(cluster_id: str, db: Session = Depends(get_db)):
    cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster_data:
        raise HTTPException(status_code=404, detail="Cluster not found")
    ms = db.query(MetricServer).filter(MetricServer.cluster_id == cluster_data.id).first()
    if not ms:
        raise HTTPException(status_code=404, detail="Metric server integration not found.")

    # If it's a custom integration, delete from Proxmox then DB
    if ms.is_custom_integration:
        try:
            result = delete_influxdb_metric_server(cluster_data)
            if result.get("error"):
                raise Exception(result["error"])
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        db.delete(ms)
        db.commit()
        return {"success": True, "message": "InfluxDB metric server deleted from Proxmox and DB."}

    # If NOT custom integration, stop migration workflow (if exists), then delete from DB
    else:
        # Stop workflow if exists
        if ms.workflow_id:
            try:
                client = await connectionWithClient()  # Your Temporal client get method
                await client.get_workflow_handle(ms.workflow_id).cancel()
            except Exception as e:
                # Log error, but proceed with deletion
                print(f"Failed to cancel workflow {ms.workflow_id}: {e}")

        db.delete(ms)
        db.commit()
        return {"success": True, "message": "InfluxDB metric server deleted from DB and migration workflow stopped (if running)."}
    



INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

@proxmox_router.post("/migrate_bucket_all_data")
async def migrate_bucket_all_data_route(
    req: MigrateRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    # Validate env is present
    if not all([INFLUXDB_URL, INFLUXDB_ORG, INFLUXDB_TOKEN, INFLUXDB_BUCKET]):
        raise HTTPException(status_code=500, detail="Destination InfluxDB is not configured")

    # Build migration payload as a dict for both source and destination
    migration_payload = {
        "src_url": req.src_url,
        "src_token": req.src_token,
        "src_org": req.src_org,
        "src_bucket": req.src_bucket,
        "email": req.email,  # Optional email for user tracking
        "dst_url": INFLUXDB_URL,
        "dst_token": INFLUXDB_TOKEN,
        "dst_org": INFLUXDB_ORG,
        "dst_bucket": INFLUXDB_BUCKET
    }
    await update_metric_server_token(req.cluster_id, req.src_token)
    # Schedule the migration as a background task
    workflow_info = await migrate_bucket_all_data(migration_payload)
    if workflow_info.get("workflow_id"):
        # Assuming you have a working DB session and ORM model
        db_metric_server = db.query(MetricServer).filter(MetricServer.cluster_id == req.cluster_id).first()
        if db_metric_server:
            db_metric_server.workflow_id = workflow_info["workflow_id"]
            db.commit()
        
    return JSONResponse(workflow_info)

@proxmox_router.get('/get_metric_server')
async def get_metric_server_endpoint(
    cluster_id: str,
   
):
    metric_server = get_metric_server_from_db(cluster_id)
    return {"metric_server": metric_server}

#----------- VM Rebuild, Reboot, Shutdown, Start, Stop Endpoints -----------#

 
@proxmox_router.post("/start_vm")
async def start_vm_endpoint(
    data: VMPowerRequest,
    vmid: int,
    pool_id: str = None,
   
):
    # Route only passes parameters and returns result
    result = await start_vm_proxmox(vmid, pool_id, data.email)
    return result
 
@proxmox_router.post("/stop_vm")
async def stop_vm_endpoint(
    data: VMPowerRequest,
    vmid: int,
    pool_id: str = None,
):
    # Route only calls pure logic, no DB access here!
    result =await stop_vm_proxmox(vmid, pool_id, data.email)
    return result

@proxmox_router.post('/reboot_vm')
async def reboot_vm_endpoint(
    data: VMPowerRequest,
    vmid: int,
    pool_id: str = None,
):
    # Only pass primitive types to logic layer!
    result = await reboot_vm_proxmox(vmid, pool_id, data.email)
    return result

@proxmox_router.post("/shutdown_vm")
async def shutdown_vm_endpoint(
    data: VMPowerRequest,
    vmid: int,
    pool_id: str = None,
):
    # Only pass primitive types to logic layer!
    result = await shutdown_vm_proxmox(vmid, pool_id, data.email)
    return result
 
@proxmox_router.post('/vm_rebuild')
async def rebuild_vm_endpoint(
    data: VMPowerRequest,
    vmid: int,
    pool_id: str = None,
):
    vm_status = await vm_rebuild(vmid,pool_id, data.email)
    print("VM Status:", vm_status)
    return {"vm_status": vm_status, "msg": "VM rebuild initiated."}


# @proxmox_router.get('/proxmox_vms_info')
# def proxmox_all_vm_details(cluster_data,db: Session = Depends(get_db)):

#     vm_infos = get_all_vm_details(db, cluster_data)
#     return {"vm_infos": vm_infos}

def get_cluster_by_id(db: Session, vm_id: int) -> Cluster:
    machine_data = db.query(Machine).filter(Machine.vm_id == vm_id).first()
    pool_data = db.query(Pool).filter(Pool.id == machine_data.pool_id).first() if machine_data else None
    cluster_id = pool_data.cluster_id.split('_')[1] 
    cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    return cluster_data


@proxmox_router.get('/proxmox_vm_infos/{vm_id}')
def proxmox_all_vm_details(
    vm_id: int,
    db: Session = Depends(get_db)
):
    cluster_data = get_cluster_by_id(db, vm_id)
    if not cluster_data:
        raise HTTPException(status_code=404, detail="Cluster not found")
    vm_infos = get_all_vm_details(db, cluster_data)
    return {"vm_infos": vm_infos}

@proxmox_router.get('/proxmox_vm_info/{vm_id}')
def proxmox_vm_details(
    vm_id: int,
    db: Session = Depends(get_db)
):
    cluster_data = get_cluster_by_id(db, vm_id)
    if not cluster_data:
        raise HTTPException(status_code=404, detail="Cluster not found")
    vm_infos = get_all_vm_details(db, cluster_data)
    # Only return the details for the requested vm_id
    vm_info = next((vm for vm in vm_infos if vm["vmid"] == vm_id), None)
    if not vm_info:
        raise HTTPException(status_code=404, detail="VM not found")
    return {"vm_info": vm_info}



    # clusters = db.query(ClusterData).all()
    # if not clusters:
    #     raise HTTPException(status_code=404, detail="No clusters found")
    # all_vm_infos = []
    # for cluster_data in clusters:
    #     try:
    #         vm_infos = get_all_vm_details(db, cluster_data)
    #         all_vm_infos.extend(vm_infos)
    #     except Exception as e:
    #         all_vm_infos.append({"cluster": cluster_data.name, "error": str(e)})
    # return all_vm_infos
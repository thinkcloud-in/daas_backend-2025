from typing import List
from fastapi import  Depends, APIRouter, HTTPException, Request, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session
from db_configuration.config import SessionLocal, get_db
import service.proxmoxService as service
from service.controllers import get_cluster_details
from service import clusterService,hyper_v_service
from models.models import Cluster, Machine,Pool
from models.proxmox_model import MigrateRequest, VMPowerRequest, MetricServer
from utils.temporal_client import TemporalClientManager
import os, logging, sys
from utils import response_format

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

proxmox_router = APIRouter(prefix = "/v1/proxmox")
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
    
 
 
 
# def get_nodes(db: Session = Depends(get_db)):
#     try:
#         cluster_vms = service.get_all_cluster_vms(db)
#         return response_format.success_response(200, "Cluster VMs retrieved successfully", cluster_vms)
#     except Exception as e:
#         return response_format.error_response(500, "Failed to retrieve cluster VMs", str(e))

async def get_templates_for_nodes(cluster_id: str = None, db: Session = Depends(get_db)):
    try:
        cluster_data = await get_cluster_details(db, cluster_id)
        templates = service.get_templates(db, cluster_data)
        return response_format.success_response(200, "Templates retrieved successfully", templates)
    except Exception as e:
        return response_format.error_response(500, "Failed to retrieve templates", str(e))
 
async def clone_vms_endpoint(payload: CloneRequest, db: Session = Depends(get_db)):
    try:
        cluster_id = payload.cluster_id
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
    except Exception as e:
        return response_format.error_response(500, "Failed", str(e))
 
 
 
def generate_name(request: NameRequest,existing_names):
    try:
        name = service.generate_machine_name(request.template)
        return {"name": name}
    except ValueError as e:
        return {"error": str(e)}
   
 
async def update_nodes_route(db: Session = Depends(get_db)):
    try:
        updated_clusters = service.update_cluster_nodes(db)
        return updated_clusters
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update cluster nodes: {str(e)}")

async def get_cluster_nodes_endpoint(cluster_id: str, db):
    try:
        cluster_data = await get_cluster_details(db, cluster_id)
        if not cluster_data:
            raise HTTPException(status_code=404, detail="Cluster not found")
        data = service.get_cluster_nodes(cluster_data)
        return response_format.success_response(200, "Cluster nodes retrieved successfully", data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve cluster nodes: {str(e)}")

async def get_influxdb_metric_server_endpoint(
    cluster_id: str,
    monitoring: bool = Query(False),
    db: Session = Depends(get_db)
):
    try:
        cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster_data:
            raise HTTPException(status_code=404, detail="Cluster not found")
        influxdb_metric_server = clusterService.get_influxdb_metric_server(cluster_data)
        # Find current value to preserve it
        ms = db.query(MetricServer).filter(MetricServer.cluster_id == cluster_id).first()
        current_is_custom = ms.is_custom_integration if ms else False
        # Only update relevant fields, always pass current is_custom_integration
        if influxdb_metric_server and not influxdb_metric_server.get("error") and monitoring:
            clusterService.save_metric_server_to_db(
                db,
                cluster_data.id,
                influxdb_metric_server,
                monitoring=True,
                is_custom_integration=current_is_custom  # preserve flag!
            )
        return {"influxdb_metric_server": influxdb_metric_server}
    except Exception as e:
        raise Exception("Failed to retrieve InfluxDB metric server: {}".format(e))

async def get_influxdb_metric_server_endpoints(
    cluster_id: int,
    db: Session = Depends(get_db)
):
    try:
        cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster_data:
            raise HTTPException(status_code=404, detail="Cluster not found")

        ms = clusterService.get_metric_server_from_db(cluster_id)
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
    except Exception as e:
        raise HTTPException(status_code=500, detail= f"{str(e)}")

async def add_influxdb_metric_server_endpoint(cluster_id: str, request: Request, db: Session = Depends(get_db)):
    cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster_data:
        raise HTTPException(status_code=404, detail="Cluster not found")
    try:
        body = await request.json()
        monitoring = body.get("monitoring", True)
        is_custom_integration = body.get("is_custom_integration", True)
        metric_info = clusterService.create_and_get_metric_server(cluster_data)
        if not metric_info.get("error"):
            clusterService.save_metric_server_to_db(
                db,
                cluster_data.id,
                metric_info,
                monitoring=monitoring,
                is_custom_integration=is_custom_integration
            )
        return response_format.success_response(200, "InfluxDB metric server added successfully")
    except Exception as e:
        raise HTTPException(500, detail= f"{str(e)}")

async def delete_influxdb_metric_server_endpoint(cluster_id: str, db: Session):
    try:
        cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster_data:
            raise HTTPException(status_code=404, detail="Cluster not found !!")
        ms = db.query(MetricServer).filter(MetricServer.cluster_id == cluster_data.id).first()
        if not ms:
            raise HTTPException(status_code=404, detail="Metric server integration not found.")

        # If it's a custom integration, delete from Proxmox then DB
        if ms.is_custom_integration:
            try:
                result = clusterService.delete_influxdb_metric_server(cluster_data)
                if result.get("error"):
                    raise Exception(result["error"])
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
            db.delete(ms)
            db.commit()
            return {"success": True, "msg": "InfluxDB metric server deleted from Proxmox and DB."}

        else:
            # Stop workflow if exists
            if ms.workflow_id:
                try:
                    client = await TemporalClientManager.get_temporal_client()  # Your Temporal client get method
                    await client.get_workflow_handle(ms.workflow_id).cancel()
                except Exception as e:
                    raise HTTPException(status_code=500, detail=f"Failed to stop migration workflow: {str(e)}")

            db.delete(ms)
            db.commit()
            return {"success": True, "msg": "InfluxDB metric server deleted from DB and migration workflow stopped (if running)."}
    except Exception as e:
        raise HTTPException(500, detail= f"{str(e)}")



INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

async def migrate_bucket_all_data_route(
    req: MigrateRequest,
    # background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    # Validate env is present
    if not all([INFLUXDB_URL, INFLUXDB_ORG, INFLUXDB_TOKEN, INFLUXDB_BUCKET]):
        raise HTTPException(status_code=500, detail="Destination InfluxDB is not configured")

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
    await service.update_metric_server_token(req.cluster_id, req.src_token)
    workflow_info = await service.migrate_bucket_all_data(migration_payload)
    if workflow_info.get("workflow_id"):
        db_metric_server = db.query(MetricServer).filter(MetricServer.cluster_id == req.cluster_id).first()
        if db_metric_server:
            db_metric_server.workflow_id = workflow_info["workflow_id"]
            db.commit()
            
    return JSONResponse(workflow_info)

async def get_metric_server_endpoint(
    cluster_id: str,
   
):
    try:
        metric_server = clusterService.get_metric_server_from_db(cluster_id)
        return {"metric_server": metric_server}
    except Exception as e:
        return response_format.error_response(500, "Failed", str(e))


async def start_vm_endpoint(
    data: VMPowerRequest,
    vmid: str,
    pool_id: str = None,
    db: Session = None
):
    try:
        cluster_data = get_cluster_by_id(db, vmid)
        cluster_type = cluster_data.type.lower()      
        result = await service.start_vm_proxmox(vmid, pool_id, data.email, cluster_type)
        return result
    except Exception as e:
        return response_format.error_response(500, "Failed to start VM", str(e))

async def stop_vm_endpoint(
    data: VMPowerRequest,
    vmid: str,
    pool_id: str = None,
    db: Session = None
):
    try:
        cluster_data = get_cluster_by_id(db, vmid)
        cluster_type = cluster_data.type.lower()
        result = await service.stop_vm_proxmox(vmid, pool_id, data.email, cluster_type)
        return result
    except Exception as e:
        return response_format.error_response(500, "Failed to stop VM", str(e))

async def reboot_vm_endpoint(
    data: VMPowerRequest,
    vmid: str,
    pool_id: str = None,
    db: Session = None
):
    try:
        cluster_data = get_cluster_by_id(db, vmid)
        cluster_type = cluster_data.type.lower()
        result = await service.reboot_vm_proxmox(vmid, pool_id, data.email, cluster_type)
        return result
    except Exception as e:
        return response_format.error_response(500, "Failed to reboot VM", str(e))

async def shutdown_vm_endpoint(
    data: VMPowerRequest,
    vmid: str,
    pool_id: str = None,
    db: Session = None
):
    try:
        print()
        cluster_data = get_cluster_by_id(db, vmid)
        cluster_type = cluster_data.type.lower()
        result = await service.shutdown_vm_proxmox(vmid, pool_id, data.email, cluster_type)
        return result
    except Exception as e:
        return response_format.error_response(500, "Failed to shutdown VM", str(e))

async def rebuild_vm_endpoint(
    data: VMPowerRequest,
    vmid: str,
    pool_id: str = None,
    db: Session = None,
):
    print('--------------vmid',vmid)
    print('--------------pool_id',pool_id)
    close_db = False
    try:
        # Handle optional db argument for reverse compatibility or standalone calls
        if db is None:
            db = SessionLocal()
            close_db = True
        
        cluster_data = get_cluster_by_id(db, vmid)
        if not cluster_data:
            raise HTTPException(status_code=404, detail="Cluster not found")

        if cluster_data.type.lower() in ("hyper-v", "hyperv"):
            # Hyper-V rebuild expects a request object or dict with vm_id, pool_id, and email
            rebuild_request = {
                "vm_id": vmid,
                "pool_id": pool_id,
                "email": data.email
            }
            print("-----rebuild request-----", rebuild_request)
            res = await hyper_v_service.vm_rebuild(rebuild_request, db)
            return res

        # Proxmox logic
        # For Proxmox, vmid is typically an integer
        try:
            proxmox_vmid = int(vmid)
        except ValueError:
            proxmox_vmid = vmid
            
        vm_status = await service.vm_rebuild(proxmox_vmid, pool_id)
        return {"vm_status": vm_status, "msg": "VM rebuild initiated."}
    # except HTTPException as he:
    #     # Re-raise HTTP exceptions, especially from get_cluster_by_id if any
    #     raise he
    except Exception as e:
        return response_format.error_response(500, "Failed", str(e))
    finally:
        if close_db:
            db.close()


def get_cluster_by_id(db: Session, vm_id: str) -> Cluster:
    try:
        vm_id_str = str(vm_id)
        # Search by both vm_id and identifier to be more robust
        machine_data = db.query(Machine).filter(
            (Machine.vm_id == vm_id_str) | (Machine.identifier == vm_id_str)
        ).first()
        if not machine_data:
            logger.error(f"Machine lookup failed for VM ID: {vm_id_str}. This might be because the machine record hasn't been committed yet or the ID is incorrect.")
            raise HTTPException(status_code=404, detail=f"Machine with vm_id {vm_id} not found")

        pool_id = machine_data.pool_id
        if pool_id is None:
            raise HTTPException(status_code=404, detail=f"Machine {vm_id} has no pool_id assigned")

        pool_data = db.query(Pool).filter(Pool.id == pool_id).first()
        if not pool_data:
            raise HTTPException(status_code=404, detail=f"Pool with id {pool_id} not found for machine {vm_id}")
        if not pool_data.cluster_id or '_' not in str(pool_data.cluster_id):
            raise HTTPException(status_code=500, detail=f"Invalid cluster_id on pool {pool_id}: {pool_data.cluster_id}")

        cluster_id = pool_data.cluster_id.split('_')[1]
        try:
            cluster_id_int = int(cluster_id)
        except Exception:
            raise HTTPException(status_code=500, detail=f"Cluster id parsed from pool is not an integer: '{cluster_id}'")

        cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id_int).first()
        if not cluster_data:
            raise HTTPException(status_code=404, detail=f"Cluster with id {cluster_id_int} not found")

        return cluster_data
    except HTTPException:
        raise
    except Exception as e:
        return response_format.error_response(500, "Failed", str(e))

def proxmox_all_vm_details(
    vm_id: int,
    db: Session = Depends(get_db)
):
        cluster_data = get_cluster_by_id(db, vm_id)
        if not cluster_data:
            raise HTTPException(status_code=404, detail="Cluster not found")
        vm_infos = service.get_all_vm_details(db, cluster_data)
        return vm_infos

async def proxmox_vm_details(vm_id: str, db):
        cluster_data = get_cluster_by_id(db, vm_id)
        if not cluster_data:
            raise HTTPException(status_code=404, detail="Cluster not found")
        
        if cluster_data.type.lower() in ("hyper-v", "hyperv"):
            res = await hyper_v_service.get_vm_info(vm_id, db)
            return res

        vm_infos = await service.get_all_vm_details_parallel(db, cluster_data)
        vm_info = next((vm for vm in vm_infos if str(vm["vmid"]) == str(vm_id)), None)
        if not vm_info:
            raise HTTPException(status_code=404, detail="VM not found")
        return vm_info

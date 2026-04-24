from typing import Optional, Union
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
import httpx
import os
from datetime import datetime
from utils.temporal_client import TemporalClientManager
import logging
import uuid
from db_configuration.config import get_db, SessionLocal
from models.models import Machine, Cluster, Pool
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

def unique_id():
    unique_id = datetime.now()
    return f"{unique_id.hour }:{unique_id.minute}:{unique_id.second}"
    
#--------helping hand-----------
def get_agent_url(cluster:Cluster)->str:
    ip = cluster.ip.split(',')[0] if cluster.ip else "localhost"
    port = cluster.agent_port or 8765
    return f"http://{ip}:{port}"

async def resolve_cluster_from_vm(vm_id: str, db: Session) -> Cluster:
    machine = db.query(Machine).filter(Machine.vm_id == str(vm_id)).first()
    if not machine:
        raise HTTPException(status_code=404, detail=f"Machine {vm_id} not found")
    pool = db.query(Pool).filter(Pool.id == machine.pool_id).first()
    if not pool:
        raise HTTPException(status_code=404, detail=f"Pool for machine {vm_id} not found")
    
    try:
        cluster_id_val = str(pool.cluster_id).split('_')[1] if '_' in str(pool.cluster_id) else pool.cluster_id
        cluster = db.query(Cluster).filter(Cluster.id == int(cluster_id_val)).first()
    except (IndexError, ValueError, TypeError):
        raise HTTPException(status_code=500, detail=f"Invalid cluster reference in pool: {pool.cluster_id}")

    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    return cluster
#--------helping hand--------------------

async def get_vms(cluster_id:int, db:Session):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    agent_url = get_agent_url(cluster)
    url = f"{agent_url}/v1/hyper-v/get_vms"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(url)
        data = response.json()
        return data['data']

async def clone_vm_hyper_v_service(request) -> dict:
    req_dict = request if isinstance(request, dict) else jsonable_encoder(request)

    cluster_id = req_dict.get("cluster_id")
    db: Session = SessionLocal()
    try:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster:
            raise HTTPException(status_code=404, detail=f"Cluster with ID {cluster_id} not found")
        agent_url = get_agent_url(cluster)
    finally:
        db.close()

    node_type = str(cluster.node_type).lower().replace(" ", "") if cluster and cluster.node_type else ""
    is_cluster = node_type == "multinode"

    url = f"{agent_url}/v1/hyper-v/clone_vm_hyper_v"
    template = req_dict.get("template_vm_id", {}) or {}

    vhdPath          = template.get("vhdPath")
    PvhdPath         = template.get("PvhdPath")
    generation       = template.get("generation")
    memory           = template.get("memory")
    switch           = template.get("switch")
    os_type          = template.get("os_type")
    ip_list          = req_dict.get("ip_list")
    password         = template.get("password")
    gateway          = template.get("gateway")
    subnet           = template.get("subnet")
    dns              = template.get("dns")
    number_of_vms    = req_dict.get("count", 1)
    base_vm_name     = req_dict.get("name_template", "cloned_vm")
    domain           = req_dict.get("domain")
    ou               = req_dict.get("ou")
    username         = req_dict.get("username")
    domain_password  = req_dict.get("domain_password")

    # Extract resource settings with fallback to template (checking both prefixed and non-prefixed keys)
    dynamic_memory  = req_dict.get("dynamic_memory")  if req_dict.get("dynamic_memory")  is not None else (template.get("dynamic_memory")  if template.get("dynamic_memory")  is not None else template.get("pool_dynamic_memory"))
    minimum_memory  = req_dict.get("minimum_memory")  if req_dict.get("minimum_memory")  is not None else (template.get("minimum_memory")  if template.get("minimum_memory")  is not None else template.get("pool_minimum_memory"))
    maximum_memory  = req_dict.get("maximum_memory")  if req_dict.get("maximum_memory")  is not None else (template.get("maximum_memory")  if template.get("maximum_memory")  is not None else template.get("pool_maximum_memory"))
    buffer_memory   = req_dict.get("buffer_memory")   if req_dict.get("buffer_memory")   is not None else (template.get("buffer_memory")   if template.get("buffer_memory")   is not None else template.get("pool_buffer_memory"))
    processor_count = req_dict.get("processor_count") if req_dict.get("processor_count") is not None else (template.get("processor_count") if template.get("processor_count") is not None else template.get("pool_processor_count"))
    priority        = req_dict.get("priority")        if req_dict.get("priority")        is not None else (template.get("priority")        if template.get("priority")        is not None else template.get("pool_priority", 2000))

    # Fetch existing VM names from agent to avoid conflicts
    try:
        db_inner: Session = SessionLocal()
        try:
            hyperv_vms = await get_vms(cluster.id, db_inner)
        finally:
            db_inner.close()
        hyperv_names = [vm.get("VMName") or vm.get("Name") for vm in hyperv_vms if vm.get("VMName") or vm.get("Name")]
    except Exception:
        hyperv_names = []

    from service import proxmoxService
    new_names = proxmoxService.generate_machine_name(base_vm_name, hyperv_names, number_of_vms)
    if not new_names:
        return {"error": "No unique VM names available for cloning."}

    result_vms = []
    for vm_name, ip in zip(new_names, ip_list):
        payload = {
            "vm_name": vm_name, "memory": memory, "vhdPath": vhdPath,
            "switch": switch, "generation": generation, "PvhdPath": PvhdPath,
            "ip": ip, "password": password, "gateway": gateway, "os_type": os_type,
            "subnet": subnet, "dns": dns, "domain": domain, "ou": ou,
            "username": username, "domain_password": domain_password,
            "dynamic_memory": dynamic_memory, "minimum_memory": minimum_memory,
            "maximum_memory": maximum_memory, "buffer_memory": buffer_memory,
            "processor_count": processor_count, "priority": priority,
            "is_cluster": is_cluster,
        }
        logger.debug("Cloning VM %s with payload: %s", vm_name, payload)
        print('----------------------------------------------------payload',payload)
        async with httpx.AsyncClient(timeout=180.0) as client:
            response = await client.post(url, json=payload)
            data = response.json()

        if data.get("code") != 200:
            logger.warning("Agent returned non-200 while cloning %s: %s", vm_name, data)
            continue

        agent_data = data.get("data")
        if isinstance(agent_data, dict) and "VM" in agent_data:
            vm_info = agent_data["VM"]
            result_vms.append({
                "name": vm_info.get("Name") or vm_info.get("VMName") or vm_name,
                "vmid": vm_info.get("Id") or vm_info.get("VMId")
            })
        elif isinstance(agent_data, list):
            for vm_item in agent_data:
                if isinstance(vm_item, dict) and "VM" in vm_item:
                    vm_info = vm_item["VM"]
                    result_vms.append({
                        "name": vm_info.get("Name") or vm_info.get("VMName") or vm_name,
                        "vmid": vm_info.get("Id") or vm_info.get("VMId")
                    })
        else:
            result_vms.append({"name": vm_name})

    return {
        "machines_created": len(result_vms),
        "created_names": new_names,
        "vms": result_vms,
    }

async def ping_agent(cluster_id: Optional[int], db: Session, ip: str, port: Union[int, str]):
    workflow_id = f"ping_agent_hyperv-{uuid.uuid4().hex}"

    client = await TemporalClientManager.get_temporal_client()
    if client is None:
        logger.error("Temporal client connection failed")
        raise HTTPException(status_code=500, detail="Temporal client connection failed")

    try:
        from service.temporalResource.workflows import workflows_hyper_v
        logger.info("Starting workflow %s with payload keys: %s", cluster_id, db, ip, port)
        handle = await client.start_workflow(
            workflows_hyper_v.PingAgentWorkflow.run,
            args=[cluster_id, db, ip, port],
            id=workflow_id,
            task_queue="hyperv-task-queue",
            # search_attributes={
            #     "Entity": [str(cluster_id)],
            #     "Action": [f"ping_agent_hyperv"],  
            #     "UserName": [email]
            # }
        )

    except Exception as e:
        logger.exception("Failed to start workflow %s: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed to start workflow: {str(e)}")

    result =  await handle.result()
    return result



async def get_vm_info(vm_id:str, db:Session, cluster_id:int=None):
    if not cluster_id:
        cluster = await resolve_cluster_from_vm(vm_id, db)
    else:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
        
    agent_url = get_agent_url(cluster)
    url = f"{agent_url}/v1/hyper-v/get_vm_info/{vm_id}"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(url)
        data = response.json()
        
        agent_data = data.get('data', '')
        # If not found by ID, we might need a fallback or better error reporting
        if isinstance(agent_data, str) and ("ObjectNotFound" in agent_data or "unable to find" in agent_data.lower()):
            # Potentially the ID changed or is misformatted. 
            # In a real scenario, we might want to search by VM Name here if we had it.
            return {"error": f"VM with ID {vm_id} not found on Hyper-V host.", "agent_error": agent_data}
            
        return agent_data
    
async def get_switches(cluster_id:int, db:Session):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    agent_url = get_agent_url(cluster)
    url = f"{agent_url}/v1/hyper-v/get_switches"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(url)
        data = response.json()
        return data['data']


async def delete_hyperv_vm(vm_id: str, db:Session, cluster_id:int=None):
    if not cluster_id:
        cluster = await resolve_cluster_from_vm(vm_id, db)
    else:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()

    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")

    agent_url = get_agent_url(cluster)
    node_type = str(cluster.node_type).lower().replace(" ", "") if cluster.node_type else ""
    is_cluster = node_type == "multinode"
    url = f"{agent_url}/v1/hyper-v/delete_vm_hyper_v/{vm_id}?is_cluster={str(is_cluster).lower()}"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.delete(url)
        data = response.json()
        return data

async def get_status(vm_id: str, db:Session, cluster_id:int=None) -> dict:
    if not cluster_id:
        cluster = await resolve_cluster_from_vm(vm_id, db)
    else:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()

    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")

    agent_url = get_agent_url(cluster)
    url = f"{agent_url}/v1/hyper-v/get_status/{vm_id}"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get("data", {})
    
async def handle_action(request, db:Session, cluster_id:int=None) -> dict:
    payload = request.dict() if hasattr(request, "dict") else request
    vm_id_val = payload.get("vm_id") or payload.get("vm_name")
    
    if not cluster_id:
        cluster = await resolve_cluster_from_vm(vm_id_val, db)
    else:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()

    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
        
    # Inject cluster_id into the payload so the Temporal activity knows which agent to contact
    payload["cluster_id"] = cluster.id
        
    client = await TemporalClientManager.get_temporal_client()
    if client is None:
        raise HTTPException(status_code=500, detail="Temporal client connection failed")

    try:
        from service.temporalResource.workflows import workflows_hyper_v
        workflow_id = f"hyperv-handle-action-{uuid.uuid4().hex}"
        handle = await client.start_workflow(
            workflows_hyper_v.HandleActionHyperVWorkflow.run,
            args=[payload],
            id=workflow_id,
            task_queue="hyperv-task-queue",
        )
    except Exception as e:
        logger.exception("Failed to start handle_action workflow")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start workflow: {str(e)}"
        )

    result = await handle.result()
    return result
        


async def vm_rebuild(request, db:Session, cluster_id:int=None):
    payload = request.dict() if hasattr(request, "dict") else request
    vm_id = payload.get("vm_id")
    
    if not cluster_id:
        cluster = await resolve_cluster_from_vm(vm_id, db)
    else:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()

    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    
    try:
        machine = db.query(Machine).filter(Machine.vm_id == str(vm_id)).first()
        if not machine:
            return {"status": "error", "error": f"Machine with vm_id {vm_id} not found in DB."}
        
        uniqueId = unique_id()
        client = await TemporalClientManager.get_temporal_client()
        workflow_id = f"vmrebuild_hyperv-{uniqueId}-{uuid.uuid4().hex[:4]}"
        
        from service.temporalResource.workflows import workflows_hyper_v
        handle = await client.start_workflow(
            workflows_hyper_v.VmRebuildHyperVWorkflow.run,
            args=[payload],
            id=workflow_id,
            task_queue="hyperv-task-queue",
        )
        result = await handle.result()
        return result
    except Exception as e:
        logger.error(f"Error in Hyper-V vm_rebuild: {e}")
        return {"status": "error", "error": str(e)}

async def pool_rebuild(request, db:Session, cluster_id:int = None):
    payload = request.dict() if hasattr(request, "dict") else request
    pool_id = payload.get("pool_id")
    
    if not cluster_id:
        pool = db.query(Pool).filter(Pool.id == pool_id).first()
        if not pool:
            raise HTTPException(status_code=404, detail="Pool not found")
            
        cluster_id_val = pool.cluster_id
        if cluster_id_val and "_" in str(cluster_id_val):
            cluster_id_val = int(str(cluster_id_val).split("_")[1])
            
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id_val).first()
    else:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()

    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")

    client = await TemporalClientManager.get_temporal_client()
    if client is None:
        return {"status": "error", "error": "Temporal client connection failed"}
    
    vhdPath = payload.get("vhdPath")
    workflow_id = f"poolrebuild_hyperv-{pool_id}-{unique_id()}"
    
    from service.temporalResource.workflows import workflows_hyper_v
    try:
        handle = await client.start_workflow(
            workflows_hyper_v.HyperVPoolRebuildWorkflow.run,
            args=[{"pool_id": pool_id, "vhdPath": vhdPath}],
            id=workflow_id,
            task_queue="hyperv-task-queue",
        )
        return {"workflow_id": workflow_id, "status": "success", "msg": "Hyper-V Pool Rebuild started."}
    except Exception as e:
        logger.error(f"Failed to start pool rebuild workflow: {e}")
        return {"status": "error", "error": str(e)}

async def verify_standalone_hyper_v(request, db: Session, cluster_id: Optional[int] = None):
    workflow_id = f"verify_standalone_hyper_v-{uuid.uuid4().hex}"

    client = await TemporalClientManager.get_temporal_client()
    if client is None:
        logger.error("Temporal client connection failed")
        raise HTTPException(status_code=500, detail="Temporal client connection failed")

    try:
        from service.temporalResource.workflows import workflows_hyper_v
        logger.info("Starting workflow %s with payload keys: %s",request.ip, request.username, request.password, request.agent_port, cluster_id) 
        handle = await client.start_workflow(
            workflows_hyper_v.VerifyStandaloneHyperVWorkflow.run,
            args=[request, db, cluster_id],
            id=workflow_id,
            task_queue="hyperv-task-queue",
            # search_attributes={
            #     "Entity": [str(cluster_id)],
            #     "Action": [f"ping_agent_hyperv"],  
            #     "UserName": [email]
            # }
        )

    except Exception as e:
        logger.exception("Failed to start workflow %s: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed to start workflow: {str(e)}")

    result =  await handle.result()
    return result
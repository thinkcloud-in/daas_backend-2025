"""
hyper_v_service — service layer for Hyper-V cluster/VM orchestration.

Plays the same role for Hyper-V that clusterService/controllers.py play for Proxmox:
communication with the actual Hyper-V host goes through a per-cluster "agent" (a lightweight
HTTP service running on `cluster.ip`:`cluster.agent_port` — default port 8765). Some
operations go directly over agent HTTP (get_vms, get_switches, get_vm_info), some go through
Temporal workflows (clone, delete, ping, verify, rebuild) so that the multi-step
orchestration (agent call → DB register → power-on) is properly tracked.
Used by: controllers/hyper_v_controller.py.
"""
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
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

def unique_id():
    """Build an `HH:MM:SS` string from the current time, for use as a workflow-id suffix."""
    unique_id = datetime.now()
    return f"{unique_id.hour }:{unique_id.minute}:{unique_id.second}"

#--------helping hand-----------
def get_agent_url(cluster:Cluster)->str:
    """Build the Hyper-V agent's base URL from the cluster's first IP + `agent_port` (default 8765)."""
    ip = cluster.ip.split(',')[0] if cluster.ip else "localhost"
    port = cluster.agent_port or 8765
    return f"http://{ip}:{port}"

async def resolve_cluster_from_vm(vm_id: str, db: Session) -> Cluster:
    """
    Resolve a VM's parent Cluster from its `vm_id` (Machine → Pool → Cluster chain, case-insensitive vm_id match).
    Returns: Cluster ORM object.
    Raises: HTTPException 404 (machine/pool/cluster not found), 500 (pool.cluster_id malformed).
    """
    # Machine.vm_id is a string, and GUIDs can be case-variant
    machine = db.query(Machine).filter(Machine.vm_id.ilike(str(vm_id))).first()
    if not machine:
        logger.warning(f"Machine {vm_id} not found in DB (tried case-insensitive lookup)")
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
    """
    Fetch all of the cluster's VMs directly from the Hyper-V agent (GET `/v1/hyper-v/get_vms`).
    Returns: list of VM info dicts (the agent's raw "data"; a single-dict response is also wrapped in a list).
    Raises: HTTPException 404 (cluster not found); the underlying exception is re-raised on agent call failure.
    """
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    agent_url = get_agent_url(cluster)
    is_cluster = str(cluster.node_type).lower() in ("multinode", "cluster")
    url = f"{agent_url}/v1/hyper-v/get_vms?is_cluster={is_cluster}"
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.get(url)
            data = response.json()
            response_data = data.get('data')
            if isinstance(response_data, dict):
                return [response_data]
            return response_data if isinstance(response_data, list) else []
    except Exception as e:
        raise e

async def clone_vm_hyper_v_service(request, skip_name_check: bool = False) -> dict:
    """
    Actually clone one or multiple Hyper-V VMs (by calling the agent's `/clone_vm_hyper_v` or
    `/full_clone_vm_hyper_v` endpoint — is_full_clone is decided from `destination_path`). For
    multinode clusters it dispatches each VM to the least-loaded node (by VMCount). To avoid
    naming conflicts it collects existing VM names from both the agent and the DB and generates
    unique names (`skip_name_check=True` uses the exact given name — the rebuild use-case).

    Params: request (dict or pydantic model) — cluster_id, template_vm_id/template fields,
            count, name_template, ip_list, domain-join fields, resource overrides, etc.
    Returns: {"machines_created": int, "created_names": list, "vms": [{"name","vmid","ip"}, ...]},
             or {"error": "..."} if unique names cannot be found.
    Raises: HTTPException 404 if cluster_id is not found.
    """
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
    is_cluster = node_type in ("multinode", "cluster")

    template = req_dict.get("template_vm_id", {}) or {}

    vhdPath          = template.get("vhdPath") or req_dict.get("vhdPath") or ""
    PvhdPath         = template.get("PvhdPath") or req_dict.get("PvhdPath") or ""
    generation       = template.get("generation") or req_dict.get("generation")
    memory           = template.get("memory") or req_dict.get("memory")
    switch           = template.get("switch") or req_dict.get("switch")
    os_type          = template.get("os_type") or req_dict.get("os_type")
    ip_list          = req_dict.get("ip_list")
    if not ip_list and req_dict.get("ip"):
        ip_list = [req_dict.get("ip")]
        
    password         = template.get("password") or req_dict.get("password") or ""
    gateway          = template.get("gateway") or req_dict.get("gateway") or ""
    subnet           = template.get("subnet") or req_dict.get("subnet") or ""
    dns              = template.get("dns") or req_dict.get("dns") or ""
    number_of_vms    = req_dict.get("count", 1)
    base_vm_name     = req_dict.get("name_template") or req_dict.get("vm_name") or "cloned_vm"
    domain           = req_dict.get("domain") or ""
    ou               = req_dict.get("ou") or ""
    username         = req_dict.get("username") or ""
    domain_password  = req_dict.get("domain_password") or ""
    destination_path = req_dict.get("destination_path") or template.get("destination_path") or ""
    is_full_clone    = bool(destination_path) or template.get("clone_type") == "Full Clone"

    # Extract resource settings with fallback to template (checking both prefixed and non-prefixed keys)
    dynamic_memory  = req_dict.get("dynamic_memory")  if req_dict.get("dynamic_memory")  is not None else (template.get("dynamic_memory")  if template.get("dynamic_memory")  is not None else template.get("pool_dynamic_memory"))
    minimum_memory  = req_dict.get("minimum_memory")  if req_dict.get("minimum_memory")  is not None else (template.get("minimum_memory")  if template.get("minimum_memory")  is not None else template.get("pool_minimum_memory"))
    maximum_memory  = req_dict.get("maximum_memory")  if req_dict.get("maximum_memory")  is not None else (template.get("maximum_memory")  if template.get("maximum_memory")  is not None else template.get("pool_maximum_memory"))
    buffer_memory   = req_dict.get("buffer_memory")   if req_dict.get("buffer_memory")   is not None else (template.get("buffer_memory")   if template.get("buffer_memory")   is not None else template.get("pool_buffer_memory"))
    processor_count = req_dict.get("processor_count") if req_dict.get("processor_count") is not None else (template.get("processor_count") if template.get("processor_count") is not None else template.get("pool_processor_count"))
    priority        = req_dict.get("priority")        if req_dict.get("priority")    is not None else (template.get("priority")        if template.get("priority")        is not None else template.get("pool_priority", 2000))

    # Fetch existing VM names from agent and DB to avoid conflicts
    try:
        db_inner: Session = SessionLocal()
        try:
            hyperv_vms = await get_vms(cluster.id, db_inner)
            # Collect names from Agent
            agent_names = [vm.get("VMName") or vm.get("Name") for vm in hyperv_vms if vm.get("VMName") or vm.get("Name")]
            # Collect names from DB
            db_machine_names = [m.name for m in db_inner.query(Machine).filter(Machine.name.isnot(None)).all()]
            hyperv_names = list(set(agent_names + db_machine_names))
        finally:
            db_inner.close()
    except Exception as e:
        logger.warning("Failed to fetch existing VM names: %s", e)
        hyperv_names = []

    if skip_name_check:
        # Use the exact name provided (ideal for rebuilds where identity is preserved)
        new_names = [base_vm_name]
    else:
        from service import proxmoxService
        new_names = proxmoxService.generate_machine_name(base_vm_name, hyperv_names, number_of_vms)
        
    if not new_names:
        return {"error": "No unique VM names available for cloning."}

    result_vms = []
    for vm_name, ip in zip(new_names, ip_list):
        ip = ip or ""
        # Dispatcher logic for Clusters: Pick the node with the lowest VMCount for EACH VM
        selected_node_name = None
        selected_node_ip = None
        if is_cluster:
            try:
                url_for_node_status = f"{agent_url}/v1/hyper-v/get_node_status_from_cluster"
                async with httpx.AsyncClient(timeout=10.0) as client:
                    node_response = await client.get(url_for_node_status)
                    if node_response.status_code == 200:
                        node_data = node_response.json().get('data', [])
                        min_vms = float('inf')
                        for node in node_data:
                            if node.get('Status') == 'Up':
                                current_count = node.get('VMCount', 0)
                                if current_count < min_vms:
                                    min_vms = current_count
                                    selected_node_name = node.get('Node_name')
                                    selected_node_ip = node.get('IP')
                        if selected_node_name:
                            logger.info("Dispatcher selected node %s (%s) with %s VMs for %s", selected_node_name, selected_node_ip, min_vms, vm_name)
            except Exception as e:
                logger.warning("Dispatcher failed to select node for %s: %s", vm_name, e)

        payload = {
            "vm_name": vm_name,
            "memory": memory,
            "vhdPath": vhdPath,
            "switch": switch,
            "generation": generation,
            "ip": ip,
            "password": password,
            "gateway": gateway,
            "os_type": os_type,
            "subnet": subnet,
            "dns": dns,
            "domain": domain,
            "ou": ou,
            "username": username,
            "domain_password": domain_password,
            "dynamic_memory": dynamic_memory,
            "minimum_memory": minimum_memory,
            "maximum_memory": maximum_memory,
            "buffer_memory": buffer_memory,
            "processor_count": processor_count,
            "priority": priority,
            "is_cluster": is_cluster
        }

        if is_full_clone:
            payload["destination_path"] = destination_path
        else:
            payload["PvhdPath"] = PvhdPath
        
        target_base_url = agent_url
        if is_cluster and selected_node_ip:
            port = cluster.agent_port or 8765
            target_base_url = f"http://{selected_node_ip}:{port}"
        
        endpoint = "full_clone_vm_hyper_v" if is_full_clone else "clone_vm_hyper_v"
        clone_url = f"{target_base_url}/v1/hyper-v/{endpoint}"
        
        logger.debug("Cloning VM %s with payload to %s: %s", vm_name, clone_url, payload)
        async with httpx.AsyncClient(timeout=300.0) as client:
            response = await client.post(clone_url, json=payload)
            response_data = response.json()
            logger.info("Agent response for %s from %s: %s", vm_name, target_base_url, response_data)

        if response_data.get("code") is not None and int(response_data.get("code")) != 200:
            logger.warning("Agent returned non-200 while cloning %s: %s", vm_name, response_data)
            continue

        agent_data = response_data.get("data")
        if isinstance(agent_data, dict):
            if "VM" in agent_data:
                vm_info = agent_data["VM"]
                result_vms.append({
                    "name": vm_info.get("Name") or vm_info.get("VMName") or vm_name,
                    "vmid": vm_info.get("Id") or vm_info.get("VMId"),
                    "ip": ip,
                })
            elif "Name" in agent_data or "VMId" in agent_data:
                # Handle cases where Name/VMId are at the root of the data dict
                result_vms.append({
                    "name": agent_data.get("Name") or agent_data.get("VMName") or vm_name,
                    "vmid": agent_data.get("VMId") or agent_data.get("Id"),
                    "ip": ip,
                })
            else:
                result_vms.append({"name": vm_name, "ip": ip})
        elif isinstance(agent_data, list):
            for vm_item in agent_data:
                if isinstance(vm_item, dict):
                    if "VM" in vm_item:
                        vm_info = vm_item["VM"]
                        result_vms.append({
                            "name": vm_info.get("Name") or vm_info.get("VMName") or vm_name,
                            "vmid": vm_info.get("Id") or vm_info.get("VMId"),
                            "ip": ip,
                        })
                    else:
                        result_vms.append({
                            "name": vm_item.get("Name") or vm_item.get("VMName") or vm_name,
                            "vmid": vm_item.get("VMId") or vm_item.get("Id"),
                            "ip": ip,
                        })
        else:
            result_vms.append({"name": vm_name, "ip": ip})

    return {
        "machines_created": len(result_vms),
        "created_names": new_names,
        "vms": result_vms,
    }


async def clone_vm_hyper_v(clone_payload: dict) -> dict:
    """
    Orchestrating entry point for Hyper-V clones — parallel to
    proxmoxService.clone_vm(). Starts CloneVMHyperVWorkflow (clone via agent
    -> register machine -> power on, with provisioning_status tracked at each
    step) and awaits the result, instead of calling clone_vm_hyper_v_service
    directly and leaving registration/power-on untracked.
    """
    from service.temporalResource.workflows import workflows_hyper_v

    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    workflow_id = f"clonevm-hyperv-{uniqueId}"
    try:
        handle = await client.start_workflow(
            workflows_hyper_v.CloneVMHyperVWorkflow.run,
            clone_payload,
            id=workflow_id,
            task_queue="hyperv-task-queue",
        )
        return await handle.result()
    except Exception as e:
        return {"error": str(e)}


async def ping_agent(cluster_id: Optional[int], db: Session, ip: str, port: Union[int, str]):
    """
    Start `PingAgentWorkflow` — checks whether the Hyper-V agent (at the given ip:port) is reachable.
    Returns: workflow result (connectivity status dict).
    Raises: HTTPException 500 if the Temporal client is unavailable or the workflow fails to start.
    """
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
            args=[cluster_id, ip, port],
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
    """
    Fetch a VM's details directly from the Hyper-V agent (GET `/v1/hyper-v/get_vm_info/{vm_id}`).
    If cluster_id is not given, the cluster is auto-resolved from the VM (`resolve_cluster_from_vm`).
    Returns: the agent's "data" field (dict), or {"error": "..."} on timeout/failure/not-found.
    Raises: HTTPException 404 if the cluster cannot be resolved.
    """
    if not cluster_id:
        cluster = await resolve_cluster_from_vm(vm_id, db)
    else:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    node_type = cluster.node_type
    if node_type in ['Standalone','standalone']:
        is_cluster = False
    else:
        is_cluster = True
    
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
        
    agent_url = get_agent_url(cluster)
    url = f"{agent_url}/v1/hyper-v/get_vm_info/{vm_id}?is_cluster={is_cluster}"
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(url)
            data = response.json()
    except httpx.TimeoutException:
        logger.error(f"Timeout while fetching VM info from agent: {url}")
        return {"error": "The Hyper-V agent took too long to respond. Please try again in a moment."}
    except Exception as e:
        logger.error(f"Error fetching VM info: {str(e)}")
        return {"error": f"Failed to fetch VM info: {str(e)}"}
        
    agent_data = data.get('data', '')
    # If not found by ID, we might need a fallback or better error reporting
    if isinstance(agent_data, str) and ("ObjectNotFound" in agent_data or "unable to find" in agent_data.lower()):
        # Potentially the ID changed or is misformatted. 
        # In a real scenario, we might want to search by VM Name here if we had it.
        return {"error": f"VM with ID {vm_id} not found on Hyper-V host.", "agent_error": agent_data}
        
    return agent_data
    
async def get_switches(cluster_id:int, db:Session):
    """
    List the cluster's Hyper-V virtual switches (GET `/v1/hyper-v/get_switches`, directly from the agent).
    Returns: the agent's "data" field (list of switch names/objects).
    Raises: HTTPException 404 if the cluster is not found.
    """
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    agent_url = get_agent_url(cluster)
    url = f"{agent_url}/v1/hyper-v/get_switches"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(url)
        data = response.json()
        return data['data']


async def delete_hyperv_vm(request, db:Session, cluster_id:int=None):
    """
    Delete a Hyper-V VM (by POSTing directly to the agent's `/delete_vm_hyper_v` — no Temporal
    involved). If the AD domain-join credentials are missing, they are auto-filled from the Pool record.
    In a multinode cluster it first resolves the VM's owning node (`get_node_via_vm_id`).
    Returns: the agent's raw JSON response.
    Raises: HTTPException 404 (cluster not found, or the cluster node cannot be resolved).
    """
    if isinstance(request, str):
        req_dict = {"vm_id": request}
    else:
        req_dict = request if isinstance(request, dict) else jsonable_encoder(request)
    
    vm_id = req_dict.get("vm_id")
    
    # Fetch machine and pool to get AD credentials if they are missing
    machine = db.query(Machine).filter(Machine.vm_id == str(vm_id)).first()
    if machine:
        pool = db.query(Pool).filter(Pool.id == machine.pool_id).first()
        if pool and not req_dict.get("domain") and pool.pool_ad_domain:
            req_dict["domain"] = pool.pool_ad_domain
            req_dict["username"] = pool.pool_ad_username
            req_dict["domain_password"] = pool.pool_ad_password

    if not cluster_id:
        cluster = await resolve_cluster_from_vm(vm_id, db)
    else:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()

    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")

    agent_url = get_agent_url(cluster)
    parsed = urlparse(agent_url)
    port = parsed.port or 8765
    node_type = str(cluster.node_type).lower().replace(" ", "") if cluster.node_type else ""
    is_cluster = node_type in ("multinode", "cluster")
    
    # Update is_cluster in request if it matches cluster type
    if is_cluster:
        req_dict["is_cluster"] = True
    
    if is_cluster:
        find_node = f"{agent_url}/v1/hyper-v/get_node_via_vm_id/{vm_id}?is_cluster={str(is_cluster).lower()}"
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(find_node)
            node_ip = response.json().get('data', {}).get('NodeIP')
        if not node_ip:
            raise HTTPException(status_code=404, detail=f"Could not resolve owner node for VM {vm_id}")
        agent_url = f"http://{node_ip}:{port}"
        
    url = f"{agent_url}/v1/hyper-v/delete_vm_hyper_v"

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(url, json=req_dict)
        data = response.json()
    return data

async def get_status(vm_id: str, db:Session, cluster_id:int=None) -> dict:
    """
    Fetch a VM's current power status from the Hyper-V agent (GET `/v1/hyper-v/get_status/{vm_id}/...`).
    Returns: the agent's "data" field (dict).
    Raises: HTTPException 404 (cluster not found); httpx.HTTPStatusError is re-raised on an agent error
            (a warning is logged for the 404 case — in cluster environments the VM may be on another node).
    """
    if not cluster_id:
        cluster = await resolve_cluster_from_vm(vm_id, db)
    else:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()

    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    is_cluster_ = str(cluster.node_type).lower() in ("multinode", "cluster")
    agent_url = get_agent_url(cluster)
    url = f"{agent_url}/v1/hyper-v/get_status/{vm_id}/is_cluster/{is_cluster_}"
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            return data.get("data", {})
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            logger.warning(f"Agent {agent_url} reported VM {vm_id} not found. In cluster environments, the VM might be on a different node.")
        raise e
    
async def handle_action(request, db:Session, cluster_id:int=None) -> dict:
    """
    Start `HandleActionHyperVWorkflow` — a generic VM power-action (start/stop/restart/etc.);
    resolves the cluster from the payload's `vm_id`/`vm_name` and injects `cluster_id` into the payload.
    Returns: workflow result dict.
    Raises: HTTPException 404 (cluster not found), 500 (Temporal client/workflow start failure).
    """
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
    """
    Start `VmRebuildHyperVWorkflow` — re-provisions an existing VM (rebuild from a fresh disk).
    Returns: workflow result dict on success, {"status":"error","error":"..."} on failure
             (exceptions are not raised here, they are wrapped in a dict).
    """
    payload = request.dict() if hasattr(request, "dict") else request
    vm_id = payload.get("vm_id")    
    # if not cluster_id:
    #     print('--------- cluster_id not found ------------')
    #     cluster = await resolve_cluster_from_vm(vm_id, db)
    # else:
    #     cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()

    # if not cluster:
    #     raise HTTPException(status_code=404, detail="Cluster not found")
    
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
    """
    Start `HyperVPoolRebuildWorkflow` (fire-and-forget — does not wait for the result) — rebuilds
    all of a pool's VMs from a new vhdPath.
    Returns: {"workflow_id","status":"success","msg":"..."} if the workflow started,
             {"status":"error","error":"..."} on failure.
    Raises: HTTPException 404 if the pool/cluster cannot be resolved.
    """
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

async def verify_hyper_v(request, db: Session, cluster_id: Optional[int] = None):
    """
    Start `VerifyHyperVWorkflow` — verifies reachability/auth to a Hyper-V host using the given
    agent credentials (ip/username/password/agent_port) (before adding the cluster).
    Returns: workflow result (verification status dict).
    Raises: HTTPException 500 if the Temporal client is unavailable or the workflow fails to start.
    """
    workflow_id = f"verify_hyper_v-{uuid.uuid4().hex}"

    client = await TemporalClientManager.get_temporal_client()
    if client is None:
        logger.error("Temporal client connection failed")
        raise HTTPException(status_code=500, detail="Temporal client connection failed")

    try:
        from service.temporalResource.workflows import workflows_hyper_v
        logger.info("Starting workflow %s with payload keys: %s",request.ip, request.username, request.password, request.agent_port, cluster_id) 
        handle = await client.start_workflow(
            workflows_hyper_v.VerifyHyperVWorkflow.run,
            args=[request, cluster_id],
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

async def fetch_hyper_v_cluster_nodes(request:dict):
    """
    Start `FetchClusterNodesWorkflow` — lists the member nodes of a Hyper-V failover cluster
    using the given credentials (for node discovery while adding a cluster).
    Returns: workflow result (list of nodes).
    Raises: HTTPException 500 if the Temporal client is unavailable or the workflow fails to start.
    """
    workflow_id = f"fetch_cluster_nodes-{uuid.uuid4().hex}"
    client = await TemporalClientManager.get_temporal_client()
    if client is None:
        raise HTTPException(status_code=500, detail="Temporal client connection failed")

    try:
        from service.temporalResource.workflows import workflows_hyper_v
        handle = await client.start_workflow(
            workflows_hyper_v.FetchClusterNodesWorkflow.run,
            args=[request],
            id=workflow_id,
            task_queue="hyperv-task-queue",
        )
        result = await handle.result()
        return result
    except Exception as e:
        logger.exception("Failed to start FetchClusterNodesWorkflow: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
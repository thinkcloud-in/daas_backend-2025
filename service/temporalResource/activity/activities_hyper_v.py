from service.hyper_v_service import get_agent_url, delete_hyperv_vm, clone_vm_hyper_v_service
from typing import Optional, Union
from temporalio import activity
import httpx
from service import hyper_v_service
from db_configuration.config import SessionLocal, get_db
from models.models import Cluster 
from sqlalchemy.orm import Session
import logging
from urllib.parse import quote
from models.models import Machine, Pool

logger = logging.getLogger(__name__)

# @activity.defn
# async def clone_vm_hyper_v_activity(request: dict) -> dict:
#     cluster_id = request.get("cluster_id")
#     db: Session = SessionLocal()
#     try:
#         cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
#         if not cluster:
#             raise Exception(f"Cluster with ID {cluster_id} not found")
#         agent_url = hyper_v_service.get_agent_url(cluster)
#     finally:
#         db.close()
    
#     # Derive is_cluster from cluster metadata (robust check)
#     node_type = str(cluster.node_type).lower().replace(" ", "") if cluster and cluster.node_type else ""
#     is_cluster = node_type == "multinode"
    
#     url = f"{agent_url}/v1/hyper-v/clone_vm_hyper_v"
#     template = request.get("template_vm_id", {}) or {}

#     vhdPath = template.get("vhdPath")
#     PvhdPath = template.get("PvhdPath")
#     generation = template.get("generation")
#     memory = template.get("memory")
#     switch = template.get("switch")
#     os_type = template.get("os_type")
#     ip_list = request.get('ip_list')
#     password = template.get('password')
#     gateway = template.get('gateway')
#     subnet = template.get('subnet')
#     dns = template.get('dns')
#     number_of_vms = request.get("count", 1)
#     base_vm_name = request.get("name_template", "cloned_vm")
#     domain = request.get("domain")
#     ou = request.get("ou")
#     username = request.get("username")
#     domain_password = request.get("domain_password")
    
#     # Pull resource settings from the top level (from the Pool columns)
#     # or fallback to the template if not present at the top level
#     dynamic_memory = request.get("dynamic_memory") if request.get("dynamic_memory") is not None else template.get("dynamic_memory")
#     minimum_memory = request.get("minimum_memory") if request.get("minimum_memory") is not None else template.get("minimum_memory")
#     maximum_memory = request.get("maximum_memory") if request.get("maximum_memory") is not None else template.get("maximum_memory")
#     buffer_memory = request.get("buffer_memory") if request.get("buffer_memory") is not None else template.get("buffer_memory")
#     processor_count = request.get("processor_count") if request.get("processor_count") is not None else template.get("processor_count")
#     priority = request.get("priority") if request.get("priority") is not None else template.get("priority", 2000)

#     # Only fetch existing VM names from Hyper-V
#     try:
#         db_inner: Session = SessionLocal()
#         try:
#             hyperv_vms = await hyper_v_service.get_vms(cluster.id, db_inner)
#         finally:
#             db_inner.close()
#         hyperv_names = []
#         for vm in hyperv_vms:
#             name = vm.get("VMName") or vm.get("Name")
#             if name:
#                 hyperv_names.append(name)
#     except Exception:
#         hyperv_names = []
    
#     from service import proxmoxService
#     new_names = proxmoxService.generate_machine_name(base_vm_name, hyperv_names, number_of_vms)
#     if not new_names:
#         return {"error": "No unique VM names available for cloning."}

#     result_vms = []
#     for vm_name, ip in zip(new_names, ip_list):
#         payload = {
#             "vm_name": vm_name,
#             "memory": memory,
#             "vhdPath": vhdPath,
#             "switch": switch,
#             "generation": generation,
#             "PvhdPath": PvhdPath,
#             "ip": ip,
#             "password": password,
#             "gateway": gateway,
#             "os_type": os_type,
#             "subnet": subnet,
#             "dns": dns,
#             "domain": domain,
#             "ou": ou,
#             "username": username,
#             "domain_password": domain_password,
#             "dynamic_memory": dynamic_memory,
#             "minimum_memory": minimum_memory,
#             "maximum_memory": maximum_memory,
#             "buffer_memory": buffer_memory,
#             "processor_count": processor_count,
#             "priority": priority,
#             "is_cluster": is_cluster,
#         }
#         logger.debug("Payload for clone_vm_hyper_v: %s", payload)
#         async with httpx.AsyncClient(timeout=180.0) as client:
#             response = await client.post(url, json=payload)
#             data = response.json()
            
#         if data.get("code") != 200:
#             logger.warning("Agent returned non-200 while cloning %s: %s", vm_name, data)
#             continue

#         if isinstance(data.get("data"), dict) and "VM" in data.get("data"):
#             vm_info = data["data"]["VM"]
#             result_vms.append({
#                 "name": vm_info.get("Name") or vm_info.get("VMName") or vm_name,
#                 "vmid": vm_info.get("Id") or vm_info.get("VMId")
#             })
#         elif isinstance(data.get("data"), list):
#             for vm_item in data.get("data"):
#                 if isinstance(vm_item, dict) and "VM" in vm_item:
#                     vm_info = vm_item["VM"]
#                     result_vms.append({
#                         "name": vm_info.get("Name") or vm_info.get("VMName") or vm_name,
#                         "vmid": vm_info.get("Id") or vm_info.get("VMId")
#                     })
#         else:
#             result_vms.append({"name": vm_name})

#     return {
#         "machines_created": len(result_vms),
#         "created_names": new_names,
#         "vms": result_vms,
#     }


# @activity.defn
# async def delete_vm_hyper_v_activity(request: dict) -> dict:
#     cluster_id = request.get("cluster_id")
#     db: Session = SessionLocal()
#     try:
#         try:
#             cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
#             if not cluster:
#                 raise Exception(f"Cluster with ID {cluster_id} not found")
#             agent_url = hyper_v_service.get_agent_url(cluster)
#         except Exception as e:
#             logger.error("Error fetching cluster for VM deletion: %s", str(e))
#             raise
#     finally:
#         db.close()
#     vm_id = request.get("vm_id")
    
#     # Fetch cluster to determine is_cluster
#     cluster_id = request.get("cluster_id")
#     if not cluster_id and vm_id:
#         # If no cluster_id in request, try to resolve from machine
#         db: Session = SessionLocal()
#         try:
#             from models.models import Machine
#             machine = db.query(Machine).filter(Machine.vm_id == vm_id).first()
#             if machine:
#                 raw_cluster_id = machine.cluster_id
#                 if raw_cluster_id:
#                     if isinstance(raw_cluster_id, str) and "_" in raw_cluster_id:
#                         cluster_id = raw_cluster_id.split("_")[-1]
#                     else:
#                         cluster_id = raw_cluster_id
#         finally:
#             db.close()

#     db: Session = SessionLocal()
#     try:
#         cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
#         node_type = str(cluster.node_type).lower().replace(" ", "") if cluster and cluster.node_type else ""
#         is_cluster = node_type == "multinode"
#     finally:
#         db.close()

#     if not vm_id:
#         return {"error": "vm_id is required"}

#     url = f"{agent_url}/v1/hyper-v/delete_vm_hyper_v/{vm_id}?is_cluster={str(is_cluster).lower()}"

#     logger.info("Deleting Hyper-V VM, vm_id=%s", vm_id)

#     async with httpx.AsyncClient(timeout=20.0) as client:
#         response = await client.delete(url)

#     data = response.json()

#     if data.get("code") != 200:
#         logger.error("Failed to delete VM %s: %s", vm_id, data)
#         return {
#             "status": "failed",
#             "vm_id": vm_id,
#             "error": data
#         }

#     return {
#         "status": "success",
#         "vm_id": vm_id,
#         "message": "VM deleted successfully"
#     }

@activity.defn
async def handle_action_activity(request: dict) -> dict:
    cluster_id = request.get("cluster_id")
    db: Session = SessionLocal()
    try:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        is_cluster = str(cluster.node_type).lower() == "cluster"
        if not cluster:
            raise Exception(f"Cluster with ID {cluster_id} not found")
        agent_url = hyper_v_service.get_agent_url(cluster)
    finally:
        db.close()

    if is_cluster:
        find_node = f"{agent_url}/v1/hyper-v/get_node_via_vm_id/{request.get('vm_id')}?is_cluster={str(is_cluster).lower()}"
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(find_node)
            node_ip = response.json().get('data', {}).get('NodeIP')
        current_agent_port = agent_url.split(":")[2]
        agent_url = f"http://{node_ip}:{current_agent_port}"
    
    url = f"{agent_url}/v1/hyper-v/handle_action"
    logger.info("Hyper-V handle_action called with payload: %s", request)

    agent_payload = {
        "vm_id": request.get("vm_id"),
        "action": request.get("action")
    }

    timeout = httpx.Timeout(
        connect=10.0,   
        read=120.0,     
        write=10.0,
        pool=10.0
    )
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(url, json=agent_payload)

    data = response.json()
    if data.get("code") == 200:
        action_requested = request.get("action")
        try:
            db_inner: Session = SessionLocal()
            try:
                from models.models import Machine
                vm_id_val = request.get("vm_id") or request.get("vm_name")
                if vm_id_val:
                    machine = db_inner.query(Machine).filter(Machine.vm_id == str(vm_id_val)).first()
                    if machine:
                        if action_requested == "start":
                            machine.error_message = "power-on"
                        elif action_requested in ["stop", "force_off", "shutdown"]:
                            machine.error_message = "power-off"
                        db_inner.commit()
            finally:
                db_inner.close()
        except Exception as e:
            logger.error("Failed to update DB power state: %s", str(e))

        return {
            "status": "success",
            "data": data.get("data")
        }

    logger.error("Hyper-V handle_action failed: %s", data)
    # return {
    #     "status": "failed",
    #     "error": data.get("msg", "Unknown error occurred")
    # }
    raise Exception(data.get("msg", "Unknown error occurred"))

@activity.defn
async def vm_rebuild_hyper_v_activity(request: dict) -> dict:
    vm_id = request.get("vm_id")
    db: Session = SessionLocal()
    try:
        cluster = await hyper_v_service.resolve_cluster_from_vm(vm_id, db)
        is_cluster = str(cluster.node_type).lower() in ["multi node", "cluster", "multinode"]
        clone_url_standalone = hyper_v_service.get_agent_url(cluster)
        machine = db.query(Machine).filter(Machine.vm_id == str(vm_id)).first()
        if not machine:
            return {"code": 500, "status": "error", "msg": f"Machine with vm_id {vm_id} not found in DB."}
        pool = db.query(Pool).filter(Pool.id == machine.pool_id).first()
        if not pool:
            return {"code": 500, "status": "error", "msg": f"Pool {machine.pool_id} not found."}
        template_data = pool.pool_template_vm_id
        if not template_data:
            return {"code": 500, "status": "error", "msg": "No template data found in pool."}

        vhdPath = template_data.get("vhdPath")
        PvhdPath = template_data.get("PvhdPath")
        generation = template_data.get("generation")
        memory = template_data.get("memory")
        switch = template_data.get("switch")
        os_type = template_data.get("os_type")
        password = template_data.get('password')
        gateway = template_data.get('gateway')
        subnet = template_data.get('subnet')
        dns = template_data.get('dns')
        ip = machine.hostname # Use existing IP assigned to machine
        dynamic_memory = template_data.get("dynamic_memory")
        minimum_memory = template_data.get("minimum_memory")
        maximum_memory = template_data.get("maximum_memory")
        buffer_memory = template_data.get("buffer_memory")
        processor_count = template_data.get("processor_count")

        # 1. Delete existing VM
        logger.info(f"Single VM Rebuild: Deleting {machine.name}")
        delete_status = await delete_hyperv_vm(vm_id, db)
        if delete_status.get("code") != 200:
            return {"code": 500, "status": "error", "msg": f"Delete failed: {delete_status.get('msg')}"}

        # 2. Re-clone using shared service (Maintains identity and Exact Name)
        clone_request = {
            "cluster_id": cluster.id,
            "name_template": machine.name,
            "count": 1,
            "ip_list": [ip],
            "domain": pool.pool_ad_domain,
            "ou": pool.pool_ad_path,
            "username": pool.pool_ad_username,
            "domain_password": pool.pool_ad_password,
            "template_vm_id": {
                **template_data,
                "PvhdPath": PvhdPath  # Keep existing parent path
            }
        }

        logger.info(f"Single VM Rebuild: Re-cloning {machine.name}")
        clone_result = await clone_vm_hyper_v_service(clone_request, skip_name_check=True)
        
        if "error" in clone_result or clone_result.get("code") == 500:
            err_msg = clone_result.get("error") or clone_result.get("msg")
            return {"code": 500, "status": "error", "msg": f"Cloning failed: {err_msg}"}

        # Extract new VM info from result
        new_vms = clone_result.get("vms", [])
        if not new_vms:
            return {"code": 500, "status": "error", "msg": "Clone service returned no VM data"}
            
        data = {"data": {"VM": {"Id": new_vms[0].get("vmid"), "Name": new_vms[0].get("name")}}}
        machine.name = new_vms[0].get("name") # In case it changed, though skip_name_check=True should prevent it

        # Update the database with the new VM ID
        # Post-cloning strategy: Get all VMs and find the one with the correct name to get the definitive ID
        new_vm_id = None
        try:
            all_vms = await hyper_v_service.get_vms(cluster.id, db)
            for vm in all_vms:
                # Host reports Name/VMName and Id/VMId
                curr_name = vm.get("Name") or vm.get("VMName")
                if curr_name == machine.name:
                    new_vm_id = vm.get("Id") or vm.get("VMId")
                    break
        except Exception as sync_err:
            logger.warning(f"Failed to sync VM ID by name for {machine.name}: {sync_err}")
            # Fallback to agent response ID if list failed
            agent_data = data.get("data")
            if isinstance(agent_data, dict) and "VM" in agent_data:
                new_vm_id = agent_data["VM"].get("Id") or agent_data["VM"].get("VMId")

        if new_vm_id:
            logger.info(f"Updating machine {machine.id} vm_id from {vm_id} to {new_vm_id}")
            machine.vm_id = str(new_vm_id)
            
            # Also update pool_vmids if necessary
            if pool.pool_vmids:
                # Ensure we handle list correctly
                updated_vmids = []
                for v in pool.pool_vmids:
                    if str(v) == str(vm_id):
                        updated_vmids.append(str(new_vm_id))
                    else:
                        updated_vmids.append(str(v))
                pool.pool_vmids = updated_vmids
            
            db.commit()
            vm_id = str(new_vm_id)
        else:
            logger.error(f"Could not find new VM ID for {machine.name} after cloning")

        return {"code": 200, "status": "success", "vm_id": vm_id, "machine_name": machine.name}
    except Exception as e:
        db.rollback()
        logger.error(f"Error in vm_rebuild_hyper_v_activity: {e}")
        return {"code": 500, "status": "error", "msg": str(e)}
    finally:
        db.close()

@activity.defn
async def get_pool_rebuild_data_activity(request: dict) -> dict:
    from models.models import Pool, Machine
    db: Session = SessionLocal()
    try:
        pool_id = request.get("pool_id")
        try:
            pool = db.query(Pool).filter(Pool.id == pool_id).first()
            if not pool:
                return {"status": "error", "error": f"Pool {pool_id} not found."}
            
            machines = db.query(Machine).filter(Machine.pool_id == pool_id).all()
            machine_list = []
            for m in machines:
                machine_list.append({
                    "id": m.id,
                    "vm_id": m.vm_id,
                    "name": m.name,
                    "hostname": m.hostname,
                    "ip": m.hostname 
                })
                
            return {
                "status": "success",
                "pool_id": pool_id,
                "template_data": pool.pool_template_vm_id,
                "machines": machine_list
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}
    finally:
        db.close()

import re
def normalize_template_paths(template_data: dict) -> dict:
    path_keys = ["PvhdPath", "vhdPath"]
    for key in path_keys:
        if key in template_data:
            val = template_data[key]
            val = val.replace("/", "\\")          # / → \
            val = re.sub(r"\\{2,}", r"\\", val)   # \\\\ → \
            template_data[key] = val
    return template_data

@activity.defn
async def rebuild_machine_in_pool_activity(request: dict) -> dict:
    from service.hyper_v_service import clone_vm_hyper_v_service, delete_hyperv_vm
    
    m_data = request.get("machine")
    pool_id = request.get("pool_id")
    m_id = m_data.get("id")
    old_vm_id = m_data.get("vm_id")
    machine_name = m_data.get("name")
    machine_ip = m_data.get("hostname")

    db: Session = SessionLocal()
    try:
        # 1. Resolve Cluster and Metadata
        cluster = await hyper_v_service.resolve_cluster_from_vm(old_vm_id, db)
        machine = db.query(Machine).filter(Machine.id == m_id).first()
        pool = db.query(Pool).filter(Pool.id == pool_id).first()
        template_data = pool.pool_template_vm_id

        # 2. Safely Delete the VM (Using your node-wise logic)
        logger.info(f"Rebuild: Deleting VM {machine_name} (ID: {old_vm_id})")
        delete_response = await delete_hyperv_vm(old_vm_id, db)
        
        if delete_response.get("code") != 200:
            return {
                "status": "error",
                "machine": machine_name,
                "error": f"Delete failed: {delete_response.get('msg')}"
            }

        # 3. Clone using your Load-Balanced service
        # We pass the new vhdPath as the PvhdPath for the new clone
        clone_request = {
            "cluster_id": cluster.id,
            "name_template": machine_name,
            "count": 1,
            "ip_list": [machine_ip],
            "domain": pool.pool_ad_domain,
            "ou": pool.pool_ad_path,
            "username": pool.pool_ad_username,
            "domain_password": pool.pool_ad_password,
            "template_vm_id": {
                **template_data,
                "PvhdPath": request.get("vhdPath") 
            }
        }
        
        logger.info(f"Rebuild: Re-cloning {machine_name} with parent disk {request.get('vhdPath')}")
        # Use skip_name_check=True to keep the EXACT same name during rebuild
        clone_result = await clone_vm_hyper_v_service(clone_request, skip_name_check=True)
        
        if "error" in clone_result:
            return {"status": "error", "machine": machine_name, "error": clone_result["error"]}
            
        # 4. Update Database with the new VM identity
        new_vms = clone_result.get("vms", [])
        if not new_vms:
             return {"status": "error", "machine": machine_name, "error": "Clone service returned no VM data"}
             
        new_vm_id = new_vms[0].get("vmid")
        new_name = new_vms[0].get("name") # Note: This might have a suffix like _1
        
        # Update machine record
        machine.vm_id = str(new_vm_id)
        machine.name = new_name 
        machine.error_message = "power-off" # Set to off as clones are usually created off or needs state reset
        
        # Update Pool's vmids list to replace the old ID with the new one
        if pool.pool_vmids:
            pool.pool_vmids = [
                str(new_vm_id) if str(v) == str(old_vm_id) else str(v)
                for v in pool.pool_vmids
            ]
        
        # Optionally update the pool template path for future new machines
        updated_template = dict(pool.pool_template_vm_id)
        updated_template["PvhdPath"] = request.get("vhdPath")
        pool.pool_template_vm_id = updated_template
        
        db.commit()
        
        return {
            "status": "success",
            "machine": machine_name,
            "old_vm_id": str(old_vm_id),
            "new_vm_id": str(new_vm_id),
            "new_name": new_name
        }

    except Exception as e:
        db.rollback()
        logger.error(f"rebuild_machine_in_pool_activity failed: {e}")
        return {"status": "error", "machine": machine_name, "error": str(e)}
    finally:
        db.close()


@activity.defn
async def ping_agent_activity(cluster_id: Optional[int], ip: str, port: Union[int, str]):
    db: Session = SessionLocal()
    try:
        try:
            if cluster_id:
                cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
                agent_url = get_agent_url(cluster)
            elif ip:
                agent_url = f"http://{ip}:{port or 8765}"
            else:
                raise Exception("cluster_id or ip/port required")
                
            url = f"{agent_url}/v1/hyper-v/health_check"

            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url)
                data = resp.json()
                return data
        except Exception as e:
            logger.error("ping_agent_activity failed: %s", str(e))
            raise
    finally:
        db.close()


@activity.defn
async def verify_hyper_v_activity(request, cluster_id: Optional[int] = None) -> dict:
    db: Session = SessionLocal()
    try:
        try:
            payload = request.dict() if hasattr(request, "dict") else request
            
            if cluster_id:
                cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
                agent_url = get_agent_url(cluster)
            else:
                ip = payload.get("ip")
                port = payload.get("agent_port") or 8765
                agent_url = f"http://{ip}:{port}"
            
            url = f"{agent_url}/v1/hyper-v/verify_hyper_v"
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(url, json=payload)
                data = response.json()

                # Propagate HTTP-level errors from the agent
                if response.status_code != 200:
                    detail = (
                        data.get("detail")
                        or data.get("message")
                        or data.get("msg")
                        or "Hyper-V verification failed"
                    )
                    raise Exception("Error while verifying Hyper-V- ",detail)

                agent_data = data.get("data", {})

                # Propagate logical errors returned with HTTP 200
                if isinstance(agent_data, dict) and agent_data.get("status") == "error":
                    detail = (
                        agent_data.get("message")
                        or agent_data.get("error")
                        or "Hyper-V verification failed"
                    )
                    raise Exception("Error while verifying Hyper-V",detail)

                return agent_data
        except Exception as e:
            logger.error("Error verifying Hyper-V: %s", e)
            raise Exception(f"Hyper-V verification error: {str(e)}")
    finally:
        db.close()


@activity.defn
async def fetch_cluster_nodes_activity(request:dict) -> list:
    db: Session = SessionLocal()
    try:
        if request.get("cluster_id"):
            cluster = db.query(Cluster).filter(Cluster.id == request.get("cluster_id")).first()
            if not cluster:
                raise Exception("Cluster not found")
            ip = cluster.ip
            port = cluster.agent_port
        else:
            ip = request.get("ip")
            port = request.get("agent_port")
        agent_url = f"http://{ip}:{port}"
        url = f"{agent_url}/v1/hyper-v/get_node_status_from_cluster"
        logger.info("Fetching cluster nodes from agent: %s", url)    
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url)
            if response.status_code != 200:
                raise Exception(f"Agent failed to return cluster nodes: {response.text}")
            
            data = response.json()
            return data.get("data", [])
    except Exception as e:
        logger.error("fetch_cluster_nodes_activity failed: %s", str(e))
        raise Exception(f"Failed to fetch cluster nodes: {str(e)}")
    finally:
        db.close()


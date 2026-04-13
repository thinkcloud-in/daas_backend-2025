from service.hyper_v_service import get_agent_url
from typing import Optional, Union
from temporalio import activity
import httpx
from service import hyper_v_service
from db_configuration.config import SessionLocal
from models.models import Cluster 
from sqlalchemy.orm import Session
import logging
from urllib.parse import quote
from datetime import datetime


logger = logging.getLogger(__name__)

@activity.defn
async def clone_vm_single_node_activity(request: dict) -> dict:
    cluster_id = request.get("cluster_id")
    with SessionLocal() as db:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster:
            raise Exception(f"Cluster with ID {cluster_id} not found")
        agent_url = hyper_v_service.get_agent_url(cluster)
    
        url = f"{agent_url}/v1/hyper-v/clone_vm_for_single_node"
        template = request.get("template_vm_id", {}) or {}

        vhdPath = template.get("vhdPath")
        PvhdPath = template.get("PvhdPath")
        generation = template.get("generation")
        memory = template.get("memory")
        switch = template.get("switch")
        os_type = template.get("os_type")
        ip_list = request.get('ip_list')
        password = template.get('password')
        gateway = template.get('gateway')
        subnet = template.get('subnet')
        dns = template.get('dns')
        number_of_vms = request.get("count", 1)
        base_vm_name = request.get("name_template", "cloned_vm")
        domain = request.get("domain")
        ou = request.get("ou")
        username = request.get("username")
        domain_password = request.get("domain_password")

        # Only fetch existing VM names from Hyper-V
        try:
            hyperv_vms = await hyper_v_service.get_vms(cluster.id, db)
            hyperv_names = []
            for vm in hyperv_vms:
                name = vm.get("VMName") or vm.get("Name")
                if name:
                    hyperv_names.append(name)
        except Exception:
            hyperv_names = []
        
    from service import proxmoxService
    new_names = proxmoxService.generate_machine_name(base_vm_name, hyperv_names, number_of_vms)
    if not new_names:
        return {"error": "No unique VM names available for cloning."}

    result_vms = []
    for vm_name, ip in zip(new_names, ip_list):
        payload = {
            "vm_name": vm_name,
            "memory": memory,
            "vhdPath": vhdPath,
            "switch": switch,
            "generation": generation,
            "PvhdPath": PvhdPath,
            "ip": ip,
            "password": password,
            "gateway": gateway,
            "os_type": os_type,
            "subnet": subnet,
            "dns": dns,
            "domain": domain,
            "ou": ou,
            "username": username,
            "domain_password": domain_password
        }
        logger.debug("Payload for clone_vm_for_single_node: %s", payload)
        async with httpx.AsyncClient(timeout=180.0) as client:
            response = await client.post(url, json=payload)
            data = response.json()
            
        if data.get("code") != 200:
            logger.warning("Agent returned non-200 while cloning %s: %s", vm_name, data)
            continue

        if isinstance(data.get("data"), dict) and "VM" in data.get("data"):
            vm_info = data["data"]["VM"]
            result_vms.append({
                "name": vm_info.get("Name") or vm_info.get("VMName") or vm_name,
                "vmid": vm_info.get("Id") or vm_info.get("VMId")
            })
        elif isinstance(data.get("data"), list):
            for vm_item in data.get("data"):
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


@activity.defn
async def delete_vm_single_node_activity(request: dict) -> dict:
    cluster_id = request.get("cluster_id")
    with SessionLocal() as db:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster:
            raise Exception(f"Cluster with ID {cluster_id} not found")
        agent_url = hyper_v_service.get_agent_url(cluster)
    vm_id = request.get("vm_id")

    if not vm_id:
        return {"error": "vm_id is required"}

    url = f"{agent_url}/v1/hyper-v/delete_vm/{vm_id}"

    logger.info("Deleting Hyper-V VM, vm_id=%s", vm_id)

    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.delete(url)

    data = response.json()

    if data.get("code") != 200:
        logger.error("Failed to delete VM %s: %s", vm_id, data)
        return {
            "status": "failed",
            "vm_id": vm_id,
            "error": data
        }

    return {
        "status": "success",
        "vm_id": vm_id,
        "message": "VM deleted successfully"
    }

@activity.defn
async def handle_action_activity(request: dict) -> dict:
    cluster_id = request.get("cluster_id")
    with SessionLocal() as db:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster:
            raise Exception(f"Cluster with ID {cluster_id} not found")
        agent_url = hyper_v_service.get_agent_url(cluster)
    url = f"{agent_url}/v1/hyper-v/handle_action"

    logger.info("Hyper-V handle_action called with payload: %s", request)

    # Clean payload for the agent. Agent strictly expects 'vm_id' and 'action'
    agent_payload = {
        "vm_id": request.get("vm_id") or request.get("vm_name"),
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
        # Update machine status immediately in the database so the UI enables/disables the correct buttons
        action_requested = request.get("action")
        try:
            with SessionLocal() as db:
                from models.models import Machine
                vm_id_val = request.get("vm_id") or request.get("vm_name")
                if vm_id_val:
                    machine = db.query(Machine).filter(Machine.vm_id == str(vm_id_val)).first()
                    if machine:
                        if action_requested == "start":
                            machine.error_message = "power-on"
                        elif action_requested in ["stop", "force_off", "shutdown"]:
                            machine.error_message = "power-off"
                        db.commit()
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
#---------------------needs to check this is being used or not -------------------------------------
@activity.defn
async def delete_hyperv_disk_activity(request: dict) -> dict:
    cluster_id = request.get("cluster_id")
    with SessionLocal() as db:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster:
            raise Exception(f"Cluster with ID {cluster_id} not found")
        agent_url = hyper_v_service.get_agent_url(cluster)
    disk_path = request.get("disk_path")

    if not disk_path:
        return {"status": "failed", "error": "disk_path is required"}

    encoded_path = quote(disk_path, safe=":/\\")
    url = f"{agent_url}/v1/hyper-v/delete_disk"

    logger.info("Deleting Hyper-V disk: %s", disk_path)

    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.delete(url, json=encoded_path)

    data = response.json()

    if data.get("code") == 200:
        return {
            "status": "success",
            "disk_path": disk_path,
            "result": data.get("data")
        }

    logger.error("Failed to delete disk %s: %s", disk_path, data)
    return {
        "status": "failed",
        "disk_path": disk_path,
        "error": data.get("msg", "Unknown error occurred")
    }

#---------------------needs to check this is being used or not -------------------------------------

@activity.defn
async def vm_rebuild_hyper_v_activity(request: dict) -> dict:
    from models.models import Machine, Pool
    vm_id = request.get("vm_id")
    try:
        with SessionLocal() as db:
            cluster = await hyper_v_service.resolve_cluster_from_vm(vm_id, db)
            agent_url = hyper_v_service.get_agent_url(cluster)
            machine = db.query(Machine).filter(Machine.vm_id == str(vm_id)).first()
            if not machine:
                return {"status": "error", "error": f"Machine with vm_id {vm_id} not found in DB."}
            
            pool = db.query(Pool).filter(Pool.id == machine.pool_id).first()
            if not pool:
                return {"status": "error", "error": f"Pool {machine.pool_id} not found."}

            template_data = pool.pool_template_vm_id
            if not template_data:
                return {"status": "error", "error": "No template data found in pool."}

            # 1. Delete existing VM
            delete_url = f"{agent_url}/v1/hyper-v/delete_vm/{vm_id}"
            async with httpx.AsyncClient(timeout=60.0) as client:
                del_resp = await client.delete(delete_url)
                logger.info(f"Delete VM {vm_id} response: {del_resp.status_code}")

            # 2. Clone new VM from template
            clone_url = f"{agent_url}/v1/hyper-v/clone_vm_for_single_node"
            
            # Prepare payload for clone (similar to clone_vm_single_node_activity)
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

            payload = {
                "vm_name": machine.name,
                "memory": memory,
                "vhdPath": vhdPath,
                "switch": switch,
                "generation": generation,
                "PvhdPath": PvhdPath,
                "ip": ip,
                "password": password,
                "gateway": gateway,
                "os_type": os_type,
                "subnet": subnet,
                "dns": dns,
                "domain": pool.pool_ad_domain,
                "ou": pool.pool_ad_path,
                "username": pool.pool_ad_username,
                "domain_password": pool.pool_ad_password
            }

            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(clone_url, json=payload)
                data = response.json()
            
            if data.get("code") != 200:
                return {"status": "error", "error": f"Cloning failed: {data.get('msg')}"}

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

            return {"status": "success", "vm_id": vm_id, "machine_name": machine.name}
    except Exception as e:
        logger.error(f"Error in vm_rebuild_hyper_v_activity: {e}")
        return {"status": "error", "error": str(e)}

@activity.defn
async def get_pool_rebuild_data_activity(request: dict) -> dict:
    from models.models import Pool, Machine
    with SessionLocal() as db:
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
    from models.models import Machine, Pool

    m_data = request.get("machine")
    pool_id = request.get("pool_id")
    m_id = m_data.get("id")
    old_vm_id = m_data.get("vm_id")
    machine_name = m_data.get("name")
    machine_ip = m_data.get("hostname")

    try:
        with SessionLocal() as db:
            cluster = await hyper_v_service.resolve_cluster_from_vm(old_vm_id, db)
            agent_url = hyper_v_service.get_agent_url(cluster)
            machine = db.query(Machine).filter(Machine.id == m_id).first()
            pool = db.query(Pool).filter(Pool.id == pool_id).first()
            template_data = pool.pool_template_vm_id

        delete_vm_url = f"{agent_url}/v1/hyper-v/delete_vm/{old_vm_id}"
        async with httpx.AsyncClient(timeout=60.0) as client:
            await client.delete(delete_vm_url)
        logger.info("Old VM deleted: %s", old_vm_id)

        vhd_folder = template_data.get("vhdPath", "C:\\test")
        old_vhd_path = f"{vhd_folder}\\{machine_name}\\{machine_name}.vhdx"
        delete_disk_url = f"{agent_url}/v1/hyper-v/delete_disk"
        async with httpx.AsyncClient(timeout=60.0) as client:
            del_disk_resp = await client.delete(delete_disk_url, params={"path": old_vhd_path})
            logger.info("Old VHD delete response: %s", del_disk_resp.text)

        clone_url = f"{agent_url}/v1/hyper-v/clone_vm_for_single_node"
        payload = {
            "vm_name": machine_name,
            "memory": template_data.get("memory"),
            "vhdPath": template_data.get("vhdPath"),
            "switch": template_data.get("switch"),
            "generation": template_data.get("generation"),
            "PvhdPath": request.get("vhdPath"), 
            "ip": machine_ip,
            "password": template_data.get("password"),
            "gateway": template_data.get("gateway"),
            "os_type": template_data.get("os_type"),
            "subnet": template_data.get("subnet"),
            "dns": template_data.get("dns"),
            "domain": pool.pool_ad_domain,
            "ou": pool.pool_ad_path,
            "username": pool.pool_ad_username,
            "domain_password": pool.pool_ad_password,
        }

        async with httpx.AsyncClient(timeout=180.0) as client:
            response = await client.post(clone_url, json=payload)
            data = response.json()

        logger.info("Clone response for %s: %s", machine_name, data)

        if data.get("code") != 200:
            return {
                "status": "error",
                "machine": machine_name,
                "error": f"Clone failed: {data.get('msg')} | full: {data}"
            }

        agent_vm = data.get("data", {})
        vm_obj = agent_vm.get("VM") or agent_vm.get("vm") or agent_vm
        new_vm_id = (
            vm_obj.get("Id") or vm_obj.get("VMId") or
            vm_obj.get("id") or vm_obj.get("vmId")
        )

        if not new_vm_id:
            async with httpx.AsyncClient(timeout=60.0) as client:
                vms_response = await client.get(f"{agent_url}/v1/hyper-v/get_vms")
                all_vms = vms_response.json().get("data", [])
            for vm in all_vms:
                vm_name = vm.get("Name") or vm.get("VMName") or vm.get("name")
                if vm_name == machine_name:
                    new_vm_id = vm.get("Id") or vm.get("VMId") or vm.get("id")
                    break

        if not new_vm_id:
            return {"status": "error", "machine": machine_name, "error": "VM ID not found after clone"}

        with SessionLocal() as db:
            machine = db.query(Machine).filter(Machine.id == m_id).first()
            pool = db.query(Pool).filter(Pool.id == pool_id).first()
            machine.vm_id = str(new_vm_id)
            if pool.pool_vmids:
                pool.pool_vmids = [
                    str(new_vm_id) if str(v) == str(old_vm_id) else str(v)
                    for v in pool.pool_vmids
                ]
                if pool.pool_template_vm_id is None:
                    pool.pool_template_vm_id = {}
                updated_template = dict(pool.pool_template_vm_id)
                updated_template["vhdPath"] = request.get("vhdPath")
                pool.pool_template_vm_id = updated_template
            
            machines = db.query(Machine).filter(Machine.pool_id == pool_id).all()
            for m in machines:
                m.error_message = "power-off"

            db.commit()

        return {
            "status": "success",
            "machine": machine_name,
            "old_vm_id": str(old_vm_id),
            "new_vm_id": str(new_vm_id),
        }

    except Exception as e:
        logger.error("rebuild_machine_in_pool_activity failed: %s", str(e))
        return {"status": "error", "machine": m_data.get("name"), "error": str(e)}


@activity.defn
async def ping_agent_activity(cluster_id: Optional[int], db: Session, ip: str, port: Union[int, str]):
    if cluster_id:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        agent_url = get_agent_url(cluster)
    elif ip:
        agent_url = f"http://{ip}:{port or 8765}"
    else:
        raise Exception("cluster_id or ip/port required")
        
    url = f"{agent_url}/v1/hyper-v/health"

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url)
        data = resp.json()
        return data

@activity.defn
async def verify_standalone_hyper_v_activity(request, db: Session, cluster_id: Optional[int] = None) -> dict:
    payload = request.dict() if hasattr(request, "dict") else request
    
    if cluster_id:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        agent_url = get_agent_url(cluster)
    else:
        ip = payload.get("ip")
        port = payload.get("agent_port") or 8765
        agent_url = f"http://{ip}:{port}"
    
    url = f"{agent_url}/v1/hyper-v/verify_standalone_hyper_v"
    try:
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
                raise Exception("Error while verifying standalone Hyper-V- ",detail)

            agent_data = data.get("data", {})

            # Propagate logical errors returned with HTTP 200
            if isinstance(agent_data, dict) and agent_data.get("status") == "error":
                detail = (
                    agent_data.get("message")
                    or agent_data.get("error")
                    or "Hyper-V verification failed"
                )
                raise Exception("Error while verifying standalone Hyper-V",detail)

            return agent_data
    except Exception as e:
        logger.error("Error verifying standalone Hyper-V: %s", e)
        raise Exception(f"Hyper-V verification error: {str(e)}")
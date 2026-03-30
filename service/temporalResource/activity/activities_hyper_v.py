from temporalio import activity
import httpx
import os
# from service import proxmoxService
# from service import hyper_v_service  # Move to inside functions
import logging
from urllib.parse import quote
from datetime import datetime

# If you need a DB session, import your sessionmaker factory:
# from db_configuration.config import SessionLocal

logger = logging.getLogger(__name__)
HYPER_V_AGENT_URL = (os.getenv("HYPER_V_AGENT_URL") or "").rstrip("/") + "/"

@activity.defn
async def clone_vm_single_node_activity(request: dict) -> dict:
    # If you need DB access, create a new session here:
    # db = SessionLocal()
    # try:
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/clone_vm_for_single_node"

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
        from service import hyper_v_service
        hyperv_vms = await hyper_v_service.get_vms()
        hyperv_names = []
        for vm in hyperv_vms:
            name = vm.get("VMName") or vm.get("Name")
            if name:
                hyperv_names.append(name)
    except Exception:
        hyperv_names = []
    from service import proxmoxService
    new_names =proxmoxService.generate_machine_name(base_vm_name, hyperv_names, number_of_vms)
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
        async with httpx.AsyncClient(timeout=120.0) as client:
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
    vm_id = request.get("vm_id")

    if not vm_id:
        return {"error": "vm_id is required"}

    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/delete_vm/{vm_id}"

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
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/handle_action"

    logger.info("Hyper-V handle_action called with payload: %s", request)

    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.post(url, json=request)

    data = response.json()

    if data.get("code") == 200:
        return {
            "status": "success",
            "data": data.get("data")
        }

    logger.error("Hyper-V handle_action failed: %s", data)
    return {
        "status": "failed",
        "error": data.get("msg", "Unknown error occurred")
    }

@activity.defn
async def delete_hyperv_disk_activity(request: dict) -> dict:
    disk_path = request.get("disk_path")

    if not disk_path:
        return {"status": "failed", "error": "disk_path is required"}

    encoded_path = quote(disk_path, safe=":/\\")
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/delete_disk"

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

@activity.defn
async def vm_rebuild_hyper_v_activity(request: dict) -> dict:
    from db_configuration.config import SessionLocal
    from models.models import Machine, Pool
    db = SessionLocal()
    vm_id = request.get("vm_id")
    pool_id = request.get("pool_id")    
    try:
        machine = db.query(Machine).filter(Machine.vm_id == vm_id).first()
        if not machine:
            return {"status": "error", "error": f"Machine with vm_id {vm_id} not found in DB."}
        
        pool = db.query(Pool).filter(Pool.id == pool_id).first()
        if not pool:
            return {"status": "error", "error": f"Pool {pool_id} not found."}

        template_data = pool.pool_template_vm_id
        if not template_data:
            return {"status": "error", "error": "No template data found in pool."}

        # 1. Delete existing VM
        delete_url = f"{HYPER_V_AGENT_URL}v1/hyper-v/delete_vm/{vm_id}"
        async with httpx.AsyncClient(timeout=60.0) as client:
            del_resp = await client.delete(delete_url)
            # We continue even if delete fails (e.g. already deleted manually)
            logger.info(f"Delete VM {vm_id} response: {del_resp.status_code}")

        # 2. Clone new VM from template
        clone_url = f"{HYPER_V_AGENT_URL}v1/hyper-v/clone_vm_for_single_node"
        
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
            from service import hyper_v_service
            all_vms = await hyper_v_service.get_vms()
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
    finally:
        db.close()

@activity.defn
async def get_pool_rebuild_data_activity(request: dict) -> dict:
    from db_configuration.config import SessionLocal
    from models.models import Pool, Machine
    db = SessionLocal()
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
    from db_configuration.config import SessionLocal
    from models.models import Machine, Pool
    db = SessionLocal()

    m_data = request.get("machine")
    pool_id = request.get("pool_id")
    m_id = m_data.get("id")
    old_vm_id = m_data.get("vm_id")
    machine_name = m_data.get("name")
    machine_ip = m_data.get("hostname")

    try:
        machine = db.query(Machine).filter(Machine.id == m_id).first()
        pool = db.query(Pool).filter(Pool.id == pool_id).first()
        template_data = pool.pool_template_vm_id

        delete_vm_url = f"{HYPER_V_AGENT_URL}v1/hyper-v/delete_vm/{old_vm_id}"
        async with httpx.AsyncClient(timeout=60.0) as client:
            await client.delete(delete_vm_url)
        logger.info("Old VM deleted: %s", old_vm_id)

        vhd_folder = template_data.get("vhdPath", "C:\\test")
        old_vhd_path = f"{vhd_folder}\\{machine_name}\\{machine_name}.vhdx"
        delete_disk_url = f"{HYPER_V_AGENT_URL}v1/hyper-v/delete_disk"
        async with httpx.AsyncClient(timeout=60.0) as client:
            del_disk_resp = await client.delete(delete_disk_url, params={"path": old_vhd_path})
            logger.info("Old VHD delete response: %s", del_disk_resp.text)

        clone_url = f"{HYPER_V_AGENT_URL}v1/hyper-v/clone_vm_for_single_node"
        payload = {
            "vm_name": machine_name,
            "memory": template_data.get("memory"),
            "vhdPath": template_data.get("vhdPath"),
            "switch": template_data.get("switch"),
            "generation": template_data.get("generation"),
            "PvhdPath": "C:\kaibalya_test_env\Import-VM\Parent_VM.vhdx",
            "ip": machine_ip,
            "password": template_data.get("password"),
            "gateway": template_data.get("gateway"),
            "os_type": template_data.get("os_type"),
            "subnet": template_data.get("subnet"),
            "dns": template_data.get("dns"),
        }

        async with httpx.AsyncClient(timeout=120.0) as client:
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
                vms_response = await client.get(f"{HYPER_V_AGENT_URL}v1/hyper-v/get_vms")
                all_vms = vms_response.json().get("data", [])
            for vm in all_vms:
                vm_name = vm.get("Name") or vm.get("VMName") or vm.get("name")
                if vm_name == machine_name:
                    new_vm_id = vm.get("Id") or vm.get("VMId") or vm.get("id")
                    break

        if not new_vm_id:
            return {"status": "error", "machine": machine_name, "error": "VM ID not found after clone"}

        machine.vm_id = str(new_vm_id)
        if pool.pool_vmids:
            pool.pool_vmids = [
                str(new_vm_id) if str(v) == str(old_vm_id) else str(v)
                for v in pool.pool_vmids
            ]
        db.commit()

        return {
            "status": "success",
            "machine": machine_name,
            "old_vm_id": str(old_vm_id),
            "new_vm_id": str(new_vm_id),
        }

    except Exception as e:
        db.rollback()
        logger.error("rebuild_machine_in_pool_activity failed: %s", str(e))
        return {"status": "error", "machine": m_data.get("name"), "error": str(e)}
    finally:
        db.close()
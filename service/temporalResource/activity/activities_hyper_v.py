from temporalio import activity
import httpx
import os
from service.proxmoxService import generate_machine_name
from service import hyper_v_service
import logging

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
    number_of_vms = request.get("count", 1)

    base_vm_name = request.get("name_template", "cloned_vm")

    # Only fetch existing VM names from Hyper-V
    try:
        hyperv_vms = await hyper_v_service.get_vms()
        hyperv_names = []
        for vm in hyperv_vms:
            name = vm.get("VMName") or vm.get("Name")
            if name:
                hyperv_names.append(name)
    except Exception:
        hyperv_names = []

    new_names = generate_machine_name(base_vm_name, hyperv_names, number_of_vms)
    if not new_names:
        return {"error": "No unique VM names available for cloning."}

    result_vms = []
    for vm_name in new_names:
        payload = {
            "vm_name": vm_name,
            "memory": memory,
            "vhdPath": vhdPath,
            "switch": switch,
            "generation": generation,
            "PvhdPath": PvhdPath,
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
    # finally:
    #     db.close()
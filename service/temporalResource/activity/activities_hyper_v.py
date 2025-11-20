from temporalio import activity
import httpx
import os
from service.proxmoxService import generate_machine_name
from service import hyper_v_service

HYPER_V_AGENT_URL = os.getenv('HYPER_V_AGENT_URL')
@activity.defn
async def clone_vm_singleNode_hyper_v_activity(request,db):
    url = f"{HYPER_V_AGENT_URL}v1/hyper-v/clone_vm_for_single_node"
    number_of_vms = getattr(request, "count", 1)
    print("request in clone_vm_for_single_node:", request)

    template = request.get("template_vm_id", {})
    print("template in clone_vm_for_single_node:", template)

    vhdPath = template.get("vhdPath")
    PvhdPath = template.get("PvhdPath")
    generation = template.get("generation")
    memory = template.get("memory")
    switch = template.get("switch")
    
    base_vm_name = request.get("name_template", "cloned_vm")

    # Only fetch existing VM names from Hyper-V
    try:
        hyperv_vms = await hyper_v_service.get_vms()
        # hyperv_names = [vm.get("VMName") for vm in hyperv_vms if vm.get("VMName")]
        hyperv_names = []
        for vm in hyperv_vms:
            name = vm.get("VMName") or vm.get("Name")
            if name:
                hyperv_names.append(name)
    except Exception:
        hyperv_names = []

    # Generate unique VM names using only Hyper-V names
    new_names = generate_machine_name(base_vm_name, hyperv_names, number_of_vms)
    print("Generated VM names:", new_names)

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
        print("Payload for clone_vm_for_single_node:", payload)
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(url, json=payload)
        data = response.json()
        print("Agent Response:", data)
        if data.get('code') != 200:
            continue
        # The agent response may have 'data' as a dict with 'VM' key, or as a list of dicts
        if isinstance(data.get('data'), dict) and "VM" in data.get('data'):
            vm_info = data['data']['VM']
            result_vms.append({
                "name": vm_info.get("Name") or vm_info.get("VMName") or vm_name,
                "vmid": vm_info.get("Id") or vm_info.get("VMId")
            })
        elif isinstance(data.get('data'), list):
            for vm_item in data.get('data'):
                if isinstance(vm_item, dict) and "VM" in vm_item:
                    vm_info = vm_item['VM']
                    result_vms.append({
                        "name": vm_info.get("Name") or vm_info.get("VMName") or vm_name,
                        "vmid": vm_info.get("Id") or vm_info.get("VMId")
                    })
        else:
            # Fallback: just append the name
            result_vms.append({"name": vm_name})
    return {
        "machines_created": len(result_vms),
        "created_names": new_names,
        "vms": result_vms,
    }
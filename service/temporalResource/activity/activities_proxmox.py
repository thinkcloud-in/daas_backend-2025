from http.client import HTTPException
import ipaddress
from time import time
# import time
from pydantic import BaseModel
from temporalio import activity
from sqlalchemy.orm import Session
from db_configuration.config import get_db
import requests
import asyncio
from models.models import Machine, Cluster,Pool
from typing import List
from service.clusterService import get_all_nodes
from models.IPs_model import IPEntry,IPSModel
from service import proxmoxService
from influxdb_client import InfluxDBClient, Point, WriteOptions
import dotenv
import os
dotenv.load_dotenv()

@activity.defn
async def clone_vm_activity(clone_payload: dict):
    print(">>> clone_vm_activity STARTED with payload:", clone_payload)

    db: Session = next(get_db())
    # from service.proxmoxService import get_api_token
    try:
        # Use bracket notation for all clone_payload accesses
        cluster_data = db.query(Cluster).filter(Cluster.id == clone_payload['cluster_id']).first()
        api_token = proxmoxService.get_api_token(db, cluster_data.name)
        headers = {"Authorization": f"PVEAPIToken={api_token}"}
        PROXMOX_HOST = proxmoxService.getting_Proxmox_host(cluster_data)
        if not PROXMOX_HOST:
            raise RuntimeError("No reachable Proxmox host found for the cluster.")
 
        all_vms =proxmoxService.get_all_cluster_vms(db, cluster_data)
        existing_names = [
            vm["name"] for vm in all_vms if "name" in vm and vm["name"]
        ]
        db_names = [m.name for m in db.query(Machine).all()]
        all_existing_names = set(existing_names) | set(db_names)
        new_names = proxmoxService.generate_machine_name(
            clone_payload['name_template'],
            list(all_existing_names),
            clone_payload['count'],
        )
        existing_vmids = {
            int(vm["vmid"]) for vm in all_vms if "vmid" in vm and str(vm["vmid"]).isdigit()
        }
        template_node = None
        template_vm_id_str = str(clone_payload['template_vm_id'])
        for vm in all_vms:
            if str(vm.get("vmid")) == template_vm_id_str and vm.get("template") == 1:
                template_node = vm.get("node")
                break
 
        if not template_node:
            # Return error dict, not raise!
            return {
                "error": f"Template VM {clone_payload['template_vm_id']} not found in cluster.",
                "error_type": "template_missing"
            }
 
 
        def get_free_vmids(existing_vmids: set[int], needed: int, min_vmid: int = 100) -> list[int]:
            free_vmids = []
            vmid = min_vmid
            while len(free_vmids) < needed:
                if vmid not in existing_vmids and vmid not in free_vmids:
                    free_vmids.append(vmid)
                vmid += 1
            return free_vmids
 
        needed_count = len(new_names)
        free_vmids = get_free_vmids(existing_vmids, needed_count)
 
        assigned_nodes = []
        final_new_names = []
        final_vmids = []
        for i, new_name in enumerate(new_names):
            node = clone_payload['node'][i % len(clone_payload['node'])]
            db_machine = db.query(Machine).filter(Machine.name == new_name).first()
            if db_machine:
                continue
            assigned_nodes.append(node)
            final_new_names.append(new_name)
            final_vmids.append(free_vmids[i])
 
        vms = []
        clone_workflow_id = clone_payload.get("workflowId")  
   
        for new_name, new_vmid, node in zip(final_new_names, final_vmids, assigned_nodes):
            clone_url = f"{PROXMOX_HOST}/api2/json/nodes/{template_node}/qemu/{clone_payload['template_vm_id']}/clone"
            payload_dict = {
                "newid": new_vmid,
                "name": new_name,
                "target": node,
                "storage":os.getenv("PROXMOX_STORAGE"),
                "full": 1
            }
            try:
                response = requests.post(clone_url, headers=headers, data=payload_dict, verify=False)
                response.raise_for_status()
                upid = response.json()['data']
                vms.append((new_name, new_vmid, node, upid,clone_workflow_id))
               
            except requests.exceptions.RequestException as e:
                return {
                    "error": f"Error cloning VM {new_name} on node {node}: {e}",
                    "error_type": "clone_failed"
                }
        print(">>> RETURNING from clone_vm_activity")
 
        return {
            "message": f"{len(final_new_names)} VMs cloned successfully across {len(clone_payload['node'])} nodes",
            "vms": vms
        }
    except Exception as e:
        return {"error": str(e), "error_type": "activity_exception"}
 
    finally:
        db.close()
 
def netmask_to_cidr(netmask: str) -> int:
    """
    Converts a netmask like '255.255.252.0' to CIDR prefix length (e.g., 22).
    """
    return ipaddress.IPv4Network(f"0.0.0.0/{netmask}").prefixlen
 
 
def extract_node_from_upid(upid: str) -> str:
    return upid.split(":")[1]
 
@activity.defn
async def wait_for_vm_ready_activity(args: dict):
    db: Session = next(get_db())
    try:
        cluster_id = args["cluster_id"]
        upid = args["upid"]
        node = extract_node_from_upid(upid)
        cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster_data:
            raise Exception(f"Cluster {cluster_id} not found")
        api_token = proxmoxService.get_api_token(db, cluster_data.name)
        headers = {"Authorization": f"PVEAPIToken={api_token}"}
        proxmox_host = proxmoxService.getting_Proxmox_host(cluster_data)
        if not proxmox_host:
            raise Exception(f"No reachable Proxmox host found for cluster {cluster_id}")
        url = f"{proxmox_host}/api2/json/nodes/{node}/tasks/{upid}/status"
        while True:
            try:
                resp = requests.get(url, headers=headers, verify=False, timeout=10)
                resp.raise_for_status()
                data = resp.json()['data']
                if data['status'] == 'stopped':
                    if 'exitstatus' in data and data['exitstatus'] == 'OK':
                        return True
                    else:
                        raise Exception(f"Task failed: {data}")
            except requests.RequestException as ex:
                print(f"Error polling Proxmox task status: {ex}")
            await asyncio.sleep(2)
    finally:
        db.close()
 
 
 
@activity.defn
async def assign_ip_to_vm_activity(args: dict):
    db: Session = next(get_db())
    vmid = args["vmid"]
    cluster_id = args["cluster_id"]
    ip_address = args["ip_address"]
 
    # Re-query for cluster_data
    cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster_data:
        raise Exception(f"Cluster {cluster_id} not found")
    api_token = proxmoxService.get_api_token(db, cluster_data.name)
    headers = {"Authorization": f"PVEAPIToken={api_token}"}
    PROXMOX_HOST = proxmoxService.getting_Proxmox_host(cluster_data)
    nodes = get_all_nodes(cluster_data)
    # Fetch the pool for the IP
    ip_entry = db.query(IPEntry).filter(IPEntry.ip == ip_address).first()
    if not ip_entry:
        raise Exception(f"IP entry not found for {ip_address}")
    ip_pool = db.query(IPSModel).filter(IPSModel.id == ip_entry.pool_id).first()
    if not ip_pool:
        raise Exception(f"IP pool not found for id {ip_entry.pool_id}")
    subnet = ip_pool.Subnet
    gateway = ip_pool.Gateway
    cidr = netmask_to_cidr(subnet)
    ip_with_cidr = f"{ip_address}/{cidr}"
    for node in nodes:
        node_name = node["name"]
        vm_url = f"{PROXMOX_HOST}/api2/json/nodes/{node_name}/qemu/{vmid}/status/current"
        try:
            resp = requests.get(vm_url, headers=headers, verify=False, timeout=5)
            if resp.status_code == 200:
                config_url = f"{PROXMOX_HOST}/api2/json/nodes/{node_name}/qemu/{vmid}/config"
                payload = {
                    "ipconfig0": f"ip={ip_with_cidr},gw={gateway}"
                }
                config_response = requests.put(config_url, headers=headers, data=payload, verify=False, timeout=10)
                print(f"Assigned IP {ip_with_cidr} to VMID {vmid} on node {node_name}", config_response.json())
                return {
                    "message": f"IP {ip_with_cidr} assigned successfully on node {node_name}."
                }
        except requests.RequestException as ex:
            continue
    raise Exception(f"VMID {vmid} not found on any node. Cannot assign IP or reboot.")
 
 
 
# @activity.defn
# async def delete_proxmox_vm_activity(vmid: int, cluster_data: dict):
#     db: Session = next(get_db())
#     if not Cluster:
#         raise HTTPException(status_code=404, detail="No Proxmox cluster found in the database.")

#     api_token = proxmoxService.get_api_token(db, cluster_data["name"])
#     headers = {"Authorization": f"PVEAPIToken={api_token}"}
#     nodes = get_all_nodes(cluster_data)
#     PROXMOX_HOST = proxmoxService.getting_Proxmox_host(cluster_data)
#     if not nodes:
#         raise RuntimeError("No reachable Proxmox nodes found for the cluster.")

#     # Add purge and destroy-unreferenced-disks as query string parameters
#     params = {
#         "purge": 1,
#         "destroy-unreferenced-disks": 1
#     }

#     for node in nodes:
#         vm_url = f"{PROXMOX_HOST}/api2/json/nodes/{node['name']}/qemu/{vmid}/status/current"
#         try:
#             resp = requests.get(vm_url, headers=headers, verify=False, timeout=5)
#             if resp.status_code == 200:
#                 delete_url = f"{PROXMOX_HOST}/api2/json/nodes/{node['name']}/qemu/{vmid}"
#                 delete_resp = requests.delete(
#                     delete_url,
#                     headers=headers,
#                     params=params,  # Pass parameters here
#                     verify=False,
#                     timeout=10
#                 )
#                 delete_resp.raise_for_status()
#                 return {"message": f"VM with VMID {vmid} deleted successfully on node {node['name']}."}
#         except requests.RequestException:
#             continue

#     raise HTTPException(status_code=404, detail=f"VMID {vmid} not found on any node. Consider deleting the pool.")
 
RESERVED_TAG_KEYS = {
    '_measurement', '_field', '_value', '_time', '_start', '_stop', 'result', 'table'
}
 
@activity.defn
async def migrate_bucket_new_data_activity(payload: dict):
    """
    Migrates new data from the source InfluxDB to the destination InfluxDB,
    starting from payload['time_range_start'].
    Returns a dictionary with 'total' points migrated and 'max_time' migrated.
    """
    src_url = payload["src_url"]
    src_token = payload["src_token"]
    src_org = payload["src_org"]
    src_bucket = payload["src_bucket"]
    dst_url = payload["dst_url"]
    dst_token = payload["dst_token"]
    dst_org = payload["dst_org"]
    dst_bucket = payload["dst_bucket"]
    batch_size = payload["batch_size"]
    time_range_start = payload["time_range_start"]
 
    src_client = InfluxDBClient(url=src_url, token=src_token, org=src_org)
    dst_client = InfluxDBClient(url=dst_url, token=dst_token, org=dst_org)
    src_query = src_client.query_api()
    dst_write = dst_client.write_api(write_options=WriteOptions(batch_size=batch_size))
 
    query = f'from(bucket: "{src_bucket}") |> range(start: {time_range_start})'
    print(f"[Activity] Query: {query}")
    print("[Activity] Starting migration...")
    results = src_query.query(query)
    points = []
    total = 0
    max_time = time_range_start
 
    def is_valid_tag(k):
        return k not in RESERVED_TAG_KEYS
 
    for table in results:
        for record in table.records:
            measurement = record.get_measurement()
            field = record.get_field()
            value = record.get_value()
            time = record.get_time()  # This is a Python datetime object
 
            # Update max_time if needed
            if time and str(time) > str(max_time):
                max_time = str(time)
 
            p = Point(measurement).time(time)
            for k, v in record.values.items():
                if is_valid_tag(k) and v is not None:
                    p = p.tag(k, str(v))
            p = p.field(field, value)
            points.append(p)
 
            if len(points) >= batch_size:
                dst_write.write(bucket=dst_bucket, org=dst_org, record=points)
                total += len(points)
                print(f"[Activity] Written {total} points so far...")
                points = []
 
    if points:
        dst_write.write(bucket=dst_bucket, org=dst_org, record=points)
        total += len(points)
        print(f"[Activity] Written {total} points (final batch)")
 
    print(f"[Activity] Migration complete! Total points written: {total}")
    dst_write.close()
    src_client.close()
    dst_client.close()
    return {"total": total, "max_time": max_time}
 
 
@activity.defn
async def start_vm_proxmox_activity(vmid: int, pool_id: str,email:str = None):
    db: Session = next(get_db())  # or your session manager/context
    machine = db.query(Machine).filter(Machine.vm_id == vmid).first()
    if not machine:
        return {"status": "error", "msg": f"Machine with id {vmid} not found."}
 
    details = proxmoxService.collect_proxmox_details(vmid, pool_id, db)
    if details.get("status") != "success":
        machine.error_message = f"Start failed: {details.get('error', 'Unknown error')}"
        db.commit()
        return details
 
    PROXMOX_HOST = details["PROXMOX_HOST"]
    node = details["node"]
    vmid = details["vmid"]
    headers = details["headers"]
 
    vm_status = proxmoxService.vm_start(PROXMOX_HOST, node, vmid, headers)
    if vm_status is True:
        machine.error_message = "power-on"
        msg = "VM started successfully."
    elif isinstance(vm_status, dict) and vm_status.get("error"):
        # machine.error_message = f"Start failed: {vm_status['error']}"
        msg = f"Start failed: {vm_status['error']}"
 
    db.commit()
    print("VM Status:", vm_status)
    return {"vm_status": vm_status, "msg": msg}
 
 
@activity.defn
async def stop_vm_proxmox_activity(vmid: int, pool_id: str,email: str = None):
    db: Session = next(get_db())
    machine = db.query(Machine).filter(Machine.vm_id == vmid).first()
    if not machine:
        return {"status": "error", "msg": f"Machine with id {vmid} not found."}
    details = proxmoxService.collect_proxmox_details(vmid, pool_id, db)
    if details.get("status") != "success":
        machine.error_message = f"Stop failed: {details.get('error', 'Unknown error')}"
        db.commit()
        return details
 
    PROXMOX_HOST = details["PROXMOX_HOST"]
    node = details["node"]
    vmid = details["vmid"]
    headers = details["headers"]
 
    vm_status = proxmoxService.vm_stop(PROXMOX_HOST, node, vmid, headers)
    if vm_status is True:
        machine.error_message = "power-off"
        msg = "VM stopped successfully."
    elif isinstance(vm_status, dict) and vm_status.get("error"):
        # machine.error_message = f"Stop failed: {vm_status['error']}"
        msg = f"Stop failed: {vm_status['error']}"
 
    db.commit()
    print("VM Status:", vm_status)
    return {"vm_status": vm_status, "msg": msg}


@activity.defn
async def reboot_vm_proxmox_activity(vmid: int, pool_id: str,email: str = None):
    db: Session = next(get_db())  # or use a context/session manager as appropriate
    machine = db.query(Machine).filter(Machine.vm_id == vmid).first()
    if not machine:
        return {"status": "error", "msg": f"Machine with id {vmid} not found."}
    details = proxmoxService.collect_proxmox_details(vmid, pool_id, db)
    if details.get("status") != "success":
        machine.error_message = f"Reboot failed: {details.get('error', 'Unknown error')}"
        db.commit()
        return details
 
    PROXMOX_HOST = details["PROXMOX_HOST"]
    node = details["node"]
    vmid = details["vmid"]
    headers = details["headers"]
 
    vm_status = proxmoxService.vm_reboot(PROXMOX_HOST, node, vmid, headers)
    if vm_status.get("status") == "success":
        machine.error_message = "reboot..."
        msg = "VM rebooted successfully."
    elif vm_status.get("error"):
        # machine.error_message = f"Reboot failed: {vm_status['error']}"
        msg = f"Reboot failed: {vm_status['error']}"
   
    db.commit()
    print("VM Status:", vm_status)
    return {"vm_status": vm_status, "msg": msg}
 
 
@activity.defn
async def shutdown_vm_proxmox_activity(vmid: int, pool_id: str,email: str = None):
    # Do DB operations here!
    db: Session = next(get_db())
    machine = db.query(Machine).filter(Machine.vm_id == vmid).first()
    if not machine:
        return {"status": "error", "msg": f"Machine with id {vmid} not found."}
    details = proxmoxService.collect_proxmox_details(vmid, pool_id, db)
    if details.get("status") != "success":
        machine.error_message = f"Shutdown failed: {details.get('error', 'Unknown error')}"
        db.commit()
        return details
   
    PROXMOX_HOST = details["PROXMOX_HOST"]
    node = details["node"]
    vmid = details["vmid"]
    headers = details["headers"]
 
    vm_status = proxmoxService.vm_shutdown(PROXMOX_HOST, node, vmid, headers)
    if vm_status is True:
        machine.error_message = "shutdown..."
        msg = "VM shutdown successfully."
    elif isinstance(vm_status, dict) and vm_status.get("error"):
        # machine.error_message = f"Shutdown failed: {vm_status['error']}"
        msg = f"Shutdown failed: {vm_status['error']}"
   
    db.commit()
    
    return {"vm_status": vm_status, "msg": msg}

@activity.defn
async def vm_rebuild_activity(vmid: int, pool_id: str = None, email: str = None):
    db: Session = next(get_db())
    try:
        machine = db.query(Machine).filter(Machine.vm_id == vmid).first()
        if not machine:
            return {"status": "error", "error": f"Machine with vm_id {vmid} not found in DB."}
        if not machine.pool_id:
            return {"status": "error", "error": f"Machine {vmid} has no associated pool."}

        pool = db.query(Pool).filter(Pool.id == pool_id).first()
        if not pool or not pool.pool_template_vm_id:
            return {"status": "error", "error": f"Pool {pool_id} not found or has no templateid."}

        template_vm_id = pool.pool_template_vm_id
        cluster_id = pool.cluster_id.split("_")[1]
        cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster_data:
            return {"status": "error", "error": f"Cluster not found for pool {pool.id}."}

        api_token = proxmoxService.get_api_token(db, cluster_data.name)
        headers = {
            "Authorization": f"PVEAPIToken={api_token}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        PROXMOX_HOST = proxmoxService.getting_Proxmox_host(cluster_data)
        if not PROXMOX_HOST:
            return {"status": "error", "error": "No reachable Proxmox host found for the cluster."}

        all_vms = proxmoxService.get_all_cluster_vms(db, cluster_data)
        node = None
        for vm in all_vms:
            if str(vm.get("vmid")) == str(vmid):
                node = vm.get("node")
                break
        if not node:
            node = getattr(cluster_data, "nodes", ["pve"])[0]

        # --- Deletion logic for ALL cases (healthy/unhealthy) ---
        delete_url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}"
        resp = requests.get(delete_url, headers=headers, verify=False)
        if resp.status_code == 200:
            # VM exists: stop if running, then delete
            status_url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/status/current"
            status_resp = requests.get(status_url, headers=headers, verify=False)
            running = False
            if status_resp.status_code == 200:
                running = status_resp.json().get("data", {}).get("status") == "running"
            if running:
                
                proxmoxService.vm_stop(PROXMOX_HOST, node, vmid, headers)
                proxmoxService.vm_shutdown(PROXMOX_HOST, node, vmid, headers)
                proxmoxService.wait_for_vm_stopped(PROXMOX_HOST, node, vmid, headers, timeout=120)
           
            del_resp = requests.delete(delete_url, headers=headers, verify=False)
            del_resp.raise_for_status()
            await asyncio.sleep(5)  # Poll for deletion

        # --- Template node selection ---
        template_node = None
        for vm in all_vms:
            if str(vm.get("vmid")) == str(template_vm_id) and vm.get("template") == 1:
                template_node = vm.get("node")
                break
        if not template_node:
            return {"status": "error", "error": f"Template VM {template_vm_id} not found in cluster."}

        # --- Cloning logic ---
        try:
            clone_url = f"{PROXMOX_HOST}/api2/json/nodes/{template_node}/qemu/{template_vm_id}/clone"
            payload = {
                "newid": vmid,
                "name": machine.name,
                "target": node,
                "storage": "prxpool01",
                "full": 1
            }
            resp = requests.post(clone_url, headers=headers, data=payload, verify=False)
            resp.raise_for_status()
            upid = resp.json()["data"]
            print(f"Cloning VM {vmid} with name {machine.name} on node {node}, UPID: {upid}")

            
            # DO NOT assign IP here. Leave that to assign_ip_to_vm_activity!
     
            return {"status": "success", "upid": upid, "node": node, "cluster_id": cluster_id,"machine_name": machine.name,"ip_address": machine.hostname}
        except Exception as e:
            return {"status": "error", "error": "Cloning failed" }
    finally:
        db.close()




















 
 
 
 
 
 
 
 
 
 
 

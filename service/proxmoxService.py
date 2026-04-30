import asyncio
from collections import OrderedDict
from datetime import datetime
import ipaddress
import time 
from http.client import HTTPException
from typing import Dict, List, Optional
from fastapi.encoders import jsonable_encoder
import requests
import urllib3
from models.proxmox_model import Proxmox
from sqlalchemy.orm import Session
from models.models import Cluster, CreateClusterBase,Pool, Machine
from db_configuration.config import SessionLocal
import re
from service.temporalResource.workflows import workflows_proxmox
from utils.temporal_client import TemporalClientManager
from service.clusterService import getting_Proxmox_host,get_api_token,get_all_nodes
from models.proxmox_model import MetricServer
from service.pollingStatus import update_workflow_status
import logging, time

# Set logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)
 
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
VERIFY_SSL = False 
 
def get_all_proxmox_users(db):
    data=db.query(Proxmox).all()
    return data
 
def is_valid_ip(ip):
    return ip and ip.strip() not in {'0', '.', ''}

def get_cluster_nodes(cluster_data, db: Optional[Session] = None):
    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True
    try:
        return _get_cluster_nodes_impl(cluster_data, db)
    finally:
        if close_db:
            db.close()

def _get_cluster_nodes_impl(cluster_data, db: Session):
    api_token = get_api_token(db, cluster_data.name)
    headers = {
        "Authorization": f"PVEAPIToken={api_token}",
        "Content-Type": "application/json"
    }
 
    if isinstance(cluster_data.ip, str):
        ip_list = [ip.strip() for ip in cluster_data.ip.split(",") if is_valid_ip(ip.strip())]
    else:
        ip_list = [ip for ip in cluster_data.ip if is_valid_ip(ip)]
 
    if not ip_list:
        raise RuntimeError("No valid IPs found for cluster.")
 
    last_exception = None
    if cluster_data.type.lower() == "proxmox":
        for ip in ip_list:
            PROXMOX_HOST = f"https://{ip}:{cluster_data.port}"
            url = f"{PROXMOX_HOST}/api2/json/cluster/status"
            try:
                response = requests.get(url, headers=headers, verify=VERIFY_SSL, timeout=5)
                response.raise_for_status()
                data = response.json()
                nodes = [
                    {
                        "name": node["name"],
                        "ip": node["ip"],
                        "status": "online"
                    }
                    for node in data["data"]
                    if node.get("type") == "node" and node.get("online", 0) == 1
                ]
                return nodes
            except Exception as e:
                last_exception = e
                continue
    raise RuntimeError(f"All cluster IPs failed. Last error: {last_exception}")

def update_cluster_nodes(db: Session) -> Dict[str, List[str]]:
    clusters = db.query(Cluster).all()
    updated_clusters = {}
 
    for cluster in clusters:
        if not cluster.type or cluster.type.lower() != "proxmox":
            continue  # Skip non-proxmox clusters
 
        try:
            ip_list = cluster.ip.split(",") if isinstance(cluster.ip, str) else cluster.ip
            cluster_data = CreateClusterBase(
                type=cluster.type,
                name=cluster.name,
                ip=ip_list,
                port=cluster.port,
                username=cluster.username,
                password=cluster.password,
                tls=cluster.tls,
            )
            nodes = get_all_nodes(cluster_data)
            # Get all node IPs regardless of status
            all_ips = [node["ip"] for node in nodes]
            cluster.ip = ",".join(all_ips)
            db.add(cluster)
            updated_clusters[cluster.name] = all_ips
        except Exception as e:
            updated_clusters[cluster.name] = f"Failed to update: {e}"
 
    db.commit()
    return updated_clusters
 
def get_all_cluster_vms(db,cluster_data):
    api_token = get_api_token(db, cluster_data.name)
    headers = {
        "Authorization": f"PVEAPIToken={api_token}"
    }
    PROXMOX_HOST = getting_Proxmox_host(cluster_data)
    if not PROXMOX_HOST:
        raise RuntimeError("No reachable Proxmox host found for the cluster.")
    url = f"{PROXMOX_HOST}/api2/json/cluster/resources"
    response = requests.get(url, headers=headers, verify=False)
    response.raise_for_status()
    data = response.json()["data"]
    return data

def get_templates(db,cluster_data):
    all_vms = get_all_cluster_vms(db, cluster_data)
    templates = [
        {
            "name": vm["name"],
            "vmid": vm["vmid"],
            "status": vm["status"],
        }
        for vm in all_vms
        if vm.get("template") == 1
    ]
    return templates

def generate_machine_name(template: str, existing_names: list[str], count: int) -> list[str]:
    
    match = re.match(r"(.*)\{n:fixed=(\d+)\}(.*)", template)
    if not match:
        raise ValueError("Invalid template format. Expected pattern like 'amber-{n:fixed=3}'")

    prefix, width, suffix = match.groups()
    width = int(width)

    # Extract used numbers from existing names
    used_numbers = set()
    for name in existing_names:
        if name.startswith(prefix) and name.endswith(suffix):
            # middle = name[len(prefix):-len(suffix) if suffix else None]
            end = len(name) - len(suffix) if suffix else len(name)
            middle = name[len(prefix):end]
            if middle.isdigit() and len(middle) == width:
                used_numbers.add(middle)

    new_names = []
    i = 1
    while len(new_names) < count:
        if i not in used_numbers:
            formatted_number = str(i).zfill(width)
            new_names.append(f"{prefix}{formatted_number}{suffix}")
        i += 1

    return new_names

def unique_id():
    unique_id = datetime.now()
    logger.info(f"Generated unique ID - {unique_id}")
    return f"{unique_id.hour }:{unique_id.minute}:{unique_id.second}"

async def clone_vm(clone_payload: dict):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    workflow_id = f"clonevms-{uniqueId}"
    # clone_payload["workflowId"] = workflow_id
    try:
        clone_payload["workflowId"] = workflow_id
        handle = await client.start_workflow(
            workflows_proxmox.CloneVMWorkflow.run,
            clone_payload,
            id=workflow_id,
            task_queue="clonevm-task-queue",
        )
        result = await handle.result()  # Now gets actual workflow result
        if isinstance(result, dict) and "error" in result:
            return result
        return result 
    except Exception as e:
        return {"error": str(e)}
    

async def delete_proxmox_vm(vmid, cluster_data):
    db: Session = SessionLocal()
    try:
        if not Cluster:
            raise HTTPException(status_code=404, detail="No Proxmox cluster found in the database.")
        vmid=int(vmid)
        api_token = get_api_token(db, cluster_data.name)
        headers = {"Authorization": f"PVEAPIToken={api_token}"}
        nodes = get_cluster_nodes(cluster_data, db)
        PROXMOX_HOST = getting_Proxmox_host(cluster_data)
    finally:
        db.close()
    if not nodes:
        raise RuntimeError("No reachable Proxmox nodes found for the cluster.")

    for node in nodes:
        vm_url = f"{PROXMOX_HOST}/api2/json/nodes/{node['name']}/qemu/{vmid}/status/current"
        try:
            resp = requests.get(vm_url, headers=headers, verify=False, timeout=5)
            if resp.status_code == 200:
                vm_status = resp.json()["data"].get("status")
                # If running, stop the VM first
                if vm_status == "running":
                    stop_url = f"{PROXMOX_HOST}/api2/json/nodes/{node['name']}/qemu/{vmid}/status/stop"
                    stop_resp = requests.post(stop_url, headers=headers, verify=False, timeout=10)
                    stop_resp.raise_for_status()
                    # Optional: Wait until stopped (polling)
                    import time
                    for _ in range(12):  # Wait max ~60s
                        status_check = requests.get(vm_url, headers=headers, verify=False, timeout=5)
                        new_status = status_check.json()["data"].get("status")
                        if new_status != "running":
                            break
                        time.sleep(5)
                # Now delete the VM
                delete_url = f"{PROXMOX_HOST}/api2/json/nodes/{node['name']}/qemu/{vmid}"
                delete_resp = requests.delete(delete_url, headers=headers, verify=False, timeout=10)
                delete_resp.raise_for_status()
                return {"message": f"VM with VMID {vmid} stopped (if running) and deleted successfully on node {node['name']}."}
        except requests.RequestException:
            continue  # Try next node

    raise HTTPException(status_code=404, detail=f"VMID {vmid} not found on any node. Consider deleting the pool.")
 
async def update_metric_server_token(cluster_id: int, new_token: str):
    db: Optional[Session] = None
    try:
        db = SessionLocal()
        ms = db.query(MetricServer).filter(MetricServer.cluster_id == cluster_id).first()
        if ms:
            ms.token = new_token
            db.commit()
        else:
            logger.warning(f"No MetricServer found for cluster_id={cluster_id}")
    except Exception as e:
        logger.error(f"Error updating MetricServer token for cluster_id={cluster_id}: {e}")
    finally:
        if db:
            db.close()

 

async def migrate_bucket_all_data(migration_payload: dict):
    uniqueId = unique_id()  # Or use any unique ID generator you have
    client = await TemporalClientManager.get_temporal_client()
    workflow_id = f"Migration-{uniqueId}"
    SRC_BUCKET = migration_payload.get("src_bucket")
    DST_BUCKET = migration_payload.get("dst_bucket")
    userName = migration_payload.get("email", "unknown_user")

    try:
        handle = await client.start_workflow(
            workflows_proxmox.LiveMigrateWorkflow.run,
            migration_payload,
            id=workflow_id,
            task_queue="migration-task-queue",
            search_attributes={
            "Entity": [f"({SRC_BUCKET}) --> ({DST_BUCKET})"],
            "Action": ["DB Migration"],
            "UserName": [userName]
        },
        )
        return {
            "status": "Migration workflow started.",
            "workflow_id": handle.id,
            "run_id": handle.run_id,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

def get_metric_server_from_db(cluster_id: int) -> Optional[MetricServer]:
    db: Optional[Session] = None
    try:
        db = SessionLocal()
        ms = db.query(MetricServer).filter(MetricServer.cluster_id == cluster_id).first()
        return ms
    except Exception as e:
        logger.error(f"Error fetching MetricServer for cluster_id={cluster_id}: {e}")
        return jsonable_encoder("error",e)
    finally:
        if db:
            db.close()



#---------------------------proxmox power state operations---------------------------

def collect_proxmox_details(vmid, pool_id, db):
    machine = db.query(Machine).filter(Machine.vm_id == str(vmid)).one_or_none()
    if not machine:
        return {"status": "error", "error": f"Machine with vm_id {vmid} not found in DB."}
    if not machine.pool_id:
        return {"status": "error", "error": f"Machine {vmid} has no associated pool."}
    pool = db.query(Pool).filter(Pool.id == pool_id).one_or_none()
    if not pool or not pool.pool_template_vm_id:
        return {"status": "error", "error": f"Pool {machine.pool_id} not found or has no templateid."}
 
    cluster_id = pool.cluster_id.split("_")[1]
    cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).one_or_none()
    if not cluster_data:
        return {"status": "error", "error": f"Cluster not found for pool {pool.id}."}
 
    api_token = get_api_token(db, cluster_data.name)
    headers = {
        "Authorization": f"PVEAPIToken={api_token}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    PROXMOX_HOST = getting_Proxmox_host(cluster_data)
    if not PROXMOX_HOST:
        return {"status": "error", "error": "No reachable Proxmox host found for the cluster."}
    # Get all VMs in the cluster
    all_vms = get_all_cluster_vms(db, cluster_data)
    node = None
    for vm in all_vms:
        if str(vm.get("vmid")) == str(vmid):
            node = vm.get("node")
            vm_status = vm.get("status")
            break
    if not node:
        return {"status": "error", "error": f"VM {vmid} not found in cluster."}
    return {
        "status": "success",
        "PROXMOX_HOST": PROXMOX_HOST,
        "node": node,
        "vmid": vmid,
        "headers": headers
    }
 

def vm_start(PROXMOX_HOST, node, proxmox_vmid, headers):
    start_url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{proxmox_vmid}/status/start"
    resp = requests.post(start_url, headers=headers, verify=False)
    if resp.status_code not in (200, 202):
        return False
    return True
def vm_stop(PROXMOX_HOST, node, vmid, headers):
    
    stop_url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/status/stop"
    resp = requests.post(stop_url, headers=headers, verify=False)
    if resp.status_code not in (200, 202):
        return False
    return True
def vm_reboot(PROXMOX_HOST, node, vmid, headers):
    reboot_url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/status/reboot"
    resp = requests.post(reboot_url, headers=headers, verify=False)
    if resp.status_code not in (200, 202):
        return {"error": f"HTTP {resp.status_code}, {resp.text}"}
    return {"status": "success"}

def vm_shutdown(PROXMOX_HOST, node, vmid, headers):
    shutdown_url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/status/shutdown"
    resp = requests.post(shutdown_url, headers=headers, verify=False)
    if resp.status_code not in (200, 202):
        return False
    return True

#----------------------temporal conversion  ----------------------



async def start_vm_proxmox(vmid: str, pool_id: str, email: str, cluster_type: str):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    workflow_id = f"start_vm_{cluster_type}-{uniqueId}"
    # clone_payload["workflowId"] = workflow_id
    try:
        # clone_payload["workflowId"] = workflow_id
        handle = await client.start_workflow(
            workflows_proxmox.StartVMProxmoxWorkflow.run,
            args=[vmid, pool_id],
            id=workflow_id,
            task_queue="vmpower-task-queue",
            search_attributes={
                "Entity": [str(vmid)],
                "Action": [f"start_vm_{cluster_type}"],
                "UserName": [email]
            }
        )
        result = await handle.result()  # Now gets actual workflow result
        if isinstance(result, dict) and "error" in result:
            return result
        return result 
    except Exception as e:
        return {"error": str(e)}



async def stop_vm_proxmox(vmid: str, pool_id: str,email: str, cluster_type: str):

    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    workflow_id = f"stop_vm_{cluster_type}-{uniqueId}"

    try:
        handle = await client.start_workflow(
            workflows_proxmox.StopVMProxmoxWorkflow.run,
            args=[vmid, pool_id],
            id=workflow_id,
            task_queue="vmpower-task-queue",
            search_attributes={
                "Entity": [str(vmid)],
                "Action": ["stop_vm_proxmox"],
                "UserName": [email]
            }
        )
        result = await handle.result()  # Now gets actual workflow result
        if isinstance(result, dict) and "error" in result:
            return result
        return result 
    except Exception as e:
        return {"error": str(e)}
    
    
async def reboot_vm_proxmox(vmid: str, pool_id: str,email: str, cluster_type: str):

    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    workflow_id = f"reboot_vm_{cluster_type}-{uniqueId}"

    try:
        handle = await client.start_workflow(
            workflows_proxmox.RebootVMProxmoxWorkflow.run,
            args=[vmid, pool_id],
            id=workflow_id,
            task_queue="vmpower-task-queue",
            search_attributes={
                "Entity": [str(vmid)],
                "Action": [f"reboot_vm_{cluster_type}"],
                "UserName": [email]
            }
        )
        result = await handle.result()  # Now gets actual workflow result
        if isinstance(result, dict) and "error" in result:
            return result
        return result 
    except Exception as e:
        return {"error": str(e)}
    

async def shutdown_vm_proxmox(vmid: str, pool_id: str, email: str, cluster_type: str):

    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    workflow_id = f"shutdown_vm_{cluster_type}-{uniqueId}"

    try:
        handle = await client.start_workflow(
            workflows_proxmox.ShutdownVMProxmoxWorkflow.run,
            args=[vmid, pool_id],
            id=workflow_id,
            task_queue="vmpower-task-queue",
            search_attributes={
                "Entity": [str(vmid)],
                "Action": [f"shutdown_vm_{cluster_type}"],  
                "UserName": [email]
            }
        )
        result = await handle.result()  # Now gets actual workflow result
        if isinstance(result, dict) and "error" in result:
            return result
        return result 
    except Exception as e:
        return {"error": str(e)}
    

def wait_for_vm_stopped(PROXMOX_HOST, node, vmid, headers, timeout=120):
    status_url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/status/current"
    waited = 0
    while waited < timeout:
        resp = requests.get(status_url, headers=headers, verify=False)
        if resp.status_code == 200:
            status = resp.json().get("data", {}).get("status")
            if status == "stopped":
                return True
        time.sleep(5)
        waited += 5
    return False
def update_workflow_ids(current_ids, new_rebuild_id, new_assign_ip_id):
    updated_ids = current_ids.copy()
    while len(updated_ids) < 3:
        updated_ids.append(None)
    if new_rebuild_id:
        updated_ids[1] = new_rebuild_id
    if new_assign_ip_id:
        updated_ids[2] = new_assign_ip_id
    return updated_ids
def update_workflow_status_dict(workflow_status_dict, new_rebuild_id, new_assign_ip_id, rebuild_status, rebuild_error, assign_ip_status, assign_ip_error):

    ordered_status = OrderedDict(workflow_status_dict)
    keys = list(ordered_status.keys())

    # Replace 2nd entry with new_rebuild_id
    if len(keys) >= 2:
        keys[1] = new_rebuild_id
    elif len(keys) == 1:
        keys.append(new_rebuild_id)
    else:
        keys.extend([None, new_rebuild_id])

    # Replace 3rd entry with new_assign_ip_id
    if len(keys) >= 3:
        keys[2] = new_assign_ip_id
    elif len(keys) == 2:
        keys.append(new_assign_ip_id)
    else:
        keys.extend([None, None, new_assign_ip_id])

    new_status = OrderedDict()
    for i, k in enumerate(keys):
        if i == 1 and new_rebuild_id:
            new_status[new_rebuild_id] = {"status": rebuild_status, "error": rebuild_error}
        elif i == 2 and new_assign_ip_id:
            new_status[new_assign_ip_id] = {"status": assign_ip_status, "error": assign_ip_error}
        else:
            new_status[k] = workflow_status_dict.get(k, {"status": None, "error": None})
    return dict(new_status)

async def vm_rebuild(vmid: int, pool_id: str):
    db: Session = SessionLocal()
    try:
        uniqueId = unique_id()
        client = await TemporalClientManager.get_temporal_client()
        workflow_id = f"vmrebuild-{uniqueId}"
        try:
            # Start the VM rebuild workflow
            handle = await client.start_workflow(
                workflows_proxmox.VmRebuildWorkflow.run,
                args=[vmid, pool_id],
                id=workflow_id,
                task_queue="vm-rebuild-task-queue",
            )

            # Set status to RUNNING as soon as workflow is started
            machine_data = db.query(Machine).filter(Machine.vm_id == str(vmid)).first()
            if machine_data:
                current_ids = machine_data.workflowId or []
                new_rebuild_id = workflow_id
                new_assign_ip_id = None
                updated_ids = update_workflow_ids(current_ids, new_rebuild_id, new_assign_ip_id)
                machine_data.workflowId = updated_ids

                current_status = machine_data.workflow_status or {}
                updated_status = update_workflow_status_dict(
                    current_status,
                    new_rebuild_id,
                    new_assign_ip_id,
                    "RUNNING",
                    "",
                    "RUNNING",
                    ""
                )
                machine_data.workflow_status = updated_status
                db.commit()
                db.refresh(machine_data)

                # Also call update_workflow_status for rebuild workflow RUNNING
                update_workflow_status(db, machine_id=machine_data.id, wfid=new_rebuild_id, status="RUNNING", error="")

            result = await handle.result()  # Should contain child workflow id
            logger.info(f"VM Rebuild workflow completed with result: {result}")

            # Update with child workflow id after result
            if machine_data:
                new_assign_ip_id = result.get("wait_and_assign_result")

                # Update workflowId with child workflow
                current_ids = machine_data.workflowId or []
                updated_ids = update_workflow_ids(current_ids, new_rebuild_id, new_assign_ip_id)
                machine_data.workflowId = updated_ids

                # Determine statuses
                if isinstance(result, dict) and "error" in result:
                    rebuild_status = "FAILED"
                    rebuild_error = result["error"]
                    assign_ip_status = machine_data.workflow_status.get(new_assign_ip_id, {}).get("status", "RUNNING")
                    assign_ip_error = machine_data.workflow_status.get(new_assign_ip_id, {}).get("error", "")
                else:
                    rebuild_status = "COMPLETED"
                    rebuild_error = ""
                    assign_ip_status = "RUNNING" if new_assign_ip_id else None
                    assign_ip_error = ""

                # Update workflow_status dict for 2nd and 3rd entries
                current_status = machine_data.workflow_status or {}
                updated_status = update_workflow_status_dict(
                    current_status,
                    new_rebuild_id,
                    new_assign_ip_id,
                    rebuild_status,
                    rebuild_error,
                    assign_ip_status,
                    assign_ip_error
                )
                machine_data.workflow_status = updated_status
                db.commit()
                db.refresh(machine_data)

                # Now call update_workflow_status for both main and child workflows
                update_workflow_status(db, machine_id=machine_data.id, wfid=new_rebuild_id, status=rebuild_status, error=rebuild_error, vm_status="")
                if new_assign_ip_id and assign_ip_status:
                    update_workflow_status(db, machine_id=machine_data.id, wfid=new_assign_ip_id, status=assign_ip_status, error=assign_ip_error)
            return result
        except Exception as e:
            db.rollback()
            machine_data = db.query(Machine).filter(Machine.vm_id == str(vmid)).first()
            if machine_data:
                # On error, update workflow_status for rebuild workflow
                current_status = machine_data.workflow_status or {}
                updated_status = update_workflow_status_dict(
                    current_status,
                    workflow_id,
                    None,
                    "FAILED",
                    str(e),
                    None,
                    None
                )
                machine_data.workflow_status = updated_status
                db.commit()
                db.refresh(machine_data)
                update_workflow_status(db, machine_id=machine_data.id, wfid=workflow_id, status="FAILED", error=str(e))
            return {"error": str(e)}
    finally:
        db.close()




def get_vm_config(db, cluster_data, node, vmid):
    api_token = get_api_token(db, cluster_data.name)
    headers = {
        "Authorization": f"PVEAPIToken={api_token}"
    }
    PROXMOX_HOST = getting_Proxmox_host(cluster_data)
    url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/config"
    response = requests.get(url, headers=headers, verify=False)
    response.raise_for_status()
    return response.json()["data"]

def get_vm_datastores_from_config(config):
    datastores = []
    for key, value in config.items():
        # if key.startswith(("scsi", "ide", "sata", "virtio")) and isinstance(value, str):
        if key.startswith(("ide")) and isinstance(value, str):
            datastore = value.split(":")[0]
            datastores.append(datastore)
    return list(set(datastores))

def get_vm_ip_addresses(db, cluster_data, node, vmid):
    api_token = get_api_token(db, cluster_data.name)
    headers = {
        "Authorization": f"PVEAPIToken={api_token}"
    }
    PROXMOX_HOST = getting_Proxmox_host(cluster_data)
    url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/agent/network-get-interfaces"

    try:
        
        start_time = time.time()
        response = requests.get(url, headers=headers, verify=False, timeout=5)
        time_taken = time.time() - start_time
        logger.info(f"API call took {time_taken:.2f} seconds")
        response.raise_for_status()
        data = response.json().get("data", {})

        if not isinstance(data, dict) or "result" not in data:
            return []

        interfaces = data["result"]
        ip_addresses = []

        for iface in interfaces:
            for ip in iface.get("ip-addresses", []):
                if ip.get("ip-address-type") == "ipv4":
                    ip_addr = ip.get("ip-address")
                    try:
                        ip_obj = ipaddress.ip_address(ip_addr)

                        # Skip loopback, link-local, multicast
                        if ip_obj.is_loopback or ip_obj.is_link_local or ip_obj.is_multicast:
                            continue

                        ip_addresses.append(ip_addr)

                    except ValueError:
                        continue
        return ip_addresses 

    except requests.RequestException:
        return []
    except Exception:
        return []

def get_all_vm_details(db, cluster_data):
    vms = get_all_cluster_vms(db, cluster_data)
    vm_info_list = []
    for vm in vms:
        if vm['type'] == 'qemu':
            node = vm['node']
            vmid = vm['vmid']
            try:
                config = get_vm_config(db, cluster_data, node, vmid)
                datastores = get_vm_datastores_from_config(config)
                agent_enabled = bool(config.get("agent", 0))
                ip_addresses = get_vm_ip_addresses(db, cluster_data, node, vmid) if agent_enabled else []

                vm_info_list.append({
                    "vmid": vmid,
                    "node": node,
                    "name": vm.get("name", ""),
                    "datastores": datastores,
                    "agent_enabled": agent_enabled,
                    "ip_addresses": ip_addresses
                })
            except Exception as e:
                vm_info_list.append({
                    "vmid": vmid,
                    "node": node,
                    "name": vm.get("name", ""), 
                    "datastores": [],
                    "agent_enabled": False,
                    "ip_addresses": [],
                    "error": str(e)
                })
    return vm_info_list

async def get_vm_detail(db, cluster_data, vm):
    loop = asyncio.get_running_loop()
 
    node = vm['node']
    vmid = vm['vmid']
 
    try:
        # Run blocking functions in thread pool
        config = await loop.run_in_executor(None, get_vm_config, db, cluster_data, node, vmid)
        datastores = get_vm_datastores_from_config(config)
        agent_enabled = bool(config.get("agent", 0))
 
        if agent_enabled:
            ip_addresses = await loop.run_in_executor(None, get_vm_ip_addresses, db, cluster_data, node, vmid)
        else:
            ip_addresses = []
 
        return {
            "vmid": vmid,
            "node": node,
            "name": vm.get("name", ""),
            "datastores": datastores,
            "agent_enabled": agent_enabled,
            "ip_addresses": ip_addresses
        }
 
    except Exception as e:
        return {
            "vmid": vmid,
            "node": node,
            "name": vm.get("name", ""),
            "datastores": [],
            "agent_enabled": False,
            "ip_addresses": [],
            "error": str(e)
        }
 
 
async def get_all_vm_details_parallel(db, cluster_data):
    loop = asyncio.get_running_loop()
 
    vms = await loop.run_in_executor(None, get_all_cluster_vms, db, cluster_data)
    qemu_vms = [vm for vm in vms if vm['type'] == 'qemu']
 
    tasks = [get_vm_detail(db, cluster_data, vm) for vm in qemu_vms]
    vm_info_list = await asyncio.gather(*tasks, return_exceptions=False)
    return vm_info_list
 
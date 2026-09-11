"""
proxmoxService — service layer for the Proxmox VM lifecycle + power-state + inventory.

clusterService.py handles *cluster/user provisioning* on Proxmox; this module works with
the actual VMs inside that cluster — clone/delete/rebuild (via Temporal workflows) and
power operations (start/stop/reboot/shutdown, either directly via the Proxmox REST API or
through Temporal — both paths exist) + VM inventory/details (nodes, GPUs, IPs, datastores)
by querying Proxmox directly.
Used by: the VM/Proxmox endpoints in controllers/routes.py.
"""
import asyncio
from collections import OrderedDict
from datetime import datetime
import ipaddress
import time 
from http.client import HTTPException
from typing import Dict, List, Optional
from fastapi.encoders import jsonable_encoder
import requests
import urllib3 # test edit
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

# Logging is configured centrally in utils/logging_config.py (called from
# main.py at startup) — do not reconfigure it per-module.
logger = logging.getLogger(__name__)
 
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
VERIFY_SSL = False 
 
def get_all_proxmox_users(db):
    """Return all Proxmox (dedicated-user-per-cluster) DB records. (clusterService.py has a same-name function too.)"""
    data=db.query(Proxmox).all()
    return data

def is_valid_ip(ip):
    """Basic sanity check — reject placeholder/empty IP values ('0', '.', '')."""
    return ip and ip.strip() not in {'0', '.', ''}

def get_cluster_nodes(cluster_data, db: Optional[Session] = None):
    """
    List the cluster's online nodes (a DB-session-managing wrapper around `_get_cluster_nodes_impl`
    — if no db is given, it opens a new session and closes it too).
    Returns: list of {"name","ip","status":"online"}.
    """
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
    """Fetch the cluster's online nodes (Proxmox `/cluster/status`), trying all IPs. Returns list of {"name","ip","status"}. Raises RuntimeError if all IPs fail."""
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

def _resolve_gpu_name(device: Dict) -> str:
    """Extract the best-available GPU display name from a PCI device dict (device_name → subsystem_device_name → vendor+device-id fallback)."""
    if device.get("device_name"):
        return device["device_name"]
    if device.get("subsystem_device_name"):
        return device["subsystem_device_name"]
    vendor = device.get("vendor_name") or device.get("subsystem_vendor_name") or "Unknown Vendor"
    device_id = device.get("device", "")
    return f"{vendor} (Device ID: {device_id})"

def get_node_gpus(cluster_data, nodes: List[str], db: Session) -> Dict:
    """
    List the given nodes' PCI display-controller (class 0x03xx) devices as GPUs
    (Proxmox `/nodes/{node}/hardware/pci`).
    Returns: {node_name: list of GPU dicts (each with an added "gpu_name")}, or
             {node_name: {"error": "..."}} if the query fails for that node.
    """
    api_token = get_api_token(db, cluster_data.name)
    headers = {
        "Authorization": f"PVEAPIToken={api_token}",
        "Content-Type": "application/json"
    }
    PROXMOX_HOST = getting_Proxmox_host(cluster_data)
    result = {}
    for node in nodes:
        url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/hardware/pci"
        try:
            response = requests.get(url, headers=headers, verify=VERIFY_SSL, timeout=10)
            response.raise_for_status()
            pci_devices = response.json().get("data", [])
            # Display controllers: 0x0300 (VGA), 0x0302 (3D), 0x0380 (generic display)
            gpus = [
                {**d, "gpu_name": _resolve_gpu_name(d)}
                for d in pci_devices
                if str(d.get("class", "")).lower().startswith("0x03")
            ]
            result[node] = gpus
        except Exception as e:
            result[node] = {"error": str(e)}
    return result

def update_cluster_nodes(db: Session) -> Dict[str, List[str]]:
    """
    Refresh the `Cluster.ip` column of all Proxmox-type clusters with their actual current node IPs
    (so the cluster IP-list does not go stale when nodes join/leave).
    Returns: {cluster_name: list-of-ips-or-error-string}.
    """
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
    """
    Fetch all of the cluster's resources (VMs, containers, storage, etc.) from Proxmox
    (`/cluster/resources` — no type filter; the caller must filter by the `type` field themselves).
    Returns: list of raw Proxmox resource dicts.
    Raises: RuntimeError if the host is not reachable; requests.HTTPError on API failure.
    """
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
    """List the cluster's VM templates (Proxmox VMs flagged `template==1`). Returns list of {"name","vmid","status"}."""
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
    """
    Generate `count` unique new machine names from a template pattern (e.g. "amber-{n:fixed=3}"),
    skipping numbers already used in existing_names.
    Returns: list[str] of new names (length == count).
    Raises: ValueError if the template pattern format is invalid.
    """
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
        formatted_number = str(i).zfill(width)
        if formatted_number not in used_numbers:
            new_names.append(f"{prefix}{formatted_number}{suffix}")
        i += 1
    return new_names

def unique_id():
    """Build an `HH:MM:SS` string from the current time, for use as a workflow-id suffix."""
    unique_id = datetime.now()
    logger.info(f"Generated unique ID - {unique_id}")
    return f"{unique_id.hour }:{unique_id.minute}:{unique_id.second}"

async def clone_vm(clone_payload: dict):
    """
    Start `CloneVMWorkflow` and await its result (the Proxmox VM clone orchestration entry point).
    Returns: workflow result dict on success, {"error": "..."} dict on failure
             (exceptions are not raised, they are wrapped in a dict).
    """
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
    """
    Delete a Proxmox VM directly via the REST API (no Temporal involved) — tries all the
    cluster's nodes until the VM is found; if it is running, stop it first (polling for ~60s
    to confirm), then delete.
    Returns: {"message": "..."} on success.
    Raises: HTTPException 404 if the vmid is not found on any node.
    """
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
    """
    Update a cluster's stored MetricServer.token with a new token (a silent no-op if the
    record is not found — exceptions are only logged, not raised).
    Returns: None (side effect only).
    """
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
    """
    Start `LiveMigrateWorkflow` — migrates data from src_bucket to dst_bucket
    (fire-and-forget, does not wait for the result — workflow_id/run_id are returned immediately).
    Returns: {"status","workflow_id","run_id"}.
    Raises: HTTPException 500 if the workflow fails to start.
    """
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
    """
    Fetch a cluster's MetricServer DB record (clusterService.py has a same-name function too,
    but this one's error path returns `jsonable_encoder("error", e)` — which, being a two-arg
    call, raises a TypeError — so instead of returning it may accidentally crash; this doc-pass
    only flags the behavior, it does not fix it).
    Returns: MetricServer ORM object, or None if not found.
    """
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
    """
    Gather the context needed to run a VM's power operations (start/stop/reboot/shutdown):
    resolves the Machine → Pool → Cluster chain to prepare the API token, PROXMOX_HOST,
    the node (where the VM is running), and the headers.
    Returns: {"status":"success","PROXMOX_HOST","node","vmid","headers"} on success,
             {"status":"error","error":"..."} if any step fails to resolve.
    """
    machine = db.query(Machine).filter(Machine.vm_id == str(vmid)).one_or_none()
    if not machine:
        return {"status": "error", "error": f"Machine with vm_id {vmid} not found in DB."}
    if not machine.pool_id:
        return {"status": "error", "error": f"Machine {vmid} has no associated pool."}
    pool = db.query(Pool).filter(Pool.id == pool_id).one_or_none()
    if not pool or not pool.pool_template_vm_id:
        return {"status": "error", "error": f"Pool {machine.pool_id} not found or has no templateid."}
 
    cluster_id_raw = str(pool.cluster_id)
    if "_" in cluster_id_raw:
        cluster_id = cluster_id_raw.split("_")[1]
    else:
        cluster_id = cluster_id_raw
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
    """Start a VM via the Proxmox API. Returns bool (True on 200/202)."""
    start_url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{proxmox_vmid}/status/start"
    resp = requests.post(start_url, headers=headers, verify=False)
    if resp.status_code not in (200, 202):
        return False
    return True
def vm_stop(PROXMOX_HOST, node, vmid, headers):
    """(Hard-)stop a VM via the Proxmox API. Returns bool (True on 200/202)."""
    stop_url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/status/stop"
    resp = requests.post(stop_url, headers=headers, verify=False)
    if resp.status_code not in (200, 202):
        return False
    return True
def vm_reboot(PROXMOX_HOST, node, vmid, headers):
    """Reboot a VM via the Proxmox API. Returns {"status":"success"} or {"error":"..."} (in-band, no exception)."""
    reboot_url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/status/reboot"
    resp = requests.post(reboot_url, headers=headers, verify=False)
    if resp.status_code not in (200, 202):
        return {"error": f"HTTP {resp.status_code}, {resp.text}"}
    return {"status": "success"}

def vm_shutdown(PROXMOX_HOST, node, vmid, headers):
    """Gracefully shut down a VM via the Proxmox API (an ACPI signal, giving the guest OS time to respond). Returns bool (True on 200/202)."""
    shutdown_url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/status/shutdown"
    resp = requests.post(shutdown_url, headers=headers, verify=False)
    if resp.status_code not in (200, 202):
        return False
    return True

#----------------------temporal conversion  ----------------------



async def start_vm_proxmox(vmid: str, pool_id: str, email: str, cluster_type: str):
    """
    Start `StartVMProxmoxWorkflow` and await its result (orchestrated VM power-on).
    Returns: workflow result dict on success, {"error": "..."} dict on failure (not raised).
    """
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
    """
    Start `StopVMProxmoxWorkflow` and await its result (orchestrated VM hard stop).
    Returns: workflow result dict on success, {"error": "..."} dict on failure (not raised).
    """
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
    """
    Start `RebootVMProxmoxWorkflow` and await its result (orchestrated VM reboot).
    Returns: workflow result dict on success, {"error": "..."} dict on failure (not raised).
    """
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
    """
    Start `ShutdownVMProxmoxWorkflow` and await its result (orchestrated VM graceful shutdown).
    Returns: workflow result dict on success, {"error": "..."} dict on failure (not raised).
    """
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
    """Poll (5s interval) until the VM status becomes "stopped" or the timeout is reached. Returns bool."""
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
    """
    Replace the 2nd/3rd slots of the Machine.workflowId list with the new rebuild/assign-ip workflow-ids
    (padding the list with None as needed). Returns the updated list (length at least 3).
    """
    updated_ids = current_ids.copy()
    while len(updated_ids) < 3:
        updated_ids.append(None)
    if new_rebuild_id:
        updated_ids[1] = new_rebuild_id
    if new_assign_ip_id:
        updated_ids[2] = new_assign_ip_id
    return updated_ids
def update_workflow_status_dict(workflow_status_dict, new_rebuild_id, new_assign_ip_id, rebuild_status, rebuild_error, assign_ip_status, assign_ip_error):
    """
    Replace the 2nd/3rd (positional) entries of the Machine.workflow_status dict with the new
    rebuild/assign-ip workflow-ids + statuses/errors, preserving order.
    Returns: a new dict (workflow_id -> {"status","error"}).
    """
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
    """
    Start `VmRebuildWorkflow` (re-provision the VM from a fresh disk — wait-ready/assign-ip/
    power-on all run inline inside the workflow) and track the machine's `workflow_status`/`workflowId`
    throughout (RUNNING → COMPLETED/FAILED).
    Returns: workflow result dict on success, {"error": "..."} dict on failure (not raised).
    """
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

            # Set status to RUNNING as soon as workflow is started.
            #
            # NOTE: VmRebuildWorkflow runs wait-ready/assign-ip/power-on all
            # INLINE now (see workflows_proxmox.VmRebuildWorkflow.run) — it no
            # longer spawns a separate child "assign IP" workflow. The old
            # code below still tried to track that nonexistent child workflow
            # via result.get("wait_and_assign_result"), which no longer
            # exists in the result dict, so it was always None. Passing that
            # None through update_workflow_status_dict inserted a phantom
            # `None`-keyed entry into machine.workflow_status on every single
            # rebuild — and since pollingStatus.update_workflow_status treats
            # any non-"COMPLETED" entry (including that phantom None) as
            # blocking overall completion, machine.status came out blank
            # (None) after every rebuild, success or failure. Track only the
            # one real workflow — the rebuild itself — to fix this.
            machine_data = db.query(Machine).filter(Machine.vm_id == str(vmid)).first()
            if machine_data:
                current_ids = machine_data.workflowId or []
                while len(current_ids) < 2:
                    current_ids.append(None)
                current_ids[1] = workflow_id
                machine_data.workflowId = current_ids
                db.commit()
                db.refresh(machine_data)

                update_workflow_status(db, machine_id=machine_data.id, wfid=workflow_id, status="RUNNING", error="")

            result = await handle.result()
            logger.info(f"VM Rebuild workflow completed with result: {result}")

            if machine_data:
                if isinstance(result, dict) and "error" in result:
                    rebuild_status = "FAILED"
                    rebuild_error = result["error"]
                else:
                    rebuild_status = "COMPLETED"
                    rebuild_error = ""

                update_workflow_status(db, machine_id=machine_data.id, wfid=workflow_id, status=rebuild_status, error=rebuild_error)
            return result
        except Exception as e:
            db.rollback()
            machine_data = db.query(Machine).filter(Machine.vm_id == str(vmid)).first()
            if machine_data:
                update_workflow_status(db, machine_id=machine_data.id, wfid=workflow_id, status="FAILED", error=str(e))
            return {"error": str(e)}
    finally:
        db.close()




def get_vm_config(db, cluster_data, node, vmid):
    """Fetch a VM's full config (disks, agent flag, etc.) from Proxmox (`/qemu/{vmid}/config`). Returns config dict."""
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
    """Extract the attached datastore names from a VM config dict (only checks keys with the "ide" prefix). Returns list[str] (deduped)."""
    datastores = []
    for key, value in config.items():
        # if key.startswith(("scsi", "ide", "sata", "virtio")) and isinstance(value, str):
        if key.startswith(("ide")) and isinstance(value, str):
            datastore = value.split(":")[0]
            datastores.append(datastore)
    return list(set(datastores))

def get_vm_ip_addresses(db, cluster_data, node, vmid):
    """
    Fetch a VM's own IPv4 addresses from its guest-agent (Proxmox `/agent/network-get-interfaces`
    — the QEMU guest agent must be installed/enabled). Loopback/link-local/multicast are filtered out.
    Returns: list[str] of IPv4 addresses, [] on any error (agent not installed, timeout, etc. — silent).
    """
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
    """
    Compile detailed info for all of the cluster's QEMU VMs sequentially (config+datastores+IPs).
    Returns: list of {"vmid","node","name","datastores","agent_enabled","ip_addresses", ["error"]}.
    """
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
    """
    Compile the detail of a single VM, running the blocking Proxmox calls (config/IPs) in a
    thread pool (so the async caller is not blocked — a helper for `get_all_vm_details_parallel`).
    Returns: {"vmid","node","name","datastores","agent_enabled","ip_addresses", ["error"]}.
    """
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
    """
    The parallel version of `get_all_vm_details` — fetches the detail of all QEMU VMs
    concurrently (`asyncio.gather`) instead of in a sequential loop, faster for large clusters.
    Returns: list of VM detail dicts (same shape as `get_all_vm_details`).
    """
    loop = asyncio.get_running_loop()
 
    vms = await loop.run_in_executor(None, get_all_cluster_vms, db, cluster_data)
    qemu_vms = [vm for vm in vms if vm['type'] == 'qemu']
 
    tasks = [get_vm_detail(db, cluster_data, vm) for vm in qemu_vms]
    vm_info_list = await asyncio.gather(*tasks, return_exceptions=False)
    return vm_info_list
 
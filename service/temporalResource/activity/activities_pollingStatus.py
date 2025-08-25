import asyncio
from sqlalchemy.orm import Session
from models.models import Machine, Pool
from service.gucamoleService import connectionWithClient
from temporalio.client import WorkflowExecutionStatus
from db_configuration.config import get_db
from service import pollingStatus
import logging
from temporalio import activity
 
logger = logging.getLogger(__name__)
 

@activity.defn
async def poll_and_update_machine_status_activity():
    statuses = {}
    error_details = []
    power_states = {}

    db: Session = next(get_db())
    try:
        all_machines = db.query(Machine).all()
        if not all_machines:
            logger.info("No machines found")
            return {
                "message": "No machines to poll",
                "statuses": {},
                "errors": [],
                "power_states": {}
            }

        # Get Proxmox VM status for all pools just once per poll
        try:
            proxmox_vm_status = await get_proxmox_vm_status_activity()
        except Exception as e:
            logger.error(f"Failed to fetch Proxmox VM status: {e}")
            proxmox_vm_status = []

        # Build a mapping from VMID to status from Proxmox
        vmid_status_map = {}
        for pool in proxmox_vm_status:
            for vm in pool["vms"]:
                vmid_status_map[vm["vmid"]] = vm["status"]

        # Store power_states per pool for API/return
        for pool in proxmox_vm_status:
            power_states[pool["pool_id"]] = pool["vms"]

        # Poll workflow status ONLY for machines whose workflows are not all completed/failed
        for machine in all_machines:
            workflow_ids = machine.workflowId or []
            machine_statuses = []
            if not workflow_ids:
                logger.warning(f"Machine {machine.id} has no workflow IDs")
                continue

            # Check if all workflow statuses are COMPLETED or FAILED
            # If workflow_status is missing, poll anyway
            workflow_statuses = machine.workflow_status or {}
            if (
                workflow_statuses and
                all(
                    ws.get("status") in ("COMPLETED", "FAILED")
                    for ws in workflow_statuses.values()
                )
            ):
                continue  # Skip polling for fully completed/failed machines

            try:
                for wfid in workflow_ids:
                    client = await connectionWithClient()
                    handle = client.get_workflow_handle(wfid)
                    desc = await handle.describe()
                    if isinstance(desc.status, int):
                        status = WorkflowExecutionStatus(desc.status).name
                    else:
                        status = str(desc.status)
                    error = None
                    vm_status = None

                    if status not in ("RUNNING", "COMPLETED"):
                        # Set error_message from workflow failure, if not running or completed
                        try:
                            failure_info = await pollingStatus.get_workflow_failure_message_simple(wfid)
                            error = failure_info.get("failure_message")
                            machine.error_message = error
                            logger.info(f"Updated Machine row {machine.id} with workflow error: {error}")
                        except Exception as e:
                            logger.warning(f"Failed to extract failure for workflow {wfid}: {str(e)}")

                    # If status is RUNNING: Do NOT touch error_message

                    pollingStatus.update_workflow_status(
                        db, machine.id, wfid, status, error, vm_status
                    )

                    machine_statuses.append({
                        "workflow_id": wfid,
                        "status": status,
                        "error": error,
                    })

                statuses[machine.id] = machine_statuses

            except Exception as e:
                error_details.append({"machine_id": machine.id, "error": str(e)})
                logger.error(f"Error polling machine {machine.id}: {str(e)}")
                continue

        # Now update error_message for all machines that are COMPLETED
        for machine in all_machines:
            if machine.status == "COMPLETED" and machine.vm_id in vmid_status_map:
                machine.error_message = vmid_status_map[machine.vm_id]
                logger.info(f"Set Machine.id={machine.id} vm_id={machine.vm_id} error_message={machine.error_message}")
        db.commit()

        return {
            "message": "Poll complete",
            "statuses": statuses,
            "errors": error_details,
            "power_states": power_states
        }
    except Exception as e:
        logger.error(f"Error in poll_and_update_machine_status_activity: {str(e)}")
        if db:
            db.rollback()
        raise
    finally:
        if db:
            db.close()


            
from service.clusterService import getting_Proxmox_host,get_api_token
from sqlalchemy.orm import Session
from models.models import Cluster, CreateClusterBase, Pool
from db_configuration.config import get_db
from service.clusterService import get_all_nodes
import requests
VERIFY_SSL = False
@activity.defn
async def get_proxmox_vm_status_activity():
    db = next(get_db())
    pool_data = db.query(Pool).all()
    pool_map = {}
 
    for pool in pool_data:
        vm_ids = pool.pool_vmids or []
        if not vm_ids:
            print(f"No VMs found in pool {pool.pool_name}")
            continue
        clusterid = pool.cluster_id.split("_")[1]
        cluster_data = db.query(Cluster).filter(Cluster.id == clusterid).first()
        if not cluster_data:
            raise RuntimeError(f"Cluster with ID {clusterid} not found.")
        api_token = get_api_token(db, cluster_data.name)
        headers = {
            "Authorization": f"PVEAPIToken={api_token}",
            "Content-Type": "application/json"
        }
        PROXMOX_HOST = getting_Proxmox_host(cluster_data)
        if not PROXMOX_HOST:
            raise RuntimeError("No reachable Proxmox host found for the cluster.")
        Nodes = get_all_nodes(cluster_data)
        POWER_STATUS_MAP = {
            "running": "power-on",
            "stopped": "power-off",
            "shutdown": "shutdown",
            "suspended": "suspend",
           
            # etc.
        }
        for node in Nodes:
            for vmid in vm_ids:
                url = f"{PROXMOX_HOST}/api2/json/nodes/{node['name']}/qemu/{vmid}/status/current"
                try:
                    response = requests.get(url, headers=headers, verify=VERIFY_SSL, timeout=5)
                    response.raise_for_status()
                    data = response.json()
                    qmp_status = data["data"].get("qmpstatus", "unknown")
                    mapped_status = POWER_STATUS_MAP.get(qmp_status, "UNKNOWN")
                    if pool.pool_name not in pool_map:
                        pool_map[pool.pool_name] = {
                            "pool_id": pool.id,
                            "pool": pool.pool_name,
                            "cluster": cluster_data.name,
                            "vms": []
                        }
                    pool_map[pool.pool_name]["vms"].append({
                        "vmid": vmid,
                        "node": node['name'],
                        "power_status": qmp_status,
                        "status": mapped_status,
                    })
                except requests.RequestException:
                    continue
                except Exception as e:
                    print(f"Unexpected error for VMID {vmid} on node {node['name']}: {e}")
        # print(f"No VMs found in pool {pool.pool_name} or cluster {cluster_data.name} is not reachable.")
        # print(f"Pool map (type: {type(pool_map)}): {pool_map}")
 
    return list(pool_map.values())
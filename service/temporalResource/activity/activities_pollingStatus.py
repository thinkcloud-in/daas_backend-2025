from sqlalchemy.orm import Session
from models.models import Machine, Pool, Cluster
from service.gucamoleService import connectionWithClient
from temporalio.client import WorkflowExecutionStatus
from db_configuration.config import get_db
from service import pollingStatus
import logging
from temporalio import activity
import asyncio

logger = logging.getLogger(__name__)

# import the hyperv status helper (see hyperv_service.py below)
from service.hyper_v_service import get_status as get_hyperv_status


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

        # Run Proxmox and Hyper-V power-state collectors
        try:
            proxmox_vm_status = await get_proxmox_vm_status_activity(db=db)
            hyperv_vm_status = await get_hyperv_vm_status_activity(db=db)
        except Exception as e:
            logger.error(f"Failed to run power-state collectors: {e}")
            proxmox_vm_status = []
            hyperv_vm_status = []

        # Build a combined vmid -> status map (Proxmox VMIDs and Hyper-V ids may overlap
        # so if you need a namespaced key (e.g., "hyperv:vmid") adjust accordingly)
        vmid_status_map = {}
        for pool in proxmox_vm_status:
            for vm in pool.get("vms", []):
                vmid_status_map[vm["vmid"]] = vm["status"]

        for pool in hyperv_vm_status:
            for vm in pool.get("vms", []):
                # Use vmid as key; if Hyper-V vm ids collide with Proxmox vmids in your data
                # you should disambiguate here (e.g. prefix with cluster or hypervisor name)
                vmid_status_map[vm["vmid"]] = vm["status"]

        # Aggregate power states per pool id for the response
        for pool in proxmox_vm_status:
            power_states[pool["pool_id"]] = pool["vms"]
        for pool in hyperv_vm_status:
            power_states[pool["pool_id"]] = pool["vms"]

        # Now poll each machine's workflow(s)
        for machine in all_machines:
            workflow_ids = machine.workflowId or []
            machine_statuses = []
            if not workflow_ids:
                logger.warning(f"Machine {machine.id} has no workflow IDs")
                continue

            workflow_statuses = machine.workflow_status or {}
            if (
                workflow_statuses and
                all(
                    ws.get("status") in ("COMPLETED", "FAILED")
                    for ws in workflow_statuses.values()
                )
            ):
                # all workflows finished -> skip polling
                continue

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
                        try:
                            failure_info = await pollingStatus.get_workflow_failure_message_simple(wfid)
                            error = failure_info.get("failure_message")
                            machine.error_message = error
                            logger.info(f"Updated Machine row {machine.id} with workflow error: {error}")
                        except Exception as e:
                            logger.warning(f"Failed to extract failure for workflow {wfid}: {str(e)}")

                    # if you want to set vm_status from vmid_status_map here, do it by machine.vm_id
                    if getattr(machine, "vm_id", None) in vmid_status_map:
                        vm_status = vmid_status_map[machine.vm_id]

                    pollingStatus.update_workflow_status(
                        db, machine.id, wfid, status, error, vm_status
                    )

                    machine_statuses.append({
                        "workflow_id": wfid,
                        "status": status,
                        "error": error,
                        "vm_status": vm_status,
                    })

                statuses[machine.id] = machine_statuses

            except Exception as e:
                error_details.append({"machine_id": machine.id, "error": str(e)})
                logger.error(f"Error polling machine {machine.id}: {str(e)}")
                continue

        # Update machine rows with vm status messages for COMPLETED machines (existing behavior)
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


from service.clusterService import getting_Proxmox_host, get_api_token
from sqlalchemy.orm import Session
from models.models import Cluster, CreateClusterBase, Pool
from db_configuration.config import get_db
from service.clusterService import get_all_nodes
import requests
VERIFY_SSL = False

@activity.defn
async def get_proxmox_vm_status_activity(db: Session = None):
    """
    Existing Proxmox collector (left mostly as-is).
    """
    own_db = False
    if db is None:
        db = next(get_db())
        own_db = True
    try:
        pool_data = db.query(Pool).all()
        pool_map = {}

        for pool in pool_data:
            vm_ids = pool.pool_vmids or []
            if not vm_ids:
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
                    except Exception:
                        continue

        return list(pool_map.values())
    finally:
        if own_db:
            db.close()


async def get_hyperv_vm_status_activity(db: Session = None):
    """
    New Hyper-V collector.
    ...
    """
    own_db = False
    if db is None:
        db = next(get_db())
        own_db = True
    try:
        pool_data = db.query(Pool).all()
        pool_map = {}

        # Adjust mapping according to your Hyper-V agent semantics.
        HYPERV_STATE_MAP = {
            # example values; please adjust to match your Hyper-V agent's State codes
            2: "power-on",   # sample: State == 3 => running
            3: "power-off",  # sample: map of other codes
        }

        for pool in pool_data:
            vm_ids = pool.pool_vmids or []
            if not vm_ids:
                continue

            # Try to find the cluster referenced by the pool
            try:
                clusterid = pool.cluster_id.split("_")[1]
            except Exception:
                # If cluster id pattern differs, skip; you can adapt this line to your schema.
                continue

            cluster_data = db.query(Cluster).filter(Cluster.id == clusterid).first()
            if not cluster_data:
                continue

            # Heuristic to detect Hyper-V cluster. Adjust to your model (e.g. cluster.type == 'hyperv').
            is_hyperv = False
            if getattr(cluster_data, "type", None):
                is_hyperv = str(cluster_data.type).lower() == "hyperv"
            if not is_hyperv and getattr(cluster_data, "hypervisor", None):
                is_hyperv = str(cluster_data.hypervisor).lower() == "hyperv"
            if not is_hyperv and "hyperv" in (getattr(cluster_data, "name", "") or "").lower():
                is_hyperv = True

            if not is_hyperv:
                continue

            # For Hyper-V clusters, call the async agent per VM id
            for vmid in vm_ids:
                try:
                    # call the async helper which queries your Hyper-V agent
                    data = await get_hyperv_status(vmid)
                    # Expecting the agent response body to include a 'State' integer and a VMName
                    state_value = data.get("State")
                    mapped_status = HYPERV_STATE_MAP.get(state_value, "UNKNOWN")
                    if pool.pool_name not in pool_map:
                        pool_map[pool.pool_name] = {
                            "pool_id": pool.id,
                            "pool": pool.pool_name,
                            "cluster": cluster_data.name,
                            "vms": []
                        }
                    pool_map[pool.pool_name]["vms"].append({
                        "vmid": vmid,
                        "node": cluster_data.name,  # Hyper-V doesn't use Proxmox node names; include cluster
                        "power_status": state_value,
                        "status": mapped_status,
                    })
                except Exception as e:
                    # log and continue with other VMs
                    logger.debug(f"Failed to get Hyper-V status for vmid={vmid}: {e}")
                    continue

        return list(pool_map.values())
    finally:
        if own_db:
            db.close()


# from sqlalchemy.orm import Session
# from models.models import Machine, Pool
# from service.gucamoleService import connectionWithClient
# from temporalio.client import WorkflowExecutionStatus
# from db_configuration.config import get_db
# from service import pollingStatus
# import logging
# from temporalio import activity
 
# logger = logging.getLogger(__name__)
 

# @activity.defn
# async def poll_and_update_machine_status_activity():
#     statuses = {}
#     error_details = []
#     power_states = {}

#     db: Session = next(get_db())
#     try:
#         all_machines = db.query(Machine).all()
#         if not all_machines:
#             logger.info("No machines found")
#             return {
#                 "message": "No machines to poll",
#                 "statuses": {},
#                 "errors": [],
#                 "power_states": {}
#             }
       
#         try:
#             proxmox_vm_status = await get_proxmox_vm_status_activity()
#         except Exception as e:
#             logger.error(f"Failed to fetch Proxmox VM status: {e}")
#             proxmox_vm_status = []

#         vmid_status_map = {}
#         for pool in proxmox_vm_status:
#             for vm in pool["vms"]:
#                 vmid_status_map[vm["vmid"]] = vm["status"]

#         for pool in proxmox_vm_status:
#             power_states[pool["pool_id"]] = pool["vms"]

#         for machine in all_machines:
#             workflow_ids = machine.workflowId or []
#             machine_statuses = []
#             if not workflow_ids:
#                 logger.warning(f"Machine {machine.id} has no workflow IDs")
#                 continue

#             workflow_statuses = machine.workflow_status or {}
#             if (
#                 workflow_statuses and
#                 all(
#                     ws.get("status") in ("COMPLETED", "FAILED")
#                     for ws in workflow_statuses.values()
#                 )
#             ):
#                 continue  
#             try:
#                 for wfid in workflow_ids:
#                     client = await connectionWithClient()
#                     handle = client.get_workflow_handle(wfid)
#                     desc = await handle.describe()
#                     if isinstance(desc.status, int):
#                         status = WorkflowExecutionStatus(desc.status).name
#                     else:
#                         status = str(desc.status)
#                     error = None
#                     vm_status = None

#                     if status not in ("RUNNING", "COMPLETED"):
                        
#                         try:
#                             failure_info = await pollingStatus.get_workflow_failure_message_simple(wfid)
#                             error = failure_info.get("failure_message")
#                             machine.error_message = error
#                             logger.info(f"Updated Machine row {machine.id} with workflow error: {error}")
#                         except Exception as e:
#                             logger.warning(f"Failed to extract failure for workflow {wfid}: {str(e)}")

                    

#                     pollingStatus.update_workflow_status(
#                         db, machine.id, wfid, status, error, vm_status
#                     )

#                     machine_statuses.append({
#                         "workflow_id": wfid,
#                         "status": status,
#                         "error": error,
#                     })

#                 statuses[machine.id] = machine_statuses

#             except Exception as e:
#                 error_details.append({"machine_id": machine.id, "error": str(e)})
#                 logger.error(f"Error polling machine {machine.id}: {str(e)}")
#                 continue

        
#         for machine in all_machines:
#             if machine.status == "COMPLETED" and machine.vm_id in vmid_status_map:
#                 machine.error_message = vmid_status_map[machine.vm_id]
#                 logger.info(f"Set Machine.id={machine.id} vm_id={machine.vm_id} error_message={machine.error_message}")
#         db.commit()

#         return {
#             "message": "Poll complete",
#             "statuses": statuses,
#             "errors": error_details,
#             "power_states": power_states
#         }
#     except Exception as e:
#         logger.error(f"Error in poll_and_update_machine_status_activity: {str(e)}")
#         if db:
#             db.rollback()
#         raise
#     finally:
#         if db:
#             db.close()


            
# from service.clusterService import getting_Proxmox_host,get_api_token
# from sqlalchemy.orm import Session
# from models.models import Cluster, CreateClusterBase, Pool
# from db_configuration.config import get_db
# from service.clusterService import get_all_nodes
# import requests
# VERIFY_SSL = False
# @activity.defn
# async def get_proxmox_vm_status_activity():
#     db = next(get_db())
#     pool_data = db.query(Pool).all()
#     pool_map = {}
 
#     for pool in pool_data:
#         vm_ids = pool.pool_vmids or []
#         if not vm_ids:
#             continue
#         clusterid = pool.cluster_id.split("_")[1]
#         cluster_data = db.query(Cluster).filter(Cluster.id == clusterid).first()
#         if not cluster_data:
#             raise RuntimeError(f"Cluster with ID {clusterid} not found.")
#         api_token = get_api_token(db, cluster_data.name)
#         headers = {
#             "Authorization": f"PVEAPIToken={api_token}",
#             "Content-Type": "application/json"
#         }
#         PROXMOX_HOST = getting_Proxmox_host(cluster_data)
#         if not PROXMOX_HOST:
#             raise RuntimeError("No reachable Proxmox host found for the cluster.")
#         Nodes = get_all_nodes(cluster_data)
#         POWER_STATUS_MAP = {
#             "running": "power-on",
#             "stopped": "power-off",
#             "shutdown": "shutdown",
#             "suspended": "suspend",
           
            
#         }
#         for node in Nodes:
#             for vmid in vm_ids:
#                 url = f"{PROXMOX_HOST}/api2/json/nodes/{node['name']}/qemu/{vmid}/status/current"
#                 try:
#                     response = requests.get(url, headers=headers, verify=VERIFY_SSL, timeout=5)
#                     response.raise_for_status()
#                     data = response.json()
#                     qmp_status = data["data"].get("qmpstatus", "unknown")
#                     mapped_status = POWER_STATUS_MAP.get(qmp_status, "UNKNOWN")
#                     if pool.pool_name not in pool_map:
#                         pool_map[pool.pool_name] = {
#                             "pool_id": pool.id,
#                             "pool": pool.pool_name,
#                             "cluster": cluster_data.name,
#                             "vms": []
#                         }
#                     pool_map[pool.pool_name]["vms"].append({
#                         "vmid": vmid,
#                         "node": node['name'],
#                         "power_status": qmp_status,
#                         "status": mapped_status,
#                     })
#                 except requests.RequestException:
#                     continue
#                 except Exception as e:
#                     continue
 
#     return list(pool_map.values())
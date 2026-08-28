from utils.temporal_client import TemporalClientManager
from temporalio.client import WorkflowExecutionStatus
from db_configuration.config import SessionLocal, get_db
from service import pollingStatus
from temporalio import activity
from service.hyper_v_service import get_status as get_hyperv_status
from service.clusterService import getting_Proxmox_host, get_api_token
from sqlalchemy.orm import Session
from models.models import Cluster, Machine, Pool
from service.clusterService import get_all_nodes
import requests
VERIFY_SSL = False

logger = activity.logger
def split_pools_by_cluster_type(db: Session):
    pool_data = db.query(Pool).all()
    proxmox_pools = []
    hyperv_pools = []
    
    logger.info(f"[split_pools_by_cluster_type] Total pools found in DB: {len(pool_data)}")

    for pool in pool_data:
        try:
            if not pool.cluster_id:
                logger.warning(f"[split_pools_by_cluster_type] Pool {pool.id} has no cluster_id. Skipping.")
                continue

            raw_cluster_id = str(pool.cluster_id)
            if "_" in raw_cluster_id:
                parts = raw_cluster_id.split("_")
                clusterid_str = parts[1] if len(parts) > 1 else parts[0]
            else:
                clusterid_str = raw_cluster_id
            
            if not clusterid_str or clusterid_str.lower() == "nan":
                logger.warning(f"[split_pools_by_cluster_type] Pool {pool.id} has invalid clusterid '{clusterid_str}'. Skipping.")
                continue

            # Ensure we use an integer for the ID lookup
            try:
                cluster_id_int = int(float(clusterid_str))
            except ValueError:
                logger.error(f"[split_pools_by_cluster_type] Could not convert clusterid '{clusterid_str}' to int for pool {pool.id}")
                continue

            cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id_int).first()
            if not cluster_data:
                logger.warning(f"[split_pools_by_cluster_type] Cluster ID {cluster_id_int} not found in DB for pool {pool.id}. Skipping.")
                continue

            # Normalize type: "Hyper-V", "hyper_v", "HyperV" -> "hyperv"
            cluster_type = (getattr(cluster_data, "type", "") or "").lower().replace("-", "").replace("_", "").replace(" ", "")
            
            if "hyperv" in cluster_type: 
                hyperv_pools.append((pool, cluster_data))
            elif "proxmox" in cluster_type:
                proxmox_pools.append((pool, cluster_data))
            else:
                logger.warning(f"[split_pools_by_cluster_type] Unknown cluster type '{cluster_type}' for cluster '{cluster_data.name}', skipping")
        except Exception as e:
            logger.error(f"Error parsing cluster_id for pool {pool.id}: {e}")
            continue

    logger.info(f"[split_pools_by_cluster_type] Categorized: {len(proxmox_pools)} Proxmox, {len(hyperv_pools)} Hyper-V pools.")
    return proxmox_pools, hyperv_pools

@activity.defn
async def poll_and_update_machine_status_activity():
    print("\n[Poller Activity] Starting status update cycle...")
    statuses = {}
    error_details = []
    power_states = {}

    db: Session = SessionLocal()
    try:
        try:
            all_machines = db.query(Machine).all()
            if not all_machines:
                logger.info("[Poller Activity] No machines found to poll.")
                return {
                    "message": "No machines to poll",
                    "statuses": {},
                    "errors": [],
                    "power_states": {}
                }

            proxmox_pools, hyperv_pools = split_pools_by_cluster_type(db)
            
            # Run Proxmox and Hyper-V power-state collectors
            try:
                proxmox_vm_status = await get_proxmox_vm_status_activity(db=db, pools=proxmox_pools)
                hyperv_vm_status = await get_hyperv_vm_status_activity(db=db, pools=hyperv_pools)
            except Exception as e:
                logger.error(f"[Poller Activity] ERROR collecting pool states: {e}")
                proxmox_vm_status = []
                hyperv_vm_status = []

            # Build a combined vmid -> status map (Standardize keys to lowercase strings)
            vmid_status_map = {}
            for pool in proxmox_vm_status:
                for vm in pool.get("vms", []):
                    if "vmid" in vm:
                        vmid_status_map[str(vm["vmid"]).lower()] = vm.get("status", "UNKNOWN")

            for pool in hyperv_vm_status:
                for vm in pool.get("vms", []):
                    if "vmid" in vm:
                        vmid_status_map[str(vm["vmid"]).lower()] = vm.get("status", "UNKNOWN")

            logger.info(f"[Poller Activity] Agent status map built with {len(vmid_status_map)} entries.")

            power_states = {}
            # Aggregate power states per pool id for the response
            for pool in proxmox_vm_status:
                power_states[pool["pool_id"]] = pool["vms"]
            for pool in hyperv_vm_status:
                power_states[pool["pool_id"]] = pool["vms"]

            # Now poll each machine's workflow(s)
            match_count = 0
            for machine in all_machines:
                m_vmid = str(getattr(machine, "vm_id", "") or "").lower()
                vm_status = vmid_status_map.get(m_vmid)
                if vm_status:
                    match_count += 1

                # Update power status even if no workflows or all finished
                if vm_status and machine.error_message != vm_status:
                    machine.error_message = vm_status
                    db.commit()

                workflow_ids = machine.workflowId or []
                machine_statuses = []
                
                if not workflow_ids:
                    continue

                workflow_statuses = machine.workflow_status or {}
                if (
                    workflow_statuses and
                    all(
                        ws.get("status") in ("COMPLETED", "FAILED")
                        for ws in workflow_statuses.values()
                    )
                ):
                    continue

                try:
                    for wfid in workflow_ids:
                        client = await TemporalClientManager.get_temporal_client()
                        handle = client.get_workflow_handle(wfid)
                        try:
                            desc = await handle.describe()
                            if isinstance(desc.status, int):
                                status = WorkflowExecutionStatus(desc.status).name
                            else:
                                status = str(desc.status)
                        except Exception as e:
                            logger.warning(f"Workflow {wfid} not found: {e}")
                            status = "UNKNOWN"
                        
                        error = None
                        if status not in ("RUNNING", "COMPLETED"):
                            try:
                                failure_info = await pollingStatus.get_workflow_failure_message_simple(wfid)
                                error = failure_info.get("failure_message")
                                machine.error_message = error
                            except Exception as e:
                                logger.warning(f"Failed to extract failure for workflow {wfid}: {str(e)}")

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

            # Update machine rows with vm status messages for COMPLETED machines
            for machine in all_machines:
                m_vmid_str = str(getattr(machine, "vm_id", "") or "").lower()
                if machine.status == "COMPLETED" and m_vmid_str in vmid_status_map:
                    machine.error_message = vmid_status_map[m_vmid_str]

            db.commit()

            return {
                "message": "Poll complete",
                "diagnostics": {
                    "total_pools": len(proxmox_pools) + len(hyperv_pools),
                    "proxmox_pools_count": len(proxmox_pools),
                    "hyperv_pools_count": len(hyperv_pools),
                    "machines_processed": len(all_machines),
                    "matches_found": match_count
                },
                "status": "success"
            }
        except Exception as e:
            logger.error(f"Error in poll_and_update_machine_status_activity: {str(e)}")
            db.rollback()
            raise Exception("Error during poll activity") from e
    finally:
        db.close()

async def get_proxmox_vm_status_activity(db: Session = None, pools: list = None):
    if db is None:
        db = SessionLocal()
        try:
            return await _get_proxmox_vm_status_logic(db, pools)
        finally:
            db.close()
    else:
        return await _get_proxmox_vm_status_logic(db, pools)

async def _get_proxmox_vm_status_logic(db: Session, pools: list = None):
    try:
        if pools is None:
            proxmox_pools, _ = split_pools_by_cluster_type(db)
        else:
            proxmox_pools = pools

        pool_map = {}
        POWER_STATUS_MAP = {
            "running": "power-on",
            "stopped": "power-off",
            "shutdown": "power-off",
            "suspended": "power-off",
        }

        for pool, cluster_data in proxmox_pools:
            pool_map[pool.pool_name] = {
                "pool_id": pool.id,
                "pool": pool.pool_name,
                "cluster": cluster_data.name,
                "vms": []
            }
            
            # For Proxmox, we can get all VM statuses at once for the cluster
            try:
                vms_in_cluster = await TemporalClientManager.get_temporal_client() # placeholder for client if needed
                # Actually use the existing service function
                from service.proxmoxService import get_all_cluster_vms
                vms_in_cluster = get_all_cluster_vms(db, cluster_data)
            except Exception as e:
                logger.error(f"Error fetching Proxmox cluster VMs: {e}")
                continue

            vm_ids_in_db = [m.vm_id for m in db.query(Machine).filter(Machine.pool_id == pool.id).all()]
            
            for vm_item in vms_in_cluster:
                vmid_str = str(vm_item.get("vmid"))
                if vmid_str in vm_ids_in_db:
                    status = vm_item.get("status", "unknown")
                    mapped_status = POWER_STATUS_MAP.get(status, "UNKNOWN")
                    pool_map[pool.pool_name]["vms"].append({
                        "vmid": vmid_str,
                        "node": vm_item.get("node"),
                        "power_status": status,
                        "status": mapped_status,
                    })

        return list(pool_map.values())
    except Exception as e:
        logger.error(f"Error in get_proxmox_vm_status_activity logic: {str(e)}")
        raise

async def get_hyperv_vm_status_activity(db: Session = None, pools: list = None):
    if db is None:
        db = SessionLocal()
        try:
            return await _get_hyperv_vm_status_logic(db, pools)
        finally:
            db.close()
    else:
        return await _get_hyperv_vm_status_logic(db, pools)

async def _get_hyperv_vm_status_logic(db: Session, pools: list = None):
    try:
        if pools is None:
            _, hyperv_pools = split_pools_by_cluster_type(db)
        else:
            hyperv_pools = pools
        
        pool_map = {}
        HYPERV_STATE_MAP = {
            2: "power-on",
            3: "power-off",
        }

        for pool, cluster_data in hyperv_pools:
            pool_map[pool.pool_name] = {
                "pool_id": pool.id,
                "pool": pool.pool_name,
                "cluster": cluster_data.name,
                "vms": []
            }

            machines = db.query(Machine).filter(Machine.pool_id == pool.id).all()
            vm_ids = [m.vm_id for m in machines if m.vm_id]
            
            if not vm_ids:
                logger.info(f"[hyperv_status] No VM IDs found for pool {pool.pool_name}")
                continue

            for vmid in vm_ids:
                try:
                    data = await get_hyperv_status(vmid, db, cluster_data.id)
                    mapped_status = HYPERV_STATE_MAP.get(data, "UNKNOWN")                    
                    pool_map[pool.pool_name]["vms"].append({
                        "vmid": vmid,
                        "node": cluster_data.name,
                        "power_status": mapped_status,
                        "status": mapped_status,
                    })
                except Exception as e:
                    logger.warning(f"[hyperv_status] Agent call failed for vmid={vmid}: {e}")
                    pool_map[pool.pool_name]["vms"].append({
                        "vmid": vmid,
                        "node": cluster_data.name,
                        "power_status": None,
                        "status": "UNKNOWN",
                    })

        return list(pool_map.values())
    except Exception as e:
        logger.error(f"Error in get_hyperv_vm_status_activity logic: {str(e)}")
        raise
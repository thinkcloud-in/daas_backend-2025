from service.temporalResource.activity.activities_hyper_v import handle_action_activity
import service.hyper_v_service
import ipaddress
from temporalio import activity
from sqlalchemy.orm import Session
from db_configuration.config import SessionLocal, get_db
import requests
import aiohttp
import asyncio
from models.models import Machine, Cluster,Pool
from service.clusterService import get_all_nodes
from models.IPs_model import IPEntry,IPSModel
from service import proxmoxService
from influxdb_client import InfluxDBClient, Point, WriteOptions
import dotenv
import os
dotenv.load_dotenv()
logger = activity.logger
# @activity.defn
# async def clone_vm_activity(clone_payload: dict):
#     db: Session = next(get_db())
#     try:

#         cluster_data = db.query(Cluster).filter(Cluster.id == clone_payload['cluster_id']).first()
#         api_token = proxmoxService.get_api_token(db, cluster_data.name)
#         headers = {"Authorization": f"PVEAPIToken={api_token}"}
#         PROXMOX_HOST = proxmoxService.getting_Proxmox_host(cluster_data)
#         if not PROXMOX_HOST:
#             raise RuntimeError("No reachable Proxmox host found for the cluster.")
 
#         all_vms =proxmoxService.get_all_cluster_vms(db, cluster_data)
#         existing_names = [
#             vm["name"] for vm in all_vms if "name" in vm and vm["name"]
#         ]
#         db_names = [m.name for m in db.query(Machine).all()]
#         all_existing_names = set(existing_names) | set(db_names)
#         new_names = proxmoxService.generate_machine_name(
#             clone_payload['name_template'],
#             list(all_existing_names),
#             clone_payload['count'],
#         )
#         existing_vmids = {
#             int(vm["vmid"]) for vm in all_vms if "vmid" in vm and str(vm["vmid"]).isdigit()
#         }
#         template_node = None
#         template_vm_id_str = str(clone_payload['template_vm_id'])
#         for vm in all_vms:
#             if str(vm.get("vmid")) == template_vm_id_str and vm.get("template") == 1:
#                 template_node = vm.get("node")
#                 break
 
#         if not template_node:
            
#             return {
#                 "error": f"Template VM {clone_payload['template_vm_id']} not found in cluster.",
#                 "error_type": "template_missing"
#             }
 
 
#         def get_free_vmids(existing_vmids: set[int], needed: int, min_vmid: int = 100) -> list[int]:
#             free_vmids = []
#             vmid = min_vmid
#             while len(free_vmids) < needed:
#                 if vmid not in existing_vmids and vmid not in free_vmids:
#                     free_vmids.append(vmid)
#                 vmid += 1
#             return free_vmids
 
#         needed_count = len(new_names)
#         free_vmids = get_free_vmids(existing_vmids, needed_count)
 
#         assigned_nodes = []
#         final_new_names = []
#         final_vmids = []
#         for i, new_name in enumerate(new_names):
#             node = clone_payload['node'][i % len(clone_payload['node'])]
#             db_machine = db.query(Machine).filter(Machine.name == new_name).first()
#             if db_machine:
#                 continue
#             assigned_nodes.append(node)
#             final_new_names.append(new_name)
#             final_vmids.append(free_vmids[i])
 
#         vms = []
#         clone_workflow_id = clone_payload.get("workflowId")  
   
#         for new_name, new_vmid, node in zip(final_new_names, final_vmids, assigned_nodes):
#             clone_url = f"{PROXMOX_HOST}/api2/json/nodes/{template_node}/qemu/{clone_payload['template_vm_id']}/clone"
#             payload_dict = {
#                 "newid": new_vmid,
#                 "name": new_name,
#                 "target": node,
#                 "storage":os.getenv("PROXMOX_STORAGE"),
#                 "full": 1
#             }
#             try:
#                 response = requests.post(clone_url, headers=headers, data=payload_dict, verify=False)
#                 response.raise_for_status()
#                 upid = response.json()['data']
#                 vms.append((new_name, new_vmid, node, upid,clone_workflow_id))
               
#             except requests.exceptions.RequestException as e:
#                 return {
#                     "error": f"Error cloning VM {new_name} on node {node}: {e}",
#                     "error_type": "clone_failed"
#                 }

#         return {
#             "message": f"{len(final_new_names)} VMs cloned successfully across {len(clone_payload['node'])} nodes",
#             "vms": vms
#         }
#     except Exception as e:
#         return {"error": str(e), "error_type": "activity_exception"}
 
#     finally:
#         db.close()


from typing import Dict
import os
import requests

PROXMOX_STORAGE = os.getenv("PROXMOX_STORAGE")

async def _get_existing_guacamole_connection_names() -> set:
    """Best-effort fetch of current Guacamole connection names, so name
    generation can avoid colliding with stale/orphaned connections that
    Proxmox/DB checks alone wouldn't see."""
    try:
        payload = {
            "username": os.getenv("USER_GUACA"),
            "password": os.getenv("GUACA_PASS"),
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{os.getenv('GUCAMOLE_BASE_URL')}/api/tokens", data=payload, headers=headers) as token_resp:
                token_data = await token_resp.json()
                auth_token = token_data.get("authToken")
                if not auth_token:
                    return set()
                url = f"{os.getenv('GUCAMOLE_BASE_URL')}/api/session/data/{os.getenv('GUCAMOLE_DATASOURCE')}/connections?token={auth_token}"
                async with session.get(url) as conn_resp:
                    if conn_resp.status != 200:
                        return set()
                    connections = await conn_resp.json()
                    return {c.get("name") for c in connections.values() if c.get("name")}
    except Exception as e:
        logger.warning(f"Could not fetch existing Guacamole connection names (continuing without them): {e}")
        return set()


@activity.defn
async def clone_vm_activity(clone_payload: dict):
    db: Session = SessionLocal()
    try:
        try:
            cluster_data = db.query(Cluster).filter(Cluster.id == clone_payload['cluster_id']).first()
            api_token = proxmoxService.get_api_token(db, cluster_data.name)
            headers = {"Authorization": f"PVEAPIToken={api_token}"}
            PROXMOX_HOST = proxmoxService.getting_Proxmox_host(cluster_data)
            if not PROXMOX_HOST:
                raise RuntimeError("No reachable Proxmox host found for the cluster.")

            # Validate provided storage environment variable
            storage = clone_payload['pool_storage']
            if not storage:
                return {"error": "PROXMOX_STORAGE environment variable not set", "error_type": "invalid_config"}

            all_vms = proxmoxService.get_all_cluster_vms(db, cluster_data)
            existing_names = [vm["name"] for vm in all_vms if "name" in vm and vm["name"]]
            db_names = [m.name for m in db.query(Machine).all()]
            # Also exclude names already used by Guacamole connections — a
            # stale/orphaned connection (e.g. left over from a previous failed
            # attempt) is invisible to the Proxmox/DB checks above, and would
            # otherwise let us generate a name that collides at registration
            # time, leaving a freshly-cloned VM orphaned (see finalize_cloned_machine_activity).
            guacamole_names = await _get_existing_guacamole_connection_names()
            all_existing_names = set(existing_names) | set(db_names) | guacamole_names
            new_names = proxmoxService.generate_machine_name(
                clone_payload['name_template'],
                list(all_existing_names),
                clone_payload['count'],
            )
            existing_vmids = {
                int(vm["vmid"]) for vm in all_vms if "vmid" in vm and str(vm["vmid"]).isdigit()
            }

            # Normalize template_vm_id (accept int/string/dict/list)
            raw_template = clone_payload.get('template_vm_id')
            template_vm_id = None
            if isinstance(raw_template, dict):
                template_vm_id = raw_template.get('vmid') or raw_template.get('id')
            elif isinstance(raw_template, list) and raw_template:
                first = raw_template[0]
                if isinstance(first, dict):
                    template_vm_id = first.get('vmid') or first.get('id')
                else:
                    template_vm_id = first
            else:
                template_vm_id = raw_template

            # Coerce and prepare string for URL comparison
            try:
                if template_vm_id is not None and template_vm_id != "":
                    template_vm_id_int = int(template_vm_id)
                    template_vm_id_str = str(template_vm_id_int)
                    template_vm_id = template_vm_id_int
                else:
                    return {"error": "template_vm_id is empty", "error_type": "invalid_payload"}
            except (ValueError, TypeError):
                template_vm_id_str = str(template_vm_id)

            # find template node
            template_node = None
            for vm in all_vms:
                if str(vm.get("vmid")) == template_vm_id_str and vm.get("template") == 1:
                    template_node = vm.get("node")
                    break

            if not template_node:
                return {
                    "error": f"Template VM {clone_payload.get('template_vm_id')} not found in cluster.",
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
            nodes_list = clone_payload.get('node') or []
            if not nodes_list:
                return {"error": "No target node(s) provided in clone payload.", "error_type": "invalid_payload"}

            assigned_nodes = []
            final_new_names = []
            final_vmids = []
            for i, new_name in enumerate(new_names):
                node = nodes_list[i % len(nodes_list)]
                db_machine = db.query(Machine).filter(Machine.name == new_name).first()
                if db_machine:
                    continue
                assigned_nodes.append(node)
                final_new_names.append(new_name)
                final_vmids.append(free_vmids[i])

            ip_list = clone_payload.get("ip_list", [])
            vms = []
            clone_workflow_id = clone_payload.get("workflowId")

            # Return vms as tuples to preserve original behavior expected by workflow:
            # (name, vmid, node, upid, clone_workflow_id)
            for idx, (new_name, new_vmid, node) in enumerate(zip(final_new_names, final_vmids, assigned_nodes)):
                clone_url = f"{PROXMOX_HOST}/api2/json/nodes/{template_node}/qemu/{template_vm_id_str}/clone"
                payload_dict = {
                    "newid": new_vmid,
                    "name": new_name,
                    "target": node,
                    "storage": storage,
                    "full": 1
                }
                try:
                    response = requests.post(clone_url, headers=headers, data=payload_dict, verify=False)
                    # if non-200, include body to help debugging
                    if response.status_code >= 400:
                        return {
                            "error": f"Error cloning VM {new_name} on node {node}: {response.status_code} {response.text}",
                            "error_type": "clone_failed"
                        }
                    upid = response.json().get('data')
                    # keep the old tuple format exactly as workflow expects
                    vms.append((new_name, new_vmid, node, upid, clone_workflow_id))
                except requests.exceptions.RequestException as e:
                    resp_text = getattr(e, "response", None)
                    resp_body = resp_text.text if resp_text is not None else str(e)
                    return {
                        "error": f"Error cloning VM {new_name} on node {node}: {resp_body}",
                        "error_type": "clone_failed"
                    }

            # return the ip_list as-is so external code can zip(vms, ip_list)
            return {
                "message": f"{len(vms)} VMs cloned successfully across {len(nodes_list)} nodes",
                "vms": vms,
                "ip_list": ip_list
            }

        except Exception as e:
            return {"error": str(e), "error_type": "activity_exception"}
    finally:
        db.close()

def netmask_to_cidr(netmask: str) -> int:
    return ipaddress.IPv4Network(f"0.0.0.0/{netmask}").prefixlen


def extract_node_from_upid(upid: str) -> str:
    return upid.split(":")[1]
 
@activity.defn
async def wait_for_vm_ready_activity(args: dict):
    db: Session = SessionLocal()
    try:
        try:
            vmid = args.get("vmid")
            upid = args["upid"]
            pool_id = args.get("pool_id")
            cluster_id = args.get("cluster_id")

            if not cluster_id and pool_id:
                pool = db.query(Pool).filter(Pool.id == int(pool_id)).first()
                if pool:
                    cluster_id_raw = str(pool.cluster_id)
                    cluster_id = cluster_id_raw.split("_")[1] if "_" in cluster_id_raw else cluster_id_raw

            if not cluster_id:
                raise Exception("Either cluster_id or pool_id must be provided")

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
                            if vmid:
                                machine = db.query(Machine).filter(
                                    Machine.vm_id == str(vmid)
                                ).first()
                                if machine:
                                    machine.error_message = "power-off"
                                    db.commit()
                            return True
                        else:
                            raise Exception(f"Task failed: {data}")
                except requests.RequestException as ex:
                    raise Exception(f"Error checking task status: {ex}")
                await asyncio.sleep(2)
        except Exception as e:
            raise Exception(f"Error in wait_for_vm_ready_activity: {str(e)}")
    finally:
        db.close()

 
 
 
@activity.defn
async def update_machine_provisioning_status_activity(args: dict):
    db: Session = SessionLocal()
    try:
        vmid = args["vmid"]
        step = args["step"]
        machine = db.query(Machine).filter(Machine.vm_id == str(vmid)).one_or_none()
        if not machine:
            logger.warning(f"update_machine_provisioning_status_activity: machine with vmid {vmid} not found, skipping step '{step}'")
            return {"status": "skipped", "reason": "machine_not_found"}
        machine.provisioning_status = step
        db.commit()
        return {"status": "ok"}
    finally:
        db.close()


@activity.defn
async def assign_ip_to_vm_activity(args: dict):
    db: Session = SessionLocal()
    try:
        try:
            vmid = args["vmid"]
            ip_address = args["ip_address"]
            pool_id = args.get("pool_id")
            cluster_id = args.get("cluster_id")

            if not cluster_id and pool_id:
                pool = db.query(Pool).filter(Pool.id == int(pool_id)).first()
                if pool:
                    cluster_id_raw = str(pool.cluster_id)
                    cluster_id = cluster_id_raw.split("_")[1] if "_" in cluster_id_raw else cluster_id_raw

            if not cluster_id:
                raise Exception("Either cluster_id or pool_id must be provided")

            cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
            if not cluster_data:
                raise Exception(f"Cluster {cluster_id} not found")
            api_token = proxmoxService.get_api_token(db, cluster_data.name)
            headers = {"Authorization": f"PVEAPIToken={api_token}"}
            PROXMOX_HOST = proxmoxService.getting_Proxmox_host(cluster_data)
            nodes = get_all_nodes(cluster_data)
            
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
                            "ipconfig0": f"ip={ip_with_cidr},gw={gateway}",
                            # Force the OpenStack ConfigDrive format Cloudbase-Init
                            # requires on Windows. Without this, Proxmox defaults to
                            # citype=nocloud, which writes a Linux-style drive the
                            # guest can't fully read — hostname and user-data
                            # (domain join) silently don't apply on first boot. Set
                            # here so every clone gets it before power-on, regardless
                            # of how the source template was configured.
                            "citype": "configdrive2",
                        }
                        config_response = requests.put(config_url, headers=headers, data=payload, verify=False, timeout=10)
                        
                        return {
                            "message": f"IP {ip_with_cidr} assigned successfully on node {node_name}."
                        }
                except requests.RequestException as ex:
                    continue
            raise Exception(f"VMID {vmid} not found on any node. Cannot assign IP or reboot.")
        except Exception as e:
            raise Exception(f"Error in assign_ip_to_vm_activity: {str(e)}")
    except Exception as e:
        raise Exception(f"Error in assign_ip_to_vm_activity: {str(e)}")
    finally:
        db.close()
 
 
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
            time = record.get_time()  
 
            
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
                
                points = []
 
    if points:
        dst_write.write(bucket=dst_bucket, org=dst_org, record=points)
        total += len(points)
        
 
    
    dst_write.close()
    src_client.close()
    dst_client.close()
    return {"total": total, "max_time": max_time}
 
 
@activity.defn
async def start_vm_proxmox_activity(vmid: str, pool_id: str):
    db: Session = SessionLocal()
    try:
        try:
            logger.info(f"start_vm_proxmox_activity started for vmid={vmid}, pool_id={pool_id}")
            machine = db.query(Machine).filter(Machine.vm_id == str(vmid)).one_or_none()
            if not machine:
                logger.error(f"Machine with vmid {vmid} not found in database.")
                return {"status": "error", "msg": f"Machine with id {vmid} not found."}

            pool = db.query(Pool).filter(Pool.id == int(pool_id)).first()
            if not pool or not pool.pool_template_vm_id:
                return {"status": "error", "error": f"Pool {pool_id} not found or has no templateid."}

            cluster_id_raw = str(pool.cluster_id)
            if "_" in cluster_id_raw:
                cluster_id = cluster_id_raw.split("_")[-1]
            else:
                cluster_id = cluster_id_raw

            cluster = db.query(Cluster).filter(Cluster.id == int(cluster_id)).first()
            if not cluster:
                return {"status": "error", "error": f"Cluster {cluster_id} not found."}
            # Hyper-V check
            if cluster.type and cluster.type.lower() in ("hyper-v", "hyperv"):
                hyperv_request = {
                    "vm_id":machine.vm_id,
                    "vm_name":machine.name,
                    "action":"start",
                    "cluster_id":cluster_id
                }
                try:
                    result = await handle_action_activity(hyperv_request)
                    if result.get("status") == "success":
                        machine.error_message = "power-on"
                        db.commit()
                        return {"vm_status": "power-on", "msg": "VM started successfully."}
                except Exception as e:
                    machine.error_message = f"Power-on failed: {str(e)}"
                    db.commit()
                    logger.error(f"Hyper-V start failed for {machine.name}: {e}")
                    raise e

            details = proxmoxService.collect_proxmox_details(vmid, pool_id, db)
            if details.get("status") != "success":
                machine.error_message = f"Start failed: {details.get('error', 'Unknown error')}"
                db.commit()
                return details
     
            PROXMOX_HOST = details["PROXMOX_HOST"]
            node = details["node"]
            proxmox_vmid = details["vmid"]
            headers = details["headers"]
            vm_status = proxmoxService.vm_start(PROXMOX_HOST, node, proxmox_vmid, headers)
            if vm_status is True:
                machine.error_message = "power-on"
                msg = "VM started successfully."
            elif isinstance(vm_status, dict) and vm_status.get("error"):
                msg = f"Start failed: {vm_status['error']}"
            db.commit()
            return {"vm_status": vm_status, "msg": msg}
        except Exception as e:
            db.rollback()
            return {"status": "error", "msg": f"Error in start_vm_proxmox_activity: {str(e)}"}
    finally:
        db.close()

 
 
@activity.defn
async def stop_vm_proxmox_activity(vmid: str, pool_id: str,email: str = None):
    db: Session = SessionLocal()
    try:
        try:
            machine = db.query(Machine).filter(Machine.vm_id == str(vmid)).one_or_none()
            if not machine:
                return {"status": "error", "msg": f"Machine with id {vmid} not found."}
            
            pool = db.query(Pool).filter(Pool.id == int(pool_id)).first()
            if not pool or not pool.pool_template_vm_id:
                return {"status": "error", "error": f"Pool {pool_id} not found or has no templateid."}

            cluster_id_raw = str(pool.cluster_id)
            if "_" in cluster_id_raw:
                cluster_id = cluster_id_raw.split("_")[1]
            else:
                cluster_id = cluster_id_raw

            cluster = db.query(Cluster).filter(Cluster.id == int(cluster_id)).first()
            if not cluster:
                return {"status": "error", "error": f"Cluster {cluster_id} not found."}
            # Hyper-V check
            if cluster.type and cluster.type.lower() in ("hyper-v", "hyperv"):
                hyperv_request = {
                    "vm_id":machine.vm_id,
                    "vm_name":machine.name,
                    "action":"force_off",
                    "cluster_id":cluster_id
                }
                result = await handle_action_activity(hyperv_request)
                if result.get("status") == "success":
                    machine.error_message = "power-off"
                    db.commit()
                    return {"vm_status": "power-off", "msg": "VM stopped successfully."}
                else:
                    return {"vm_status": "error", "msg": f"Stop failed: {result.get('error', 'Unknown error')}"}

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
                
                msg = f"Stop failed: {vm_status['error']}"
     
            db.commit()
            
            return {"vm_status": vm_status, "msg": msg}
        except Exception as e:
            db.rollback()
            return {"status": "error", "msg": f"Error in stop_vm_proxmox_activity: {str(e)}"}
    finally:
        db.close()



@activity.defn
async def reboot_vm_proxmox_activity(vmid: str, pool_id: str,email: str = None):
    db: Session = SessionLocal()
    try:
        try:
            machine = db.query(Machine).filter(Machine.vm_id == str(vmid)).one_or_none()
            if not machine:
                return {"status": "error", "msg": f"Machine with id {vmid} not found."}

            pool = db.query(Pool).filter(Pool.id == int(pool_id)).first()
            if not pool or not pool.pool_template_vm_id:
                return {"status": "error", "error": f"Pool {pool_id} not found or has no templateid."}

            cluster_id_raw = str(pool.cluster_id)
            if "_" in cluster_id_raw:
                cluster_id = cluster_id_raw.split("_")[1]
            else:
                cluster_id = cluster_id_raw

            cluster = db.query(Cluster).filter(Cluster.id == int(cluster_id)).first()
            if not cluster:
                return {"status": "error", "error": f"Cluster {cluster_id} not found."}
            # Hyper-V check
            if cluster.type and cluster.type.lower() in ("hyper-v", "hyperv"):
                hyperv_request = {
                    "vm_id":machine.vm_id,
                    "vm_name":machine.name,
                    "action":"reboot",
                    "cluster_id":cluster_id
                }
                result = await handle_action_activity(hyperv_request)
                if result.get("status") == "success":
                    machine.error_message = "power-on"
                    db.commit()
                    return {"vm_status": "reboot", "msg": "VM rebooted successfully."}
                else:
                    return {"vm_status": "error", "msg": f"Reboot failed: {result.get('error', 'Unknown error')}"}

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
                machine.error_message = "power-on"
                msg = "VM rebooted successfully."
            elif vm_status.get("error"):
                
                msg = f"Reboot failed: {vm_status['error']}"
        
            db.commit()
            
            return {"vm_status": vm_status, "msg": msg}
        except Exception as e:
            db.rollback()
            return {"status": "error", "msg": f"Error in reboot_vm_proxmox_activity: {str(e)}"}
    finally:
        db.close()
 
 
@activity.defn
async def shutdown_vm_proxmox_activity(vmid: str, pool_id: str,email: str = None):
    db: Session = SessionLocal()
    try:
        try:
            machine = db.query(Machine).filter(Machine.vm_id == str(vmid)).one_or_none()
            if not machine:
                return {"status": "error", "msg": f"Machine with id {vmid} not found."}

            pool = db.query(Pool).filter(Pool.id == int(pool_id)).first()
            if not pool or not pool.pool_template_vm_id:
                return {"status": "error", "error": f"Pool {pool_id} not found or has no templateid."}

            cluster_id_raw = str(pool.cluster_id)
            if "_" in cluster_id_raw:
                cluster_id = cluster_id_raw.split("_")[1]
            else:
                cluster_id = cluster_id_raw

            cluster = db.query(Cluster).filter(Cluster.id == int(cluster_id)).first()
            if not cluster:
                return {"status": "error", "error": f"Cluster {cluster_id} not found."}
            # Hyper-V check
            if cluster.type and cluster.type.lower() in ("hyper-v", "hyperv"):
                hyperv_request = {
                    "vm_id":machine.vm_id,
                    "vm_name":machine.name,
                    "action":"stop",
                    "cluster_id":cluster_id
                }
                result = await handle_action_activity(hyperv_request)
                if result.get("status") == "success":
                    machine.error_message = "power-off"
                    db.commit()
                    return {"vm_status": "power-off", "msg": "VM stopped successfully."}
                else:
                    return {"vm_status": "error", "msg": f"Stop failed: {result.get('error', 'Unknown error')}"}

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
                
                msg = f"Shutdown failed: {vm_status['error']}"
        
            db.commit()
            
            return {"vm_status": vm_status, "msg": msg}
        except Exception as e:
            db.rollback()
            return {"status": "error", "msg": f"Error in shutdown_vm_proxmox_activity: {str(e)}"}
    finally:
        db.close()


@activity.defn
async def vm_rebuild_activity(vmid: int, pool_id: str = None):
    db: Session = SessionLocal()
    try:
        try:
            machine = db.query(Machine).filter(Machine.vm_id == str(vmid)).first()
            if not machine:
                return {"status": "error", "error": f"Machine with vm_id {vmid} not found in DB."}
            if not machine.pool_id:
                return {"status": "error", "error": f"Machine {vmid} has no associated pool."}

            pool = db.query(Pool).filter(Pool.id == int(pool_id)).first()
            if not pool or not pool.pool_template_vm_id:
                return {"status": "error", "error": f"Pool {pool_id} not found or has no templateid."}

            raw_template = pool.pool_template_vm_id
            if isinstance(raw_template, dict):
                template_vm_id = str(raw_template.get('vmid') or raw_template.get('id'))
            elif isinstance(raw_template, list) and raw_template:
                first = raw_template[0]
                template_vm_id = str(first.get('vmid') or first.get('id')) if isinstance(first, dict) else str(first)
            else:
                template_vm_id = str(raw_template)
            cluster_id_str = str(pool.cluster_id)
            if "_" in cluster_id_str:
                cluster_id = int(cluster_id_str.split("_")[1])
            else:
                cluster_id = int(cluster_id_str)
            cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
            if not cluster_data:
                return {"status": "error", "error": f"Cluster {cluster_id} not found for pool {pool.id}."}

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
                if int(vm.get("vmid")) == vmid:
                    node = vm.get("node")
                    break
            if not node:
                node = getattr(cluster_data, "nodes", ["pve"])[0]

            
            delete_url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}"
            resp = requests.get(delete_url, headers=headers, verify=False)
            if resp.status_code == 200:
                
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
                await asyncio.sleep(5)  

            
            template_node = None
            for vm in all_vms:
                if str(vm.get("vmid")) == str(template_vm_id):
                    template_node = vm.get("node")
                    break
            if not template_node:
                return {"status": "error", "error": f"Template VM {template_vm_id} not found in cluster."}

            clone_url = f"{PROXMOX_HOST}/api2/json/nodes/{template_node}/qemu/{template_vm_id}/clone"
            clone_payload = {
                "newid": vmid,
                "name": machine.name,
                "target": node,
                "storage": pool.pool_storage,
                "full": 1
            }
            clone_resp = requests.post(clone_url, headers=headers, data=clone_payload, verify=False)
            clone_resp.raise_for_status()
            upid = clone_resp.json().get("data")

            
            machine.error_message = "cloning..."
            machine.provisioning_status = "cloned"

            # Self-heal pool_vmids: this vmid's Machine row already exists and
            # is being reused (rebuild never creates a new row), but its entry
            # in pool.pool_vmids may be missing from an earlier, unrelated
            # issue (e.g. name-collision cleanup during original creation) —
            # that desyncs the pool's reported VM count from its actual
            # machines even though nothing is otherwise wrong. Ensure it's
            # present and keep pool_number_of_vms consistent with it.
            existing_vmids = pool.pool_vmids or []
            if str(vmid) not in existing_vmids:
                pool.pool_vmids = existing_vmids + [str(vmid)]
                pool.pool_number_of_vms = len(pool.pool_vmids)

            # Same self-heal for pool_machines — the frontend's pool-list
            # "Machines" count (ShowPools.js) reads len(pool.pool_machines),
            # a DIFFERENT list (of machine.identifier, not vmid) normally
            # populated only by the fresh-creation path in activities_machine.py.
            # Rebuild reuses this Machine row directly and never goes through
            # that path, so if this machine's identifier was ever missing from
            # pool_machines (same root cause as the pool_vmids gap), rebuild
            # had no way to notice or fix it either — leaving the pool list
            # showing "No machines" even with a perfectly healthy VM inside.
            existing_machine_ids = pool.pool_machines or []
            if machine.identifier and machine.identifier not in existing_machine_ids:
                pool.pool_machines = existing_machine_ids + [machine.identifier]

            db.commit()

            return {
                "status": "success",
                "vmid": vmid,
                "upid": upid,
                "node": node,
                "cluster_id": str(cluster_id),
                "machine_name": machine.name,
                "ip_address": machine.hostname,
                # AD-join config was previously not returned at all here, so
                # VmRebuildWorkflow had no way to know whether to attach the
                # domain-join snippet — rebuilt VMs silently skipped domain
                # join even when the pool had it configured. join_ad mirrors
                # the same derivation used in update_pool_activity: a pool has
                # AD join configured if it has an AD username set.
                "join_ad": bool(pool.pool_ad_username),
                "pool_ad_domain": pool.pool_ad_domain,
                "pool_ad_path": pool.pool_ad_path,
                "pool_ad_username": pool.pool_ad_username,
                "pool_ad_password": pool.pool_ad_password,
            }

        except Exception as e:
            db.rollback()
            return {"status": "error", "error": str(e)}
    finally:
        db.close()

            
        #     try:
        #         clone_url = f"{PROXMOX_HOST}/api2/json/nodes/{template_node}/qemu/{template_vm_id}/clone"
        #         # template_vm_id is already a str at this point
        #         payload = {
        #             "newid": vmid,
        #             "name": machine.name,
        #             "target": node,
        #             "storage": PROXMOX_STORAGE,
        #             "full": 1
        #         }
        #         resp = requests.post(clone_url, headers=headers, data=payload, verify=False)
        #         resp.raise_for_status()
        #         upid = resp.json()["data"]
        #         machine.error_message = "power-off"
        #         db.commit()
        #         return {"status": "success", "upid": str(upid), "node": str(node), "cluster_id": str(cluster_id),"machine_name": machine.name,"ip_address": machine.hostname}
        #     except Exception as e:
        #         return {"status": "error", "error": "Cloning failed" }
        # except Exception as e:
        #     db.rollback()
        #     return {"status": "error", "error": f"Error in vm_rebuild_activity: {str(e)}"}


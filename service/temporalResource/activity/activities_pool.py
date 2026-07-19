import time
from temporalio import activity
from sqlalchemy.orm import Session
from db_configuration.config import SessionLocal
from models.models import CreateMachineBase, Machine, Pool ,Cluster,CreatePoolBase,UpdateMachineBase
from fastapi import HTTPException
from service.gucamoleService import delete_connection
from fastapi.encoders import jsonable_encoder
from service import controllers
from service.IPService import allocate_ips_across_pools
from models.IPs_model import IPEntry 
from service.proxmoxService import clone_vm
from service.hyper_v_service import delete_hyperv_vm,clone_vm_hyper_v_service
import json
import logging

logger = logging.getLogger(__name__)


@activity.defn()
async def create_pool_activity(request: dict) -> dict:
    db = SessionLocal()
    try:
        pool_data = {key: request[key] for key in CreatePoolBase.__annotations__.keys() if key in request}
        email = pool_data.pop("email", None)
        ip_pool_names = pool_data.get("pool_ip_pool_names")

        # Automated-pool pre-flight checks 
        if pool_data.get("pool_type") == "Automated":
            if not ip_pool_names or not isinstance(ip_pool_names, list) or not ip_pool_names:
                return {
                    "msg": "No IP pools selected for automated assignment. Please select at least one IP pool."
                }

            vm_count = pool_data.get("pool_number_of_vms", 0)
            cluster_id = pool_data.get("cluster_id")

            # Guard against NaN cluster_id from frontend
            if not cluster_id or str(cluster_id).lower() == "nan":
                return {
                    "msg": (
                        "Invalid Cluster Selection: Cluster ID is missing or invalid (NaN). "
                        "Please select a valid cluster."
                    )
                }

            node = pool_data.get("pool_selected_nodes")
            template_vm_id = pool_data.get("pool_template_vm_id")
            name_template = pool_data.get("pool_naming_pattern")
            pool_storage = pool_data.get("pool_storage")

            # Parse template_vm_id if it's a JSON string (common for Hyper-V)
            if isinstance(template_vm_id, str):
                try:
                    template_vm_id = json.loads(template_vm_id)
                except Exception:
                    pass

        #  Database operations (shared by ALL pool types) 
        try:
            existing_pool = db.query(Pool).filter(Pool.pool_name == pool_data["pool_name"]).first()
            if existing_pool:
                # If pool exists, check if it already has machines. 
                # If it has machines, it might be a successful retry.
                # If it has no machines, it's a failed previous attempt.
                machines_count = db.query(Machine).filter(Machine.pool_id == existing_pool.id).count()
                if machines_count > 0:
                    return {
                        "msg": f"Pool '{existing_pool.pool_name}' already exists and contains {machines_count} machine(s).",
                        "pool": jsonable_encoder(existing_pool),
                        "status": "already_exists"
                    }
                else:
                    return {
                        "status": "error",
                        "error_type": "pool_exists_no_machines",
                        "error": f"Pool '{existing_pool.pool_name}' exists but has no machines. Please delete it before retrying.",
                        "msg": f"Pool '{existing_pool.pool_name}' exists but has no machines. Please delete it before retrying."
                    }

            # Second guard before database operations
            if str(pool_data.get("cluster_id", "")).lower() == "nan":
                return {
                    "msg": "Critical Error: Cluster ID is NaN. Aborting pool creation to prevent corruption."
                }

            pool = Pool(**pool_data)
            db.add(pool)
            db.commit()
            db.refresh(pool)
            id_pool = pool.id

            if pool_data.get("cluster_id"):
                pool.cluster_id = f"{id_pool}_{pool_data.get('cluster_id')}"
                db.commit()
                db.refresh(pool)

            machines_json = []

            # Automated pool: clone VMs
            if pool.pool_type == "Automated":
                cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
                if not cluster_data:
                    # Roll back the pool row we just created before returning.
                    db.rollback()
                    return {
                        "status": "error",
                        "error_type": "cluster_not_found",
                        "error": f"Cluster not found for id: {cluster_id}",
                        "msg": f"Cluster not found for id: {cluster_id}"
                    }

                nodes = node if isinstance(node, list) else [node]
                allocated_ips = allocate_ips_across_pools(db, ip_pool_names, vm_count)
                num_allocated = len(allocated_ips)
                num_requested = vm_count
                num_missing = num_requested - num_allocated

                if num_allocated < num_requested:
                    db.rollback()
                    # Release the IPs we just allocated since we are aborting
                    for ip_entry, _ in allocated_ips:
                        ip_obj = db.query(IPEntry).filter(IPEntry.ip == ip_entry["ip"]).first()
                        if ip_obj:
                            ip_obj.status = "unused"
                    db.commit()
                    return {
                        "status": "error",
                        "error_type": "insufficient_ips", 
                        "error": f"Insufficient IPs: Requested {num_requested}, but only {num_allocated} available in the selected pools.",
                        "msg": f"Insufficient IPs: Requested {num_requested}, but only {num_allocated} available in the selected pools."
                    }

                ip_list = [ip_entry["ip"] for ip_entry, _ in allocated_ips]
                ip_pool_assignments = [pool_name for _, pool_name in allocated_ips]

                # Robust cluster detection
                node_type = str(cluster_data.node_type).lower().replace(" ", "") if cluster_data and cluster_data.node_type else ""
                actual_is_cluster = node_type in ("multinode", "cluster")

                clone_payload_dict = {
                    "cluster_id": str(cluster_data.id),
                    "pool_id": id_pool,
                    "node": nodes,
                    "template_vm_id": template_vm_id,
                    "name_template": name_template,
                    "ip_pool_names": ip_pool_assignments,
                    "count": num_allocated,
                    "ip_list": ip_list,
                    "pool_storage": pool_storage,
                    "domain": pool_data.get("pool_ad_domain"),
                    "ou": pool_data.get("pool_ad_path"),
                    "username": pool_data.get("pool_ad_username"),
                    "domain_password": pool_data.get("pool_ad_password"),
                    "join_ad": bool(pool_data.get("join_ad", False)),
                    "email": email,
                    "is_cluster": actual_is_cluster,
                }

                cluster_type = (cluster_data.type or "").strip().lower() if cluster_data else ""
                if cluster_type in ("hyper-v", "hyperv"):
                    response = await clone_vm_hyper_v_service(clone_payload_dict)
                elif cluster_type == "proxmox":
                    response = await clone_vm(clone_payload_dict)
                    if isinstance(response, dict) and "error" in response:
                        raise Exception(response["error"])
                    if response.get("error_type") == "clone_failed":
                        raise Exception(response.get("error", "Proxmox cloning failed"))

                assigned_vms = response.get("vms", [])
                logger.info(f"Cloning response received. Number of VMs: {len(assigned_vms)}")
                logger.debug(f"Raw assigned_vms: {assigned_vms}")

                # ── Release any unused IPs (if cloning failed or partially failed) ───
                used_ip_values = {vm.get("ip") for vm in assigned_vms if vm.get("ip")}
                for ip_entry_dict, _ in allocated_ips:
                    ip_val = ip_entry_dict["ip"]
                    if ip_val not in used_ip_values:
                        ip_obj = db.query(IPEntry).filter(IPEntry.ip == ip_val).first()
                        if ip_obj:
                            ip_obj.status = "unused"
                            ip_obj.vm_id = None
                db.commit()

                if cluster_type == "proxmox":
                    # CloneVMWorkflow already created the Machine rows (and set
                    # pool.pool_vmids) step-by-step as each VM was cloned/IP'd/
                    # powered on — that happened in a separate DB session, so
                    # refresh to see it, and just read back what it built.
                    db.refresh(pool)
                    machines_json = [
                        jsonable_encoder(m) for m in response.get("machines", []) if m
                    ]
                else:
                    # Hyper-V path is unchanged: it doesn't create machine rows
                    # itself, so build them here exactly as before.
                    pool.pool_vmids = [str(vm.get("vmid")) for vm in assigned_vms if vm.get("vmid")]
                    db.commit()
                    db.refresh(pool)

                    for idx, vm in enumerate(assigned_vms):
                        name = vm.get("name") or f"vm-{vm.get('vmid', '')}"
                        vmid = vm.get("vmid")
                        vm_node = vm.get("node") or (nodes[0] if nodes else None)
                        ip = vm.get("ip") or (ip_list[idx] if idx < len(ip_list) else None)

                        if ip:
                            ip_entry = db.query(IPEntry).filter(IPEntry.ip == ip).first()
                            if ip_entry:
                                ip_entry.status = "used"
                                ip_entry.vm_id = str(vmid)

                        try:
                            workflow_ids = [
                                wid
                                for wid in [vm.get("clone_workflow_id"), vm.get("wait_assign_workflow_id")]
                                if wid
                            ]
                            machine_data = machinedata(
                                email, None, pool,
                                vm_id=str(vmid) if vmid is not None else None,
                                name=name, hostname=ip or "",
                                workflow_ids=workflow_ids,
                            )
                            machine_data_obj = CreateMachineBase(**machine_data)
                            machine_result = await controllers.create_machine(machine_data_obj, db=db)
                            machines_json.append(jsonable_encoder(machine_result))
                        except Exception as e:
                            logger.error(f"Error creating machine for vmid {vmid}: {e}")
                            continue

                    db.commit()

                msg = f"Pool and {num_allocated} VM(s) created successfully."
                if num_missing > 0:
                    msg += (
                        f" {num_missing} VM(s) not created due to insufficient "
                        "available IPs in the selected pools."
                    )

                result = {
                    "msg": msg,
                    "pool": jsonable_encoder(pool),
                    "machines": machines_json,
                }
                if cluster_type == "proxmox" and response.get("partial_failure"):
                    # Don't let orphaned Proxmox VMs (cloned but not registered
                    # due to e.g. a stale Guacamole name collision) go unnoticed.
                    result["partial_failure"] = True
                    result["failed_vms"] = response.get("failed_vms", [])
                    result["msg"] = response.get("msg", msg)
                return result

            return {
                "msg": "Pool created successfully",
                "pool": jsonable_encoder(pool),
                "machines": machines_json,
            }

        except Exception as e:
            db.rollback()
            raise Exception(str(e))

    finally:
        db.close()
 
def machinedata(email, machine, db_pool, *, vm_id=None, name=None, hostname=None,
                 workflow_ids=None, provisioning_status=None):
    """
    Builds the machine_data dict from pool settings.
    - machine given (update-pool path, unchanged): fields come from the existing row.
    - machine=None (clone path): fields come from vm_id/name/hostname/workflow_ids,
      since there's no row yet — one being created for the first time.
    """
    machine_data = {
        "vm_id": machine.vm_id if machine else vm_id,
        "name": machine.name if machine else name,
        "hostname": machine.hostname if machine else hostname,
        "guacd_hostname": machine.guacd_hostname if machine else db_pool.pool_guacd_hostname,
        "identifier": machine.identifier if machine else None,
        "status": machine.status if machine else "RUNNING",
        "error_message": machine.error_message if machine else None,
        "workflow_status": machine.workflow_status if machine else None,
        "workflowId": machine.workflowId if machine else (workflow_ids or []),
        "clone_workflow_id": None if machine else (workflow_ids or []),
        "provisioning_status": (machine.provisioning_status if machine else None) or provisioning_status or "cloned",
        "pool_id": db_pool.id,
        "port": db_pool.pool_port,
        "protocol": db_pool.pool_protocol,
        "guacd_port": db_pool.pool_guacd_port,
        "guacd_encryption": db_pool.pool_guacd_encryption,
        "guacd_hostname": db_pool.pool_guacd_hostname,
        "pool_id": db_pool.id,
        "weight": db_pool.pool_weight,
        "os_type": db_pool.pool_os_type,
        "failover_only": db_pool.pool_failover_only,
        "username": db_pool.pool_username,
        "password": db_pool.pool_password,
        "security": db_pool.pool_security,
        "domain": db_pool.pool_domain,
        "disable_auth": db_pool.pool_disable_auth,
        "ignore_cert": db_pool.pool_ignore_cert,
        "max_connections": db_pool.pool_max_connections,
        "max_connections_per_user": db_pool.pool_max_connections_per_user,
        "gateway_port": db_pool.pool_gateway_port,
        "gateway_username": db_pool.pool_gateway_username,
        "gateway_password": db_pool.pool_gateway_password,
        "gateway_domain": db_pool.pool_gateway_domain,
        "initial_program": db_pool.pool_initial_program,
        "client_name": db_pool.pool_client_name,
        "timezone": db_pool.pool_timezone,
        "console": db_pool.pool_console,
        "width": db_pool.pool_width,
        "height": db_pool.pool_height,
        "dpi": db_pool.pool_dpi,
        "color_depth": db_pool.pool_color_depth,
        "resize_method": db_pool.pool_resize_method,
        "read_only": db_pool.pool_read_only,
        "clipboard_encoding": db_pool.pool_clipboard_encoding,
        "disable_copy": db_pool.pool_disable_copy,
        "disable_paste": db_pool.pool_disable_paste,
        "console_audio": db_pool.pool_console_audio,
        "enable_audio_input": db_pool.pool_enable_audio_input,
        "enable_printing": db_pool.pool_enable_printing,
        "printer_name": db_pool.pool_printer_name,
        "enable_drive": db_pool.pool_enable_drive,
        "drive_name": db_pool.pool_drive_name,
        "drive_path": db_pool.pool_drive_path,
        "cursor": db_pool.pool_cursor,
        "enable_wallpaper": db_pool.pool_enable_wallpaper,
        "enable_theming": db_pool.pool_enable_theming,
        "enable_font_smoothing": db_pool.pool_enable_font_smoothing,
        "enable_full_window_drag": db_pool.pool_enable_full_window_drag,
        "enable_desktop_composition": db_pool.pool_enable_desktop_composition,
        "enable_menu_animations": db_pool.pool_enable_menu_animations,
        "disable_bitmap_caching": db_pool.pool_disable_bitmap_caching,
        "disable_offscreen_caching": db_pool.pool_disable_offscreen_caching,
        "disable_glyph_caching": db_pool.pool_disable_glyph_caching,
        "load_balance_info": db_pool.pool_load_balance_info,
        "recording_path": db_pool.pool_recording_path,
        "recording_name": db_pool.pool_recording_name,
        "create_recording_path": db_pool.pool_create_recording_path,
        "recording_exclude_mouse": db_pool.pool_recording_exclude_mouse,
        "recording_include_keys": db_pool.pool_recording_include_keys,
        "exclude_touch_events": db_pool.pool_exclude_touch_events,
        "enable_sftp": db_pool.pool_enable_sftp,
        "sftp_port": db_pool.pool_sftp_port,
        "sftp_username": db_pool.pool_sftp_username,
        "font_name": db_pool.pool_font_name,
        "sftp_password": db_pool.pool_sftp_password,
        "sftp_host_key": db_pool.pool_sftp_host_key,
        "sftp_private_key": db_pool.pool_sftp_private_key,
        "sftp_passphrase": db_pool.pool_sftp_passphrase,
        "sftp_root_directory": db_pool.pool_sftp_root_directory,
        "sftp_directory": db_pool.pool_sftp_directory,
        "sftp_server_alive_interval": db_pool.pool_sftp_server_alive_interval,
        "private_key": db_pool.pool_private_key,
        "passphrase": db_pool.pool_passphrase,
        "color_scheme": db_pool.pool_color_scheme,
        "scrollback": db_pool.pool_scrollback,
        "font_size": db_pool.pool_font_size,
        "backspace": db_pool.pool_backspace,
        "terminal_type": db_pool.pool_terminal_type,
        "typescript_path": db_pool.pool_typescript_path,
        "typescript_name": db_pool.pool_typescript_name,
        "create_typescript_path": db_pool.pool_create_typescript_path,
        "swap_red_blue": db_pool.pool_swap_red_blue,
        "destination_host": db_pool.pool_dest_host,
        "destination_port": db_pool.pool_dest_port,
        "exclude_mouse": db_pool.pool_exclude_mouse,
        "exclude_graphics_streams": db_pool.pool_exclude_graphics_streams,
        "enable_audio": db_pool.pool_enable_audio,
        "audio_servername": db_pool.pool_audio_servername,
        "args": db_pool.pool_args,
        "is_custom_machine": False,
        "email": email,
        "users_assigned": machine.users_assigned if machine else None,
        }
    return machine_data


@activity.defn
async def finalize_cloned_machine_activity(payload: dict) -> dict:
    """
    Runs right after a VM is cloned in Proxmox: marks its IP as used, records
    the vmid on the pool, and creates the Machine DB row + Guacamole connection
    by calling the raw create_machine_activity directly (not via
    controllers.create_machine/CreateMachineWorkflow) so its automatic
    power-on trigger doesn't fire early — the clone workflow powers VMs on
    itself, after IP + domain-join are configured.
    """
    from service.temporalResource.activity import activities_machine

    pool_id = payload["pool_id"]
    vmid = payload["vmid"]
    name = payload["name"]
    ip = payload.get("ip")
    email = payload.get("email")
    workflow_ids = payload.get("workflow_ids") or []

    db = SessionLocal()
    try:
        pool = db.query(Pool).filter(Pool.id == pool_id).first()
        if not pool:
            return {"status": "error", "error": f"Pool {pool_id} not found"}

        if ip:
            ip_entry = db.query(IPEntry).filter(IPEntry.ip == ip).first()
            if ip_entry:
                ip_entry.status = "used"
                ip_entry.vm_id = str(vmid)

        existing_vmids = pool.pool_vmids or []
        if str(vmid) not in existing_vmids:
            pool.pool_vmids = existing_vmids + [str(vmid)]

        db.commit()

        machine_data = machinedata(
            email, None, pool,
            vm_id=str(vmid), name=name, hostname=ip or "",
            workflow_ids=workflow_ids,
        )
    finally:
        db.close()

    result = await activities_machine.create_machine_activity(machine_data)
    if result.get("msg") != "Machine created successfully":
        if result.get("error_type") == "name_collision":
            # The VM is real in Proxmox but can never be registered under this
            # name — keeping it around just wastes resources and blocks the
            # IP. Undo the clone: delete the VM and release the IP/vmid.
            await _cleanup_unregisterable_vm(pool_id, vmid, ip)
        return {"status": "error", "error": result}
    return {"status": "ok", "machine": result.get("machine")}


async def _cleanup_unregisterable_vm(pool_id: int, vmid, ip: str | None) -> None:
    from service.proxmoxService import delete_proxmox_vm

    db = SessionLocal()
    try:
        pool = db.query(Pool).filter(Pool.id == pool_id).first()
        if not pool:
            return

        try:
            cluster_id_raw = str(pool.cluster_id)
            cluster_id = cluster_id_raw.split("_")[-1] if "_" in cluster_id_raw else cluster_id_raw
            cluster = db.query(Cluster).filter(Cluster.id == int(cluster_id)).first()
            if cluster:
                await delete_proxmox_vm(vmid, cluster)
                logger.info(f"Cleaned up orphaned (unregisterable) VM {vmid} in Proxmox.")
        except Exception as e:
            logger.error(f"Failed to delete orphaned VM {vmid} in Proxmox — needs manual cleanup: {e}")

        if ip:
            ip_entry = db.query(IPEntry).filter(IPEntry.ip == ip).first()
            if ip_entry:
                ip_entry.status = "unused"
                ip_entry.vm_id = None

        existing_vmids = pool.pool_vmids or []
        if str(vmid) in existing_vmids:
            pool.pool_vmids = [v for v in existing_vmids if v != str(vmid)]

        db.commit()
    finally:
        db.close()


@activity.defn
async def configure_domain_join_activity(payload: dict) -> dict:
    """Thin wrapper so the clone workflow can attach the AD-join cloud-init
    snippet to already-cloned VMs before they're powered on."""
    return await domain_join_activity(
        payload["pool_id"],
        payload["pool_ad_domain"],
        payload["pool_ad_password"],
        payload["pool_ad_username"],
        payload.get("pool_ad_path", ""),
    )


@activity.defn()
async def update_pool_activity(pool_id: int, pool_data: dict) -> dict:
    from fastapi.encoders import jsonable_encoder

    db = SessionLocal()
    try:
        # ── Fetch pool ───────────────────────────────────────────────────────
        db_pool = db.query(Pool).filter(Pool.id == pool_id).first()
        if db_pool is None:
            raise Exception("Pool not found")

        is_automated = db_pool.pool_type == "Automated"
        old_vm_count = db_pool.pool_number_of_vms if is_automated else 0

        # ── Enforce no-decrease policy for Automated pools ───────────────────
        if is_automated:
            requested_vm_count = pool_data.get("pool_number_of_vms", old_vm_count)
            if requested_vm_count is None:
                requested_vm_count = old_vm_count
            try:
                requested_vm_count = int(requested_vm_count)
            except Exception:
                requested_vm_count = old_vm_count

            # If user tries to decrease, silently keep existing count
            new_vm_count = max(requested_vm_count, old_vm_count)
        else:
            new_vm_count = 0

        added_count = new_vm_count - old_vm_count

        machines_json = []
        vm_add_error = None
        vms = []
        response = {}
        cluster_type = ""
        email = pool_data.get("email", None)

        # ── Clone additional VMs when count increased ────────────────────────
        if is_automated and added_count > 0:
            raw_cluster_id = db_pool.cluster_id
            cluster_id_str = raw_cluster_id.split("_")[-1] if "_" in raw_cluster_id else raw_cluster_id
            try:
                cluster_id = int(cluster_id_str)
            except ValueError:
                # FIX: raise a plain Exception — HTTPException is not meaningful
                #      inside a Temporal activity; let the caller handle HTTP mapping.
                raise Exception(f"Invalid cluster_id format: {raw_cluster_id}")

            cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
            if not cluster_data:
                raise Exception(f"Cluster not found for id: {cluster_id}")

            ip_pool_names = db_pool.pool_ip_pool_names

            # Prefer new template value from incoming pool_data if provided
            template_vm_id = pool_data.get("pool_template_vm_id", db_pool.pool_template_vm_id)
            if isinstance(template_vm_id, str):
                try:
                    template_vm_id = json.loads(template_vm_id)
                except Exception:
                    try:
                        template_vm_id = int(template_vm_id)
                    except Exception:
                        pass

            nodes = (
                db_pool.pool_selected_nodes
                if isinstance(db_pool.pool_selected_nodes, list)
                else [db_pool.pool_selected_nodes]
            )
            name_template = db_pool.pool_naming_pattern
            pool_storage = pool_data.get("pool_storage", db_pool.pool_storage)

            allocated_ips = allocate_ips_across_pools(db, ip_pool_names, added_count)
            if not allocated_ips:
                return {
                    "status": "error",
                    "error_type": "insufficient_ips",
                    "error": "No available IPs in the selected IP pools to create any additional VMs.",
                    "msg": "No available IPs in the selected IP pools to create any additional VMs."
                }

            num_allocated = len(allocated_ips)
            ip_list = [ip_entry["ip"] for ip_entry, _ in allocated_ips]
            ip_pool_assignments = [pool_name for _, pool_name in allocated_ips]

            # Resolve template value for the target cluster type
            cluster_type = (cluster_data.type or "").strip().lower()
            if cluster_type == "proxmox":
                if isinstance(template_vm_id, dict):
                    if "vmid" in template_vm_id:
                        try:
                            template_for_clone = int(template_vm_id["vmid"])
                        except Exception:
                            raise Exception("Invalid template_vm_id value for Proxmox: must be numeric")
                    else:
                        raise Exception("Invalid template data for Proxmox: missing 'vmid' key")
                else:
                    try:
                        template_for_clone = int(template_vm_id)
                    except Exception:
                        raise Exception("Invalid template id for Proxmox cluster: must be numeric")
            elif cluster_type in ("hyper-v", "hyperv"):
                if not isinstance(template_vm_id, dict):
                    raise Exception("Invalid template data for Hyper-V: expected JSON object with template properties")
                template_for_clone = template_vm_id
            else:
                template_for_clone = template_vm_id

            # Robust cluster detection
            node_type = str(cluster_data.node_type).lower().replace(" ", "") if cluster_data and cluster_data.node_type else ""
            actual_is_cluster = node_type in ("multinode", "cluster")

            clone_payload_dict = {
                "cluster_id": str(cluster_data.id),
                "pool_id": db_pool.id,
                "node": nodes,
                "template_vm_id": template_for_clone,
                "name_template": name_template,
                "ip_pool_names": ip_pool_assignments,
                "count": num_allocated,
                "ip_list": ip_list,
                "pool_storage": pool_storage,
                "domain": pool_data.get("pool_ad_domain", db_pool.pool_ad_domain),
                "ou": pool_data.get("pool_ad_path", db_pool.pool_ad_path),
                "username": pool_data.get("pool_ad_username", db_pool.pool_ad_username),
                "domain_password": pool_data.get("pool_ad_password", db_pool.pool_ad_password),
                "join_ad": bool(pool_data.get("join_ad", False)),
                "email": email,
                "is_cluster": actual_is_cluster,
            }

            try:
                if cluster_type in ("hyper-v", "hyperv"):
                    response = await clone_vm_hyper_v_service(clone_payload_dict)
                else:
                    response = await clone_vm(clone_payload_dict)

                if isinstance(response, dict) and "error" in response:
                    err_msg = response["error"]
                    if "Template VM" in err_msg and "not found in cluster" in err_msg:
                        vm_add_error = f"but new VMs were NOT added: {err_msg}"
                        vms = []
                    else:
                        raise Exception(err_msg)
                else:
                    vms = response.get("vms", [])
            except Exception as e:
                err_msg = str(e)
                if "Template VM" in err_msg and "not found in cluster" in err_msg:
                    vm_add_error = f"but new VMs were NOT added: {err_msg}"
                    vms = []
                else:
                    raise

            # ── Release any unused IPs (if cloning failed or partially failed) ───
            used_ip_values = {vm.get("ip") for vm in vms if vm.get("ip")}
            for ip_entry_dict, _ in allocated_ips:
                ip_val = ip_entry_dict["ip"]
                if ip_val not in used_ip_values:
                    ip_obj = db.query(IPEntry).filter(IPEntry.ip == ip_val).first()
                    if ip_obj:
                        ip_obj.status = "unused"
                        ip_obj.vm_id = None
            db.commit()

        # ── Apply field updates to the pool row ──────────────────────────────
        # FIX: always exclude pool_number_of_vms and email from the blind setattr
        #      loop — these are managed explicitly below / not a DB column.
        EXCLUDED_FIELDS = {"pool_number_of_vms", "email"}
        for field, value in pool_data.items():
            if field not in EXCLUDED_FIELDS:
                setattr(db_pool, field, value)

        if is_automated and added_count > 0 and vms:
            db_pool.pool_number_of_vms = old_vm_count + len(vms)
            if cluster_type != "proxmox":
                # For Proxmox, CloneVMWorkflow already appended each new vmid
                # to pool.pool_vmids (in its own DB session) as part of
                # finalize_cloned_machine_activity — recomputing it here from
                # this function's stale pre-clone `db_pool` snapshot would
                # race with / overwrite that. Hyper-V still needs it set here.
                existing_vmids = db_pool.pool_vmids or []
                new_vmids = [str(vm["vmid"]) for vm in vms if "vmid" in vm]
                db_pool.pool_vmids = existing_vmids + new_vmids

        db.commit()
        db.refresh(db_pool)

        # ── Create machine records for each newly cloned VM ──────────────────
        # For Proxmox, CloneVMWorkflow already created these rows (and set
        # pool.pool_vmids) as part of its clone→IP→domain-join→power-on
        # sequence — read them back from the workflow's response instead of
        # creating them again here. Hyper-V keeps its own machine-creation
        # loop since clone_vm_hyper_v_service doesn't create rows itself.
        if is_automated and added_count > 0 and vms:
            if cluster_type == "proxmox":
                machines_json.extend(
                    jsonable_encoder(m) for m in response.get("machines", []) if m
                )
            else:
                for idx, vm in enumerate(vms):
                    name = vm.get("name") or f"vm-{vm.get('vmid', '')}"
                    vmid = vm.get("vmid")
                    ip = vm.get("ip") or (ip_list[idx] if idx < len(ip_list) else None)

                    # Mark IP as used
                    try:
                        if ip:
                            ip_entry = db.query(IPEntry).filter(IPEntry.ip == ip).first()
                            if ip_entry:
                                ip_entry.status = "used"
                                ip_entry.vm_id = str(vmid)
                    except Exception as e:
                        print(f"Failed to mark IP {ip} as used: {e}")

                    # Create machine record
                    try:
                        workflow_ids = [
                            wid
                            for wid in [vm.get("clone_workflow_id"), vm.get("wait_assign_workflow_id")]
                            if wid
                        ]
                        machine_data = machinedata(
                            email, None, db_pool,
                            vm_id=str(vmid) if vmid is not None else None,
                            name=name, hostname=ip or "",
                            workflow_ids=workflow_ids,
                        )
                        machine_data["printer_name"] = pool_data.get("pool_printer_name", db_pool.pool_printer_name)
                        machine_data_obj = CreateMachineBase(**machine_data)
                        machine_result = await controllers.create_machine(machine_data_obj, db=db)
                        machines_json.append(jsonable_encoder(machine_result))
                    except Exception as e:
                        logger.error(f"[update_pool] Warning: failed to create machine for vmid={vmid}: {e}")
                        continue

            db.commit()
            db.refresh(db_pool)

        # ── Sync existing (non-custom) machines with updated pool config ──────
        existing_machines = (
            db.query(Machine)
            .filter(Machine.pool_id == db_pool.id, Machine.is_custom_machine == False)
            .all()
        )
        for machine in existing_machines:
            try:
                machine_update_data = machinedata(email, machine, db_pool)
                machine_update_data["identifier"] = machine.identifier
                machine_update_data_obj = UpdateMachineBase(**machine_update_data)
                await controllers.update_machine(machine.identifier, machine_update_data_obj)
            except Exception as e:
                db.rollback()
                # FIX: same swallowed-continue pattern — log so failures are visible
                print(f"[update_pool] Warning: failed to update machine {machine.identifier}: {e}")
                continue

        # ── Build response ───────────────────────────────────────────────────
        db_pool_json = jsonable_encoder(db_pool)
        machines_in_pool = jsonable_encoder(
            db.query(Machine).filter(Machine.pool_id == db_pool.id).all()
        )

        msg = "Pool updated successfully"
        if is_automated and added_count > 0:
            if vms:
                msg += f", {len(vms)} new VM(s) added."
            elif vm_add_error:
                msg += f", {vm_add_error}"

        result = {"msg": msg, "pool": db_pool_json, "machines": machines_in_pool}
        if cluster_type == "proxmox" and response.get("partial_failure"):
            # Don't let orphaned Proxmox VMs (cloned but not registered due to
            # e.g. a stale Guacamole name collision) go unnoticed.
            result["partial_failure"] = True
            result["failed_vms"] = response.get("failed_vms", [])
            result["msg"] = response.get("msg", msg)
        return result

    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()

@activity.defn()
async def delete_pool_activity(pool_id: int) -> dict:
    from service.proxmoxService import delete_proxmox_vm
    db: Session = SessionLocal()
    try:
        try:
            pool = db.query(Pool).filter(Pool.id == pool_id).first()
            if not pool:
                return {
                    "status": "error",
                    "error_type": "pool_not_found",
                    "error": f"Pool not found with id - {pool_id}",
                    "msg": f"Pool not found with id - {pool_id}"
                }
            pool_machines = pool.pool_machines or []
            pool_vmids = pool.pool_vmids or []
            cluster_pool_id = pool.cluster_id
            if pool.pool_type == "Automated":
                try:
                    parts = cluster_pool_id.split("_")
                    id_cluster = parts[1] if len(parts) >= 2 else parts[0]
                except Exception as e:
                    raise RuntimeError(str(e))
                cluster_data = db.query(Cluster).filter(Cluster.id == id_cluster).first()
                for vmid in pool_vmids:
                    machine = db.query(Machine).filter(Machine.vm_id == str(vmid)).first()
                    if vmid:
                        try:
                            if cluster_data.type.lower()=="proxmox":
                                
                                await delete_proxmox_vm(vmid, cluster_data)
                                vmid_str = str(vmid)
                            elif cluster_data.type.lower() in ("hyper-v", "hyperv"):
                                vmid_str =vmid
                                vhdpath = pool.pool_template_vm_id.get("vhdPath", "")
                                vhdpath += machine.name
                                await delete_hyperv_vm(vmid, db)
                            ip_entries = db.query(IPEntry).filter(IPEntry.vm_id == vmid_str, IPEntry.status == "used").all()
                            for ip_entry in ip_entries:
                                ip_entry.status = "unused"
                                ip_entry.vm_id = None

                            db.commit()

                            for ip_entry in ip_entries:
                                db.refresh(ip_entry)
                        except Exception as e:
                            db.rollback()

            for machine_item in pool_machines:
                machine = db.query(Machine).filter(Machine.identifier == machine_item).first()
                if machine:
                    try:
                        await delete_connection(machine.identifier)
                        db.delete(machine)
                        db.commit()
                    except Exception as e:
                        db.rollback()
            db.delete(pool)
            db.commit()
            all_pools = db.query(Pool).all()
            all_pools_json = jsonable_encoder(all_pools)
            return {"pools": all_pools_json}
        except Exception as e:
            db.rollback()
            return {"msg": f"An error occurred while deleting the pool: {str(e)}"}
    finally:
        db.close()

@activity.defn()
async def retrieve_pool_data_activity(pool_name: str) :
    db: Session = SessionLocal()
    try:
        try:
            pool = db.query(Pool).filter(Pool.pool_name == pool_name).first()
            pool_json = jsonable_encoder(pool)
            
            if pool_json:
                cluster_id_raw = pool_json.get("cluster_id")
                if cluster_id_raw:
                    c_id = cluster_id_raw.split("_")[-1] if "_" in cluster_id_raw else cluster_id_raw
                    try:
                        c_id_int = int(c_id)
                        cluster = db.query(Cluster).filter(Cluster.id == c_id_int).first()
                        if cluster:
                            pool_json["cluster"] = cluster.name
                        else:
                            pool_json["cluster"] = "NA"
                    except (ValueError, TypeError):
                        pool_json["cluster"] = "NA"
                else:
                    pool_json["cluster"] = "NA"
                    
                return {"msg": f"{pool_name} Pool found ", "pool": pool_json}
            else:
                return {"msg": f"{pool_name} Pool not found"}
        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=500, detail=f"An error occurred while retrieving the pool: {str(e)}")
    finally:
        db.close()

    


@activity.defn()
async def list_all_pool_names_activity():
    db = SessionLocal()
    try:
        try:
            pools = db.query(Pool).all()
            pool_names = [pool_item.pool_name for pool_item in pools]
            return {"msg": "listed all the Pool names successfully", "pool_names": pool_names}
        except Exception as e:
            db.rollback()
            raise Exception(str(e))
    finally:
        db.close()

    

@activity.defn()
async def get_all_pools_activity():
    db = SessionLocal()
    try:
        try:
            pools = db.query(Pool).all()
            pools_json = jsonable_encoder(pools)
            
            # Fetch cluster names for each pool
            for pool_data in pools_json:
                cluster_id_raw = pool_data.get("cluster_id")
                if cluster_id_raw:
                    # Handle the case where cluster_id is formatted as "poolid_clusterid"
                    c_id = cluster_id_raw.split("_")[-1] if "_" in cluster_id_raw else cluster_id_raw
                    try:
                        c_id_int = int(c_id)
                        cluster = db.query(Cluster).filter(Cluster.id == c_id_int).first()
                        if cluster:
                            pool_data["cluster"] = cluster.name
                        else:
                            pool_data["cluster"] = "NA"
                    except (ValueError, TypeError):
                        pool_data["cluster"] = "NA"
                else:
                    pool_data["cluster"] = "NA"
                    
            return {"msg": "listed all the Pools successfully", "pools": pools_json}
        except Exception as e:
            db.rollback()  
            raise Exception(str(e))
    finally:
        db.close()


    

@activity.defn()
async def get_pool_details_id_activity(pool_id: int):

    db = SessionLocal()
    try:
        try:
            pool = db.query(Pool).filter(Pool.id == pool_id).first()
            pool_json = jsonable_encoder(pool)
            if pool_json:
                cluster_id_raw = pool_json.get("cluster_id")
                if cluster_id_raw:
                    c_id = cluster_id_raw.split("_")[-1] if "_" in cluster_id_raw else cluster_id_raw
                    try:
                        c_id_int = int(c_id)
                        cluster = db.query(Cluster).filter(Cluster.id == c_id_int).first()
                        if cluster:
                            pool_json["cluster"] = cluster.name
                        else:
                            pool_json["cluster"] = "NA"
                    except (ValueError, TypeError):
                        pool_json["cluster"] = "NA"
                else:
                    pool_json["cluster"] = "NA"
                    
                return {"msg": f"Pool Retrived Successfully", "pool": pool_json}
            else:
                return {"msg": f"Pool not found "}
        except Exception as e:
            db.rollback()
            raise Exception(str(e))
    finally:
        db.close()



@activity.defn()
async def domain_join_activity(pool_id: int, pool_ad_domain: str, pool_ad_password: str, pool_ad_username: str, pool_ad_path: str) -> dict:
    import paramiko
    db: Session = SessionLocal()
    try:
        try:
            pool = db.query(Pool).filter(Pool.id == pool_id).first()
            if not pool:
                return {"msg": f"Pool not found with id {pool_id}"}
            
            vm_ids = pool.pool_vmids or []
            if not vm_ids:
                return {"msg": "No VMs found in the pool to join domain."}
                
            cluster_id = pool.cluster_id
            if cluster_id:
                cluster_id = cluster_id.split("_")[-1] if "_" in str(cluster_id) else cluster_id
            cluster = db.query(Cluster).filter(Cluster.id == int(cluster_id)).first()
            if not cluster:
                return {"msg": "Cluster not found"}
            host = cluster.ip.split(",")[0].strip() if cluster.ip else ""
            user = cluster.username.split("@")[0] if cluster.username else ""
            proxmox_password = cluster.password
    
            domain = pool_ad_domain #"rcvdev.team"
            username = pool_ad_username #"rcvdev\\administrator"
            password = pool_ad_password #"Teamw0rk@1"
            ou_path_input = pool_ad_path # "OU11/OU1"
            
            ou_components = []
            if ou_path_input:
                ou_parts = [p.strip() for p in ou_path_input.split("/") if p.strip()]
                for part in reversed(ou_parts):
                    ou_components.append(f"OU={part}")
    
            domain_parts = [p.strip() for p in domain.split(".") if p.strip()]
            for part in domain_parts:
                ou_components.append(f"DC={part}")
    
            final_ou_path = ",".join(ou_components)
            ou_args = f'-OUPath "{final_ou_path}" `' if final_ou_path else ""
    
            yaml_content = f"""#cloud-config
            write_files:
                - path: "C:\\\\join-domain.ps1"
                  content: |
                    $domain = "{domain}"
                    $username = "{username}"
                    $password = "{password}"
    
                    $securePassword = ConvertTo-SecureString $password -AsPlainText -Force
                    $credential = New-Object System.Management.Automation.PSCredential ($username, $securePassword)
    
                    Write-Host "Waiting for network..."
    
                    do {{
                        $net = Test-NetConnection -ComputerName "172.16.0.51" -InformationLevel Quiet
                        Start-Sleep -Seconds 10
                    }} until ($net -eq $true)
    
                    do {{
                        nltest /dsgetdc:$domain
                        Start-Sleep -Seconds 10
                    }} until ($LASTEXITCODE -eq 0)
    
                    Add-Computer `
                    -DomainName $domain `
                    -Credential $credential `
                    {ou_args}
                    -Force
                    
                    Start-Sleep -Seconds 30
                    Restart-Computer -Force
    
            runcmd:
                - powershell.exe -ExecutionPolicy Bypass -File "C:\\\\join-domain.ps1"
            """
    
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            try:
                ssh.connect(host, username=user, password=proxmox_password)
                ssh.exec_command("mkdir -p /var/lib/vz/snippets")
    
                sftp = ssh.open_sftp()
                file = sftp.file(f"/var/lib/vz/snippets/join-domain-pool-{pool_id}.yml", "w")
                file.write(yaml_content)
                file.close()
    
                for vm_id in vm_ids:
    
                    cmd = f"qm set {vm_id} --cicustom user=local:snippets/join-domain-pool-{pool_id}.yml"
    
                    max_retry = 10
    
                    for i in range(max_retry):
    
                        stdin, stdout, stderr = ssh.exec_command(cmd)
    
                        exit_status = stdout.channel.recv_exit_status()
                        out = stdout.read().decode()
                        err = stderr.read().decode()
    
                        print("EXIT:", exit_status)
                        print("OUT:", out)
                        print("ERR:", err)
    
                        if "can't lock file" in err:
                            print(f"VM {vm_id} locked, waiting 5s...")
                            time.sleep(5)
                        else:
                            print("Script attached successfully")
                            break
    
            except Exception as e:
                return {"status": "error", "error": f"SSH/Proxmox error: {str(e)}"}
            finally:
                ssh.close()
                
            return {"msg": "Domain join workflow executed successfully"}
        except Exception as e:
            db.rollback()
            raise Exception(str(e))
    finally:
        db.close()

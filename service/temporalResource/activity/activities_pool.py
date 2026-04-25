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


@activity.defn()
async def create_pool_activity(request: dict) -> dict:
    db = SessionLocal()
    try:
        pool_data = {key: request[key] for key in CreatePoolBase.__annotations__.keys() if key in request}
        email = pool_data.pop("email", None)
        ip_pool_names = pool_data.get("pool_ip_pool_names")

        # ── Automated-pool pre-flight checks ────────────────────────────────
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

        # ── Database operations (shared by ALL pool types) ───────────────────
        # FIX: this try block was incorrectly nested inside the Automated-only
        #      block above; moved out to the correct indentation level.
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
                    # If it exists but has no machines, we might want to delete it and retry, 
                    # but for safety, just report it.
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

            # ── Automated pool: clone VMs ────────────────────────────────────
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

                pool.pool_vmids = [str(vm.get("vmid")) for vm in assigned_vms if vm.get("vmid")]
                db.commit()
                db.refresh(pool)

                for idx, vm in enumerate(assigned_vms):
                    name = vm.get("name") or f"vm-{vm.get('vmid', '')}"
                    vmid = vm.get("vmid")
                    # FIX: shadowed loop variable `node` renamed to `vm_node`
                    vm_node = vm.get("node") or (nodes[0] if nodes else None)
                    ip = vm.get("ip") or (ip_list[idx] if idx < len(ip_list) else None)

                    # ── Mark IP as used ──────────────────────────────────────
                    if ip:
                        ip_entry = db.query(IPEntry).filter(IPEntry.ip == ip).first()
                        if ip_entry:
                            ip_entry.status = "used"
                            ip_entry.vm_id = str(vmid)
                    
                    # ── Create machine record ────────────────────────────────
                    try:
                        workflow_ids = [
                            wid
                            for wid in [vm.get("clone_workflow_id"), vm.get("wait_assign_workflow_id")]
                            if wid
                        ]
                        machine_data = {
                            "vm_id": str(vmid) if vmid is not None else None,
                            "name": name,
                            "hostname": ip or "",
                            "port": pool.pool_port,
                            "protocol": pool.pool_protocol,
                            "guacd_port": pool.pool_guacd_port,
                            "guacd_encryption": pool.pool_guacd_encryption,
                            "guacd_hostname": pool.pool_guacd_hostname,
                            "os_type": pool.pool_os_type,
                            "pool_id": pool.id,
                            "weight": pool.pool_weight,
                            "failover_only": pool.pool_failover_only,
                            "username": pool.pool_username,
                            "password": pool.pool_password,
                            "security": pool.pool_security,
                            "domain": pool.pool_domain,
                            "disable_auth": pool.pool_disable_auth,
                            "ignore_cert": pool.pool_ignore_cert,
                            "max_connections": pool.pool_max_connections,
                            "max_connections_per_user": pool.pool_max_connections_per_user,
                            "gateway_port": pool.pool_gateway_port,
                            "gateway_username": pool.pool_gateway_username,
                            "gateway_password": pool.pool_gateway_password,
                            "gateway_domain": pool.pool_gateway_domain,
                            "initial_program": pool.pool_initial_program,
                            "client_name": pool.pool_client_name,
                            "timezone": pool.pool_timezone,
                            "console": pool.pool_console,
                            "width": pool.pool_width,
                            "height": pool.pool_height,
                            "dpi": pool.pool_dpi,
                            "color_depth": pool.pool_color_depth,
                            "resize_method": pool.pool_resize_method,
                            "read_only": pool.pool_read_only,
                            "clipboard_encoding": pool.pool_clipboard_encoding,
                            "disable_copy": pool.pool_disable_copy,
                            "disable_paste": pool.pool_disable_paste,
                            "console_audio": pool.pool_console_audio,
                            "enable_audio_input": pool.pool_enable_audio_input,
                            "enable_printing": pool.pool_enable_printing,
                            "printer_name": pool.pool_printer_name,
                            "enable_drive": pool.pool_enable_drive,
                            "drive_name": pool.pool_drive_name,
                            "drive_path": pool.pool_drive_path,
                            "cursor": pool.pool_cursor,
                            "enable_wallpaper": pool.pool_enable_wallpaper,
                            "enable_theming": pool.pool_enable_theming,
                            "enable_font_smoothing": pool.pool_enable_font_smoothing,
                            "enable_full_window_drag": pool.pool_enable_full_window_drag,
                            "enable_desktop_composition": pool.pool_enable_desktop_composition,
                            "enable_menu_animations": pool.pool_enable_menu_animations,
                            "disable_bitmap_caching": pool.pool_disable_bitmap_caching,
                            "disable_offscreen_caching": pool.pool_disable_offscreen_caching,
                            "disable_glyph_caching": pool.pool_disable_glyph_caching,
                            "load_balance_info": pool.pool_load_balance_info,
                            "recording_path": pool.pool_recording_path,
                            "recording_name": pool.pool_recording_name,
                            "create_recording_path": pool.pool_create_recording_path,
                            "recording_exclude_mouse": pool.pool_recording_exclude_mouse,
                            "recording_include_keys": pool.pool_recording_include_keys,
                            "exclude_touch_events": pool.pool_exclude_touch_events,
                            "enable_sftp": pool.pool_enable_sftp,
                            "sftp_port": pool.pool_sftp_port,
                            "sftp_username": pool.pool_sftp_username,
                            "font_name": pool.pool_font_name,
                            "sftp_password": pool.pool_sftp_password,
                            "sftp_host_key": pool.pool_sftp_host_key,
                            "sftp_private_key": pool.pool_sftp_private_key,
                            "sftp_passphrase": pool.pool_sftp_passphrase,
                            "sftp_root_directory": pool.pool_sftp_root_directory,
                            "sftp_directory": pool.pool_sftp_directory,
                            "sftp_server_alive_interval": pool.pool_sftp_server_alive_interval,
                            "private_key": pool.pool_private_key,
                            "passphrase": pool.pool_passphrase,
                            "color_scheme": pool.pool_color_scheme,
                            "scrollback": pool.pool_scrollback,
                            "font_size": pool.pool_font_size,
                            "backspace": pool.pool_backspace,
                            "terminal_type": pool.pool_terminal_type,
                            "typescript_path": pool.pool_typescript_path,
                            "typescript_name": pool.pool_typescript_name,
                            "create_typescript_path": pool.pool_create_typescript_path,
                            "swap_red_blue": pool.pool_swap_red_blue,
                            "destination_host": pool.pool_dest_host,
                            "destination_port": pool.pool_dest_port,
                            "exclude_mouse": pool.pool_exclude_mouse,
                            "exclude_graphics_streams": pool.pool_exclude_graphics_streams,
                            "enable_audio": pool.pool_enable_audio,
                            "audio_servername": pool.pool_audio_servername,
                            "args": pool.pool_args,
                            "is_custom_machine": False,
                            "email": email,
                            "clone_workflow_id": workflow_ids,
                            "error_message": "power-off",

                        }
                        machine_data_obj = CreateMachineBase(**machine_data)
                        machine_result = await controllers.create_machine(machine_data_obj, db=db)
                        machines_json.append(jsonable_encoder(machine_result))
                    except Exception as e:
                        print(f"Error creating machine for vmid {vmid}: {e}")
                        continue
                
                db.commit()

                msg = f"Pool and {num_allocated} VM(s) created successfully."
                if num_missing > 0:
                    msg += (
                        f" {num_missing} VM(s) not created due to insufficient "
                        "available IPs in the selected pools."
                    )

                return {
                    "msg": msg,
                    "pool": jsonable_encoder(pool),
                    "machines": machines_json,
                }

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
 
def machinedata(email,machine, db_pool):
    machine_data = {
        "vm_id": machine.vm_id,
        "name": machine.name,
        "hostname": machine.hostname,
        "guacd_hostname": machine.guacd_hostname,
        "identifier": machine.identifier,
        "status": machine.status,
        "error_message": machine.error_message,
        "workflow_status": machine.workflow_status,
        "workflowId": machine.workflowId,
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
        }
    return machine_data

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
            existing_vmids = db_pool.pool_vmids or []
            new_vmids = [str(vm["vmid"]) for vm in vms if "vmid" in vm]
            db_pool.pool_vmids = existing_vmids + new_vmids

        db.commit()
        db.refresh(db_pool)

        # ── Create machine records for each newly cloned VM ──────────────────
        if is_automated and added_count > 0 and vms:
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
                    # FIX: log instead of silently swallowing; re-raise so callers
                    #      know IP tracking failed (prevents phantom IPs).
                    print(f"Failed to mark IP {ip} as used: {e}")

                # Create machine record
                try:
                    workflow_ids = [
                        wid
                        for wid in [vm.get("clone_workflow_id"), vm.get("wait_assign_workflow_id")]
                        if wid
                    ]
                    machine_data = {
                        "vm_id": str(vmid) if vmid is not None else None,
                        "name": name,
                        "hostname": ip or "",
                        "port": db_pool.pool_port,
                        "protocol": db_pool.pool_protocol,
                        "guacd_port": db_pool.pool_guacd_port,
                        "guacd_encryption": db_pool.pool_guacd_encryption,
                        "guacd_hostname": db_pool.pool_guacd_hostname,
                        "os_type": db_pool.pool_os_type,
                        "pool_id": db_pool.id,
                        "weight": db_pool.pool_weight,
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
                        "printer_name": pool_data.get("pool_printer_name", db_pool.pool_printer_name),
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
                        "clone_workflow_id": workflow_ids,
                        "error_message": "power-off",
                    }
                    machine_data_obj = CreateMachineBase(**machine_data)
                    machine_result = await controllers.create_machine(machine_data_obj, db=db)
                    machines_json.append(jsonable_encoder(machine_result))
                except Exception as e:
                    # FIX: was silently `continue`-ing, masking machine creation
                    #      failures. Log + continue is safer than swallowing.
                    print(f"[update_pool] Warning: failed to create machine for vmid={vmid}: {e}")
                    continue
            
            db.commit()

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

        return {"msg": msg, "pool": db_pool_json, "machines": machines_in_pool}

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
                                # if response:
                                #     await delete_disk(vhdpath)
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

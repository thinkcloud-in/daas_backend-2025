import time
from temporalio import activity
from sqlalchemy.orm import Session
from db_configuration.config import get_db  
from models.models import CreateMachineBase, Machine, Pool ,Cluster,CreatePoolBase,UpdateMachineBase
from fastapi import HTTPException
from service.gucamoleService import delete_connection
from fastapi.encoders import jsonable_encoder
from service import controllers
from service.IPService import allocate_ips_across_pools
from models.IPs_model import IPEntry 
from service.proxmoxService import clone_vm
from service.hyper_v_service import clone_vm_for_single_node,delete_hyperv_vm,delete_disk
import json


@activity.defn()
async def create_pool_activity(request: dict) -> dict:
    db: Session = next(get_db())
    pool_data = {key: request[key] for key in CreatePoolBase.__annotations__.keys() if key in request}
    email = pool_data.pop("email", None)
    ip_pool_names = pool_data.get("pool_ip_pool_names")
    if pool_data.get("pool_type") == "Automated":
        if not ip_pool_names or not isinstance(ip_pool_names, list) or not ip_pool_names:
            return {
                "msg": "No IP pools selected for automated assignment. Please select at least one IP pool."
            }
            # raise HTTPException(status_code=400, detail="No IP pools selected for automated assignment. Please select at least one IP pool.")
        vm_count = pool_data.get("pool_number_of_vms", 0)
        cluster_id = pool_data.get("cluster_id")
        node = pool_data.get("pool_selected_nodes")
        template_vm_id = pool_data.get("pool_template_vm_id")
        name_template = pool_data.get("pool_naming_pattern")
        pool_storage = pool_data.get("pool_storage")
        # os_type = pool_data.get("os_type")
        # password = pool_data.get("password")
        # gateway = pool_data.get("gateway")
    try:
        existing_pool = db.query(Pool).filter(Pool.pool_name == pool_data["pool_name"]).first()
        if existing_pool:
            return {
                "msg": f"Pool already exists with this pool_name {existing_pool.pool_name}."
            }
        pool = Pool(**pool_data)
        db.add(pool)
        # db.flush()
        db.commit()
        db.refresh(pool)
        id_pool = pool.id
        if pool_data.get("cluster_id"):
            pool.cluster_id = f"{id_pool}_{pool_data.get('cluster_id')}"
            db.commit()
            db.refresh(pool)
        
        machines_json = []
        if pool.pool_type == "Automated":
            cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
            if not cluster_data:
                return {
                    "msg": f"Cluster not found for id: {cluster_id}"
                }
                # raise HTTPException(status_code=404, detail="Cluster not found")
            nodes = node if isinstance(node, list) else [node]
            allocated_ips = allocate_ips_across_pools(db, ip_pool_names, vm_count)
            num_allocated = len(allocated_ips)
            num_requested = vm_count
            num_missing = num_requested - num_allocated
            if num_allocated == 0:
                db.rollback()
                return {"msg": "No available IPs in the selected IP pools to create any VMs."}
                #raise Exception("No available IPs in the selected IP pools to create any VMs.")
            ip_list = [ip_entry['ip'] for ip_entry, _ in allocated_ips]
            ip_pool_assignments = [pool_name for _, pool_name in allocated_ips]
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
            }
            # clone_payload_HyperV = {
            #     "cluster_id": str(cluster_data.id),
            #     "node": nodes,
            #     "template_vm_id": template_vm_id,
            #     "name_template": name_template,
            #     "ip_pool_names": ip_pool_assignments,
            #     "count": num_allocated,
            #     # "ip_list": ip_list,
            # }
            # Use correct clone function based on cluster type
            
            cluster_type = (cluster_data.type or "").strip().lower() if cluster_data else ""
            if cluster_type in ("hyper-v", "hyperv"):
                response = await clone_vm_for_single_node(clone_payload_dict)
            elif cluster_type == "proxmox":
                response = await clone_vm(clone_payload_dict)
                if response.get("error_type") == "clone_failed":
                    return {
                        "status": "error",
                        "error_type": response.get("error_type"),
                        "error": response.get("error")
                    }
            assigned_vms = response.get("vms", [])
    
            pool.pool_vmids = [str(vm.get("vmid")) for vm in assigned_vms if vm.get("vmid")]
            db.commit()
            db.refresh(pool)

            for idx, vm in enumerate(assigned_vms):
                name = vm.get("name") or f"vm-{vm.get('vmid','') }"
                vmid = vm.get("vmid")
                node = vm.get("node") or (nodes[0] if nodes else None)
                upid = vm.get("upid")
                ip = vm.get("ip") or (ip_list[idx] if idx < len(ip_list) else None)

                try:
                    ip_entry = None
                    if ip:
                        ip_entry = db.query(IPEntry).filter(IPEntry.ip == ip).first()
                        if ip_entry:
                            ip_entry.status = "used"
                            ip_entry.vm_id = str(vmid)
                    db.commit()
                except Exception as e:
                    db.rollback()
                    raise e

                try:
                    workflow_ids = [vm.get("clone_workflow_id"), vm.get("wait_assign_workflow_id")]
                    workflow_ids = [wid for wid in workflow_ids if wid]
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
                    }
                    machine_data_obj = CreateMachineBase(**machine_data)
                    machine_result = await controllers.create_machine(machine_data_obj, db=db)
                    machines_json.append(jsonable_encoder(machine_result))
                except Exception as e:
                    db.rollback()

            msg = f"pool, {num_allocated} VM(s) created successfully."
            if num_missing > 0:
                msg += f" {num_missing} VM(s) not created due to insufficient available IPs in the selected pools."
 
            return {
                "msg": msg,
                "pool": jsonable_encoder(pool),
                "machines": machines_json
            }

        # db.commit()
        # db.refresh(pool)

        return {
            "msg": "Pool created successfully",
            "pool": jsonable_encoder(pool),
            "machines": machines_json
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Pool creation failed: {str(e)}")
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

    db: Session = next(get_db())
    try:
        db_pool = db.query(Pool).filter(Pool.id == pool_id).first()
        if db_pool is None:
            raise Exception("Pool not found")

        is_automated = db_pool.pool_type == "Automated"
        old_vm_count = db_pool.pool_number_of_vms if is_automated else 0
        
        # Enforce no-decrease policy for Automated pools
        if is_automated:
            requested_vm_count = pool_data.get("pool_number_of_vms", old_vm_count)
            if requested_vm_count is None:
                requested_vm_count = old_vm_count
            try:
                requested_vm_count = int(requested_vm_count)
            except Exception:
                requested_vm_count = old_vm_count
            
            if requested_vm_count < old_vm_count:
                # If user tries to decrease, ignore it and keep existing count
                new_vm_count = old_vm_count
            else:
                new_vm_count = requested_vm_count
        else:
            new_vm_count = 0

        added_count = new_vm_count - old_vm_count

        machines_json = []
        vm_add_error = None
        vms = []

        email = pool_data.get("email", None)

        
        if is_automated and added_count > 0:
            cluster_id = db_pool.cluster_id.split("_")[-1] if "_" in db_pool.cluster_id else db_pool.cluster_id
            try:
                cluster_id = int(cluster_id)
            except ValueError:
                raise HTTPException(status_code=500, detail=f"Invalid cluster_id format: {db_pool.cluster_id}")

            cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
            if not cluster_data:
                raise HTTPException(status_code=404, detail=f"Cluster not found for id: {cluster_id}")

            ip_pool_names = db_pool.pool_ip_pool_names
            # Prefer new template value from incoming pool_data if provided (and parse JSON strings)
            template_vm_id = pool_data.get("pool_template_vm_id", db_pool.pool_template_vm_id)
            if isinstance(template_vm_id, str):
                try:
                    template_vm_id = json.loads(template_vm_id)
                except Exception:
                    try:
                        template_vm_id = int(template_vm_id)
                    except Exception:
                        pass
            nodes = db_pool.pool_selected_nodes if isinstance(db_pool.pool_selected_nodes, list) else [db_pool.pool_selected_nodes]
            name_template = db_pool.pool_naming_pattern
            
            # Fix: Retrieve pool_storage from payload or fallback to DB
            pool_storage = pool_data.get("pool_storage", db_pool.pool_storage)

            allocated_ips = allocate_ips_across_pools(db, ip_pool_names, added_count)
            if not allocated_ips:
                return {
                    "msg": 'No available IPs in the selected IP pools to create any additional VMs.',
                }
            num_allocated = len(allocated_ips)
            ip_list = [ip_entry['ip'] for ip_entry, _ in allocated_ips]
            ip_pool_assignments = [pool_name for _, pool_name in allocated_ips]

            # Prepare template value based on cluster type (same rules as create flow)
            cluster_type = (cluster_data.type or "").lower() if cluster_data else ""
            template_for_clone = template_vm_id
            if cluster_type == "proxmox":
                if isinstance(template_vm_id, dict):
                    if "vmid" in template_vm_id:
                        try:
                            template_for_clone = int(template_vm_id["vmid"])
                        except Exception:
                            raise HTTPException(status_code=400, detail="Invalid template_vm_id value for Proxmox: must be numeric")
                    else:
                        raise HTTPException(status_code=400, detail="Invalid template data for Proxmox: missing 'template_vm_id' key")
                else:
                    try:
                        template_for_clone = int(template_vm_id)
                    except Exception:
                        raise HTTPException(status_code=400, detail="Invalid template id for Proxmox cluster: must be numeric")
            elif cluster_type in ("hyper-v", "hyperv"):
                if not isinstance(template_vm_id, dict):
                    raise HTTPException(status_code=400, detail="Invalid template data for Hyper-V: expected JSON object with template properties")

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
            }

            try:
                # Use the appropriate clone function based on cluster type
                if cluster_type in ("hyper-v", "hyperv"):
                    response = await clone_vm_for_single_node(clone_payload_dict, db)
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
                    raise Exception(err_msg)

        
        pool_data_excluding_vmcount = dict(pool_data)
        if is_automated:
            # Always prevent overwriting the VM count with the raw request value,
            # because we manage 'pool_number_of_vms' specifically based on added_count logic.
            pool_data_excluding_vmcount.pop("pool_number_of_vms", None)

        for field, value in pool_data_excluding_vmcount.items():
            setattr(db_pool, field, value)

        if is_automated and added_count > 0 and vms:
            db_pool.pool_number_of_vms = old_vm_count + len(vms)
            existing_vmids = db_pool.pool_vmids or []
            new_vmids = [str(vm["vmid"]) for vm in vms if "vmid" in vm]
            db_pool.pool_vmids = existing_vmids + new_vmids

        db.commit()
        db.refresh(db_pool)

        
        if is_automated and added_count > 0 and vms:
            
            for idx, vm in enumerate(vms):
                # align with create flow: use safe getters and fall back to allocated IPs
                name = vm.get("name") or f"vm-{vm.get('vmid','') }"
                vmid = vm.get("vmid")
                ip = vm.get("ip") or (ip_list[idx] if idx < len(ip_list) else None)

                try:
                    if ip:
                        ip_entry = db.query(IPEntry).filter(IPEntry.ip == ip).first()
                        if ip_entry:
                            ip_entry.status = "used"
                            ip_entry.vm_id = str(vmid)
                            db.commit()
                except Exception as e:
                    db.rollback()

                try:
                    workflow_ids = [vm.get("clone_workflow_id"), vm.get("wait_assign_workflow_id")]
                    workflow_ids = [wid for wid in workflow_ids if wid]  
                    
                    machine_data = {
                        "vm_id": str(vmid),
                        "name": name,
                        "hostname": ip,
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
                        "clone_workflow_id": workflow_ids,
                    }

                    machine_data_obj = CreateMachineBase(**machine_data)
                    machine_result = await controllers.create_machine(machine_data_obj, db=db)
                    machines_json.append(jsonable_encoder(machine_result))
                except Exception as e:
                    db.rollback()
                    continue

        existing_machines = db.query(Machine).filter(Machine.pool_id == db_pool.id, Machine.is_custom_machine == False).all()
        for machine in existing_machines:
            
            try:

                machine_update_data = machinedata(email,machine, db_pool)

                machine_update_data["identifier"] = machine.identifier
                
                machine_update_data_obj = UpdateMachineBase(**machine_update_data)
                
                await controllers.update_machine(machine.identifier, machine_update_data_obj)
            except Exception as e:
                db.rollback()
                continue

        db_pool_json = jsonable_encoder(db_pool)
        machines_in_pool = jsonable_encoder(db.query(Machine).filter(Machine.pool_id == db_pool.id).all())
        msg = "Pool updated successfully"
        if is_automated and added_count > 0:
            if vms:
                msg += f", {added_count} new VM(s) added."
            elif vm_add_error:
                msg += f", {vm_add_error}"

        return {"pool": db_pool_json, "machines": machines_in_pool}

    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()
 


@activity.defn()
async def delete_pool_activity(pool_id: int) -> dict:
    from service.proxmoxService import delete_proxmox_vm
    db: Session = next(get_db())  
    try:
        pool = db.query(Pool).filter(Pool.id == pool_id).first()
        if not pool:
            return {"msg": f"Pool not found with id - {pool_id}"}
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
                            await delete_hyperv_vm(vmid)
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

@activity.defn()
async def retrieve_pool_data_activity(pool_name: str) :
    db: Session = next(get_db())  
    try:
        pool = db.query(Pool).filter(Pool.pool_name == pool_name).first()
        pool_json = jsonable_encoder(pool)
        
        if pool_json:
            return {"msg": f"{pool_name} Pool found ", "pool": pool_json}
        else:
            
            return {"msg": f"{pool_name} Pool not found"}
    except Exception as e:
        db.rollback()
        
        raise HTTPException(status_code=500, detail=f"An error occurred while retrieving the pool: {str(e)}")
    


@activity.defn()
async def list_all_pool_names_activity():
    db: Session = next(get_db())  
    try:
        pools = db.query(Pool).all()
        pool_names = [pool_item.pool_name for pool_item in pools]
        return {"msg": "listed all the Pool names successfully", "pool_names": pool_names}
    except Exception as e:
        db.rollback()
        
        raise HTTPException(status_code=500, detail=f"An error occurred while retrieving all pools: {str(e)}")
    

@activity.defn()
async def get_all_pools_activity():
    db: Session = next(get_db())  
    try:
        pools = db.query(Pool).all()
        pools_json = jsonable_encoder(pools)
        return {"msg": "listed all the Pools successfully", "pools": pools_json}
    except Exception as e:
        db.rollback()  
        raise HTTPException(status_code=500, detail=f"An error occurred while retrieving all pools: {str(e)}")
    

@activity.defn()
async def get_pool_details_id_activity(pool_id: int):

    db: Session = next(get_db())  
    try:
        pool = db.query(Pool).filter(Pool.id == pool_id).first()
        pool_json = jsonable_encoder(pool)
        if pool_json:
            return {"msg": f"Pool Retrived Successfully", "pool": pool_json}
        else:
            
            return {"msg": f"Pool not found "}, 404
    except Exception as e:
        db.rollback()
        
        raise HTTPException(status_code=500, detail=f"An error occurred while retrieving the pool: {str(e)}")


@activity.defn()
async def domain_join_activity(pool_id: int, pool_ad_domain: str, pool_ad_password: str, pool_ad_username: str, pool_ad_path: str) -> dict:
    import paramiko
    db: Session = next(get_db())
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

                # cmd = f"qm set {vm_id} --cicustom user=local:snippets/join-domain-pool-{pool_id}.yml"

                # stdin, stdout, stderr = ssh.exec_command(cmd)

                # exit_status = stdout.channel.recv_exit_status()
                # out = stdout.read().decode()
                # err = stderr.read().decode()

                # print("EXIT:", exit_status)
                # print("OUT:", out)
                # print("ERR:", err)

        except Exception as e:
            return {"status": "error", "error": f"SSH/Proxmox error: {str(e)}"}
        finally:
            ssh.close()
            
        return {"msg": "Domain join workflow executed successfully"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Domain join failed: {str(e)}")
    finally:
        db.close()

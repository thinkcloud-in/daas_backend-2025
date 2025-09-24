import asyncio
from temporalio import activity
from sqlalchemy.orm import Session
from db_configuration.config import get_db  
from models.models import CreateMachineBase, Machine, Pool ,Cluster,CreatePoolBase,UpdateMachineBase
from fastapi import HTTPException, logger
from service.gucamoleService import delete_connection
from fastapi.encoders import jsonable_encoder
from service import controllers
from service.IPService import allocate_ips_across_pools
from models.IPs_model import IPEntry 
from service.proxmoxService import clone_vm
import logging
from datetime import datetime


@activity.defn()
async def create_pool_activity(request: dict) -> dict:
    db: Session = next(get_db())
    pool_data = {key: request[key] for key in CreatePoolBase.__annotations__.keys() if key in request}
    email = pool_data.pop("email", None)
      # All necessary fields are now in pool_data
    ip_pool_names = pool_data.get("pool_ip_pool_names")
    if pool_data.get("pool_type") == "Automated":
        if not ip_pool_names or not isinstance(ip_pool_names, list) or not ip_pool_names:
            raise HTTPException(status_code=400, detail="No IP pools selected for automated assignment. Please select at least one IP pool.")
        vm_count = pool_data.get("pool_number_of_vms", 0)
        cluster_id = pool_data.get("cluster_id")
        node = pool_data.get("pool_selected_nodes")
        template_vm_id = pool_data.get("pool_template_vm_id")
        name_template = pool_data.get("pool_naming_pattern")
    try:
        # Check for duplicate pool
        existing_pool = db.query(Pool).filter(Pool.pool_name == pool_data["pool_name"]).first()
        if existing_pool:
            return {
                "msg": f"Pool already exists with this pool_name {existing_pool.pool_name} --- please try with another pool name."
            }
 
        # Create Pool instance
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
       
        if pool.pool_type == "Automated":
            cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
            nodes = node if isinstance(node, list) else [node]
 
            # Allocate IPs
            allocated_ips = allocate_ips_across_pools(db, ip_pool_names, vm_count)
            num_allocated = len(allocated_ips)
            num_requested = vm_count
            num_missing = num_requested - num_allocated
 
            if num_allocated == 0:
                raise HTTPException(status_code=400, detail="No available IPs in the selected IP pools to create any VMs.")
 
            ip_list = [ip_entry.ip for ip_entry, _ in allocated_ips]
            ip_pool_assignments = [pool_name for _, pool_name in allocated_ips]
            # Prepare clone payload
            clone_payload_dict = {
                "cluster_id": str(cluster_data.id),
                "node": nodes,
                "template_vm_id": template_vm_id,
                "name_template": name_template,
                "ip_pool_names": ip_pool_assignments,
                "count": num_allocated,
                "ip_list": ip_list,
            }
            # Call Temporal workflow and wait for result
            response = await clone_vm(clone_payload_dict)
            print("Response from clone_vm:", response)
           

            assigned_vms = response.get("vms", [])  # Expecting a list of {name, vmid, node, upid, ip}
            # clone_workflow_id = assigned_vms[0].get("clone_workflow_id") if assigned_vms else None
            pool.pool_vmids = [str(vm["vmid"]) for vm in assigned_vms]
            db.commit()
            db.refresh(pool)
 
            print("About to write machines and IPEntry updates...")
            for vm in assigned_vms:
                name = vm["name"]
                vmid = vm["vmid"]
                node = vm["node"]
                upid = vm["upid"]
                ip = vm["ip"]
                try:
                    ip_entry = db.query(IPEntry).filter(IPEntry.ip == ip).first()
                    if ip_entry:
                        ip_entry.status = "used"
                        ip_entry.vm_id = int(vmid)
                        print(f"Updating IPEntry: {ip}, vmid={vmid}")
                    else:
                        print(f"IPEntry not found for {ip}")
                    db.commit()
                    print("Committed IPEntry update")
                except Exception as e:
                    print(f"Failed to update IPEntry: {e}")
                    db.rollback()
                # Prepare machine data (fill out as needed)
                try:
                      # Collect both workflow IDs
                    workflow_ids = [vm.get("clone_workflow_id"), vm.get("wait_assign_workflow_id")]
                    workflow_ids = [wid for wid in workflow_ids if wid]  # Filter out None
                    machine_data = {
                        "vm_id": int(vmid),
                        "name": name,
                        "hostname": ip,
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
                    machine_result = await controllers.create_machine(machine_data_obj)
                    machines_json.append(jsonable_encoder(machine_result))
                    # db.commit()
                    print("Committed machine to DB")
                except Exception as e:
                    print(f"Failed to create machine: {e}")
                    db.rollback()
       
            msg = f"pool, {num_allocated} VM(s) created successfully."
            if num_missing > 0:
                msg += f" {num_missing} VM(s) not created due to insufficient available IPs in the selected pools."
 
            return {
                "msg": msg,
                "pool": jsonable_encoder(pool),
                "machines": machines_json
            }
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
    import asyncio

    db: Session = next(get_db())
    try:
        db_pool = db.query(Pool).filter(Pool.id == pool_id).first()
        if db_pool is None:
            raise HTTPException(status_code=404, detail="Pool not found")

        is_automated = db_pool.pool_type == "Automated"
        old_vm_count = db_pool.pool_number_of_vms if is_automated else 0
        new_vm_count = pool_data.get("pool_number_of_vms", old_vm_count)
        if new_vm_count is None:
            new_vm_count = old_vm_count
        try:
            new_vm_count = int(new_vm_count)
        except Exception:
            new_vm_count = old_vm_count
        added_count = new_vm_count - old_vm_count

        machines_json = []
        vm_add_error = None
        vms = []

        email = pool_data.get("email", None)

        # If scaling up automated pool, do all checks and VM creation BEFORE pool update
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
            template_vm_id = db_pool.pool_template_vm_id
            nodes = db_pool.pool_selected_nodes if isinstance(db_pool.pool_selected_nodes, list) else [db_pool.pool_selected_nodes]
            name_template = db_pool.pool_naming_pattern

            allocated_ips = allocate_ips_across_pools(db, ip_pool_names, added_count)
            if not allocated_ips:
                return {
                    "msg": "Pool update failed: No available IPs in the selected IP pools to add more VMs.",
                    "error": True
                }

            ip_list = [ip_entry.ip for ip_entry, _ in allocated_ips]
            ip_pool_assignments = [pool_name for _, pool_name in allocated_ips]

            clone_payload_dict = {
                "cluster_id": str(cluster_data.id),
                "node": nodes,
                "template_vm_id": template_vm_id,
                "name_template": name_template,
                "ip_pool_names": ip_pool_assignments,
                "count": added_count,
                "ip_list": ip_list,
            }

            try:
                response = await clone_vm(clone_payload_dict)
                print("DEBUG: clone_vm response in update_pool_activity:", response)
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

        # Now update pool data
        pool_data_excluding_vmcount = dict(pool_data)
        if is_automated and added_count > 0:
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

        # Assign IPs and create machines for scaled VMs
        if is_automated and added_count > 0 and vms:
            print("debug:vms to add machines for:", vms)
            for vm in vms:
                print("debug:processing vm:", vm)
                name = vm["name"]
                vmid = vm["vmid"]
                ip = vm["ip"]

                try:
                    ip_entry = db.query(IPEntry).filter(IPEntry.ip == ip).first()
                    if ip_entry:
                        ip_entry.status = "used"
                        ip_entry.vm_id = int(vmid)
                        db.commit()
                    else:
                        print(f"IPEntry not found for {ip}")
                except Exception as e:
                    print(f"Failed to update IPEntry: {e}")
                    db.rollback()

                try:
                    # collect both workflow IDs
                    workflow_ids = [vm.get("clone_workflow_id"), vm.get("wait_assign_workflow_id")]
                    workflow_ids = [wid for wid in workflow_ids if wid]  # Filter out None
                    print(f"Workflow IDs for VM {name}: {workflow_ids}")
                    machine_data = {
                        "vm_id": int(vmid),
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
                    machine_result = await controllers.create_machine(machine_data_obj)
                    machines_json.append(jsonable_encoder(machine_result))
                except Exception as e:
                    print(f"Failed to create machine: {e}")
                    continue

        # 🔄 Update existing machines (excluding custom)
        existing_machines = db.query(Machine).filter(Machine.pool_id == db_pool.id, Machine.is_custom_machine == False).all()
        for machine in existing_machines:
            print(f"Type of machine: {type(machine)}")
            try:

                machine_update_data = machinedata(email,machine, db_pool)

                machine_update_data["identifier"] = machine.identifier
                print(f"Data for update_machine: {machine_update_data}")
                machine_update_data_obj = UpdateMachineBase(**machine_update_data)
                print(f"Machine update data object: {machine_update_data_obj}")
                await controllers.update_machine(machine.identifier, machine_update_data_obj)
            except Exception as e:
                print(f"Failed to update existing machine {machine.identifier}: {e}")
                continue

        db_pool_json = jsonable_encoder(db_pool)
        machines_in_pool = jsonable_encoder(db.query(Machine).filter(Machine.pool_id == db_pool.id).all())
        msg = "Pool updated successfully"
        if is_automated and added_count > 0:
            if vms:
                msg += f", {added_count} new VM(s) added."
            elif vm_add_error:
                msg += f", {vm_add_error}"

        return {"msg": msg, "pool": db_pool_json, "machines": machines_in_pool}

    except Exception as e:
        db.rollback()
        return {
            "msg": f"Pool update failed: {str(e)}",
            "error": True
        }
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
                id_cluster = cluster_pool_id.split("_")[1]
            except Exception as e:
                # raise HTTPException(status_code=400, detail="Malformed cluster_pool_id")
                raise RuntimeError(str(e))
            cluster_data = db.query(Cluster).filter(Cluster.id == id_cluster).first()

            # Delete VMs and free their IPs
            for vmid in pool_vmids:
                if vmid:
                    try:
                        delete_proxmox_vm(vmid, cluster_data)
                        vmid_int = int(vmid)
                        ip_entries = db.query(IPEntry).filter(IPEntry.vm_id == vmid_int, IPEntry.status == "used").all()
                        print(f"Freeing IPs for VMID {vmid_int}: {[ip.ip for ip in ip_entries]}")
                        for ip_entry in ip_entries:
                            ip_entry.status = "unused"
                            ip_entry.vm_id = None
                        print("About to commit IP status updates...")
                        db.commit()
                        print("Commit done.")
                        # Optional: Re-fetch and print for debugging
                        for ip_entry in ip_entries:
                            db.refresh(ip_entry)
                            print(f"After commit: IP {ip_entry.ip} status={ip_entry.status}, vm_id={ip_entry.vm_id}")
                    except Exception as e:
                        db.rollback()
                        print(f"Exception occurred: {e}")

        # Delete machines
        for machine_item in pool_machines:
            machine = db.query(Machine).filter(Machine.identifier == machine_item).first()
            if machine:
                try:
                    await delete_connection(machine.identifier)
                except Exception as e:
                    print(f"Failed to delete connection for machine {machine.identifier}: {e}")
                db.delete(machine)

        db.delete(pool)
        db.commit()
        all_pools = db.query(Pool).all()
        all_pools_json = jsonable_encoder(all_pools)
        return {"msg": f"Pool deleted successfully", "pools": all_pools_json}
    except Exception as e:
       
        raise HTTPException(status_code=500, detail=f"An error occurred while deleting the Pool: {str(e)}")
# Activity funtion for retrieve pool data
@activity.defn()
async def retrieve_pool_data_activity(pool_name: str) :
    db: Session = next(get_db())  
    try:
        pool = db.query(Pool).filter(Pool.pool_name == pool_name).first()
        pool_json = jsonable_encoder(pool)
        # print(f"pool_dict type-----------: {type(pool_dict)}")
        if pool_json:
            return {"msg": f"{pool_name} Pool found ", "pool": pool_json}
        else:
            print("Pool not found")
            return {"msg": f"{pool_name} Pool not found"}, 404
    except Exception as e:
        db.rollback()
        print("Pool retrieval failed")
        raise HTTPException(status_code=500, detail=f"An error occurred while retrieving the pool: {str(e)}")
    

# Activity function for getting all the pool names 
@activity.defn()
async def list_all_pool_names_activity():
    db: Session = next(get_db())  
    try:
        pools = db.query(Pool).all()
        pool_names = [pool_item.pool_name for pool_item in pools]
        return {"msg": "listed all the Pool names successfully", "pool_names": pool_names}
    except Exception as e:
        db.rollback()
        print("Error while retrieving all pools")
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
        print("Error while retrieving all pools")
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
            # print("Pool not found")
            return {"msg": f"Pool not found "}, 404
    except Exception as e:
        db.rollback()
        print("Pool retrieval failed")
        raise HTTPException(status_code=500, detail=f"An error occurred while retrieving the pool: {str(e)}")

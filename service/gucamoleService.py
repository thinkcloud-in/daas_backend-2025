import asyncio
import base64
from datetime import datetime
import logging
from typing import Dict, List
from fastapi import HTTPException, status
import  requests
import json
import os
import psycopg2
from .temporalResource.workflows import workflows_guacmole
from temporalio.client import Client
from utils.temporal_client import TemporalClientManager
from dto.machineDto import MachineDto
from models.models import CreateMachineBase, Machine
from models.Rbac_models import RoleComponentSubmitRequest,RBACRequest
from fastapi.encoders import jsonable_encoder
import httpx
from starlette.responses import StreamingResponse
from service.temporalResource.workflows import workflows_RBAC
import logging
# import time
from dotenv import load_dotenv
load_dotenv()
logger = logging.getLogger(__name__)

 



def unique_id():
    unique_id = datetime.now()
    return f"{unique_id.hour }:{unique_id.minute}:{unique_id.second}"

        
async def startup_event_client():
    logger.info("Starting up FastAPI server...")
    # Initialize the singleton connection
    await TemporalClientManager.get_temporal_client()
    logger.info("Temporal connection initialization started...")

# Logging is configured centrally in utils/logging_config.py (called from
# main.py at startup) — do not reconfigure it per-module.
logger = logging.getLogger("guacamole_login_logger")

async def login_with_guacamole():
    uniqueId = unique_id()
    logger.info(f"Starting login process with Guacamole, unique ID: {uniqueId}.")
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established client connection with Guacamole.")
    
    try:
        logger.info("Starting LoginWorkflow workflow.")
        handle = await client.start_workflow(
            workflows_guacmole.LoginWorkflow.run,
            id=f"login_with_guacamole-{uniqueId}",
            task_queue="login-task-queue",
        )
        result = await handle.result()
        logger.info(f"Workflow completed successfully. Result: {result}")
        return result
    except Exception as e:
        logger.error("Error occurred while running LoginWorkflow workflow.", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to complete login workflow: {str(e)}")


async def logout_from_guacamole(token: str):
    """
    Invalidates a Guacamole authentication token to prevent session leaks.
    """
    if not token:
        return
    try:
        url = f"{os.getenv('GUCAMOLE_BASE_URL')}/api/tokens/{token}"
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.delete(url)
            if response.status_code != 204:
                logger.warning(f"Guacamole logout returned unexpected status: {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to logout from Guacamole: {str(e)}")


#------------------------------------------------------Connection/machine ----------------------------------------------------
def return_payload(machine_data:MachineDto):
    protocol = machine_data['protocol'].lower()
    match protocol:
        case "rdp":   
            payload = json.dumps({
                "parentIdentifier": "ROOT",
                "name": machine_data['name'],
                "protocol": "rdp",
                "parameters": {
                    "port": machine_data["port"],
                    "read-only": machine_data["read_only"],
                    "swap-red-blue": machine_data["swap_red_blue"],
                    "cursor": machine_data["cursor"],
                    "color-depth": machine_data["color_depth"],
                    "clipboard-encoding": machine_data["clipboard_encoding"],
                    "disable-copy": machine_data["disable_copy"],
                    "disable-paste": machine_data["disable_paste"],
                    "dest-port": machine_data["dest_port"],
                    "recording-exclude-output": machine_data["recording_exclude_output"],
                    "recording-exclude-mouse": machine_data["recording_exclude_mouse"],
                    "recording-include-keys": machine_data["recording_include_keys"],
                    "create-recording-path": machine_data["create_recording_path"],
                    "enable-sftp": machine_data["enable_sftp"],
                    "sftp-port": machine_data["sftp_port"],
                    "sftp-server-alive-interval": machine_data["sftp_server_alive_interval"],
                    "enable-audio": machine_data["enable_audio"],
                    "security": machine_data["security"],
                    "disable-auth": machine_data["disable_auth"],
                    "ignore-cert": machine_data["ignore_cert"],
                    "gateway-port": machine_data["gateway_port"],
                    "server-layout": machine_data["server_layout"],
                    "timezone": machine_data["timezone"],
                    "console": machine_data["console"],
                    "width": machine_data["width"],
                    "height": machine_data["height"],
                    "dpi": machine_data["dpi"],
                    "resize-method": machine_data["resize_method"],
                    "console-audio": machine_data["console_audio"],
                    "disable-audio": machine_data["disable_audio"],
                    "enable-audio-input": machine_data["enable_audio_input"],
                    "enable-printing": machine_data["enable_printing"],
                    "enable-drive": machine_data["enable_drive"],
                    "create-drive-path": machine_data["create_drive_path"],
                    "enable-wallpaper": machine_data["enable_wallpaper"],
                    "enable-theming": machine_data["enable_theming"],
                    "enable-font-smoothing": machine_data["enable_font_smoothing"],
                    "enable-full-window-drag": machine_data["enable_full_window_drag"],
                    "enable-desktop-composition": machine_data["enable_desktop_composition"],
                    "enable-menu-animations": machine_data["enable_menu_animations"],
                    "disable-bitmap-caching": machine_data["disable_bitmap_caching"],
                    "disable-offscreen-caching": machine_data["disable_offscreen_caching"],
                    "disable-glyph-caching": machine_data["disable_glyph_caching"],
                    "preconnection-id": machine_data["preconnection_id"],
                    "hostname": machine_data["hostname"],
                    "username": machine_data["username"],
                    "password": machine_data["password"],
                    "domain": machine_data["domain"],
                    "gateway-hostname": machine_data["gateway_hostname"],
                    "gateway-username": machine_data["gateway_username"],
                    "gateway-password": machine_data["gateway_password"],
                    "gateway-domain": machine_data["gateway_domain"],
                    "initial-program": machine_data["initial_program"],
                    "client-name": machine_data["client_name"],
                    "printer-name": machine_data["printer_name"],
                    "drive-name": machine_data["drive_name"],
                    "drive-path": machine_data["drive_path"],
                    "static-channels": machine_data["static_channels"],
                    "remote-app": machine_data["remote_app"],
                    "remote-app-dir": machine_data["remote_app_dir"],
                    "remote-app-args": machine_data["remote_app_args"],
                    "preconnection-blob": machine_data["preconnection_blob"],
                    "load-balance-info": machine_data["load_balance_info"],
                    "recording-path": machine_data["recording_path"],
                    "recording-name": machine_data["recording_name"],
                    "sftp-hostname": machine_data["sftp_hostname"],
                    "sftp-host-key": machine_data["sftp_host_key"],
                    "sftp-username": machine_data["sftp_username"],
                    "sftp-password": machine_data["sftp_password"],
                    "sftp-private-key": machine_data["sftp_private_key"],
                    "sftp-passphrase": machine_data["sftp_passphrase"],
                    "sftp-root-directory": machine_data["sftp_root_directory"],
                    "sftp-directory": machine_data["sftp_directory"]
                },
                "attributes": {
                    "max-connections": machine_data["max_connections"],
                    "max-connections-per-user": machine_data["max_connections_per_user"],
                    "weight": machine_data["weight"],
                    "failover-only": machine_data["failover_only"],
                    "guacd-port": machine_data["guacd_port"],
                    "guacd-encryption": machine_data["guacd_encryption"],
                    "guacd-hostname": machine_data["guacd_hostname"]
                }
            })
        case "vnc" | "VNC":
            payload = json.dumps({
                "parentIdentifier": "ROOT",
                "name": machine_data["name"],
                "protocol": "vnc",
                "parameters": {
                    "port": machine_data["port"],
                    "read-only": machine_data["read_only"],
                    "swap-red-blue": machine_data["swap_red_blue"],
                    "cursor": machine_data["cursor"],
                    "color-depth": machine_data["color_depth"],
                    "clipboard-encoding": machine_data["clipboard_encoding"],
                    "disable-copy": machine_data["disable_copy"],
                    "disable-paste": machine_data["disable_paste"],
                    "dest-port": machine_data["dest_port"],
                    "recording-exclude-output": machine_data["recording_exclude_output"],
                    "recording-exclude-mouse": machine_data["recording_exclude_mouse"],
                    "recording-include-keys": machine_data["recording_include_keys"],
                    "create-recording-path": machine_data["create_recording_path"],
                    "enable-sftp": machine_data["enable_sftp"],
                    "sftp-port": machine_data["sftp_port"],
                    "sftp-server-alive-interval": machine_data["sftp_server_alive_interval"],
                    "enable-audio": machine_data["enable_audio"],
                    "audio-servername": machine_data["audio_servername"],
                    "sftp-directory": machine_data["sftp_directory"],
                    "sftp-root-directory": machine_data["sftp_root_directory"],
                    "sftp-passphrase": machine_data["sftp_passphrase"],
                    "sftp-private-key": machine_data["sftp_private_key"],
                    "sftp-username": machine_data["sftp_username"],
                    "sftp-password": machine_data["sftp_password"],
                    "sftp-host-key": machine_data["sftp_host_key"],
                    "sftp-hostname": machine_data["sftp_hostname"],
                    "recording-name": machine_data["recording_name"],
                    "recording-path": machine_data["recording_path"],
                    "dest-host": machine_data["dest_host"],
                    "password": machine_data["password"],
                    "username": machine_data["username"],
                    "hostname": machine_data["hostname"]
                },
                "attributes": {
                    "max-connections": machine_data["max_connections"],
                    "max-connections-per-user": machine_data["max_connections_per_user"],
                    "weight": machine_data["weight"],
                    "failover-only": machine_data["failover_only"],
                    "guacd-port": machine_data["guacd_port"],
                    "guacd-encryption": machine_data["guacd_encryption"],
                    "guacd-hostname": machine_data["guacd_hostname"]
                }
            })
        case "telnet":
            payload = json.dumps({
                "parentIdentifier": "ROOT",
                "name": machine_data["name"],
                "protocol": "telnet",
                "parameters": {
                    "port": machine_data["port"],
                    "read-only": machine_data["read_only"],
                    "swap-red-blue": machine_data["swap_red_blue"],
                    "cursor": machine_data["cursor"],
                    "color-depth": machine_data["color_depth"],
                    "clipboard-encoding": machine_data["clipboard_encoding"],
                    "disable-copy": machine_data["disable_copy"],
                    "disable-paste": machine_data["disable_paste"],
                    "dest-port": machine_data["dest_port"],
                    "recording-exclude-output": machine_data["recording_exclude_output"],
                    "recording-exclude-mouse": machine_data["recording_exclude_mouse"],
                    "recording-include-keys": machine_data["recording_include_keys"],
                    "create-recording-path": machine_data["create_recording_path"],
                    "enable-sftp": machine_data["enable_sftp"],
                    "sftp-port": machine_data["sftp_port"],
                    "sftp-server-alive-interval": machine_data["sftp_server_alive_interval"],
                    "enable-audio": machine_data["enable_audio"],
                    "color-scheme": machine_data["color_scheme"],
                    "font-size": machine_data["font_size"],
                    "scrollback": machine_data["scrollback"],
                    "backspace": machine_data["backspace"],
                    "terminal-type": machine_data["terminal_type"],
                    "create-typescript-path": machine_data["create_typescript_path"],
                    "hostname": machine_data["hostname"],
                    "username": machine_data["username"],
                    "password": machine_data["password"],
                    "username-regex": machine_data["username_regex"],
                    "password-regex": machine_data["password_regex"],
                    "login-success-regex": machine_data["login_success_regex"],
                    "login-failure-regex": machine_data["login_failure_regex"],
                    "font-name": machine_data["font_name"],
                    "typescript-path": machine_data["typescript_path"],
                    "typescript-name": machine_data["typescript_name"],
                },
                "attributes": {
                    "max-connections": machine_data["max_connections"],
                    "max-connections-per-user": machine_data["max_connections_per_user"],
                    "weight": machine_data["weight"],
                    "failover-only": machine_data["failover_only"],
                    "guacd-port": machine_data["guacd_port"],
                    "guacd-encryption": machine_data["guacd_encryption"],
                    "guacd-hostname": machine_data["guacd_hostname"]
                }
            })
        case "kubernetes":
            payload = json.dumps({
                "parentIdentifier": "ROOT",
                "name": machine_data["name"],
                "protocol": "kubernetes",
                "parameters": {
                    "port": machine_data["port"],
                    "read-only": machine_data["read_only"],
                    "swap-red-blue": machine_data["swap_red_blue"],
                    "cursor": machine_data["cursor"],
                    "color-depth": machine_data["color_depth"],
                    "clipboard-encoding": machine_data["clipboard_encoding"],
                    "disable-copy": machine_data["disable_copy"],
                    "disable-paste": machine_data["disable_paste"],
                    "dest-port": machine_data["dest_port"],
                    "recording-exclude-output": machine_data["recording_exclude_output"],
                    "recording-exclude-mouse": machine_data["recording_exclude_mouse"],
                    "recording-include-keys": machine_data["recording_include_keys"],
                    "create-recording-path": machine_data["create_recording_path"],
                    "enable-sftp": machine_data["enable_sftp"],
                    "sftp-port": machine_data["sftp_port"],
                    "sftp-server-alive-interval": machine_data["sftp_server_alive_interval"],
                    "enable-audio": machine_data["enable_audio"],
                    "kubeconfig": machine_data["kubeconfig"],
                    "namespace": machine_data["namespace"],
                    "pod-name": machine_data["pod_name"],
                    "container-name": machine_data["container_name"],
                    "command": machine_data["command"],
                    "args": machine_data["args"],
                    "tty": machine_data["tty"],
                    "stdin": machine_data["stdin"],
                    "stdin-tty": machine_data["stdin_tty"],
                    "shell": machine_data["shell"]
                },
                "attributes": {
                    "max-connections": machine_data["max_connections"],
                    "max-connections-per-user": machine_data["max_connections_per_user"],
                    "weight": machine_data["weight"],
                    "failover-only": machine_data["failover_only"],
                    "guacd-port": machine_data["guacd_port"],
                    "guacd-encryption": machine_data["guacd_encryption"],
                    "guacd-hostname": machine_data["guacd_hostname"]
                }
            })
        case "ssh":
            payload = json.dumps({
                "parentIdentifier": "ROOT",
                "name": machine_data["name"],
                "protocol": "ssh",
                "parameters": {
                    "port": machine_data["port"],
                    "read-only": machine_data["read_only"],
                    "swap-red-blue": machine_data["swap_red_blue"],
                    "cursor": machine_data["cursor"],
                    "color-depth": machine_data["color_depth"],
                    "clipboard-encoding": machine_data["clipboard_encoding"],
                    "disable-copy": machine_data["disable_copy"],
                    "disable-paste": machine_data["disable_paste"],
                    "dest-port": machine_data["dest_port"],
                    "recording-exclude-output": machine_data["recording_exclude_output"],
                    "recording-exclude-mouse": machine_data["recording_exclude_mouse"],
                    "recording-include-keys": machine_data["recording_include_keys"],
                    "create-recording-path": machine_data["create_recording_path"],
                    "enable-sftp": machine_data["enable_sftp"],
                    "sftp-port": machine_data["sftp_port"],
                    "sftp-server-alive-interval": machine_data["sftp_server_alive_interval"],
                    "enable-audio": machine_data["enable_audio"],
                    "color-scheme": machine_data["color_scheme"],
                    "font-size": machine_data["font_size"],
                    "scrollback": machine_data["scrollback"],
                    "timezone": machine_data["timezone"] or "",  # Handle None
                    "server-alive-interval": machine_data["server_alive_interval"],
                    "backspace": machine_data["backspace"],
                    "terminal-type": machine_data["terminal_type"],
                    "create-typescript-path": machine_data["create_typescript_path"],
                    "hostname": machine_data["hostname"],
                    "host-key": machine_data["host_key"],
                    "private-key": machine_data["private_key"],
                    "username": machine_data["username"],
                    "password": machine_data["password"],
                    "passphrase": machine_data["passphrase"],
                    "font-name": machine_data["font_name"],
                    "command": machine_data["command"],
                    "locale": machine_data["locale"],
                    "typescript-path": machine_data["typescript_path"],
                    "typescript-name": machine_data["typescript_name"],
                    "recording-path": machine_data["recording_path"],
                    "recording-name": machine_data["recording_name"],
                    "sftp-root-directory": machine_data["sftp_root_directory"]
                },
                "attributes": {
                    "max-connections": machine_data["max_connections"],
                    "max-connections-per-user": machine_data["max_connections_per_user"],
                    "weight": machine_data["weight"],
                    "failover-only": machine_data["failover_only"],
                    "guacd-port": machine_data["guacd_port"],
                    "guacd-encryption": machine_data["guacd_encryption"],
                    "guacd-hostname": machine_data["guacd_hostname"]
                }
            })
        case _:
            raise ValueError(f"Unsupported protocol: {protocol}")
    return payload 

async def creating_connection(machine_data: CreateMachineBase):
    uniqueId = unique_id()
    logger.info(f"Starting to create connection with unique ID: {uniqueId}.")
    
    client = await TemporalClientManager.get_temporal_client()
    logger.debug("Connected with the Temporal client.")
    
    try:
        # Start the workflow
        logger.info(f"Starting the CreateMachineWorkflow with ID: create-connection-{uniqueId}.")
        handle = await client.start_workflow(
            workflows_guacmole.CreateMachineWorkflow.run,
            args=[machine_data],
            id=f"create-connection-{uniqueId}",
            task_queue="create-connection",
        )
        logger.info("Workflow started successfully, waiting for the result.")
        result = await handle.result()
        logger.info("Workflow completed successfully.")
        return result
    except Exception as e:
        logger.error("An error occurred during the workflow execution.", exc_info=True)
        raise

   
# Assign User to Connections/machine   Create a pool manully
async def assign_connection_to_user(username:str, connection:str):
    token = await login_with_guacamole()
    gucamole_connection_To_user_url = f"{os.getenv('GUCAMOLE_BASE_URL')}/api/session/data/{os.getenv('GUCAMOLE_DATASOURCE')}/users/"
    url = gucamole_connection_To_user_url + username + "/permissions?token="+ token
    payload= json.dumps(
        [
    {
        "op": "add",
        "path": "/connectionPermissions/"+connection,
        "value": "READ"
    }
]
    )
    headers = {
  'Content-Type': 'application/json'
}
    response= requests.request("PATCH", url, headers=headers, data=payload)
 
    return response.status_code

# Delete machines/connection from guacamole
async def revoke_user_from_connection(username:str, connection:str):
    token = await login_with_guacamole()
    gucamole_connection_To_user_url = f"{os.getenv('GUCAMOLE_BASE_URL')}/api/session/data/{os.getenv('GUCAMOLE_DATASOURCE')}/users/"
    url = gucamole_connection_To_user_url + username + "/permissions?token="+ token
    # here to find out the connection of the  databse
    payload= json.dumps(
        [
    {
        "op": "remove",
        "path": "/connectionPermissions/"+connection,
        "value": "READ"
    }
]
    )
    headers = {
  'Content-Type': 'application/json'
}
    response= requests.request("PATCH", url, headers=headers, data=payload)
    return response.status_code

# LIST OF MACHINE OR CONNECTION
async def list_machines():
    uniqueId = unique_id()
    logger.info(f"Generated unique ID: {uniqueId}")
    
    try:
        client = await TemporalClientManager.get_temporal_client()
        logger.info("Successfully established connection with the client.")
        
        # Start Temporal workflow
        handle = await client.start_workflow(
            workflows_guacmole.ListOfMachinesWorkflow.run,
            id=f"list_of_machine{uniqueId}",
            task_queue="list-of-machine",
        )
        logger.info(f"Workflow started with ID: list_of_machine{uniqueId}")
        
        # Get result from workflow
        result = await handle.result()
        logger.info("Successfully retrieved workflow result.")
        
        return result
    
    except Exception as e:
        logger.error("An error occurred during the list_machines process.", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

 
# Delete users from assine machine/connection
async def delete_connection(connection):
    gucamole_update_url = f"{os.getenv('GUCAMOLE_BASE_URL')}/api/session/data/{os.getenv('GUCAMOLE_DATASOURCE')}/connections/"
    token = await login_with_guacamole()
    url = gucamole_update_url +connection+ "?token=" + token
    response = requests.request("DELETE", url)
    return response.status_code
 
# modify machine/connection
async def modify_connection(machine_data:Machine):
    gucamole_update_url = f"{os.getenv('GUCAMOLE_BASE_URL')}/api/session/data/{os.getenv('GUCAMOLE_DATASOURCE')}/connections/"
    guacamole_login =  await login_with_guacamole()
    url = gucamole_update_url+str(machine_data['identifier'])+"?token="+ guacamole_login
    payload=return_payload(machine_data)
    headers = {
    'Content-Type': 'application/json'
    }
    response = requests.request("PUT", url, headers=headers, data=payload)
    return response.status_code
   
#------------------------------------------------------User----------------------------------------------------
# LIST OF USER from GUACAMOLE
async def list_of_users():
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")
    
    handle = await client.start_workflow(
        workflows_guacmole.ListOfGuacoUsersWorkflow.run,
        id=f"list_of_user_guaco-{uniqueId}",
        task_queue="list-guaco-users-task",
    )
    result =  await handle.result()
    logger.info("Successfully retrieved list of guaco users.")
    return result
# get user paricular user  
async def get_user_details(username):
    token = await login_with_guacamole()
    gucamole_create_user_url = f"{os.getenv('GUCAMOLE_BASE_URL')}/api/session/data/{os.getenv('GUCAMOLE_DATASOURCE')}/users"
    url = gucamole_create_user_url+"/"+username+"?token="+ token
    response= requests.request("GET", url, )
    return response.status_code
# get user paricular user  
# def delete_user_from_guca(username):
#     token = login_with_guacamole()
#     gucamole_create_user_url = f"{os.getenv('GUCAMOLE_BASE_URL')}/api/session/data/{os.getenv('GUCAMOLE_DATASOURCE')}/users"
#     url = gucamole_create_user_url+"/"+username+"?token="+ token
#     response= requests.request("DELETE", url, )
#     return response.status_code

# Create a user in guacamole
async def create_user(username):
    try:
        token = await login_with_guacamole()
        gucamole_create_user_url = f"{os.getenv('GUCAMOLE_BASE_URL')}/api/session/data/{os.getenv('GUCAMOLE_DATASOURCE')}/users"
        url = gucamole_create_user_url + "?token=" + token
        payload = json.dumps({
            "username": username,
            "password": "",
            "attributes": {
                "disabled": "",
                "expired": "",
                "access-window-start": "",
                "access-window-end": "",
                "valid-from": "",
                "valid-until": "",
                "timezone": "",
                "guac-full-name": "",
                "guac-organization": "",
                "guac-organizational-role": ""
            }
        })
        headers = {
            'Content-Type': 'application/json'
        }
        response = requests.post(url, headers=headers, data=payload)
        return response.status_code
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def delete_user(username):    
    try:
        token = await login_with_guacamole()
        url = f"{os.getenv('GUCAMOLE_BASE_URL')}/api/session/data/postgresql/users/{username}?token={token}"
        payload = {}
        headers = {}
        response = requests.delete(url, headers=headers, data=payload)
        return response.status_code
    except Exception as e:
        return e 
#   Get List of from Keyclaok --------------------------------
async def get_userList_from_keycloak(first: int, limit: int, search: str):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")
    
    handle = await client.start_workflow(
        workflows_guacmole.GetUserlistFromKeycloakWorkflow.run,
        args=[first, limit, search],
        id=f"list_of_user_guaco_keyclock-{uniqueId}",
        task_queue="list-guaco-users-with-keycloak-task",
    )
    result =  await handle.result()
    logger.info("Successfully retrieved list of guaco users with keyclock.")
    return result


#-------------------------------------------------------------------------------------------------------
async def get_users_connection_history(token):
    if not token:
        return []
    # f"{baseurl}/guacamole/api/session/data/postgresql/history/users"
    
    url = f"{os.getenv('GUCAMOLE_BASE_URL')}/api/session/data/{os.getenv('GUCAMOLE_DATASOURCE')}/history/connections" 
    # Fetch recent connection usage history (which includes connectionName, username, etc.)
    params = {'order': '-startDate', 'limit': 1000, 'token': token}
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        return response.json()  # Return the JSON response directly
    else:
        logger.error(f"Failed to fetch Guacamole connection history: {response.status_code} - {response.text}")
        return []

async def get_session_reports(start_date_range: datetime, end_date_range: datetime):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")
    # Convert datetime objects to ISO 8601 strings
    start_date_str = start_date_range
    end_date_str = end_date_range
    
    handle = await client.start_workflow(
        workflows_guacmole.GetSessionReportWorkflow.run,
        args = [start_date_str,end_date_str],
        id=f"get_session_reports-{uniqueId}",
        task_queue="get_session_reports",
    )
    result =  await handle.result()
    logger.info("Task created for get session report")
    return result
async def get_users_in_timerange(session_reports):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")
    # Convert datetime objects to ISO 8601 strings
    handle = await client.start_workflow(
        workflows_guacmole.GetAllUsersVamanitWorkflow.run,
        args = [session_reports],
        id=f"get_allUsers_vamanit-{uniqueId}",
        task_queue="get_all_users_vamanit",
    )
    result =  await handle.result()
    logger.info("Task created for get all reports on vamanit")
    return result

async def get_perticular_user_sessionreports(session_reports, username):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")
    # Convert datetime objects to ISO 8601 strings
    handle = await client.start_workflow(
        workflows_guacmole.GetPerticularUserSessionReportWorkflow.run,
        args = [session_reports,username],
        id=f"get_perticular_user_session_report-{uniqueId}",
        task_queue="get_perticular_user_session_report",
    )
    result =  await handle.result()
    logger.info("Successfully task created for get perticular user session report")
    return result

async def get_daily_reports(session_reports):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")
    # Convert datetime objects to ISO 8601 strings
    handle = await client.start_workflow(
        workflows_guacmole.GetDailyReportsWorkflow.run,
        args = [session_reports],
        id=f"get_daily_report-{uniqueId}",
        task_queue="get_daily_report",
    )
    result =  await handle.result()
    logger.info("Successfully task created for get daily reports from guacamole")
    return result

async def get_perticular_user_daily_reports(daily_reports, username):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")
    handle = await client.start_workflow(
        workflows_guacmole.getPerticularUserDailyReportWorkflow.run,
        args = [daily_reports,username],
        id=f"get_perticular_user_daily_reports-{uniqueId}",
        task_queue="get_perticular_user_daily_reports",
    )
    result =  await handle.result()
   
    logger.info("Successfully task created for get perticular user daily reports.")
    return result




# Database connection function
def get_db_connection():
    connection = psycopg2.connect(
        user=os.getenv('USER_NAME'),
        password=os.getenv('PASSWORD'),
        host=os.getenv('HOST_NAME'),
        port=os.getenv("PORT"),
        database="thinkclouddb"
    )
    return connection

# Function to insert company data

async def insert_report(company_name: str, company_logo: bytes, report_type: str):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    handle = await client.start_workflow(
        workflows_guacmole.InsertReportWorkflow.run,
        args=[company_name, company_logo, report_type],
        id=f"insert_report-{uniqueId}",
        task_queue="insert_report_taskqueue",
    )
    result =  await handle.result()
    return result

# Function to fetch all companies
async def get_companies():
    db = get_db_connection()
    try:
        with db.cursor() as cursor:
            select_query = "SELECT company_name, company_logo, report_type FROM reporttemplate;"
            cursor.execute(select_query)
            companies = cursor.fetchall()
            
            companies_data = [
                {
                    "company_name": row[0],
                    "company_logo": base64.b64encode(row[1]).decode("utf-8") if row[1] else None,
                    "report_type": row[2],
                }
                for row in companies
            ]
            return companies_data
    except Exception as error:
        logger.error(f"Error in get_companies: {error}", exc_info=True)
        return {"msg": "Error occurred", "error": str(error)}
    finally:
        db.close()

async def get_companies_by_report_type(report_type :str):
    db = get_db_connection()
    try:
        with db.cursor() as cursor:
            select_query = "SELECT company_name, company_logo, report_type FROM reporttemplate WHERE report_type = %s;"
            cursor.execute(select_query, (report_type,))
            companies = cursor.fetchall()
            companies_data = []
            for company in companies:
                company_name = company[0]
                report_type = company[2]
                company_logo_bytes = company[1]
                company_logo_base64 = base64.b64encode(company_logo_bytes).decode('utf-8') if company_logo_bytes else None
                companies_data.append({
                    "company_name": company_name,
                    "company_logo": company_logo_base64,
                    "report_type": report_type
                })
            return companies_data
    except Exception as error:
        logger.error(f"Error in get_companies_by_report_type: {error}", exc_info=True)
        return {"msg": "Error occurred", "error": str(error)}
    finally:
        db.close()



async def update_report(company_name: str, company_logo: str, report_type: str):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")

    handle = await client.start_workflow(
        workflows_guacmole.UpdateReportWorkflow.run,
        args=[company_name, company_logo, report_type],
        id=f"update_reports-{uniqueId}",
        task_queue="update_report_taskqueue",
    )
    result =  await handle.result()
    logger.info("Successfully updated report ")
    return result

# Function to delete company data
async def delete_report(report_type: str):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")
    handle = await client.start_workflow(
        workflows_guacmole.DeleteReportWorkflow.run,
        report_type,
        id=f"delete_report-{uniqueId}",
        task_queue="delete_report_taskqueue",
    )
    result =  await handle.result()
    logger.info("Successfully deleted report.")                                                                                                                                                                                                                                                                                                                                                                  
    return result
 
async def get_auth_headers():
    # Get and sanitize environment variables
    root_url = os.getenv('KEYCLOAK_ROOT_URL', '').strip().rstrip('/')
    admin_user = os.getenv('KEYCLOAK_ADMIN', 'admin').strip()
    admin_pass = os.getenv('KEYCLOAK_PASSWORD', 'admin').strip()
    
    if not root_url:
         raise Exception("KEYCLOAK_ROOT_URL is not set")

    token_url = f"{root_url}/realms/master/protocol/openid-connect/token"
    
     # Get access token
    resp = requests.post(
        token_url,
        data={
            "client_id": "admin-cli",
            "username": admin_user,
            "password": admin_pass,
            "grant_type": "password"
        }
    )
    resp.raise_for_status()
    data = resp.json()
    access_token = data.get("access_token")
    
    # Set authorization headers
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
async def get_client():
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")
    handle = await client.start_workflow(
        workflows_RBAC.GetClientWorkflow.run,
        id=f"get-client-{uniqueId}",
        task_queue="get_client_taskqueue",
    )
    result =  await handle.result()
    logger.info("Successfully got the client ")
    return result

 
async def get_client_roles():
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")
    handle = await client.start_workflow(
        workflows_RBAC.GetClientRolesWorkflow.run,
        id=f"get-client-roles-{uniqueId}",
        task_queue="get_client_roles_taskqueue",
    )
    result =  await handle.result()
    logger.info("Successfully got the client ")
    return result
 
async def create_client_role(client_id, role_name):
    try:
        headers = await get_auth_headers()
        realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
        keycloak_url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/clients/{client_id}/roles"
        realm_url = keycloak_url
        payload = {
            "name": role_name
           
        }
        realm_response = requests.post(realm_url, headers=headers, json=payload)
        realm_response.raise_for_status()
        return realm_response.json()
    except requests.RequestException as e:
        return None
 
async def role_exists(client_id, role_name):
    try:
        headers = await get_auth_headers()
        realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
        keycloak_url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/clients/{client_id}/roles"
        response = requests.get(keycloak_url, headers=headers)
        response.raise_for_status()
        roles = response.json()
        return any(role["name"] == role_name for role in roles)
    except requests.RequestException as e:
        
        return False
#delecte role in keyclock


async def role_exists_keycloak(client_id, role_name):
    try:
        headers = await get_auth_headers()
        realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
        keycloak_url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/roles"
        
        response = requests.get(keycloak_url, headers=headers, verify=False)
        response.raise_for_status()
        roles = response.json()
        return any(role["name"] == role_name for role in roles)
    except requests.RequestException as e:
        return False

async def get_keycloak_roles():
    try:
        headers = await get_auth_headers()
        realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
        # Wahi Realm roles wala endpoint jahan se poori list milegi
        keycloak_url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/roles"
        
        # GET request maari list nikalne ke liye
        response = requests.get(keycloak_url, headers=headers, verify=False)
        response.raise_for_status()
        
        return response.json()  # Yeh Keycloak ke saare realm roles ki list array/list me dega
    except requests.RequestException as e:
        logger.error(f"Failed to fetch realm roles from Keycloak: {e}")
        return None
    
async def create__keycloak_client_role(client_id, role_name):
    try:
        headers = await get_auth_headers()
        realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
        keycloak_url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/roles"
        payload = {"name": role_name}
        
        realm_response = requests.post(keycloak_url, headers=headers, json=payload, verify=False)
        realm_response.raise_for_status()
        return realm_response.json()
    except requests.RequestException as e:
        return None


async def delete_client_role(client_id, role_name):
    try:
        headers = await get_auth_headers()
        realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
        keycloak_url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/clients/{client_id}/roles/{role_name}"
        realm_url = keycloak_url
        realm_response = requests.delete(realm_url, headers=headers)
        realm_response.raise_for_status()
        return realm_response.json()
    except requests.RequestException as e:
        return None
def get_user_roles(auth_headers, user_id):
    try:
        headers = auth_headers
        realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
        keycloak_url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/users/{user_id}/role-mappings"
        realm_url = keycloak_url
        realm_response = requests.get(realm_url, headers=headers)
        realm_response.raise_for_status()
        realm_data = realm_response.json()
        return realm_data
    except requests.RequestException as e:
 
        return None
async def assign_role(user_id, role_id, role_name):
    headers = await get_auth_headers()
    realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
    keycloak_url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/users/{user_id}/role-mappings/realm"
    response = requests.post(
        keycloak_url,
        headers=headers,
        json=[{"id": role_id, "name": role_name}],
    )
    response.raise_for_status()
    return {"status": "assigned"}

async def get_service_account_token():
    """Token for devraq-backend's own Keycloak service account.

    Used for internal/background calls (e.g. scheduled report generation)
    that have no logged-in user attached to them at all.
    """
    root_url = os.getenv('KEYCLOAK_ROOT_URL', '').strip().rstrip('/')
    realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
    token_url = f"{root_url}/realms/{realm}/protocol/openid-connect/token"
    resp = requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": "devraq-backend",
            "client_secret": os.getenv("DEVRAQ_BACKEND_CLIENT_SECRET", "devraq-backend-secret"),
        }
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

async def get_role_by_name(role_name):
    headers = await get_auth_headers()
    realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
    keycloak_url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/roles/{role_name}"
    response = requests.get(keycloak_url, headers=headers, verify=False)
    response.raise_for_status()
    return response.json()

async def remove_role(user_id, role_id, role_name):
    headers = await get_auth_headers()
    realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
    keycloak_url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/users/{user_id}/role-mappings/realm"
    response = requests.delete(
        keycloak_url,
        headers=headers,
        json=[{"id": role_id, "name": role_name}],
    )
    response.raise_for_status()
    return {"status": "removed"}
    
async def posting_role(role_name: str, authorization: str):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")
    handle = await client.start_workflow(
        workflows_RBAC.CreatingRoleWorkflow.run,
        args=[role_name, authorization],
        id=f"create_role-{uniqueId}",
        task_queue="creating_role_taskqueue",
    )
    logger.info("workflow started successfully")
    result =  await handle.result()
    logger.info("Successfully created role.")
    if result["status"] == "Error":
        raise HTTPException(status_code=result['code'], detail=result['message'])                                                                                                  
    if result["code"] == 200:
        return {"msg": result["message"]}
    return result


async def deleting_role(role_name: str, authorization: str):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")
    handle = await client.start_workflow(
        workflows_RBAC.DeletingRoleWorkflow.run,
        args=[role_name, authorization],
        id=f"delete_role-{uniqueId}",
        task_queue="deleting_role_taskqueue",
    )
    logger.info("workflow started successfully")
    result =  await handle.result()
    logger.info("Successfully created role.")
    if result["code"] == 500:
        raise HTTPException(status_code=result['code'], detail=result['message'])                                                                                                  
    if result["code"] == 200:
        return {"msg": result["message"]}                                                                                         
    return result

async def updating_role_component(request: RoleComponentSubmitRequest, authorization):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")
    handle = await client.start_workflow(
        workflows_RBAC.UpdateRoleComponentWorkflow.run,
        args=[request.dict(), authorization],
        id=f"update_role_component-{uniqueId}",
        task_queue="updating_role_component_taskqueue",
    )
    logger.info("workflow started successfully")
    result =  await handle.result()
    logger.info("Successfully created role.")
    if result["status"] == "Error":
        raise HTTPException(status_code=result['code'], detail=result['message'])                                                                                                  
    if result["status"] == "Ok":
        return {"msg": result["message"]}                                                                                                  
    return result


async def getting_role_component(role: str):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")
    handle = await client.start_workflow(
        workflows_RBAC.GetRoleComponentWorkflow.run,
        role,
        id=f"get_role_component-{uniqueId}",
        task_queue="getting_role_component_taskqueue",
    )
    logger.info("workflow started successfully")
    result =  await handle.result()
    logger.info("Successfully created role.")                                                                                                  
    return result

async def assignning_user_role(request: RBACRequest):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")
    handle = await client.start_workflow(
        workflows_RBAC.AssignUserRoleworkflow.run,
        request.dict(),
        id=f"assign_user_role_component-{uniqueId}",
        task_queue="assign_user_role_taskqueue",
    )
    logger.info(f"Workflow started successfully: {handle.id}")
    result = await handle.result()
    return result


async def get_user_permissions(request, username: str):
    uniqueId = unique_id()
    auth_header = request.state._state
    # if not auth_header:
    #     raise HTTPException(
    #         status_code=status.HTTP_401_UNAUTHORIZED, 
    #         detail="Authorization header missing"
    #     )
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")
    workflow_input = {
        "username": username,
        "auth_header": auth_header
    }
    handle = await client.start_workflow(
        workflows_RBAC.GetUserPermissionsWorkflow.run,
        workflow_input,
        id=f"get_user_permissions-{uniqueId}",
        task_queue="get_user_permissions_taskqueue",
    )
    logger.info("workflow started successfully")
    result =  await handle.result()
    logger.info("Successfully created role.")                                                                                                  
    return result


async def delete_role_from_user(request: RBACRequest):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")
    handle = await client.start_workflow(
        workflows_RBAC.DeleteRoleFromUserWorkflow.run,
        request.dict(),
        id=f"delete_role_from_user-{uniqueId}",
        task_queue="delete_role_from_user_taskqueue",
    )
    logger.info("workflow started successfully")
    result =  await handle.result()
    logger.info("Successfully Deleted role.")                                                                                                 
    if result["code"] != 200:
        raise HTTPException(status_code=result['code'], detail=result['msg'])
    return result

# --------------------------------------------------------------------------------------------------------------------

async def generate_userbased_report(start_date: str, end_date: str, report_type: str,username:str):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")
    handle = await client.start_workflow(
        workflows_guacmole.GenerateUserBasedReportWorkflow.run,
        args = [start_date, end_date, report_type, username],
        id=f"generate_user_based_report-{uniqueId}",
        task_queue="generate_user_based_report_taskqueue",
    )
    logger.info("generate_userbased_report workflow started successfully")
    result =  await handle.result()
    logger.info("Report Generated Successfully.")                                                                                                  
    return result 
 
def format_datetime(datetime_str):
    if not datetime_str:
        return "Not Applicable"
    try:
        dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%d/%m/%Y %H:%M:%S")  # New format
    except:
        return jsonable_encoder(datetime_str)
 
 
def calculate_duration(duration):
    if duration == "Not Applicable":
        return "00:00:00"
    try:
       
 
        hours = int(duration // 3600)
        minutes = int((duration % 3600) // 60)
        seconds = int(duration % 60)
 
        return f"{hours:02}:{minutes:02}:{seconds:02}"
    except:
        return "00:00:00"


async def get_users_total_duration_within_timerange(day_duration: List[Dict]):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")
    handle = await client.start_workflow(
        workflows_guacmole.GetUsersTotalDurationWithinTimerangeWorkflow.run,
        day_duration,
        id=f"get_users_total_duration_within_timerange-{uniqueId}",
        task_queue="get_users_total_duration_within_timerange_taskqueue",
    )
    logger.info("get_users_total_duration_within_timerange workflow started successfully")
    result =  await handle.result()
    logger.info("Report Generated Successfully.")                                                                                                  
    return result
async def consolidate_report_perticular_user(user_total_duration, user):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")
    handle = await client.start_workflow(
        workflows_guacmole.ConsolidateReportPerticularUserWorkflow.run,
        args = [user_total_duration, user],
        id=f"consolidate_report_perticular_user-{uniqueId}",
        task_queue="consolidate_report_perticular_user_taskqueue",
    )
    logger.info("consolidate_report_perticular_user workflow started successfully")
    result =  await handle.result()
    logger.info("Report Generated Successfully.")                                                                                                  
    return result

async def generate_report(start_date: str, end_date: str, report_type: str) :
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    logger.info("Successfully established connection with the client.")
    handle = await client.start_workflow(
        workflows_guacmole.GenerateReportWorkflow.run,
        args = [start_date, end_date, report_type],
        id=f"generate-report-{uniqueId}",
        task_queue="generate_report_taskqueue",
    )
    logger.info("generate_report workflow started successfully")
    result =  await handle.result()
    logger.info("Report Generated Successfully.")                                                                                                  
    return result


async def get_guacamole_history():
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    handle = await client.start_workflow(
        workflows_guacmole.GetGuacamoleHistoryWorkflow.run,
        id=f"get_guacamole_history-{uniqueId}",
        task_queue="get_guacamole_history_taskqueue",
    )
    logger.info("get_gucamole_history workflow started successfully")
    result = await handle.result()
    logger.info("Guacamole history retrieved successfully.")
    return result


async def get_guacamole_ActiveSessions():
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    handle = await client.start_workflow(
        workflows_guacmole.GetGuacamoleActiveSessionsWorkflow.run,
        id=f"get_gucamole_ActiveSessions-{uniqueId}",
        task_queue="guacamole_active_sessions_taskqueue",
    )
    logger.info("get_guacamole_active_sessions workflow started successfully")
    result = await handle.result()
    logger.info("Guacamole active sessions retrieved successfully.")
    return result





async def generate_guacamole_session_url(session_uuid, datasource=None):
    guac_token = await login_with_guacamole()
    base_url = os.getenv('GUCAMOLE_BASE_URL')
    if not datasource or datasource == "undefined":
        datasource = os.getenv('GUACAMOLE_DATASOURCE')

    raw_bytes = f"{session_uuid}\x00{datasource}".encode()
    logger.info("Raw bytes: %s", list(raw_bytes))

    encoded = base64.b64encode(raw_bytes).decode().rstrip("=")
    logger.info("Encoded fragment: %s", encoded)

    decoded_bytes = base64.b64decode(encoded + '=' * ((4 - len(encoded) % 4) % 4))


    client_url = f"{base_url}/#/client/{encoded}"
 

    response = Response(status_code=302)
    response.headers["Location"] = client_url
    response.set_cookie(
        key="GUAC_AUTH",
        value=guac_token,
        path="/guacamole",
        httponly=True
    )
    return response

 
async def get_recording_log(identifier: str, log_uuid: str):
 
    token = await login_with_guacamole()
 
    base_url = os.getenv("GUCAMOLE_BASE_URL")
    datasource = os.getenv("GUCAMOLE_DATASOURCE")
 
    if not base_url or not datasource:
        raise HTTPException(status_code=500, detail="Guacamole config missing")
 
    rec_url = (
        f"{base_url}/api/session/data/{datasource}/history/connections/"
        f"{identifier}/logs/{log_uuid}?token={token}"
    )
 
    client = httpx.AsyncClient(timeout=None)
 
    r = await client.get(rec_url, timeout=None)
    if r.status_code != 200:
        text = await r.aread()
        await client.aclose()
        raise HTTPException(
            status_code=r.status_code,
            detail=text.decode(errors="ignore") or "Failed to fetch recording",
        )
 
    async def iter_bytes():
        try:
            async for chunk in r.aiter_bytes():
                yield chunk
        finally:
            await client.aclose()
 
    headers = {
        "Content-Type": "application/octet-stream",
        "Cache-Control": "no-store",
        "Content-Disposition": f'inline; filename="recording-{identifier}-{log_uuid}.guac"',
    }
    return StreamingResponse(iter_bytes(), media_type="application/octet-stream", headers=headers)

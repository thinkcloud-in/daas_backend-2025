import asyncio
from fastapi import HTTPException
import os
import psycopg2
import select
import json
import logging
from dto.machineDto import MachineDto
from models.models import Machine
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from utils.session_manager import SessionManager

logger = logging.getLogger("machine_listener")
# Logging managed by utils.logger

DATABASE_USER = os.getenv('USER_NAME')
DATABASE_PASSWORD = os.getenv('PASSWORD')
DATABASE_NAME = 'thinkclouddb'
DATABASE_HOST = os.getenv('HOST_NAME')
DATABASE_PORT = os.getenv('PORT')

def login_with_guacamole():
    url =  f"{os.getenv('GUCAMOLE_BASE_URL')}/api/tokens"
    username  = 'guacadmin'
    password = 'guacadmin' 
    payload = 'username='+username+'&password='+password
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    session = SessionManager.get_session()
    response = session.post(url, headers=headers, data=payload)
    if response.status_code == 200:
        return response.json().get('authToken')
    else:
      raise HTTPException(status_code=response.status_code, detail="Failed to authenticate with Guacamole")
    

def return_payload(machine_data_: MachineDto):
    machine_data = machine_data_.dict()
    protocol = machine_data.get('protocol', '').lower()
    match protocol:
        case "rdp":
            payload = json.dumps({
                "parentIdentifier": "ROOT",
                "name": machine_data.get('name'),
                "protocol": "rdp",
                "parameters": {
                    "port": machine_data.get('port'),
                    "read-only": machine_data.get('read_only'),
                    "swap-red-blue": machine_data.get('swap_red_blue'),
                    "cursor": machine_data.get('cursor'),
                    "color-depth": machine_data.get('color_depth'),
                    "clipboard-encoding": machine_data.get('clipboard_encoding'),
                    "disable-copy": machine_data.get('disable_copy'),
                    "disable-paste": machine_data.get('disable_paste'),
                    "dest-port": machine_data.get('dest_port'),
                    "recording-exclude-output": machine_data.get('recording_exclude_output'),
                    "recording-exclude-mouse": machine_data.get('recording_exclude_mouse'),
                    "recording-include-keys": machine_data.get('recording_include_keys'),
                    "create-recording-path": machine_data.get('create_recording_path'),
                    "enable-sftp": machine_data.get('enable_sftp'),
                    "sftp-port": machine_data.get('sftp_port'),
                    "sftp-server-alive-interval": machine_data.get('sftp_server_alive_interval'),
                    "enable-audio": machine_data.get('enable_audio'),
                    "security": machine_data.get('security'),
                    "disable-auth": machine_data.get('disable_auth'),
                    "ignore-cert": machine_data.get('ignore_cert'),
                    "gateway-port": machine_data.get('gateway_port'),
                    "server-layout": machine_data.get('server_layout'),
                    "timezone": machine_data.get('timezone'),
                    "console": machine_data.get('console'),
                    "width": machine_data.get('width'),
                    "height": machine_data.get('height'),
                    "dpi": machine_data.get('dpi'),
                    "resize-method": machine_data.get('resize_method'),
                    "console-audio": machine_data.get('console_audio'),
                    "disable-audio": machine_data.get('disable_audio'),
                    "enable-audio-input": machine_data.get('enable_audio_input'),
                    "enable-printing": machine_data.get('enable_printing'),
                    "enable-drive": machine_data.get('enable_drive'),
                    "create-drive-path": machine_data.get('create_drive_path'),
                    "enable-wallpaper": machine_data.get('enable_wallpaper'),
                    "enable-theming": machine_data.get('enable_theming'),
                    "enable-font-smoothing": machine_data.get('enable_font_smoothing'),
                    "enable-full-window-drag": machine_data.get('enable_full_window_drag'),
                    "enable-desktop-composition": machine_data.get('enable_desktop_composition'),
                    "enable-menu-animations": machine_data.get('enable_menu_animations'),
                    "disable-bitmap-caching": machine_data.get('disable_bitmap_caching'),
                    "disable-offscreen-caching": machine_data.get('disable_offscreen_caching'),
                    "disable-glyph-caching": machine_data.get('disable_glyph_caching'),
                    "preconnection-id": machine_data.get('preconnection_id'),
                    "hostname": machine_data.get('hostname'),
                    "username": machine_data.get('username'),
                    "password": machine_data.get('password'),
                    "domain": machine_data.get('domain'),
                    "gateway-hostname": machine_data.get('gateway_hostname'),
                    "gateway-username": machine_data.get('gateway_username'),
                    "gateway-password": machine_data.get('gateway_password'),
                    "gateway-domain": machine_data.get('gateway_domain'),
                    "initial-program": machine_data.get('initial_program'),
                    "client-name": machine_data.get('client_name'),
                    "printer-name": machine_data.get('printer_name'),
                    "drive-name": machine_data.get('drive_name'),
                    "drive-path": machine_data.get('drive_path'),
                    "static-channels": machine_data.get('static_channels'),
                    "remote-app": machine_data.get('remote_app'),
                    "remote-app-dir": machine_data.get('remote_app_dir'),
                    "remote-app-args": machine_data.get('remote_app_args'),
                    "preconnection-blob": machine_data.get('preconnection_blob'),
                    "load-balance-info": machine_data.get('load_balance_info'),
                    "recording-path": machine_data.get('recording_path'),
                    "recording-name": machine_data.get('recording_name'),
                    "sftp-hostname": machine_data.get('sftp_hostname'),
                    "sftp-host-key": machine_data.get('sftp_host_key'),
                    "sftp-username": machine_data.get('sftp_username'),
                    "sftp-password": machine_data.get('sftp_password'),
                    "sftp-private-key": machine_data.get('sftp_private_key'),
                    "sftp-passphrase": machine_data.get('sftp_passphrase'),
                    "sftp-root-directory": machine_data.get('sftp_root_directory'),
                    "sftp-directory": machine_data.get('sftp_directory')
                },
                "attributes": {
                    "max-connections": machine_data.get('max_connections'),
                    "max-connections-per-user": machine_data.get('max_connections_per_user'),
                    "weight": machine_data.get('weight'),
                    "failover-only": machine_data.get('failover_only'),
                    "guacd-port": machine_data.get('guacd_port'),
                    "guacd-encryption": machine_data.get('guacd_encryption'),
                    "guacd-hostname": machine_data.get('guacd_hostname')
                }
            })
        case "vnc" | "VNC":
            payload = json.dumps({
                "parentIdentifier": "ROOT",
                "name": machine_data.get('name'),
                "protocol": "vnc",
                "parameters": {
                    "port": machine_data.get('port'),
                    "read-only": machine_data.get('read_only'),
                    "swap-red-blue": machine_data.get('swap_red_blue'),
                    "cursor": machine_data.get('cursor'),
                    "color-depth": machine_data.get('color_depth'),
                    "clipboard-encoding": machine_data.get('clipboard_encoding'),
                    "disable-copy": machine_data.get('disable_copy'),
                    "disable-paste": machine_data.get('disable_paste'),
                    "dest-port": machine_data.get('dest_port'),
                    "recording-exclude-output": machine_data.get('recording_exclude_output'),
                    "recording-exclude-mouse": machine_data.get('recording_exclude_mouse'),
                    "recording-include-keys": machine_data.get('recording_include_keys'),
                    "create-recording-path": machine_data.get('create_recording_path'),
                    "enable-sftp": machine_data.get('enable_sftp'),
                    "sftp-port": machine_data.get('sftp_port'),
                    "sftp-server-alive-interval": machine_data.get('sftp_server_alive_interval'),
                    "enable-audio": machine_data.get('enable_audio'),
                    "audio-servername": machine_data.get('audio_servername'),
                    "sftp-directory": machine_data.get('sftp_directory'),
                    "sftp-root-directory": machine_data.get('sftp_root_directory'),
                    "sftp-passphrase": machine_data.get('sftp_passphrase'),
                    "sftp-private-key": machine_data.get('sftp_private_key'),
                    "sftp-username": machine_data.get('sftp_username'),
                    "sftp-password": machine_data.get('sftp_password'),
                    "sftp-host-key": machine_data.get('sftp_host_key'),
                    "sftp-hostname": machine_data.get('sftp_hostname'),
                    "recording-name": machine_data.get('recording_name'),
                    "recording-path": machine_data.get('recording_path'),
                    "dest-host": machine_data.get('dest_host'),
                    "password": machine_data.get('password'),
                    "username": machine_data.get('username'),
                    "hostname": machine_data.get('hostname')
                },
                "attributes": {
                    "max-connections": machine_data.get('max_connections'),
                    "max-connections-per-user": machine_data.get('max_connections_per_user'),
                    "weight": machine_data.get('weight'),
                    "failover-only": machine_data.get('failover_only'),
                    "guacd-port": machine_data.get('guacd_port'),
                    "guacd-encryption": machine_data.get('guacd_encryption'),
                    "guacd-hostname": machine_data.get('guacd_hostname')
                }
            })
        case "telnet":
            payload = json.dumps({
                "parentIdentifier": "ROOT",
                "name": machine_data.get('name'),
                "protocol": "telnet",
                "parameters": {
                    "port": machine_data.get('port'),
                    "read-only": machine_data.get('read_only'),
                    "swap-red-blue": machine_data.get('swap_red_blue'),
                    "cursor": machine_data.get('cursor'),
                    "color-depth": machine_data.get('color_depth'),
                    "clipboard-encoding": machine_data.get('clipboard_encoding'),
                    "disable-copy": machine_data.get('disable_copy'),
                    "disable-paste": machine_data.get('disable_paste'),
                    "dest-port": machine_data.get('dest_port'),
                    "recording-exclude-output": machine_data.get('recording_exclude_output'),
                    "recording-exclude-mouse": machine_data.get('recording_exclude_mouse'),
                    "recording-include-keys": machine_data.get('recording_include_keys'),
                    "create-recording-path": machine_data.get('create_recording_path'),
                    "enable-sftp": machine_data.get('enable_sftp'),
                    "sftp-port": machine_data.get('sftp_port'),
                    "sftp-server-alive-interval": machine_data.get('sftp_server_alive_interval'),
                    "enable-audio": machine_data.get('enable_audio'),
                    "color-scheme": machine_data.get('color_scheme'),
                    "font-size": machine_data.get('font_size'),
                    "scrollback": machine_data.get('scrollback'),
                    "backspace": machine_data.get('backspace'),
                    "terminal-type": machine_data.get('terminal_type'),
                    "create-typescript-path": machine_data.get('create_typescript_path'),
                    "hostname": machine_data.get('hostname'),
                    "username": machine_data.get('username'),
                    "password": machine_data.get('password'),
                    "username-regex": machine_data.get('username_regex'),
                    "password-regex": machine_data.get('password_regex'),
                    "login-success-regex": machine_data.get('login_success_regex'),
                    "login-failure-regex": machine_data.get('login_failure_regex'),
                    "font-name": machine_data.get('font_name'),
                    "typescript-path": machine_data.get('typescript_path'),
                    "typescript-name": machine_data.get('typescript_name'),
                },
                "attributes": {
                    "max-connections": machine_data.get('max_connections'),
                    "max-connections-per-user": machine_data.get('max_connections_per_user'),
                    "weight": machine_data.get('weight'),
                    "failover-only": machine_data.get('failover_only'),
                    "guacd-port": machine_data.get('guacd_port'),
                    "guacd-encryption": machine_data.get('guacd_encryption'),
                    "guacd-hostname": machine_data.get('guacd_hostname')
                }
            })
        case "kubernetes":
            payload = json.dumps({
                "parentIdentifier": "ROOT",
                "name": machine_data.get('name'),
                "protocol": "kubernetes",
                "parameters": {
                    "port": machine_data.get('port'),
                    "read-only": machine_data.get('read_only'),
                    "swap-red-blue": machine_data.get('swap_red_blue'),
                    "cursor": machine_data.get('cursor'),
                    "color-depth": machine_data.get('color_depth'),
                    "clipboard-encoding": machine_data.get('clipboard_encoding'),
                    "disable-copy": machine_data.get('disable_copy'),
                    "disable-paste": machine_data.get('disable_paste'),
                    "dest-port": machine_data.get('dest_port'),
                    "recording-exclude-output": machine_data.get('recording_exclude_output'),
                    "recording-exclude-mouse": machine_data.get('recording_exclude_mouse'),
                    "recording-include-keys": machine_data.get('recording_include_keys'),
                    "create-recording-path": machine_data.get('create_recording_path'),
                    "enable-sftp": machine_data.get('enable_sftp'),
                    "sftp-port": machine_data.get('sftp_port'),
                    "sftp-server-alive-interval": machine_data.get('sftp_server_alive_interval'),
                    "enable-audio": machine_data.get('enable_audio'),
                    "kubeconfig": machine_data.get('kubeconfig'),
                    "namespace": machine_data.get('namespace'),
                    "pod-name": machine_data.get('pod_name'),
                    "container-name": machine_data.get('container_name'),
                    "command": machine_data.get('command'),
                    "args": machine_data.get('args'),
                    "tty": machine_data.get('tty'),
                    "stdin": machine_data.get('stdin'),
                    "stdin-tty": machine_data.get('stdin_tty'),
                    "shell": machine_data.get('shell')
                },
                "attributes": {
                    "max-connections": machine_data.get('max_connections'),
                    "max-connections-per-user": machine_data.get('max_connections_per_user'),
                    "weight": machine_data.get('weight'),
                    "failover-only": machine_data.get('failover_only'),
                    "guacd-port": machine_data.get('guacd_port'),
                    "guacd-encryption": machine_data.get('guacd_encryption'),
                    "guacd-hostname": machine_data.get('guacd_hostname')
                }
            })
        case "ssh":
            payload = json.dumps({
                "parentIdentifier": "ROOT",
                "name": machine_data.get('name'),
                "protocol": "ssh",
                "parameters": {
                    "port": machine_data.get('port'),
                    "read-only": machine_data.get('read_only'),
                    "swap-red-blue": machine_data.get('swap_red_blue'),
                    "cursor": machine_data.get('cursor'),
                    "color-depth": machine_data.get('color_depth'),
                    "clipboard-encoding": machine_data.get('clipboard_encoding'),
                    "disable-copy": machine_data.get('disable_copy'),
                    "disable-paste": machine_data.get('disable_paste'),
                    "dest-port": machine_data.get('dest_port'),
                    "recording-exclude-output": machine_data.get('recording_exclude_output'),
                    "recording-exclude-mouse": machine_data.get('recording_exclude_mouse'),
                    "recording-include-keys": machine_data.get('recording_include_keys'),
                    "create-recording-path": machine_data.get('create_recording_path'),
                    "enable-sftp": machine_data.get('enable_sftp'),
                    "sftp-port": machine_data.get('sftp_port'),
                    "sftp-server-alive-interval": machine_data.get('sftp_server_alive_interval'),
                    "enable-audio": machine_data.get('enable_audio'),
                    "color-scheme": machine_data.get('color_scheme'),
                    "font-size": machine_data.get('font_size'),
                    "scrollback": machine_data.get('scrollback'),
                    "timezone": machine_data.get('timezone', ""),
                    "server-alive-interval": machine_data.get('server_alive_interval'),
                    "backspace": machine_data.get('backspace'),
                    "terminal-type": machine_data.get('terminal_type'),
                    "create-typescript-path": machine_data.get('create_typescript_path'),
                    "hostname": machine_data.get('hostname'),
                    "host-key": machine_data.get('host_key'),
                    "private-key": machine_data.get('private_key'),
                    "username": machine_data.get('username'),
                    "password": machine_data.get('password'),
                    "passphrase": machine_data.get('passphrase'),
                    "font-name": machine_data.get('font_name'),
                    "command": machine_data.get('command'),
                    "locale": machine_data.get('locale'),
                    "typescript-path": machine_data.get('typescript_path'),
                    "typescript-name": machine_data.get('typescript_name'),
                    "recording-path": machine_data.get('recording_path'),
                    "recording-name": machine_data.get('recording_name'),
                    "sftp-root-directory": machine_data.get('sftp_root_directory')
                },
                "attributes": {
                    "max-connections": machine_data.get('max_connections'),
                    "max-connections-per-user": machine_data.get('max_connections_per_user'),
                    "weight": machine_data.get('weight'),
                    "failover-only": machine_data.get('failover_only'),
                    "guacd-port": machine_data.get('guacd_port'),
                    "guacd-encryption": machine_data.get('guacd_encryption'),
                    "guacd-hostname": machine_data.get('guacd_hostname')
                }
            })
        case _:
            raise ValueError(f"Unsupported protocol: {protocol}")
    return payload
  
def modify_connection(machine_data:Machine):
    gucamole_update_url = f"{os.getenv('GUCAMOLE_BASE_URL')}/api/session/data/{os.getenv('GUCAMOLE_DATASOURCE')}/connections/"
    try:
        url = gucamole_update_url + machine_data.get('identifier')+"?token="+login_with_guacamole()
        payload=return_payload(machine_data)
        headers = {
        'Content-Type': 'application/json'
        }
    except TypeError:
       return 'null' 
    try:
        session = SessionManager.get_session()
        response = session.put(url, headers=headers, data=payload)
        return response.status_code

    except Exception as e:
        return None


async def listen_for_machine_changes():
    conn = psycopg2.connect(user=DATABASE_USER, password=DATABASE_PASSWORD, 
                            database=DATABASE_NAME, host=DATABASE_HOST, port=DATABASE_PORT)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    
    cur.execute("LISTEN machine_change;")


    while True:
        # Wait for notifications
        if select.select([conn], [], [], 5) == ([], [], []):
            logger.info("No notification received.")
        else:
            conn.poll()
            while conn.notifies:
                notify = conn.notifies.pop(0)
                logger.info("Notification received:")

                if not notify.payload:
                    logger.warning("Received empty payload, skipping...")
                    continue
                
                try:
                    machine_data = json.loads(notify.payload)
                    logger.info("Machine data: %s", machine_data)
                    modify_connection(machine_data)
                except json.JSONDecodeError as e:
                    logger.error("Failed to decode JSON: %s, payload: %s", str(e), notify.payload)

        await asyncio.sleep(1)





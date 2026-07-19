from sqlalchemy import JSON, Text, Column, Integer, String, DateTime, Boolean,ARRAY, Sequence, UniqueConstraint
from pydantic import BaseModel, Field, validator# type: ignore
from typing import Any, Dict, List, Optional, Union
from db_configuration.config import Base
import datetime
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.mutable import MutableDict

#declarative base model for pool
class Pool(Base):
    __tablename__ = 'pools_new_1'

    id = Column(Integer, Sequence('pool_id_seq'), primary_key=True)
    pool_type = Column(String)
    entitled = Column(Integer)
    pool_name = Column(String) # pool name
    pool_machines=Column(ARRAY(String))
    pool_os_type=Column(String,nullable=True)
    pool_ad_domain = Column(String, nullable=True)
    pool_ad_password = Column(String, nullable=True)
    pool_ad_username = Column(String, nullable=True)
    pool_ad_path = Column(String, nullable=True)

    # -------------------------
    # disable_audio = Column(Boolean, nullable=True)
    pool_guacd_encryption = Column(String, nullable=False)
    pool_guacd_port = Column(Integer, nullable=True)
    pool_guacd_hostname = Column(String, nullable=False)
    pool_max_connections = Column(Integer, nullable=True)
    pool_max_connections_per_user = Column(Integer, nullable=True)
    pool_weight = Column(Integer, nullable=True)
    pool_failover_only = Column(String, nullable=False)
    pool_name = Column(String, nullable=True)
    pool_protocol = Column(String, nullable=True)
    pool_port = Column(Integer, nullable=True)
    pool_read_only = Column(Boolean, nullable=True)
    pool_swap_red_blue = Column(Boolean, nullable=True)
    pool_cursor = Column(String, nullable=True)
    pool_color_depth = Column(String, nullable=True)
    pool_clipboard_encoding = Column(String, nullable=True)
    pool_disable_copy = Column(Boolean, nullable=True)
    pool_disable_paste = Column(Boolean, nullable=True)
    pool_dest_port = Column(Integer, nullable=True)
    pool_recording_exclude_output = Column(Boolean, nullable=True)
    pool_recording_exclude_mouse = Column(Boolean, nullable=True)
    pool_recording_include_keys = Column(Boolean, nullable=True)
    pool_create_recording_path = Column(Boolean, nullable=True)
    pool_enable_sftp = Column(Boolean, nullable=True)
    pool_sftp_port = Column(Integer, nullable=True)
    pool_sftp_server_alive_interval = Column(Integer, nullable=True)
    pool_enable_audio = Column(Boolean, nullable=True)
    pool_color_scheme = Column(String, nullable=True)
    pool_font_size = Column(Integer, nullable=True)
    pool_scrollback = Column(Integer, nullable=True)
    pool_timezone = Column(String, nullable=True)
    pool_server_alive_interval = Column(Integer, nullable=True)
    pool_backspace = Column(String, nullable=True)
    pool_terminal_type = Column(String, nullable=True)
    pool_create_typescript_path = Column( Boolean, nullable=True)
    pool_hostname = Column(String, nullable=True)
    pool_host_key = Column(String, nullable=True)
    pool_private_key = Column(String, nullable=True)
    pool_username = Column(String, nullable=True)
    pool_password = Column(String, nullable=True)
    pool_passphrase = Column(String, nullable=True)
    # pool_font_name = Column(String, nullable=True)
    pool_command = Column(String, nullable=True)
    pool_locale = Column(String, nullable=True)
    pool_typescript_path = Column(String, nullable=True)
    pool_typescript_name = Column(String, nullable=True)
    pool_recording_path = Column(String, nullable=True)
    pool_recording_name = Column(String, nullable=True)
    pool_sftp_root_directory = Column(String, nullable=True)
    pool_sftp_directory = Column(String, nullable=True)
    pool_sftp_passphrase = Column(String, nullable=True)
    pool_sftp_private_key = Column(String, nullable=True)
    pool_sftp_username = Column(String, nullable=True)
    pool_sftp_password = Column(String, nullable=True)
    pool_sftp_host_key = Column(String, nullable=True)
    # sftp_hostname = Column(String, nullable=True)
    pool_dest_host = Column(String, nullable=True)
    pool_audio_servername = Column(String, nullable=True)
    pool_kubeconfig = Column(String, nullable=True)
    pool_namespace = Column(String, nullable=True)
    pool_pod_name = Column(String, nullable=True)
    pool_container_name = Column(String, nullable=True)
    pool_args = Column(ARRAY(String), nullable=True)
    pool_tty = Column(Boolean, nullable=True)
    pool_stdin = Column(Boolean, nullable=True)
    pool_stdin_tty = Column(Boolean, nullable=True)
    pool_shell = Column(String, nullable=True)

    # Protocol-specific fields
    pool_security = Column(String, nullable=True)
    pool_disable_auth = Column(Boolean, nullable=True)
    pool_ignore_cert = Column(Boolean, nullable=True)
    pool_gateway_port = Column(Integer, nullable=True)
    pool_server_layout = Column(String, nullable=True)
    pool_console = Column(Boolean, nullable=True)
    pool_width = Column(Integer, nullable=True)
    pool_height = Column(Integer, nullable=True)
    pool_dpi = Column(Integer, nullable=True)
    pool_resize_method = Column(String, nullable=True)
    pool_console_audio = Column(Boolean, nullable=True)
    pool_enable_audio_input = Column(Boolean, nullable=True)
    pool_enable_printing = Column(Boolean, nullable=True)
    pool_enable_drive = Column(Boolean, nullable=True)
    pool_create_drive_path = Column(String, nullable=True)
    pool_enable_wallpaper = Column(Boolean, nullable=True)
    pool_enable_theming = Column(Boolean, nullable=True)
    pool_enable_font_smoothing = Column(Boolean, nullable=True)
    pool_enable_full_window_drag = Column(Boolean, nullable=True)
    pool_enable_desktop_composition = Column(Boolean, nullable=True)
    pool_enable_menu_animations = Column(Boolean, nullable=True)
    pool_disable_bitmap_caching = Column(Boolean, nullable=True)
    pool_disable_offscreen_caching = Column(Boolean, nullable=True)
    pool_disable_glyph_caching = Column(Boolean, nullable=True)
    pool_preconnection_id = Column(String, nullable=True)
    pool_domain = Column(String, nullable=True)
    # gateway_hostname = Column(String, nullable=True)
    pool_gateway_username = Column(String, nullable=True)
    pool_gateway_password = Column(String, nullable=True)
    pool_gateway_domain = Column(String, nullable=True)
    pool_initial_program = Column(String, nullable=True)
    pool_client_name = Column(String, nullable=True)
    pool_printer_name = Column(String, nullable=True)
    pool_drive_name = Column(String, nullable=True)
    pool_drive_path = Column(String, nullable=True)
    pool_static_channels = Column(String, nullable=True)
    pool_remote_app = Column(String, nullable=True)
    pool_remote_app_dir = Column(String, nullable=True)
    pool_remote_app_args = Column(String, nullable=True)
    pool_preconnection_blob = Column(String, nullable=True)
    pool_load_balance_info = Column(String, nullable=True)
    pool_ca_cert = Column(String, nullable=True)
    pool_login_success_regex = Column(String, nullable=True)
    pool_login_failure_regex = Column(String, nullable=True)

    # Attributes for all protocols
    pool_attributes_max_connections = Column(Integer, nullable=True)
    pool_attributes_max_connections_per_user = Column(Integer, nullable=True)
    pool_attributes_weight = Column(Integer, nullable=True)
    pool_attributes_failover_only = Column(Boolean, nullable=True)
    pool_attributes_guacd_port = Column(Integer, nullable=True)
    pool_attributes_guacd_encryption = Column(String, nullable=True)
    # attributes_guacd_hostname = Column(String, nullable=True)

    # Custom fields
    pool_font_name = Column(String, nullable=True)
    pool_disable_audio = Column(Boolean, nullable=True)

    # Uncommon fields
    # pool_destination_host = Column(String, nullable=True)
    # pool_destination_port = Column(Integer, nullable=True)
    # pool_automatically_create_typescript_path = Column(Boolean, nullable=True)
    pool_automatically_create_recording_path = Column(Boolean, nullable=True)
    pool_exclude_graphics_streams = Column(Boolean, nullable=True)
    pool_exclude_mouse = Column(Boolean, nullable=True)
    pool_exclude_touch_events = Column(Boolean, nullable=True)
    pool_include_key_events = Column(Boolean, nullable=True)
 
    pool_storage = Column(String, nullable=True)
    pool_date_created = Column(DateTime, default=datetime.datetime.utcnow)
      #newly added fields by pushpa
    pool_vmids = Column(ARRAY(String),nullable=True)  # List of VM IDs associated with the pool
    cluster_id = Column(String,  nullable=True)
    pool_ip_pool_names = Column(ARRAY(String), nullable=True)  # List of IP pools associated with the pool
    pool_number_of_vms = Column(Integer, nullable=True, default=1)  # Number of VMs in the pool
    pool_naming_pattern = Column(String, nullable=True)  # Naming pattern for VMs in the pool
    pool_template_vm_id = Column(MutableDict.as_mutable(JSON), default=dict)
    # pool_template_vm_id = Column(Integer, nullable=True) #Column(JSON, nullable=True) #Column(Integer, nullable=True)  # Template VM ID for cloning VMs in the pool
    pool_selected_nodes = Column(ARRAY(String), nullable=True)  # Selected node for the pool
    pool_status = Column(String, nullable=True)  # Status of the pool

#Pydantic model for create pool
class CreatePoolBase(BaseModel):
    pool_type: Optional[str] = None
    email: Optional[str] = None
    # name: str
    pool_ad_domain: Optional[str] = None
    pool_ad_password: Optional[str] = None
    pool_ad_username: Optional[str] = None
    pool_ad_path: Optional[str] = None
    pool_name: Optional[str] = None
    pool_machines: Optional[List[str]] = None  # <--- add this line!
    pool_os_type: Optional[str] = None
    pool_storage: Optional[str] = None
    pool_disable_audio:Optional[bool] = None
    pool_guacd_port:Optional[int] = None
    pool_guacd_encryption: Optional[str] = None
    pool_guacd_hostname:str
    pool_max_connections: Optional[int] = None
    pool_max_connections_per_user: Optional[int] = None
    pool_weight:Optional[int] = None
    pool_failover_only: Optional[str] = None
    pool_protocol: Optional[str] = None
    pool_port: Optional[int] = None
    pool_read_only: Optional[bool] = None
    pool_swap_red_blue: Optional[bool] = None
    pool_cursor: Optional[str] = None
    pool_color_depth: Optional[str] = None
    pool_clipboard_encoding: Optional[str] = None
    pool_disable_copy: Optional[bool] = None
    pool_disable_paste: Optional[bool] = None
    pool_dest_port: Optional[int] = None
    pool_recording_exclude_output: Optional[bool] = None
    pool_recording_exclude_mouse: Optional[bool] = None
    pool_recording_include_keys: Optional[bool] = None
    pool_create_recording_path: Optional[bool] = None
    pool_enable_sftp: Optional[bool] = None
    pool_sftp_port: Optional[int] = None
    pool_sftp_server_alive_interval: Optional[int] = None
    pool_enable_audio: Optional[bool] = None
    pool_color_scheme: Optional[str] = None
    pool_font_size: Optional[int] = None
    pool_scrollback: Optional[int] = None
    pool_timezone: Optional[str] = None
    pool_server_alive_interval: Optional[int] = None
    pool_backspace: Optional[str] = None
    pool_terminal_type: Optional[str] = None
    pool_create_typescript_path: Optional[bool] = None
    pool_hostname: Optional[str] = None
    pool_host_key: Optional[str] = None
    pool_private_key: Optional[str] = None
    pool_username: Optional[str] = None
    pool_password: Optional[str] = None
    pool_passphrase: Optional[str] = None
    # pool_font_name: Optional[str] = None
    pool_command: Optional[str] = None
    pool_locale: Optional[str] = None
    pool_typescript_path: Optional[str] = None
    pool_typescript_name: Optional[str] = None
    pool_recording_path: Optional[str] = None
    pool_recording_name: Optional[str] = None
    pool_sftp_root_directory: Optional[str] = None
    pool_sftp_directory: Optional[str] = None
    pool_sftp_passphrase: Optional[str] = None
    pool_sftp_private_key: Optional[str] = None
    pool_sftp_username: Optional[str] = None
    pool_sftp_password: Optional[str] = None
    # sftp_hostname: Optional[str] = None
    pool_sftp_host_key: Optional[str] = None
    pool_dest_host: Optional[str] = None
    pool_audio_servername: Optional[str] = None
    pool_kubeconfig: Optional[str] = None
    pool_namespace: Optional[str] = None
    pool_pod_name: Optional[str] = None
    pool_container_name: Optional[str] = None
    pool_args: Optional[List[str]] = None
    pool_tty: Optional[bool] = None
    pool_stdin: Optional[bool] = None
    pool_stdin_tty: Optional[bool] = None
    pool_shell: Optional[str] = None

    # Protocol-specific fields
    pool_security: Optional[str] = None
    pool_disable_auth: Optional[bool] = None
    pool_ignore_cert: Optional[bool] = None
    pool_gateway_port: Optional[int] = None
    pool_server_layout: Optional[str] = None
    pool_console: Optional[bool] = None
    pool_width: Optional[int] = None
    pool_height: Optional[int] = None
    pool_dpi: Optional[int] = None
    pool_resize_method: Optional[str] = None
    pool_console_audio: Optional[bool] = None
    pool_enable_audio_input: Optional[bool] = None
    pool_enable_printing: Optional[bool] = None
    pool_enable_drive: Optional[bool] = None
    pool_create_drive_path: Optional[str] = None
    pool_enable_wallpaper: Optional[bool] = None
    pool_enable_theming: Optional[bool] = None
    pool_enable_font_smoothing: Optional[bool] = None
    pool_enable_full_window_drag: Optional[bool] = None
    pool_enable_desktop_composition: Optional[bool] = None
    pool_enable_menu_animations: Optional[bool] = None
    pool_disable_bitmap_caching: Optional[bool] = None
    pool_disable_offscreen_caching: Optional[bool] = None
    pool_disable_glyph_caching: Optional[bool] = None
    pool_preconnection_id: Optional[str] = None
    pool_domain: Optional[str] = None
    # gateway_hostname: Optional[str] = None
    pool_gateway_username: Optional[str] = None
    pool_gateway_password: Optional[str] = None
    pool_gateway_domain: Optional[str] = None
    pool_initial_program: Optional[str] = None
    pool_client_name: Optional[str] = None
    pool_printer_name: Optional[str] = None
    pool_drive_name: Optional[str] = None
    pool_drive_path: Optional[str] = None
    pool_static_channels: Optional[str] = None
    pool_remote_app: Optional[str] = None
    pool_remote_app_dir: Optional[str] = None
    pool_remote_app_args: Optional[str] = None
    pool_preconnection_blob: Optional[str] = None
    pool_load_balance_info: Optional[str] = None
    pool_ca_cert: Optional[str] = None
    pool_login_success_regex: Optional[str] = None
    pool_login_failure_regex: Optional[str] = None

    # Attributes for all protocols
    pool_exclude_touch_events:Optional[bool] = None
    pool_attributes_max_connections: Optional[int] = None
    pool_attributes_max_connections_per_user: Optional[int] = None
    pool_exclude_graphics_streams : Optional[bool] = None
    pool_attributes_weight: Optional[int] = None
    pool_attributes_failover_only: Optional[bool] = None
    pool_attributes_guacd_port: Optional[int] = None
    pool_attributes_guacd_encryption: Optional[str] = None
    # attributes_guacd_hostname: Optional[str] = None

    
    # -------------------------------------
    pool_date_created: Optional[str] = None
    #new added fields by pushpa
    pool_vmids: Optional[List[str]] = None  # List of VM IDs associated with the pool
    cluster_id: Optional[str] = None  # Foreign key to Cluster model
    pool_ip_pool_names: Optional[List[str]] = None  # List of IP pools associated with the pool
    pool_number_of_vms: Optional[int] = 1  # Number of VMs in the pool
    pool_naming_pattern: Optional[str] = None  # Naming pattern for VMs in the pool
    # pool_template_vm_id: Optional[int] = None  # Template VM ID for cloning VMs in the pool
    # pool_template_vm_id: Optional[Union[int, Dict[str, Any]]] = None 
    pool_template_vm_id: Optional[Dict[str, Any]] = None 
    pool_selected_nodes: Optional[List[str]] = None  # Selected nodes for the pool
    pool_status: Optional[str] = None  # Status of the pool

class NodeRequest(BaseModel):
    nodes: Optional[List[str]] = None
    cluster_id: Optional[str] = None
    
#Pydantic model for update pool
class UpdatePoolBase(BaseModel):
    pool_type: Optional[str] = None
    # name: str
    pool_name: Optional[str] = None
    pool_machines: Optional[List[str]] = None  # <--- add this line!
    pool_os_type: Optional[str] = None
    # ---------------------------------------
    pool_disable_audio:Optional[bool] = None
    pool_guacd_port:Optional[int] = None
    pool_guacd_encryption:Optional[str] = None
    pool_guacd_hostname:str
    pool_max_connections: Optional[int] = None
    pool_max_connections_per_user: Optional[int] = None
    pool_weight:Optional[int] = None
    pool_failover_only: Optional[str] = None
    pool_protocol: Optional[str] = None
    pool_port: Optional[int] = None
    pool_read_only: Optional[bool] = None
    pool_swap_red_blue: Optional[bool] = None
    pool_cursor: Optional[str] = None
    pool_color_depth: Optional[str] = None
    pool_clipboard_encoding: Optional[str] = None
    pool_disable_copy: Optional[bool] = None
    pool_disable_paste: Optional[bool] = None
    pool_dest_port: Optional[int] = None
    pool_recording_exclude_output: Optional[bool] = None
    pool_recording_exclude_mouse: Optional[bool] = None
    pool_recording_include_keys: Optional[bool] = None
    pool_create_recording_path: Optional[bool] = None
    pool_enable_sftp: Optional[bool] = None
    pool_sftp_port: Optional[int] = None
    pool_sftp_server_alive_interval: Optional[int] = None
    pool_enable_audio: Optional[bool] = None
    pool_color_scheme: Optional[str] = None
    pool_font_size: Optional[int] = None
    pool_scrollback: Optional[int] = None
    pool_timezone: Optional[str] = None
    pool_server_alive_interval: Optional[int] = None
    pool_backspace: Optional[str] = None
    pool_terminal_type: Optional[str] = None
    pool_create_typescript_path: Optional[bool] = None
    pool_hostname: Optional[str] = None
    pool_host_key: Optional[str] = None
    pool_private_key: Optional[str] = None
    pool_username: Optional[str] = None
    pool_password: Optional[str] = None
    pool_passphrase: Optional[str] = None
    pool_font_name: Optional[str] = None
    pool_command: Optional[str] = None
    pool_locale: Optional[str] = None
    pool_typescript_path: Optional[str] = None
    pool_typescript_name: Optional[str] = None
    pool_recording_path: Optional[str] = None
    pool_recording_name: Optional[str] = None
    pool_sftp_root_directory: Optional[str] = None
    pool_sftp_directory: Optional[str] = None
    pool_sftp_passphrase: Optional[str] = None
    pool_sftp_private_key: Optional[str] = None
    pool_sftp_username: Optional[str] = None
    pool_sftp_password: Optional[str] = None
    # sftp_
    # 
    # : Optional[str] = None
    pool_sftp_host_key: Optional[str] = None
    pool_dest_host: Optional[str] = None
    pool_audio_servername: Optional[str] = None
    pool_kubeconfig: Optional[str] = None
    pool_namespace: Optional[str] = None
    pool_pod_name: Optional[str] = None
    pool_container_name: Optional[str] = None
    pool_args: Optional[List[str]] = None
    pool_tty: Optional[bool] = None
    pool_stdin: Optional[bool] = None
    pool_stdin_tty: Optional[bool] = None
    pool_shell: Optional[str] = None

    # Protocol-specific fields
    pool_security: Optional[str] = None
    pool_disable_auth: Optional[bool] = None
    pool_ignore_cert: Optional[bool] = None
    pool_gateway_port: Optional[int] = None
    pool_server_layout: Optional[str] = None
    pool_console: Optional[bool] = None
    pool_width: Optional[int] = None
    pool_height: Optional[int] = None
    pool_dpi: Optional[int] = None
    pool_resize_method: Optional[str] = None
    pool_console_audio: Optional[bool] = None
    pool_enable_audio_input: Optional[bool] = None
    pool_enable_printing: Optional[bool] = None
    pool_enable_drive: Optional[bool] = None
    pool_create_drive_path: Optional[str] = None
    pool_enable_wallpaper: Optional[bool] = None
    pool_enable_theming: Optional[bool] = None
    pool_enable_font_smoothing: Optional[bool] = None
    pool_enable_full_window_drag: Optional[bool] = None
    pool_enable_desktop_composition: Optional[bool] = None
    pool_enable_menu_animations: Optional[bool] = None
    pool_disable_bitmap_caching: Optional[bool] = None
    pool_disable_offscreen_caching: Optional[bool] = None
    pool_disable_glyph_caching: Optional[bool] = None
    pool_preconnection_id: Optional[str] = None
    pool_domain: Optional[str] = None
    # gateway_hostname: Optional[str] = None
    pool_gateway_username: Optional[str] = None
    pool_gateway_password: Optional[str] = None
    pool_gateway_domain: Optional[str] = None
    pool_initial_program: Optional[str] = None
    pool_client_name: Optional[str] = None
    pool_printer_name: Optional[str] = None
    pool_drive_name: Optional[str] = None
    pool_drive_path: Optional[str] = None
    pool_static_channels: Optional[str] = None
    pool_remote_app: Optional[str] = None
    pool_remote_app_dir: Optional[str] = None
    pool_remote_app_args: Optional[str] = None
    pool_preconnection_blob: Optional[str] = None
    pool_load_balance_info: Optional[str] = None
    pool_ca_cert: Optional[str] = None
    pool_login_success_regex: Optional[str] = None
    pool_login_failure_regex: Optional[str] = None

    # Attributes for all protocols
    pool_exclude_touch_events:Optional[bool] = None
    pool_attributes_max_connections: Optional[int] = None
    pool_attributes_max_connections_per_user: Optional[int] = None
    pool_exclude_graphics_streams : Optional[bool] = None
    pool_attributes_weight: Optional[int] = None
    pool_attributes_failover_only: Optional[bool] = None
    pool_attributes_guacd_port: Optional[int] = None
    pool_attributes_guacd_encryption: Optional[str] = None
    # attributes_guacd_hostname: Optional[str] = None

    
    # -------------------------------------
    pool_date_created: Optional[str] = None

#declarative base model for machine
    pool_vmids: Optional[List[str]] = None  # List of VM IDs associated with the pool
    cluster_id: Optional[str] = None  # Foreign key to Cluster model
    pool_ip_pool_names: Optional[List[str]] = None  # List of IP pools associated with the pool
    pool_number_of_vms: Optional[int] = 1  # Number of VMs in the pool
    pool_naming_pattern: Optional[str] = None  # Naming pattern for VMs in the pool
    pool_template_vm_id: Optional[Dict[str, Any]] = None 
    # pool_template_vm_id: Optional[int] = None  # Template VM ID for cloning VMs in the pool
    pool_selected_nodes: Optional[List[str]] = None  # Selected nodes for the pool

class Machine(Base):
    __tablename__ = 'machines_clone'
    id = Column(Integer, Sequence('machine_id_seq'), primary_key=True)
    vm_id = Column(String, nullable=True)
    # General fields
    # disable_audio = Column(Boolean, nullable=True)
    os_type = Column(String, nullable=True)
    guacd_port = Column(Integer, nullable=True)
    guacd_encryption = Column(String, nullable=False)
    guacd_hostname = Column(String, nullable=False)
    max_connections = Column(Integer, nullable=True)
    max_connections_per_user = Column(Integer, nullable=True)
    weight = Column(Integer, nullable=True)
    failover_only = Column(String, nullable=False)
    name = Column(String, nullable=True)
    protocol = Column(String, nullable=True)
    port = Column(Integer, nullable=True)
    read_only = Column(Boolean, nullable=True)
    swap_red_blue = Column(Boolean, nullable=True)
    cursor = Column(String, nullable=True)
    color_depth = Column(String, nullable=True)
    clipboard_encoding = Column(String, nullable=True)
    disable_copy = Column(Boolean, nullable=True)
    disable_paste = Column(Boolean, nullable=True)
    dest_port = Column(Integer, nullable=True)
    recording_exclude_output = Column(Boolean, nullable=True)
    recording_exclude_mouse = Column(Boolean, nullable=True)
    recording_include_keys = Column(Boolean, nullable=True)
    create_recording_path = Column(Boolean, nullable=True)
    enable_sftp = Column(Boolean, nullable=True)
    sftp_port = Column(Integer, nullable=True)
    sftp_server_alive_interval = Column(Integer, nullable=True)
    enable_audio = Column(Boolean, nullable=True)
    color_scheme = Column(String, nullable=True)
    font_size = Column(Integer, nullable=True)
    scrollback = Column(Integer, nullable=True)
    timezone = Column(String, nullable=True)
    server_alive_interval = Column(Integer, nullable=True)
    backspace = Column(String, nullable=True)
    terminal_type = Column(String, nullable=True)
    create_typescript_path = Column(Boolean, nullable=True)
    hostname = Column(String, nullable=True)
    host_key = Column(String, nullable=True)
    private_key = Column(String, nullable=True)
    username = Column(String, nullable=True)
    password = Column(String, nullable=True)
    passphrase = Column(String, nullable=True)
    # font_name = Column(String, nullable=True)
    command = Column(String, nullable=True)
    locale = Column(String, nullable=True)
    typescript_path = Column(String, nullable=True)
    typescript_name = Column(String, nullable=True)
    recording_path = Column(String, nullable=True)
    recording_name = Column(String, nullable=True)
    sftp_root_directory = Column(String, nullable=True)
    sftp_directory = Column(String, nullable=True)
    sftp_passphrase = Column(String, nullable=True)
    sftp_private_key = Column(String, nullable=True)
    sftp_username = Column(String, nullable=True)
    sftp_password = Column(String, nullable=True)
    sftp_host_key = Column(String, nullable=True)
    sftp_hostname = Column(String, nullable=True)
    dest_host = Column(String, nullable=True)
    audio_servername = Column(String, nullable=True)
    kubeconfig = Column(String, nullable=True)
    namespace = Column(String, nullable=True)
    pod_name = Column(String, nullable=True)
    container_name = Column(String, nullable=True)
    args = Column(ARRAY(String), nullable=True)
    tty = Column(Boolean, nullable=True)
    stdin = Column(Boolean, nullable=True)
    stdin_tty = Column(Boolean, nullable=True)
    shell = Column(String, nullable=True)

    # Protocol-specific fields
    security = Column(String, nullable=True)
    disable_auth = Column(Boolean, nullable=True)
    ignore_cert = Column(Boolean, nullable=True)
    gateway_port = Column(Integer, nullable=True)
    server_layout = Column(String, nullable=True)
    console = Column(Boolean, nullable=True)
    width = Column(Integer, nullable=True)
    height = Column(Integer, nullable=True)
    dpi = Column(Integer, nullable=True)
    resize_method = Column(String, nullable=True)
    console_audio = Column(Boolean, nullable=True)
    enable_audio_input = Column(Boolean, nullable=True)
    enable_printing = Column(Boolean, nullable=True)
    enable_drive = Column(Boolean, nullable=True)
    create_drive_path = Column(String, nullable=True)
    enable_wallpaper = Column(Boolean, nullable=True)
    enable_theming = Column(Boolean, nullable=True)
    enable_font_smoothing = Column(Boolean, nullable=True)
    enable_full_window_drag = Column(Boolean, nullable=True)
    enable_desktop_composition = Column(Boolean, nullable=True)
    enable_menu_animations = Column(Boolean, nullable=True)
    disable_bitmap_caching = Column(Boolean, nullable=True)
    disable_offscreen_caching = Column(Boolean, nullable=True)
    disable_glyph_caching = Column(Boolean, nullable=True)
    preconnection_id = Column(String, nullable=True)
    domain = Column(String, nullable=True)
    gateway_hostname = Column(String, nullable=True)
    gateway_username = Column(String, nullable=True)
    gateway_password = Column(String, nullable=True)
    gateway_domain = Column(String, nullable=True)
    initial_program = Column(String, nullable=True)
    client_name = Column(String, nullable=True)
    printer_name = Column(String, nullable=True)
    drive_name = Column(String, nullable=True)
    drive_path = Column(String, nullable=True)
    static_channels = Column(String, nullable=True)
    remote_app = Column(String, nullable=True)
    remote_app_dir = Column(String, nullable=True)
    remote_app_args = Column(String, nullable=True)
    preconnection_blob = Column(String, nullable=True)
    load_balance_info = Column(String, nullable=True)
    ca_cert = Column(String, nullable=True)
    login_success_regex = Column(String, nullable=True)
    login_failure_regex = Column(String, nullable=True)

    # Attributes for all protocols
    attributes_max_connections = Column(Integer, nullable=True)
    attributes_max_connections_per_user = Column(Integer, nullable=True)
    attributes_weight = Column(Integer, nullable=True)
    attributes_failover_only = Column(Boolean, nullable=True)
    attributes_guacd_port = Column(Integer, nullable=True)
    attributes_guacd_encryption = Column(String, nullable=True)
    attributes_guacd_hostname = Column(String, nullable=True)

    # Custom fields
    font_name = Column(String, nullable=True)
    disable_audio = Column(Boolean, nullable=True)

    # Uncommon fields
    destination_host = Column(String, nullable=True)
    destination_port = Column(Integer, nullable=True)
    automatically_create_typescript_path = Column(Boolean, nullable=True)
    automatically_create_recording_path = Column(Boolean, nullable=True)
    exclude_graphics_streams = Column(Boolean, nullable=True)
    exclude_mouse = Column(Boolean, nullable=True)
    exclude_touch_events = Column(Boolean, nullable=True)
    include_key_events = Column(Boolean, nullable=True)

    # Other fields
    pool_id = Column(Integer, nullable=True)
    users_assigned = Column(ARRAY(String), nullable=True)
    date_created = Column(DateTime, default=datetime.datetime.utcnow)
    is_custom_machine = Column(Boolean)
    identifier = Column(String) 
    workflowId = Column(ARRAY(String), default=[])
    status = Column(String, default='RUNNING') # e.g., 'RUNNING', 'COMPLETED', etc.
    error_message = Column(Text, nullable=True, default='power-off')
    provisioning_status = Column(String, nullable=True, default='cloned')

    # workflow_status = Column(JSON, default=dict) # NEW: workflow_status field for tracking status of all workflow IDs
    workflow_status = Column(MutableDict.as_mutable(JSON), default=dict)

#pydantic model for creating machine
class CreateMachineBase(BaseModel):
    vm_id: Optional[str] = None
    os_type: Optional[str] = None
    email: Optional[str] = None
    disable_audio:Optional[bool] = None
    guacd_port:Optional[int]=None
    guacd_encryption:str
    guacd_hostname:str
    max_connections: Optional[int] = None
    max_connections_per_user: Optional[int] = None
    weight:Optional[int] = None
    failover_only:str
    name: Optional[str] = None
    protocol: Optional[str] = None
    port: Optional[int] = None
    read_only: Optional[bool] = None
    swap_red_blue: Optional[bool] = None
    cursor: Optional[str] = None
    color_depth: Optional[str] = None
    clipboard_encoding: Optional[str] = None
    disable_copy: Optional[bool] = None
    disable_paste: Optional[bool] = None
    dest_port: Optional[int] = None
    recording_exclude_output: Optional[bool] = None
    recording_exclude_mouse: Optional[bool] = None
    recording_include_keys: Optional[bool] = None
    create_recording_path: Optional[bool] = None
    enable_sftp: Optional[bool] = None
    sftp_port: Optional[int] = None
    sftp_server_alive_interval: Optional[int] = None
    enable_audio: Optional[bool] = None
    color_scheme: Optional[str] = None
    font_size: Optional[int] = None
    scrollback: Optional[int] = None
    timezone: Optional[str] = None
    server_alive_interval: Optional[int] = None
    backspace: Optional[str] = None
    terminal_type: Optional[str] = None
    create_typescript_path: Optional[bool] = None
    hostname: Optional[str] = None
    host_key: Optional[str] = None
    private_key: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    passphrase: Optional[str] = None
    font_name: Optional[str] = None
    command: Optional[str] = None
    locale: Optional[str] = None
    typescript_path: Optional[str] = None
    typescript_name: Optional[str] = None
    recording_path: Optional[str] = None
    recording_name: Optional[str] = None
    sftp_root_directory: Optional[str] = None
    sftp_directory: Optional[str] = None
    sftp_passphrase: Optional[str] = None
    sftp_private_key: Optional[str] = None
    sftp_username: Optional[str] = None
    sftp_password: Optional[str] = None
    sftp_host_key: Optional[str] = None
    sftp_hostname: Optional[str] = None
    dest_host: Optional[str] = None
    audio_servername: Optional[str] = None
    kubeconfig: Optional[str] = None
    namespace: Optional[str] = None
    pod_name: Optional[str] = None
    container_name: Optional[str] = None
    args: Optional[List[str]] = None
    tty: Optional[bool] = None
    stdin: Optional[bool] = None
    stdin_tty: Optional[bool] = None
    shell: Optional[str] = None

    # Protocol-specific fields
    security: Optional[str] = None
    disable_auth: Optional[bool] = None
    ignore_cert: Optional[bool] = None
    gateway_port: Optional[int] = None
    server_layout: Optional[str] = None
    console: Optional[bool] = None
    width: Optional[int] = None
    height: Optional[int] = None
    dpi: Optional[int] = None
    resize_method: Optional[str] = None
    console_audio: Optional[bool] = None
    enable_audio_input: Optional[bool] = None
    enable_printing: Optional[bool] = None
    enable_drive: Optional[bool] = None
    create_drive_path: Optional[str] = None
    enable_wallpaper: Optional[bool] = None
    enable_theming: Optional[bool] = None
    enable_font_smoothing: Optional[bool] = None
    enable_full_window_drag: Optional[bool] = None
    enable_desktop_composition: Optional[bool] = None
    enable_menu_animations: Optional[bool] = None
    disable_bitmap_caching: Optional[bool] = None
    disable_offscreen_caching: Optional[bool] = None
    disable_glyph_caching: Optional[bool] = None
    preconnection_id: Optional[str] = None
    domain: Optional[str] = None
    gateway_hostname: Optional[str] = None
    gateway_username: Optional[str] = None
    gateway_password: Optional[str] = None
    gateway_domain: Optional[str] = None
    initial_program: Optional[str] = None
    client_name: Optional[str] = None
    printer_name: Optional[str] = None
    drive_name: Optional[str] = None
    drive_path: Optional[str] = None
    static_channels: Optional[str] = None
    remote_app: Optional[str] = None
    remote_app_dir: Optional[str] = None
    remote_app_args: Optional[str] = None
    preconnection_blob: Optional[str] = None
    load_balance_info: Optional[str] = None
    ca_cert: Optional[str] = None
    login_success_regex: Optional[str] = None
    login_failure_regex: Optional[str] = None
    
    # Attributes for all protocols
    attributes_max_connections: Optional[int] = None
    attributes_max_connections_per_user: Optional[int] = None
    attributes_weight: Optional[int] = None
    attributes_failover_only: Optional[bool] = None
    attributes_guacd_port: Optional[int] = None
    attributes_guacd_encryption: Optional[str] = None
    attributes_guacd_hostname: Optional[str] = None
    # these three value should be 
    pool_id: int
    users_assigned: Optional[List[str]] = None
    is_custom_machine: Optional[bool] = False 
    identifier: Optional[str] = None
   # these are added new which is not in create_machine 
    exclude_graphics_streams:Optional[bool] = False
    exclude_touch_events:Optional[bool] = False
    # workflowId: Optional[List[str]] = None
    workflowId: List[str] = Field(default_factory=list)
    clone_workflow_id: Optional[Union[str, List[str]]] = None
    status: Optional[str] = "RUNNING"
    error_message: Optional[str] = "power-off"
    provisioning_status: Optional[str] = "cloned"
    workflow_status: Optional[Dict[str, Dict[str, Optional[str]]]] = Field(default_factory=dict)
#pydantic model to update machine
class UpdateMachineBase(BaseModel):
    vm_id: Optional[str] = None
    email: Optional[str] = None
    os_type: Optional[str] = None
    disable_audio:Optional[bool] = None
    guacd_port:Optional[int] = None
    guacd_encryption:str
    guacd_hostname:str
    pool_dynamic_memory: Optional[bool] = None
    pool_minimum_memory: Optional[int] = None
    pool_maximum_memory: Optional[int] = None
    pool_buffer_memory: Optional[int] = None
    pool_processor_count: Optional[int] = None
    max_connections: Optional[int] = None
    max_connections_per_user: Optional[int] = None
    weight:Optional[int] = None
    failover_only:str
    name: Optional[str] = None
    protocol: Optional[str] = None
    port: Optional[int] = None
    read_only: Optional[bool] = None
    swap_red_blue: Optional[bool] = None
    cursor: Optional[str] = None
    color_depth: Optional[str] = None
    clipboard_encoding: Optional[str] = None
    disable_copy: Optional[bool] = None
    disable_paste: Optional[bool] = None
    dest_port: Optional[int] = None
    recording_exclude_output: Optional[bool] = None
    recording_exclude_mouse: Optional[bool] = None
    recording_include_keys: Optional[bool] = None
    create_recording_path: Optional[bool] = None
    enable_sftp: Optional[bool] = None
    sftp_port: Optional[int] = None
    sftp_server_alive_interval: Optional[int] = None
    enable_audio: Optional[bool] = None
    color_scheme: Optional[str] = None
    font_size: Optional[int] = None
    scrollback: Optional[int] = None
    timezone: Optional[str] = None
    server_alive_interval: Optional[int] = None
    backspace: Optional[str] = None
    terminal_type: Optional[str] = None
    create_typescript_path: Optional[bool] = None
    hostname: Optional[str] = None
    host_key: Optional[str] = None
    private_key: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    passphrase: Optional[str] = None
    font_name: Optional[str] = None
    command: Optional[str] = None
    locale: Optional[str] = None
    typescript_path: Optional[str] = None
    typescript_name: Optional[str] = None
    recording_path: Optional[str] = None
    recording_name: Optional[str] = None
    sftp_root_directory: Optional[str] = None
    sftp_directory: Optional[str] = None
    sftp_passphrase: Optional[str] = None
    sftp_private_key: Optional[str] = None
    sftp_username: Optional[str] = None
    sftp_password: Optional[str] = None
    sftp_host_key: Optional[str] = None
    sftp_hostname: Optional[str] = None
    dest_host: Optional[str] = None
    audio_servername: Optional[str] = None
    kubeconfig: Optional[str] = None
    namespace: Optional[str] = None
    pod_name: Optional[str] = None
    container_name: Optional[str] = None
    args: Optional[List[str]] = None
    tty: Optional[bool] = None
    stdin: Optional[bool] = None
    stdin_tty: Optional[bool] = None
    shell: Optional[str] = None

    # Protocol-specific fields
    security: Optional[str] = None
    disable_auth: Optional[bool] = None
    ignore_cert: Optional[bool] = None
    gateway_port: Optional[int] = None
    server_layout: Optional[str] = None
    console: Optional[bool] = None
    width: Optional[int] = None
    height: Optional[int] = None
    dpi: Optional[int] = None
    resize_method: Optional[str] = None
    console_audio: Optional[bool] = None
    enable_audio_input: Optional[bool] = None
    enable_printing: Optional[bool] = None
    enable_drive: Optional[bool] = None
    create_drive_path: Optional[str] = None
    enable_wallpaper: Optional[bool] = None
    enable_theming: Optional[bool] = None
    enable_font_smoothing: Optional[bool] = None
    enable_full_window_drag: Optional[bool] = None
    enable_desktop_composition: Optional[bool] = None
    enable_menu_animations: Optional[bool] = None
    disable_bitmap_caching: Optional[bool] = None
    disable_offscreen_caching: Optional[bool] = None
    disable_glyph_caching: Optional[bool] = None
    preconnection_id: Optional[str] = None
    domain: Optional[str] = None
    gateway_hostname: Optional[str] = None
    gateway_username: Optional[str] = None
    gateway_password: Optional[str] = None
    gateway_domain: Optional[str] = None
    initial_program: Optional[str] = None
    client_name: Optional[str] = None
    printer_name: Optional[str] = None
    drive_name: Optional[str] = None
    drive_path: Optional[str] = None
    static_channels: Optional[str] = None
    remote_app: Optional[str] = None
    remote_app_dir: Optional[str] = None
    remote_app_args: Optional[str] = None
    preconnection_blob: Optional[str] = None
    load_balance_info: Optional[str] = None
    ca_cert: Optional[str] = None
    login_success_regex: Optional[str] = None
    login_failure_regex: Optional[str] = None

    # Attributes for all protocols
    attributes_max_connections: Optional[int] = None
    attributes_max_connections_per_user: Optional[int] = None
    attributes_weight: Optional[int] = None
    attributes_failover_only: Optional[bool] = None
    attributes_guacd_port: Optional[int] = None
    attributes_guacd_encryption: Optional[str] = None
    attributes_guacd_hostname: Optional[str] = None
    # some thing
    pool_id: Optional[int] = None
    users_assigned: Optional[List[str]] = None
    is_custom_machine: Optional[bool] = False 
    identifier: Optional[str] = None
    # these are updated new fields for machine
    exclude_graphics_streams:Optional[bool] = False
    exclude_touch_events:Optional[bool] = False
    workflowId: List[str] = Field(default_factory=list)
    clone_workflow_id: Optional[Union[str, List[str]]] = None
    status: Optional[str]
    error_message: Optional[str] = None
    provisioning_status: Optional[str] = None
    workflow_status: Optional[Dict[str, Dict[str, Optional[str]]]] = Field(default_factory=dict)

    # date_created: Optional[str] = None

# #declarative base model
# class User(Base):
#     __tablename__ = 'users'

#     id = Column(String, primary_key=True)
#     username = Column(String)

# #pydantic model
# class UserBase(BaseModel):
#     # id: str = str(uuid.uuid4())
#     username: str
#     role: Optional[str]=None

#base model for cluster
class Cluster(Base):
    __tablename__='clusters'

    id = Column(Integer, Sequence('cluster_id_seq'), primary_key=True)
    type = Column(String)
    name=Column(String)
    ip = Column(String)
    port = Column(Integer)
    agent_port = Column(Integer)
    username = Column(String)
    password = Column (String)
    tls = Column(Boolean)
    node_type = Column(String)
    
# pydantic model to create cluster
class CreateClusterBase(BaseModel):
    type : str
    name : str
    ip : List[str]
    port : Optional[Union[int, str]] = None
    agent_port : Optional[Union[int, str]] = None
    username : str
    password : str
    tls : bool
    email: Optional[str] = None
    node_type: Optional[str] = None

    @validator('port', 'agent_port', pre=True)
    def clean_empty_string(cls, v):
        if v == "":
            return None
        return v


# pydantic model to update cluster
class UpdateClusterBase(BaseModel):
    type :Optional[str]
    name : Optional[str]
    ip : Optional[List[str]]
    port : Optional[Union[int, str]] = None
    agent_port : Optional[Union[int, str]] = None
    username : Optional[str]
    password : Optional[str]
    tls : Optional[bool]
    email: Optional[str] = None
    node_type: Optional[str] = None

    @validator('port', 'agent_port', pre=True)
    def clean_empty_string(cls, v):
        if v == "":
            return None
        return v


# here LDAP pydeantic model class
class LDAPCredential(BaseModel):
    enabled: Optional[bool] = True
    vendor: Optional[str] = 'ad'
    connectionUrl: Optional[str] = ''
    connectionTimeout: Optional[str] = ''
    bindDn: Optional[str] = ''
    bindCredential: Optional[str] = ''
    startTls: Optional[bool] = False
    useTruststoreSpi: Optional[str] = ''
    connectionPooling: Optional[bool] = False
    authType: Optional[str] = ''
    usersDn: Optional[str] = ''
    usernameLDAPAttribute: Optional[str] = ''
    rdnLDAPAttribute: Optional[str] = ''
    uuidLDAPAttribute: Optional[str] = ''
    userObjectClasses: Optional[str] = ''
    customUserSearchFilter: Optional[str] = ''
    readTimeout: Optional[str] = ''
    editMode: Optional[str] = ''
    referral: Optional[str] = ''
    searchScope: Optional[str] = ''
    pagination: Optional[bool] = False
    batchSizeForSync: Optional[str] = ''
    importEnabled: Optional[bool] = True
    syncRegistrations: Optional[bool] = True
    allowKerberosAuthentication: Optional[bool] = False
    useKerberosForPasswordAuthentication: Optional[bool] = False
    cachePolicy: Optional[str] = 'DEFAULT'
    usePasswordModifyExtendedOp: Optional[bool] = False
    validatePasswordPolicy: Optional[bool] = False
    trustEmail: Optional[bool] = False
    fullSyncPeriod: Optional[int] = ''
    changedSyncPeriod: Optional[int] = ''
    providerId: Optional[str] = 'ldap'
    providerType: Optional[str] = 'org.keycloak.storage.UserStorageProvider'
    name: Optional[str] = 'ldap'  
    debug:Optional[bool]=False
    kerberosRealm:Optional[str]=''
    keyTab:Optional[str] = ''
    lastSync:Optional[str] = ''
    serverPrincipal:Optional[str]=''
    krbPrincipalAttribute:Optional[str]=''

class LDAP_test_connection_model(BaseModel):
    authType: Optional[str] = ''
    bindCredential: Optional[str] = ''
    bindDn: Optional[str] = ''
    componentId:Optional[str]=''
    connectionTimeout: Optional[str] = ''
    connectionUrl: Optional[str] = ''
    startTls: Optional[bool] = False
    useTruststoreSpi: Optional[str] = ''

class IsCustomeValue(BaseModel):
       is_custom_machine :bool

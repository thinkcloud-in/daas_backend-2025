from pydantic import BaseModel # type: ignore
from typing import Optional, List

class MachineDto(BaseModel):
    # ip: str
    disable_audio:Optional[bool] = None
    guacd_port:int
    guacd_encryption:str
    guacd_hostname:str
    max_connections: Optional[int] = None
    max_connections_per_user: Optional[int] = None
    weight:str
    failover_only:str
    name: Optional[str] = None
    protocol: Optional[str] = None
    port: Optional[int] = None
    read_only: Optional[bool] = None
    swap_red_blue: Optional[bool] = None
    cursor: Optional[bool] = None
    color_depth: Optional[str] = None
    clipboard_encoding: Optional[str] = None
    disable_copy: Optional[bool] = None
    disable_paste: Optional[bool] = None
    dest_port: Optional[int] = None
    recording_exclude_output: Optional[bool] = None
    recording_exclude_mouse: Optional[bool] = None
    recording_include_keys: Optional[bool] = None
    create_recording_path: Optional[str] = None
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
    create_typescript_path: Optional[str] = None
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
 
# class MachineDto(BaseModel):
#     name: str
#     description: str | None = None
#     price: float
#     tax: float | None = None
#     tags: list = [] 
from datetime import datetime
import uuid
from .proxmox_context import *

def generate_username():
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
    return f"devraqUser{timestamp}@pve"

def generate_password():
    return f"Teamw0rk@{uuid.uuid4().hex[:4]}"

def generate_token():
    return "proxmoxtoken" + uuid.uuid4().hex[:8]

credentials = None
def init_proxmox_context():
    global credentials
    if credentials is None:
        token_user = PROXMOX_NEW_USER_ID.set(generate_username())
        token_pass = PROXMOX_NEW_PASSWORD.set(generate_password())
        token_token = PROXMOX_NEW_TOKEN_ID.set(generate_token())
    
        credentials = {
            "username": PROXMOX_NEW_USER_ID.get(),
            "password": PROXMOX_NEW_PASSWORD.get(),
            "token": PROXMOX_NEW_TOKEN_ID.get(),
            "token_user": token_user,
            "token_pass": token_pass,
            "token_token": token_token
        }
    
    return credentials

def cleanup_proxmox_context():
    global credentials
    if credentials:
        PROXMOX_NEW_USER_ID.reset(credentials['token_user'])
        PROXMOX_NEW_PASSWORD.reset(credentials['token_pass'])
        PROXMOX_NEW_TOKEN_ID.reset(credentials['token_token'])
        credentials = None
from datetime import datetime
import json
import random
from typing import Dict, List, Optional
from fastapi import logger
import requests
from sqlalchemy import and_, create_engine
from sqlalchemy.orm import sessionmaker
import urllib3
from db_configuration.config import SessionLocal, get_db
from models.proxmox_model import Proxmox
from sqlalchemy.orm import Session
from models.models import Cluster, CreateClusterBase
from service.gucamoleService import connectionWithClient
# from controllers import store_proxmox_user,get_all_proxmox_users,clone_vm_from_template
# Disable SSL warning (not recommended for production)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import os
import logging
import asyncio
from fastapi import HTTPException
from service.temporalResource.workers import workers_cluster
from service.temporalResource.workflows import workflows_cluster
from models.proxmox_model import Proxmox, MetricServer

 
# NEW_USER_ID = "testuser@pve"
# NEW_TOKEN_ID = "testtoken"
# NEW_PASSWORD = "Teamw0rk@1"

NEW_USER_ID = os.getenv("PROXMOX_NEW_USER_ID")
NEW_TOKEN_ID = os.getenv("PROXMOX_NEW_TOKEN_ID")
NEW_PASSWORD = os.getenv("PROXMOX_NEW_PASSWORD")
VERIFY_SSL = False


def unique_id():
    unique_id = datetime.now()
    logger.info(f"Generated unique ID - {unique_id}")
    return f"{unique_id.hour }:{unique_id.minute}:{unique_id.second}"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("create_machine_activity")
 
def root_proxmox_login(PROXMOX_HOST,ROOT_USERNAME,ROOT_PASSWORD):
    url = f"{PROXMOX_HOST}/api2/json/access/ticket"
    payload = {"username": ROOT_USERNAME, "password": ROOT_PASSWORD}
    response = requests.post(url, data=payload, verify=VERIFY_SSL)
    response.raise_for_status()
    data = response.json()["data"]
    headers = {"CSRFPreventionToken": data["CSRFPreventionToken"]}
    cookies = {"PVEAuthCookie": data["ticket"]}
    return headers, cookies
 
# Step 2: Create a new user

_worker_started = False  # Global flag to track if worker has started
async def create_user(cluster_data: dict, root_username: str, root_password: str):
    uniqueId = unique_id()
    client = await connectionWithClient()
    userName = cluster_data.get('email', "UnknownUser")

    # try:
    #     global _worker_started
    #     if not _worker_started:
    #         asyncio.create_task(workers_cluster.create_user_worker())
    #     _worker_started = True
    #     logger.info(f"Started worker task for creating user with unique ID {uniqueId}.")
    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=str(e))

    handle = await client.start_workflow(
        workflows_cluster.CreateUserWorkflow.run,
        args=[cluster_data, root_username, root_password],  # Pass the full cluster_data dict
        id=f'create-user-{uniqueId}',
        task_queue="cluster-task-queue",
        search_attributes={
            "Entity": [NEW_USER_ID],
            "Action": ["User-Creation"],
            "UserName": [userName]
        },
    )
    print("Workflow started! Workflow ID:", handle.id)

    # Get result and validate
    result = await handle.result()
    print("Workflow result:", result)

    if result.get('status') != 'success':
        raise Exception("User creation failed in workflow.")

    return result



async def assign_role_to_user(cluster_data: dict, role: str, path: str, root_username: str, root_password: str):
    uniqueId = unique_id()
    client = await connectionWithClient()
    userName = cluster_data.get('email', "UnknownUser")
    # Role = cluster_data.get('role')

    # try:
    #     global _worker_started_assign_role
    #     if not _worker_started_assign_role:
    #         asyncio.create_task(workers_cluster.assign_role_to_user_worker())
    #         _worker_started_assign_role = True
    #     logger.info(f"Started worker task for assigning role with unique ID {uniqueId}.")
    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=str(e))

    handle = await client.start_workflow(
        workflows_cluster.AssignRoleToUserWorkflow.run,
        args=[cluster_data, role, path, root_username, root_password],
        id=f'assign-role-{uniqueId}',
        task_queue="cluster-task-queue",
        search_attributes={
            "Entity": [f"{NEW_USER_ID}-{role}"],
            "Action": ["Role-Assignment"],
            "UserName": [userName]
        },
    )
    print("Workflow started! Workflow ID:", handle.id)
    result = await handle.result()
    print("Workflow result:", result)
    return result


# def create_user(PROXMOX_HOST,ROOT_USERNAME,ROOT_PASSWORD):
#     headers, cookies = root_proxmox_login(PROXMOX_HOST,ROOT_USERNAME,ROOT_PASSWORD)
#     url = f"{PROXMOX_HOST}/api2/json/access/users"
#     payload = {
#         "userid": NEW_USER_ID,
#         "password": NEW_PASSWORD
#     }
#     response = requests.post(url, headers=headers, cookies=cookies, data=payload, verify=VERIFY_SSL)
#     if response.status_code == 200:
#         print(f"User '{NEW_USER_ID}' created.")
#     elif response.status_code == 400 and "already exists" in response.text:
#         print(f"ℹUser '{NEW_USER_ID}' already exists.")
#     else:
#         print("Failed to create user:", response.text)
#     return response.json()
 
def new_user_proxmox_login(PROXMOX_HOST):
    url = f"{PROXMOX_HOST}/api2/json/access/ticket"
    payload = {"username": NEW_USER_ID, "password": NEW_PASSWORD}
    response = requests.post(url, data=payload, verify=VERIFY_SSL)
    response.raise_for_status()
    data = response.json()["data"]
    headers = {"CSRFPreventionToken": data["CSRFPreventionToken"]}
    cookies = {"PVEAuthCookie": data["ticket"]}
    return headers, cookies
 
# Step 5: Create API token as the new user
def create_api_token_newUser(PROXMOX_HOST):
    headers, cookies = new_user_proxmox_login(PROXMOX_HOST)
    url = f"{PROXMOX_HOST}/api2/json/access/users/{NEW_USER_ID}/token/{NEW_TOKEN_ID}"
    response = requests.post(url, headers=headers, cookies=cookies, verify=VERIFY_SSL)
    response.raise_for_status()
    data = response.json()["data"]
    full_token = data["full-tokenid"]
    secret = data["value"]
    api_token = f"{full_token}={secret}"
    print("API Token created with create_api_token_newUser------:", api_token)
    return api_token,full_token,secret
 
# Step 3: Assign role to new user (must be done as root)
# def assign_role_to_user(PROXMOX_HOST,role, path,ROOT_USERNAME,ROOT_PASSWORD):
#     headers, cookies = root_proxmox_login(PROXMOX_HOST,ROOT_USERNAME,ROOT_PASSWORD)
#     # create_api_token_newUser()
#     url = f"{PROXMOX_HOST}/api2/json/access/acl"
#     payload = {
#         "path": path,
#         "roles": role,
#         "users": NEW_USER_ID,
#         "tokens":f"{NEW_USER_ID}!{NEW_TOKEN_ID}",
#         "propagate": 1
#     }
#     response = requests.put(url, headers=headers, cookies=cookies, data=payload, verify=VERIFY_SSL)
#     response.raise_for_status()
#     print(f"Role '{role}' assigned to user '{NEW_USER_ID}' on path '{path}'.")
 
# Step 4: Login as the new user
 
 
 
def get_all_proxmox_users(db: Session):
    data=db.query(Proxmox).all()
    # for obj in data:
    #     print(obj.api_token)
    return data
def get_api_token(db, cluster_name):
    obj = db.query(Proxmox).filter(Proxmox.cluster_name == cluster_name).first()
    if obj:
        # print(obj.api_token)
        return obj.api_token
    else:
        print("No API token found for cluster:", cluster_name)
        return None
 
 
 
 
 
def store_proxmox_user(db: Session, role, path, api_token, full_token, secret, cluster_name):
    # Check if a record already exists with the same cluster_name
    existing_user = db.query(Proxmox).filter(
        Proxmox.cluster_name == cluster_name
    ).first()
 
    if existing_user:
        # Update existing fields
        existing_user.new_password = ""
        existing_user.token_id = NEW_TOKEN_ID
        existing_user.full_token = full_token
        existing_user.secret_key = secret
        existing_user.api_token = api_token
        existing_user.role = role
        existing_user.path = path
 
        db.commit()
        db.refresh(existing_user)
        return existing_user
    else:
        # Create new record
        proxmox_user = Proxmox(
            user=NEW_USER_ID,
            cluster_name=cluster_name,
            new_password="",
            token_id=NEW_TOKEN_ID,
            full_token=full_token,
            secret_key=secret,
            api_token=api_token,
            role=role,
            path=path
        )
        db.add(proxmox_user)
        db.commit()
        db.refresh(proxmox_user)
        return proxmox_user
async def create_cluster_proxmox(cluster_data):
    db = next(get_db())
    PROXMOX_HOST = f"https://{cluster_data.ip[0]}:{cluster_data.port}"  # Use the first IP
    ROOT_USERNAME = cluster_data.username
    ROOT_PASSWORD = cluster_data.password
    cluster_data_dict = cluster_data.dict()
    # FIX: Pass the full cluster_data dict, not just the host string!
    await create_user(cluster_data_dict, ROOT_USERNAME, ROOT_PASSWORD)
    api_token, full_token, secret = create_api_token_newUser(PROXMOX_HOST)
    role = "Administrator"
    path = "/"
    await assign_role_to_user(cluster_data_dict, role, path, ROOT_USERNAME, ROOT_PASSWORD)
    store_proxmox_user(db, role, path, api_token, full_token, secret, cluster_data.name)
 
def getting_Proxmox_host(cluster_data, timeout: float = 3.0) -> str:
    # Support both dict and Pydantic object
    if isinstance(cluster_data, dict):
        ip_field = cluster_data.get('ip')
        port = cluster_data.get('port')
    else:
        ip_field = cluster_data.ip
        port = cluster_data.port

    if isinstance(ip_field, str):
        ip_list = [ip.strip() for ip in ip_field.split(",") if ip.strip()]
    elif isinstance(ip_field, list):
        ip_list = [ip.strip() for ip in ip_field if isinstance(ip, str) and ip.strip()]
    else:
        raise ValueError("Cluster IPs must be provided as a comma-separated string or list.")

    if not ip_list:
        raise ValueError("No valid IPs found in provided cluster IPs.")

    for ip in ip_list:
        url = f"https://{ip}:{port}"
        try:
            requests.get(url, verify=False, timeout=timeout)
            return url
        except Exception:
            continue

    raise ValueError("No reachable Proxmox host found in provided IPs.")



def get_all_nodes(cluster_data):
    db = next(get_db())
    api_token = get_api_token(db, cluster_data.name)
    headers = {
        "Authorization": f"PVEAPIToken={api_token}",
        "Content-Type": "application/json"
    }
    # Try each IP in cluster_data.ip until one works
    last_exception = None
    for ip in cluster_data.ip:
        PROXMOX_HOST = getting_Proxmox_host(cluster_data)
        url = f"{PROXMOX_HOST}/api2/json/cluster/status"
        try:
            response = requests.get(url, headers=headers, verify=VERIFY_SSL, timeout=5)
            response.raise_for_status()
            data = response.json()
            # print(f"Cluster status data from {ip}:", data)
 
            # Filter: Only ONLINE nodes
            nodes = [
                {
                    "name": node["name"],
                    "ip": node["ip"],
                    "status": "online"
                }
                for node in data["data"]
                if node.get("type") == "node" and node.get("online", 0) == 1
            ]
 
            # print("Filtered ONLINE nodes in the cluster:", nodes)
            return nodes
        except Exception as e:
            print(f"Failed to connect to {ip}: {e}")
            last_exception = e
            continue
 
    # If none of the IPs worked, raise the last exception
    raise RuntimeError(f"All cluster IPs failed. Last error: {last_exception}")
 
def delete_cluster_proxmox(cluster_data):
    db = next(get_db())
    ip_list = [ip.strip() for ip in cluster_data.ip.split(",") if ip.strip()]
    any_ip = random.choice(ip_list) if ip_list else None
    if not any_ip:
        raise Exception("No valid IP found for Proxmox cluster.")
 
    PROXMOX_HOST = f"https://{any_ip}:{cluster_data.port}"
    api_token = get_api_token(db, cluster_data.name)
    headers = {
        "Authorization": f"PVEAPIToken={api_token}",
        "Content-Type": "application/json"
    }
    url = f"{PROXMOX_HOST}/api2/json/access/users/{NEW_USER_ID}"
 
    try:
        response = requests.delete(url, headers=headers, verify=VERIFY_SSL)
        # Accept 401/404 errors or "no such user" in the error text
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            # If the error is 401 or 404, or the body contains "no such user", ignore it
            if response.status_code in (401, 404) or "no such user" in response.text:
                pass  # Ignore and proceed to DB deletion
            else:
                raise  # Re-raise for any other error
    except Exception as e:
        # Other unexpected exceptions (network, etc)
        print(f"Ignoring exception during user deletion: {e}")
 
    # Now delete the cluster from DB
    proxmox_cluster = db.query(Proxmox).filter(Proxmox.cluster_name == cluster_data.name).first()
    if proxmox_cluster:
        db.delete(proxmox_cluster)
        db.commit()
        return f"Cluster '{cluster_data.name}' deleted successfully."
    else:
        print("Cluster not found in the database.")
        return f"Cluster '{cluster_data.name}' not found in the database."
 
 
 

# Example: fetch from env
INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")
import requests
 
def add_influxdb_metric_server(cluster_data, payload):
    api_token = get_api_token(next(get_db()), cluster_data.name)
    headers = {
        "Authorization": f"PVEAPIToken={api_token}",
        "Content-Type": "application/json"
    }
    PROXMOX_HOST = getting_Proxmox_host(cluster_data)
    url = f"{PROXMOX_HOST}/api2/json/cluster/metrics/server/{cluster_data.name}"
    payload = {k: v for k, v in payload.items() if v}
    response = requests.post(url, headers=headers, data=json.dumps(payload), verify=False)
    response.raise_for_status()
    return response.json()
 
def get_influxdb_metric_server(cluster_data):
    """
    Get InfluxDB metric server for the given cluster.
    """
    db = next(get_db())
    api_token = get_api_token(db, cluster_data.name)
    headers = {
        "Authorization": f"PVEAPIToken={api_token}",
        "Content-Type": "application/json"
    }
    print("API Token for get_influxdb_metric_server:", api_token)
    print("headers",headers)
    PROXMOX_HOST = getting_Proxmox_host(cluster_data)
    url = f"{PROXMOX_HOST}/api2/json/cluster/metrics/server"
    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        data = response.json().get("data", [])
    except Exception as e:
        return {"error": "Failed to fetch metric servers from Proxmox API."}
 
    if isinstance(data, list) and data:
        for server in data:
            if server.get("type") == "influxdb":
                server_id = server.get("id")
                if server_id:
                    detail_url = f"{url}/{server_id}"
                    try:
                        detail_resp = requests.get(detail_url, headers=headers, verify=False)
                        detail_resp.raise_for_status()
                        return detail_resp.json().get("data", {})
                    except Exception as e:
                        return {"error": f"Failed to fetch details for metric server ID {server_id}."}
        return {"error": "No InfluxDB metric server ID found in the cluster."}
    else:
        return {"error": "No InfluxDB metric server found for the cluster."}
 
def create_and_get_metric_server(cluster_data):
    """
    Checks if a metric server exists. If not, creates a new InfluxDB bucket and registers the metric server.
    """    
    influxdb_payload = {
        "type": "influxdb",
        "id": cluster_data.name,
        "server": INFLUXDB_URL.split("://")[1].split(":")[0],
        "port": int(INFLUXDB_URL.split(":")[-1]),
        "influxdbproto": INFLUXDB_URL.split("://")[0],
        "organization": INFLUXDB_ORG,
        "bucket": INFLUXDB_BUCKET,
        "token": INFLUXDB_TOKEN
    }
    add_influxdb_metric_server(cluster_data, influxdb_payload)
    metric_info = get_influxdb_metric_server(cluster_data)
    return metric_info
 
def save_metric_server_to_db(
    db: Session,
    cluster_id: int,
    metric_data: dict,
    monitoring: bool,
    is_custom_integration: Optional[bool] = None
):
    ms = db.query(MetricServer).filter(MetricServer.cluster_id == cluster_id).first()
    if not ms:
        # Always set is_custom_integration on new row, default to False if not provided
        ms = MetricServer(
            cluster_id=cluster_id,
            is_custom_integration=is_custom_integration if is_custom_integration is not None else False
        )
        db.add(ms)
    ms.type = metric_data.get("type", "influxdb")
    ms.server = metric_data.get("server", "")
    ms.port = metric_data.get("port", 8086)
    ms.proto = metric_data.get("influxdbproto") or metric_data.get("proto", "http")
    ms.organization = metric_data.get("organization", "")
    ms.bucket = metric_data.get("bucket", "")
    ms.token = metric_data.get("token", "")
    ms.disabled = metric_data.get("disabled", False)
    ms.monitoring = monitoring
    # Only update if provided (prevents overwriting on GET)
    if is_custom_integration is not None:
        ms.is_custom_integration = is_custom_integration
    db.commit()
    db.refresh(ms)
    return ms
 

def delete_influxdb_metric_server(cluster_data):
    """
    Deletes the InfluxDB metric server for the given cluster in Proxmox.
    """
    db = next(get_db())
    api_token = get_api_token(db, cluster_data.name)
    headers = {
        "Authorization": f"PVEAPIToken={api_token}",
        "Content-Type": "application/json"
    }

    PROXMOX_HOST = getting_Proxmox_host(cluster_data)
    url = f"{PROXMOX_HOST}/api2/json/cluster/metrics/server/{cluster_data.name}"
    try:
        response = requests.delete(url, headers=headers, verify=False)
        response.raise_for_status()
        return {"status": "success"}
    except Exception as e:
        print(f"Error deleting metric server: {e}")
        return {"error": "Failed to delete metric server from Proxmox API."}

def can_delete_metric_server(db, cluster_id):
    """
    Checks if the metric server can be deleted based on monitoring and is_custom_integration fields.
    Returns (allowed: bool, message: str)
    """
    ms = db.query(MetricServer).filter(MetricServer.cluster_id == cluster_id).first()
    if not ms:
        return False, "Metric server integration not found."
    if not (ms.monitoring and ms.is_custom_integration):
        return False, "You cannot delete this integration because it is not a custom DB integration managed by you."
    return True, ""

def get_metric_server_from_db(cluster_id: int) -> Optional[MetricServer]:
    """Fetch the MetricServer record from the database."""
    db: Optional[Session] = None
    try:
        db = SessionLocal()
        ms = db.query(MetricServer).filter(MetricServer.cluster_id == cluster_id).first()
        return ms
    except Exception as e:
        print(f"Error fetching MetricServer for cluster_id={cluster_id}: {e}")
        return None
    finally:
        if db:
            db.close()
 
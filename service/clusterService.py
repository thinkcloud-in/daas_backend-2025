"""
clusterService — service layer for Proxmox cluster onboarding/offboarding and the
InfluxDB metric-server integration.

Two big flows:
1. Cluster CRUD: `create_cluster_proxmox()` uses root credentials to provision a
   dedicated, cluster-specific user + API token on Proxmox (via the Temporal workflows
   `CreateUserWorkflow`/`AssignRoleToUserWorkflow`) — the root credentials themselves are
   never stored, they are used once only for provisioning; ongoing calls use the dedicated
   token. `delete_cluster_proxmox()` is the reverse flow (deleting the dedicated user with root).
2. InfluxDB metric server: `create_and_get_metric_server()`/`delete_influxdb_metric_server()`
   register/unregister DevRaQ's own InfluxDB metrics-export target on the Proxmox cluster
   (independent of any the customer has configured themselves, under a distinct `{name}-devraq` id).

Used by: the cluster and metric-server endpoints in controllers/routes.py.
"""
from datetime import datetime
import json
import random
from typing import  Optional
from urllib.parse import urlparse
from fastapi import logger
import requests
import urllib3
from db_configuration.config import SessionLocal, get_db
from models.proxmox_model import Proxmox
from sqlalchemy.orm import Session
from utils.temporal_client import TemporalClientManager
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import os
import logging
from service.temporalResource.workflows import workflows_cluster
from models.proxmox_model import Proxmox, MetricServer
from utils.proxmox_helper import init_proxmox_context

VERIFY_SSL = False

INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")
INFLUXDB_PORT = os.getenv("INFLUXDB_PORT")


def unique_id():
    """Build an `HH:MM:SS` string from the current time, for use as a workflow-id suffix (for uniqueness)."""
    unique_id = datetime.now()
    logger.info(f"Generated unique ID - {unique_id}")
    return f"{unique_id.hour }:{unique_id.minute}:{unique_id.second}"

# Logging is configured centrally in utils/logging_config.py (called from
# main.py at startup) — do not reconfigure it per-module.
logger = logging.getLogger("clusterService")

def root_proxmox_login(PROXMOX_HOST,ROOT_USERNAME,ROOT_PASSWORD):
    """
    Ticket-auth login with the Proxmox root (or any given) credential.
    Returns: (headers: {"CSRFPreventionToken"}, cookies: {"PVEAuthCookie"}).
    Raises: requests.HTTPError if the login fails.
    """
    url = f"{PROXMOX_HOST}/api2/json/access/ticket"
    payload = {"username": ROOT_USERNAME, "password": ROOT_PASSWORD}
    
    response = requests.post(url, data=payload, verify=VERIFY_SSL)
    
    response.raise_for_status()
    data = response.json()["data"]
    headers = {"CSRFPreventionToken": data["CSRFPreventionToken"]}
    cookies = {"PVEAuthCookie": data["ticket"]}
    return headers, cookies

# Step 2: Create a new user


async def create_user(cluster_data: dict, root_username: str, root_password: str, cred: dict = None):
    """
    Start `CreateUserWorkflow` (with root credentials) so that a new dedicated user
    (`cred['username']`) is created on Proxmox, and await its result.

    Params: cred (optional) — {"username","password","token"}; if not given, it is freshly
            generated via `init_proxmox_context()`.
    Returns: workflow result dict (must have "status": "success").
    Raises: Exception if the workflow result status is not "success".
    """
    uniqueId = unique_id()
    cred = cred or init_proxmox_context()
    client = await TemporalClientManager.get_temporal_client()
    userName = cluster_data.get('email') or "UnknownUser"
    handle = await client.start_workflow(
        workflows_cluster.CreateUserWorkflow.run,
        args=[cluster_data, root_username, root_password, cred],
        id=f'create-user-{uniqueId}',
        task_queue="cluster-task-queue",
        search_attributes={
            "Entity": [cred['username']],
            "Action": ["User-Creation"],
            "UserName": [userName]
        },
    ) # type: ignore
    result = await handle.result()
    
    if result.get('status') != 'success':
        raise Exception("User creation failed in workflow.")
    
    return result



async def assign_role_to_user(cluster_data: dict, role: str, path: str, root_username: str, root_password: str, cred: dict = None):
    """
    Start `AssignRoleToUserWorkflow` so that the given `role` (e.g. "Administrator") is
    assigned to the newly-created dedicated user on Proxmox at that `path` (e.g. "/").
    Returns: the workflow's result.
    """
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    userName = cluster_data.get('email') or "UnknownUser"
    cred = cred or init_proxmox_context()
    handle = await client.start_workflow(
        workflows_cluster.AssignRoleToUserWorkflow.run,
        args=[cluster_data, role, path, root_username, root_password, cred],
        id=f'assign-role-{uniqueId}',
        task_queue="cluster-task-queue",
        search_attributes={
            "Entity": [f"{cred['username']}-{role}"],
            "Action": ["Role-Assignment"],
            "UserName": [userName]
        },
    )
    
    result = await handle.result()
    return result
def new_user_proxmox_login(PROXMOX_HOST, cred: dict = None):
    """
    Ticket-auth login with the dedicated (root-provisioned, non-root) user.
    Returns: (headers, cookies) — same shape as `root_proxmox_login`.
    """
    cred = cred or init_proxmox_context()
    url = f"{PROXMOX_HOST}/api2/json/access/ticket"
    payload = {"username": cred['username'], "password": cred['password']}

    response = requests.post(url, data=payload, verify=VERIFY_SSL)

    response.raise_for_status()
    data = response.json()["data"]
    headers = {"CSRFPreventionToken": data["CSRFPreventionToken"]}
    cookies = {"PVEAuthCookie": data["ticket"]}
    return headers, cookies

# Step 5: Create API token as the new user
def create_api_token_newUser(PROXMOX_HOST, cred: dict = None):
    """
    Generate a new Proxmox API token for the dedicated user (as that user, not root).
    Returns: (api_token: "user@realm!tokenid=secret" combined string, full_token, secret).
    """
    cred = cred or init_proxmox_context()
    headers, cookies = new_user_proxmox_login(PROXMOX_HOST, cred)
    payload = {
        "privsep": 0,
        "comment": "automation token"
    }
    url = f"{PROXMOX_HOST}/api2/json/access/users/{cred['username']}/token/{cred['token']}"
    response = requests.post(url, headers=headers, cookies=cookies, data=payload, verify=VERIFY_SSL)
    response.raise_for_status()
    data = response.json()["data"]
    full_token = data["full-tokenid"]
    secret = data["value"]
    api_token = f"{full_token}={secret}"

    return api_token,full_token,secret

 #--------------------------------------------helper functions--------------------------------------------#
def model_to_dict(obj):
    """Convert a SQLAlchemy ORM object into a plain dict (all table columns)."""
    return {c.name: getattr(obj, c.name) for c in obj.__table__.columns}

#--------------------------------------------helper functions--------------------------------------------#

def get_all_proxmox_users(db: Session):
    """Return all Proxmox (dedicated-user-per-cluster) DB records."""
    data=db.query(Proxmox).all()
    return data
def get_api_token(db: Session, cluster_name: str):
    """Fetch the stored Proxmox API token for a cluster from the DB. Returns "" if not found."""
    obj = db.query(Proxmox).filter(Proxmox.cluster_name == cluster_name).first()
    data = model_to_dict(obj) if obj else {}
    
    return data.get("api_token", "")
 
def store_proxmox_user(db: Session, role, path, api_token, full_token, secret, cluster_name, cred: dict = None):
    """
    Upsert the cluster's dedicated Proxmox user + token details into the DB
    (update on a Proxmox.cluster_name match, otherwise insert a new row).
    Returns: Proxmox ORM object (created or updated).
    Raises: exception on insert failure (re-raised after rollback).
    """
    cred = cred or init_proxmox_context()
    existing_user = db.query(Proxmox).filter(
        Proxmox.cluster_name == cluster_name
    ).first()

    if existing_user:
        # Update existing fields. `user` is included here (the original
        # version left it stale on update) since delete_cluster_proxmox now
        # relies on this field being accurate to target the right identity.
        existing_user.user = cred['username']
        existing_user.new_password = cred['password']
        existing_user.token_id = cred['token']
        existing_user.full_token = full_token
        existing_user.secret_key = secret
        existing_user.api_token = api_token
        existing_user.role = role
        existing_user.path = path

        db.commit()
        db.refresh(existing_user)

        return existing_user
    else:
        try:
            proxmox_user = Proxmox(
                user=cred['username'],
                cluster_name=cluster_name,
                new_password=cred['password'],
                token_id=cred['token'],
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
        except Exception as e:

            db.rollback()
            raise

async def create_cluster_proxmox(cluster_data):
    """
    Original design (commit a94d798 replaced this with a root-owned token
    for every cluster, which is what caused the recurring cross-cluster
    token-invalidation issue): root is used only to *provision* a dedicated,
    cluster-specific Proxmox user, which then gets its own token that the
    app actually authenticates with going forward -- root's own credentials
    are never stored or used for ongoing API calls.

    cleanup_proxmox_context() brackets this call so each cluster gets a
    freshly generated dedicated identity instead of the module-level cached
    one being silently reused (and overwritten) across different clusters.
    """
    from utils.proxmox_helper import cleanup_proxmox_context

    cleanup_proxmox_context()  # ensure a fresh identity for THIS cluster
    db = SessionLocal()
    try:
        PROXMOX_HOST = getting_Proxmox_host(cluster_data, timeout=5.0)
        ROOT_USERNAME = cluster_data.username   # e.g. "root@pam"
        ROOT_PASSWORD = cluster_data.password
        cluster_data_dict = cluster_data.dict()

        # Generate the dedicated identity ONCE here and pass it explicitly
        # through every step below. create_user()/assign_role_to_user() start
        # Temporal workflows that run in a separate WORKER process -- if each
        # step called init_proxmox_context() independently, the worker process
        # and this process would each generate their own (different) username,
        # so the user actually created on Proxmox would never match the one
        # this process later logs in as to create the token.
        raw_cred = init_proxmox_context()
        # init_proxmox_context() also carries contextvars.Token objects
        # (used internally by cleanup_proxmox_context()) which aren't
        # JSON-serializable and can't be passed as Temporal workflow args --
        # only the plain string fields are needed downstream.
        cred = {
            "username": raw_cred["username"],
            "password": raw_cred["password"],
            "token": raw_cred["token"],
        }

        # These start CreateUserWorkflow / AssignRoleToUserWorkflow via Temporal
        # (same as the original design) rather than invoking the underlying
        # @activity.defn functions directly, which is not a supported call path
        # outside Temporal's own activity execution context.
        await create_user(cluster_data_dict, ROOT_USERNAME, ROOT_PASSWORD, cred)
        role = "Administrator"
        path = "/"
        await assign_role_to_user(cluster_data_dict, role, path, ROOT_USERNAME, ROOT_PASSWORD, cred)
        api_token, full_token, secret = create_api_token_newUser(PROXMOX_HOST, cred)
        store_proxmox_user(db, role, path, api_token, full_token, secret, cluster_data.name, cred)
    except Exception as e:
        db.rollback()
        raise Exception(str(e))
    finally:
        db.close()
        cleanup_proxmox_context()  # don't leak this cluster's identity into the next one
    
 
def getting_Proxmox_host(cluster_data, timeout: float = 3.0) -> str:
    """
    Find the first reachable IP among the cluster's multiple IPs and build its base URL.
    Params: cluster_data — a dict or model, `.ip`/`ip_field` (comma-separated string or list) + `.port`.
    Returns: "https://{ip}:{port}" for the reachable one.
    Raises: ValueError if no IP is valid/reachable.
    """
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
    """
    List all the cluster's online nodes (Proxmox `/cluster/status` API, using the dedicated user token).
    Tries multiple IPs until one responds.
    Returns: list of {"name","ip","status":"online"}.
    Raises: RuntimeError if all the cluster IPs fail.
    """
    db = next(get_db())
    try:
        api_token = get_api_token(db, cluster_data.name)
        
        headers = {
            "Authorization": f"PVEAPIToken={api_token}",
            "Content-Type": "application/json"
        }
        last_exception = None
        for ip in cluster_data.ip:
            PROXMOX_HOST = getting_Proxmox_host(cluster_data)
            url = f"{PROXMOX_HOST}/api2/json/cluster/status"
            try:
                response = requests.get(url, headers=headers, verify=VERIFY_SSL, timeout=5)
                
                response.raise_for_status()
                data = response.json()
 
                nodes = [
                    {
                        "name": node["name"],
                        "ip": node["ip"],
                        "status": "online"
                    }
                    for node in data["data"]
                    if node.get("type") == "node" and node.get("online", 0) == 1
                ]
                
                return nodes
            except Exception as e:
                
                last_exception = e
                continue
 
        raise RuntimeError(f"All cluster IPs failed. Last error: {last_exception}")
    finally:
        db.close()
 
def delete_cluster_proxmox(cluster_data, db: Session):
    """
    While deregistering a cluster, delete its dedicated Proxmox user (with root credentials),
    then remove our DB record. DB cleanup still happens even if the user is already gone
    (401/404/"no such user").
    Returns: a success message string, or "Cluster '...' not found in the database." if there is no DB record.
    Raises: Exception if the Proxmox delete fails for any other reason.
    """
    ip_list = [ip.strip() for ip in cluster_data.ip.split(",") if ip.strip()]
    any_ip = random.choice(ip_list) if ip_list else None
    if not any_ip:
        raise Exception("No valid IP found for Proxmox cluster.")

    proxmox_cluster = db.query(Proxmox).filter(Proxmox.cluster_name == cluster_data.name).first()
    if not proxmox_cluster:
        return f"Cluster '{cluster_data.name}' not found in the database."

    PROXMOX_HOST = f"https://{any_ip}:{cluster_data.port}"
    # Use root (same as provisioning) to delete the dedicated user, rather
    # than having the dedicated user delete itself with its own token --
    # matches how it was created, and doesn't depend on that token still
    # being valid at delete time.
    headers, cookies = root_proxmox_login(PROXMOX_HOST, cluster_data.username, cluster_data.password)
    # Delete the ACTUAL dedicated user this cluster's token belongs to
    # (stored in our own DB row), not a stale/unrelated cached identity.
    # Deleting the user also revokes every token that belongs to it.
    url = f"{PROXMOX_HOST}/api2/json/access/users/{proxmox_cluster.user}"

    try:
        response = requests.delete(url, headers=headers, cookies=cookies, verify=VERIFY_SSL)
        # Accept 401/404 errors or "no such user" in the error text -- the
        # user may already be gone (e.g. deleted manually on Proxmox), which
        # shouldn't block cleaning up our own DB record.
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code in (401, 404) or "no such user" in response.text:
                pass
            else:
                raise Exception("Failed to delete user from Proxmox API")
    except Exception as e:
        raise Exception(f"Failed to delete user from Proxmox API: {str(e)}")

    db.delete(proxmox_cluster)
    db.commit()
    return f"Cluster '{cluster_data.name}' deleted successfully."

def get_devraq_metric_server_id(cluster_data):
    """
    ID used for the InfluxDB metric server DevRaQ manages on Proxmox. Kept
    distinct from the bare cluster name so DevRaQ's own integration never
    collides with, overwrites, or deletes a metric server a customer may
    have configured themselves on the same cluster.
    """
    return f"{cluster_data.name}-devraq"

def add_influxdb_metric_server(cluster_data, payload):
    """
    Register DevRaQ's InfluxDB metric-server on the Proxmox cluster (POST).
    Returns: the Proxmox API's JSON response.
    Raises: Exception if the API returns an error.
    """
    db = next(get_db())
    try:
        api_token = get_api_token(db, cluster_data.name)
        headers = {
            "Authorization": f"PVEAPIToken={api_token}",
        }
        PROXMOX_HOST = getting_Proxmox_host(cluster_data)
        server_id = get_devraq_metric_server_id(cluster_data)
        url = f"{PROXMOX_HOST}/api2/json/cluster/metrics/server/{server_id}"

        # Prepare payload: ensure it matches the API schema and remove redundant 'id'
        payload = {k: v for k, v in payload.items()}
        payload.pop("id", None)  # Already in URL

        response = requests.post(url, headers=headers, data=payload, verify=False)
        if not response.ok:
            raise Exception(f"Proxmox metric server API error ({response.status_code}): {response.text}")
        return response.json()
    finally:
        db.close()

def get_influxdb_metric_server(cluster_data):
    """
    Get DevRaQ's own InfluxDB metric server for the given cluster (identified
    by its distinct id) -- never a customer-configured one that may also
    exist on the same cluster.
    """
    db = next(get_db())
    try:
        api_token = get_api_token(db, cluster_data.name)
        headers = {
            "Authorization": f"PVEAPIToken={api_token}",
            "Content-Type": "application/json"
        }
        PROXMOX_HOST = getting_Proxmox_host(cluster_data)
        server_id = get_devraq_metric_server_id(cluster_data)
        detail_url = f"{PROXMOX_HOST}/api2/json/cluster/metrics/server/{server_id}"
        try:
            detail_resp = requests.get(detail_url, headers=headers, verify=False)
            if detail_resp.status_code == 404:
                return {"error": "No DevRaQ InfluxDB metric server found for the cluster."}
            detail_resp.raise_for_status()
            return detail_resp.json().get("data", {})
        except Exception as e:
            return {"error": f"Failed to fetch DevRaQ metric server details: {e}"}
    finally:
        db.close()
 
def get_influxdb_env_defaults():
    """Get the DevRaQ InfluxDB defaults configured in `.env` (server/port/proto/org/bucket/token)."""
    parsed_url = urlparse(INFLUXDB_URL)
    return {
        "server": parsed_url.hostname,
        "port": int(INFLUXDB_PORT or (443 if parsed_url.scheme == "https" else 8086)),
        "influxdbproto": parsed_url.scheme,
        "organization": INFLUXDB_ORG,
        "bucket": INFLUXDB_BUCKET,
        "token": INFLUXDB_TOKEN,
    }

def create_and_get_metric_server(cluster_data, overrides: Optional[dict] = None):
    """
    Create DevRaQ's InfluxDB metric-server on Proxmox (building the payload from env defaults +
    optional overrides), then immediately fetch and return its details.
    Returns: the result of `get_influxdb_metric_server()` (a dict).
    """
    defaults = get_influxdb_env_defaults()
    overrides = overrides or {}
    # Field name "influxdbproto" confirmed against Proxmox VE 9.2.2's own schema
    # (`pvesh usage cluster/metrics/server/{id} --verbose`). If this ever starts
    # rejecting requests again after a Proxmox upgrade, re-run that command to
    # get the current schema before assuming the field name changed.
    influxdb_payload = {
        "type": "influxdb",
        "id": get_devraq_metric_server_id(cluster_data),
        "server": overrides.get("server") or defaults["server"],
        "port": int(overrides.get("port") or defaults["port"]),
        "influxdbproto": overrides.get("influxdbproto") or defaults["influxdbproto"],
        "organization": overrides.get("organization") or defaults["organization"],
        "bucket": overrides.get("bucket") or defaults["bucket"],
        "token": overrides.get("token") or defaults["token"],
        "verify-certificate": 0,
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
    """
    Upsert a cluster's MetricServer DB record (from the metric_data returned by Proxmox).
    `is_custom_integration` is only updated when it is explicitly given
    (to avoid an accidental overwrite on GET calls).
    Returns: MetricServer ORM object.
    """
    ms = db.query(MetricServer).filter(MetricServer.cluster_id == cluster_id).first()
    if not ms:
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
    Remove DevRaQ's InfluxDB metric-server from the Proxmox cluster (DELETE).
    Returns: {"status": "success"} on success, {"error": "..."} on failure (does not raise an exception).
    """
    db = next(get_db())
    try:
        api_token = get_api_token(db, cluster_data.name)
        headers = {
            "Authorization": f"PVEAPIToken={api_token}",
            "Content-Type": "application/json"
        }

        PROXMOX_HOST = getting_Proxmox_host(cluster_data)
        server_id = get_devraq_metric_server_id(cluster_data)
        url = f"{PROXMOX_HOST}/api2/json/cluster/metrics/server/{server_id}"
        try:
            response = requests.delete(url, headers=headers, verify=False)
            response.raise_for_status()
            return {"status": "success"}
        except Exception as e:
            return {"error": f"Failed to delete metric server from Proxmox API: {e}"}
    finally:
        db.close()

def can_delete_metric_server(db, cluster_id):
    """
    Check whether a cluster's metric-server integration is eligible for deletion
    (only DevRaQ-managed custom monitoring integrations can be deleted).
    Returns: a bare `True` (not a tuple) on success, or a `(False, "<reason>")` tuple on failure —
             the caller must unpack this with the asymmetry in mind.
    """
    ms = db.query(MetricServer).filter(MetricServer.cluster_id == cluster_id).first()
    if not ms:
        return False, "Metric server integration not found."
    if not (ms.monitoring and ms.is_custom_integration):
        return False, "You cannot delete this integration because it is not a custom DB integration managed by you."
    return True

def get_metric_server_from_db(cluster_id: int) -> Optional[MetricServer]:
    """Fetch a cluster's MetricServer DB record. Returns None if not found or if the query fails."""
    db: Optional[Session] = None
    try:
        db = SessionLocal()
        ms = db.query(MetricServer).filter(MetricServer.cluster_id == cluster_id).first()
        return ms
    except Exception as e:

        return None
    finally:
        if db:
            db.close()
 
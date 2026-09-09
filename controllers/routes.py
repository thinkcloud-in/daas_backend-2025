from utils.temporal_client import TemporalClientManager
import asyncio
import json
import logging
from fastapi import APIRouter, HTTPException, Depends, Request
from pydantic import BaseModel
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session
from db_configuration.config import get_db
from keycloak_configration import keycloak_config as key_config
from models import models
from typing import Any, List
from service import controllers as controller
from fastapi import  HTTPException
from temporalio.client import Client
import os
import pytz
from models.API_Response_model import APIResponse
from utils import response_format
from utils.crypto_utils import strip_password
from controllers.llm_inference_controller import _fetch_steps

logger = logging.getLogger(__name__)

_LLM_WF_TYPES = {
    "CreateMultiNodeLLMWorkflow", "PoolVMActionWorkflow", "DeleteLLMPoolWorkflow",
    "LibraryUploadWorkflow", "LibraryDeleteWorkflow", "LibraryUpdateWorkflow",
    "LXCRestoreWorkflow",
}

_K8S_DEPLOY_WF_TYPE   = "K8sHarborDeployWorkflow"
_HARBOR_PUSH_WF_TYPE  = "HarborPushWorkflow"


async def _enrich_k8s_deploy(wf: dict) -> dict:
    """K8s Harbor deploy workflow ko DB steps_log se enrich karo."""
    import json as _json
    from db_configuration.config import SessionLocal
    from models.kubernetes_deploy_model import KubernetesDeployment

    db = SessionLocal()
    try:
        deploy = db.query(KubernetesDeployment).filter(
            KubernetesDeployment.workflow_id == wf["workflow_id"]
        ).first()
        if not deploy:
            return wf

        try:
            raw_steps = _json.loads(deploy.steps_log) if deploy.steps_log else []
        except Exception:
            raw_steps = []

        steps = [
            {"step": i + 1, "label": s, "status": "completed"}
            for i, s in enumerate(raw_steps)
        ]

        from controllers.kubernetes_controller import _DEPLOY_PROGRESS
        progress = _DEPLOY_PROGRESS.get(
            deploy.status,
            {"step": 0, "label": deploy.status, "pct": 0},
        )

        wf.update({
            "deploy_id":     deploy.id,
            "cluster_id":    deploy.cluster_id,
            "node_ip":       deploy.node_ip,
            "namespace":     deploy.namespace,
            "deploy_status": deploy.status,
            "progress":      progress,
            "harbor_url":    deploy.harbor_url,
            "error_message": deploy.error_message,
            "steps":         steps,
            "total_steps":   len(steps),
            "completed_steps": len(steps),
            "current_step":  raw_steps[-1] if raw_steps else None,
        })
    except Exception as exc:
        logger.warning(f"[K8s enrich] {exc}")
    finally:
        db.close()

    return wf


async def _enrich_harbor_push(wf: dict) -> dict:
    """HarborPushWorkflow ko library item push_status se enrich karo."""
    from db_configuration.config import SessionLocal
    from models.library_model import LibraryItem

    db = SessionLocal()
    try:
        item = db.query(LibraryItem).filter(
            LibraryItem.push_workflow_id == wf["workflow_id"]
        ).first()
        if not item:
            return wf

        _STATUS_MAP = {
            "pending":  {"pct": 0,   "label": "Queued"},
            "pushing":  {"pct": 50,  "label": "Pushing to Harbor"},
            "pushed":   {"pct": 100, "label": "Pushed"},
            "failed":   {"pct": 0,   "label": "Failed"},
        }
        progress = _STATUS_MAP.get(item.push_status or "pending", {"pct": 0, "label": item.push_status})

        wf.update({
            "library_item_id": item.id,
            "library_name":    item.name,
            "push_status":     item.push_status,
            "harbor_image":    item.harbor_image,
            "push_error":      item.push_error,
            "progress":        progress,
        })
    except Exception as exc:
        logger.warning(f"[HarborPush enrich] {exc}")
    finally:
        db.close()

    return wf


router = APIRouter(prefix="/v1")

TEMPORAL_SERVER = os.getenv("TEMPORAL_SERVER")

# ─────────────────────────────────────────────────────────────────────────────
# Proxmox storages / VDI Pools / Machines
# ─────────────────────────────────────────────────────────────────────────────

@router.post('/get_proxmox_storages', response_model=APIResponse)
async def get_proxmox_storages(payload: models.NodeRequest, db: Session = Depends(get_db)):
    """
    Proxmox node(s) pe available storages list karo (VM/pool create karte
    waqt "storage" dropdown ke liye).

    Request body: models.NodeRequest (cluster_id + node names).
    Response `data`: [ {"storage": str, "type": str, "avail": int, ...}, ... ]
    """
    res = await controller.get_proxmox_storages(payload, db)
    return response_format.success_response(200, "Proxmox Storages retrieved successfully.", res)

@router.post('/create_pool', response_model=APIResponse)
async def create_pool_endpoint(pool_data: models.CreatePoolBase, db: Session = Depends(get_db)):
    """
    Naya VDI pool banao (Guacamole connection-group + Proxmox/Hyper-V VM
    template binding — RDP/SSH/VNC settings, IP-pool, machine-naming pattern
    sab CreatePoolBase mein).

    Response `data`: created pool ka summary/id.
    """
    pool_data_dict = pool_data.dict()
    res = await controller.create_pool(pool_data_dict,db)
    return response_format.success_response(200, "Pool created successfully.", res)

@router.get('/retrive_pool/{pool_name}', response_model=APIResponse)
async def retrive_machihe(pool_name:str,db: Session = Depends(get_db)):
    """
    Pool ki detail lo, naam se (DB se direct, Temporal ke bina — fast path).

    Response `data`: {"msg": str, "pool": {...saare Pool columns...}}
    NOTE: `pool` object mein Pool model ke saare columns raw jaate hain,
    jisme `pool_password`, `pool_ad_password`, `pool_gateway_password`,
    `pool_sftp_password`, `pool_private_key`, etc. bhi shamil hain — abhi
    plaintext mein return hote hain (encrypt/redact nahi hote). Yeh sirf
    documentation pass hai, fix nahi kiya gaya — alag se dekhna hoga
    (Cluster.password jaisa hi issue jo pehle fix kiya tha).
    """
    res = await controller.retrive_pool_data(pool_name ,db)
    return response_format.success_response(200, "Pool retrieved successfully.", res)

@router.put('/vdi_pools/update_pool/{pool_id}', response_model=APIResponse)
async def update_pool_route(pool_id: str, request: Request, db: Session = Depends(get_db)):
    """
    Pool settings update karo (raw JSON body — jo bhi keys do, unhi columns
    pe update hota hai).

    Request body: {"email": str, ...koi bhi Pool column...}
    Response `data`: update-result summary.
    """
    pool_id_int = int(pool_id)

    body = await request.json()
    email = body.get("email", "Unknown User")
    pool_data = body
    result = await controller.update_pool(pool_id_int, email, pool_data, db)
    return response_format.success_response (200, "Pool updated successfully.", result)



@router.delete('/delete_pool/{pool_id}', response_model=APIResponse)
async def delete_pool_route(pool_id: str, request: Request, db: Session = Depends(get_db)):
    """
    Pool delete karo (Temporal workflow — pool ke saare machines bhi cleanup
    hoti hain).

    Request body: {"email": str}  (audit ke liye, empty body pe 400).
    Response `data`: {"workflow_id": str, "status": "started", ...}
    """
    pool_id_int = int(pool_id)
    raw_body = await request.body()
    if not raw_body:
        raise HTTPException(status_code=400, detail="Empty request body")
    body = json.loads(raw_body)
    email = body.get("email")

    result = await controller.delete_pool(pool_id_int, email, db)
    return response_format.success_response(200, "Pool deletion started successfully.", result)



@router.get('/pools_names', response_model=APIResponse)
async def list_pools_names(db: Session = Depends(get_db)):
    """
    Sirf pool names ki list lo (dropdown ke liye).

    Response `data`: {"msg": str, "pool_names": [str, ...]}
    """
    res = await controller.get_all_pool_names(db)
    return response_format.success_response(200, "Pool names retrieved successfully.", res)

# Route to list all the pools
@router.get('/vdi_pools/pools', response_model=APIResponse)
async def list_pools(page: int = 1, page_size: int = 10):
    """
    Saare pools list karo (paginated).

    Response `data`:
        {"items": [ {..pool summary..}, ... ], "pagination": {page, page_size, total, total_pages, has_next, has_prev}}
    """
    result = await controller.get_all_pools(page=page, page_size=page_size)
    return response_format.success_response(200, "Pools retrieved successfully.", {
        "items": result.get("pools", []),
        "pagination": result.get("pagination"),
    })

#get pool details based on id
@router.get("/vdi_pools/pool/{pool_id}", response_model=APIResponse)
async def get_pool_details_route(pool_id: int):
    """
    Pool ki detail lo, id se.

    Response `data`: {"msg": str, "pool": {...saare Pool columns, "cluster": str...}}
    NOTE: same security caveat as `/retrive_pool/{pool_name}` — raw
    credential columns bhi is response mein aate hain.
    """
    data = await controller.get_pool_details(pool_id)
    return response_format.success_response(200, "Pool details retrieved successfully.", data)


@router.post('/vdi_pools/create_machine',response_model=APIResponse)
async def create_machine_endpoint(machine_data: models.CreateMachineBase):
    """
    Ek pool mein naya machine/connection add karo.

    Response `data`: created machine ka summary/id.
    """
    data = await controller.create_machine( machine_data)
    return response_format.success_response(200, "Machine created successfully.", data)

@router.delete("/vdi_pools/delete_machine/{machine_id}", response_model=APIResponse)
async def delete_machine_by_id(machine_id: str, email: str, db: Session = Depends(get_db)):
    """
    Machine (Guacamole connection) delete karo.

    Query param: `email` (required, audit ke liye).
    Response `data`: deletion summary.
    Errors: 400 agar `email` na diya ho.
    """
    if not email:
        raise HTTPException(status_code=400, detail="Email is required")

    data = await controller.delete_machine(machine_id, email, db)
    return response_format.success_response(200, "Machine deleted successfully.", data)

#Route to update a machine/connection
@router.put('/vdi_pools/update_machine/{machine_identifier}', response_model=APIResponse)
async def update_machine_endpoint(machine_identifier: str, machine_data: models.UpdateMachineBase):
    """
    Machine/connection settings update karo.

    Response `data`: updated machine summary.
    """
    data = await controller.update_machine( machine_identifier,machine_data)
    return response_format.success_response(200, "Machine updated successfully.", data)


#Route to list all the machines available
@router.get("/machines", response_model=APIResponse)
async def list_machines():
    """
    Saare machines (across all pools) list karo.

    Response `data`: [ {..machine summary: id, name, pool_id, status, ip, ...}, ... ]
    """
    res = await controller.get_machines()
    return response_format.success_response(200, "Machines retrieved successfully.", res)


@router.put("/machines/{machine_identifier}/is_custom_machine/", response_model=APIResponse)
async def update_machine_isCustom_field(machine_identifier: str, machine_data: models.IsCustomeValue):
    """
    Machine ka `is_custom` flag toggle karo (custom-named/standalone machine
    vs pool-managed/auto-named machine).

    Request body: models.IsCustomeValue = {"is_custom": bool}
    Response `data`: updated machine summary.
    """
    res = await controller.update_is_custom_machine( machine_identifier, machine_data)
    return response_format.success_response(200, "Machine is_custom field updated successfully.", res)


#route to assign user to a machine
@router.post("/add_user_to_machine/{machine_id}/{user_id}", response_model=APIResponse)
async def add_user(machine_id: str, user_id: str):
    """User ko machine access assign karo (Guacamole permission grant)."""
    res = await controller.add_user_to_machine( machine_id, user_id)
    return response_format.success_response(200, "User added to machine successfully.", res)


#delete user from the machine
@router.delete("/delete_user_from_machine/{machine_id}/{user_id}", response_model=APIResponse)
async def delete_user(machine_id: str, user_id: str):
    """User ka machine access revoke karo."""
    res = await controller.delete_user_from_machine( machine_id, user_id)
    return response_format.success_response(200, "User deleted from machine successfully.", res)


#get all the machines available in a pool
@router.get("/pool/machines/{pool_id}", response_model=APIResponse)
async def list_machines_in_pool(pool_id: str):
    """
    Ek pool ke saare machines list karo.

    Response `data`: [ {..machine summary..}, ... ]
    """
    data = await controller.list_of_all_machine_in_pool(pool_id)
    return response_format.success_response(200, "Successfully retrieved machines in pool", data)



#get all the users assigned to a machine
@router.get("/machine/users/{machine_id}", response_model=APIResponse)
async def list_assigned_users_route(machine_id: str):
    """
    Ek machine ko assigned saare users list karo.

    Response `data`: [ {"user_id": str, "username": str, ...}, ... ]
    """
    data = await controller.list_assigned_users( machine_id)
    return response_format.success_response(200, "Successfully retrieved assigned users", data)


#get machine details by id
@router.get("/machine/{machine_id}", response_model=APIResponse)
async def get_machine_details_route(machine_id: str):
    """
    Ek machine ki poori detail lo.

    Response `data`: {..machine record: id, name, pool_id, ip, status, ...}
    """
    data = await controller.get_machine_details( machine_id)
    return response_format.success_response(200, "Successfully retrieved machine details", data)


#----------------------------- done ------------------------------------

# ─────────────────────────────────────────────────────────────────────────────
# Proxmox/LXC Clusters
# ─────────────────────────────────────────────────────────────────────────────

# Create cluster
@router.post('/create_cluster', response_model=APIResponse)
async def create_cluster_endpoint(cluster_data: models.CreateClusterBase):
    """
    Naya Proxmox/VMware/Hyper-V cluster add karo (type ke hisaab se alag
    validation/setup hoti hai — Proxmox ke liye API-token bhi auto-create
    hota hai).

    Request body: models.CreateClusterBase.
    Response `data`: {"cluster": {...password field kabhi nahi..., "ip": [str,...]}}
    (password DB mein encrypted save hoti hai, response se hamesha excluded.)

    Errors: agar naam/IP already exist kare to plain error-message string
    return hoti hai (exception nahi).
    """
    cluster_data_dict = cluster_data.dict()
    res = await controller.create_cluster(cluster_data_dict)
    return response_format.success_response(200, "Cluster created successfully.", res)

#Route to list all the clusters
@router.get('/cluster/clusters', response_model=APIResponse[Any])
async def list_clusters(page: int = 1, page_size: int = 10, db: Session = Depends(get_db)):
    """
    Saare clusters list karo (paginated).

    Response `data`:
        {
          "items": [ {id, type, name, ip, port, agent_port, username, tls, node_type} , ... ],
          "pagination": {page, page_size, total, total_pages, has_next, has_prev}
        }
    `password` field kabhi response mein nahi aata (dekho `strip_password`
    helper — encrypt/decrypt DB level pe hota hai, plaintext kabhi client
    tak nahi jaata).
    """
    page = max(1, page)
    page_size = max(1, min(page_size, 100))
    query = db.query(models.Cluster).order_by(models.Cluster.id.asc())
    total = query.count()
    total_pages = max(1, (total + page_size - 1) // page_size)
    clusters = query.offset((page - 1) * page_size).limit(page_size).all()
    clusters_json = strip_password(jsonable_encoder(clusters))
    pagination = {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "has_next": page < total_pages,
        "has_prev": page > 1,
    }
    return response_format.success_response(200, "Clusters retrieved successfully.", {"items": clusters_json, "pagination": pagination})

#get pool details based on id
@router.get("/cluster/{cluster_id}", response_model=APIResponse)
async def get_cluster_details_endpoint(cluster_id: str, db: Session = Depends(get_db)):
    """
    Ek cluster ki detail lo.

    Response `data`: cluster record (upar `list_clusters` jaisa shape,
    `password` field kabhi nahi).
    Errors: 404 agar cluster_id na mile.
    """
    res = await controller.get_cluster_details(db, cluster_id)
    res_json = strip_password(jsonable_encoder(res))
    return response_format.success_response(200, "Cluster details retrieved successfully.", res_json)


@router.delete('/cluster/delete_cluster/{cluster_id}', response_model=APIResponse)
async def delete_cluster_route(cluster_id: str, request: Request, db: Session = Depends(get_db)):
    """
    Cluster delete karo (Temporal workflow).

    Request body: {"email": str}  (audit ke liye, empty body pe 400).
    Response `data`: delete-result.
    Errors: 400 agar isse linked active Pools/Machines hon (pehle unhe
    delete karo).
    """
    # cluster_id_int = int(cluster_id)
    raw_body = await request.body()
    if not raw_body:
        raise HTTPException(status_code=400, detail="Empty request body")
    body = json.loads(raw_body)
    email = body.get("email")
    res = await controller.delete_cluster(db, cluster_id, email)
    return response_format.success_response(200, "Cluster deleted successfully.", res)

#Route to update a cluster
@router.put('/cluster/update_cluster/{cluster_id}', response_model=APIResponse)
async def update_cluster_endpoint(cluster_id: str, cluster_data: models.UpdateClusterBase, db: Session = Depends(get_db)):
    """
    Cluster credentials/settings update karo (Temporal workflow).

    Request body: models.UpdateClusterBase (sab optional — sirf jo change
    karni ho wo do).
    Response `data`: update-result (`password` field kabhi nahi).
    Errors: 404 agar cluster_id na mile.
    """
    res = await controller.update_cluster(db, cluster_id, cluster_data)
    return response_format.success_response(200, "Cluster updated successfully.", res)

# ─────────────────────────────────────────────────────────────────────────────
# Active Directory / LDAP (Keycloak)
# ─────────────────────────────────────────────────────────────────────────────

# Ldap connection in keycloak
@router.post('/domain/ad_ldap_connection' ,response_model=APIResponse)
async def ldap_Configuration(ldap_data:models.LDAPCredential):
    """
    Naya AD/LDAP provider Keycloak mein configure karo (user-sync source).

    Request body: models.LDAPCredential.
    Response `data`: Keycloak ka create-result.
    Errors: 500 (error_response) agar Keycloak call fail ho.
    """
    res =  await key_config.configuration_ad(ldap_data)
    if isinstance(res, dict) and res.get("msg") == "Error occurred":
        return response_format.error_response(500, res.get("error", "LDAP configuration failed"), res)
    return response_format.success_response(200, "LDAP configuration successful.", res)

#Route to list all the LDAPs configured
@router.get('/ldaps', response_model=APIResponse)
async def get_LDAPs_from_keycloak_endpoint():
    """
    Saare configured LDAP providers list karo.

    Response `data`: [ {..Keycloak LDAP provider config..}, ... ]
    """
    res = await key_config.get_LDAPs_from_keycloak()
    if isinstance(res, dict) and res.get("msg") == "Error occurred":
        return response_format.error_response(500, res.get("error", "Failed to retrieve LDAP configurations"), res)
    return response_format.success_response(200, "LDAP configurations retrieved successfully.", res)

#Route to test LDAP connection
@router.post('/domain/test_ldap_connection', response_model=APIResponse)
async def test_ldap_connection_endpoint(ldap_data:models.LDAP_test_connection_model):
    """
    LDAP server se network/bind connection test karo (DB me save kiye
    bina).

    NOTE: yeh function seedha Keycloak ke result ko return karta hai — koi
    aur success_response wrapping nahi (comment mein bhi likha hai isko
    badalne se workflow break ho sakta hai).
    """
    res = await key_config.test_ldap_connection(ldap_data)
    return res #--------------don't change this format it may break the workflow and response format

#Route to test LDAP authentication
@router.post('/domain/test_ldap_authentication', response_model=APIResponse)
async def test_ldap_authentication_endpoint(ldap_data:models.LDAP_test_connection_model):
    """
    Ek diye gaye user-credential se LDAP authentication test karo.

    NOTE: same as `test_ldap_connection` — raw Keycloak result return hota
    hai, koi extra wrapping nahi.
    """
    res = await key_config.test_ldap_authentication(ldap_data)
    return res #--------------don't change this format it may break the workflow and response format

# Route to delete LDAP configuration
@router.delete('/domain/delete_ldap_configuration/{ldap_id}', response_model=APIResponse)
async def delete_ldap_config_endpoint(ldap_id: str):
    """LDAP provider delete karo Keycloak se."""
    res = await key_config.delete_ldap_config(ldap_id)
    if isinstance(res, dict) and res.get("msg") == "Error occurred":
        return response_format.error_response(500, res.get("error", "Failed to delete LDAP configuration"), res)
    return response_format.success_response(200, "LDAP configuration deleted successfully.", res)

# get LDAP details by id
@router.get('/domain/get_ldap_by_id/{ldap_id}', response_model=APIResponse)
async def get_LDAP_by_id_endpoint(ldap_id: str):
    """Ek LDAP provider ki config detail lo."""
    data = await key_config.get_LDAP_by_id(ldap_id)
    if isinstance(data, dict) and data.get("msg") == "Error occurred":
        return response_format.error_response(500, data.get("error", "Failed to retrieve LDAP configuration"), data)
    return response_format.success_response(200, "LDAP configuration retrieved successfully.", data)

# update_ldap_config
@router.put('/domain/update_ldap_config/{ldap_id}')
async def update_ldap_config_endpoint(ldap_data: models.LDAPCredential,ldap_id: str):
    """LDAP provider config update karo."""
    ldap_data_dict = ldap_data.dict()
    data = await key_config.update_ldap_config(ldap_data_dict,ldap_id)
    if isinstance(data, dict) and data.get("msg") == "Error occurred":
         return response_format.error_response(500, data.get("error", "Failed to update LDAP configuration"), data)
    return response_format.success_response(200, "LDAP configuration updated successfully.", data)

@router.get('/domain/sync_users/{ldap_id}', response_model=APIResponse)
async def sync_ad_user_from_keycloak(ldap_id: str):
    """LDAP se naye/existing users ko Keycloak mein sync (import) karo."""
    res = await key_config.sync_user_from_keycloak(ldap_id)
    return response_format.success_response(200, "User synced successfully.", res)

@router.get('/domain/sync_changed_users/{ldap_id}', response_model=APIResponse)
async def sync_changed_users_from_keycloak_endpoint(ldap_id: str):
    """LDAP mein change hue users (naam/email/attributes) ko re-sync karo."""
    res = await key_config.sync_changed_users_from_keycloak(ldap_id)
    return response_format.success_response(200, "Changed users synced successfully.", res)

@router.get('/domain/unlink_users/{ldap_id}', response_model=APIResponse)
async def unlink_users_from_keycloak_endpoint(ldap_id: str):
    """LDAP-linked users ko unlink karo (Keycloak account rehta hai, LDAP se disconnect ho jaata hai)."""
    res = await key_config.unlink_users_from_keycloak(ldap_id)
    return response_format.success_response(200, "Users unlinked successfully.", res)

@router.get('/domain/remove_imported_users/{ldap_id}', response_model=APIResponse)
async def remove_imported_users_from_keycloak_endpoint(ldap_id: str):
    """LDAP se import hue saare users ko Keycloak se hi delete kar do."""
    res = await key_config.remove_imported_users_from_keycloak(ldap_id)
    return response_format.success_response(200, "Imported users removed successfully.", res)

# ─────────────────────────────────────────────────────────────────────────────
# TOTP / 2FA (Keycloak)
# ─────────────────────────────────────────────────────────────────────────────

@router.put('/totp/enable-disable-totp-browser/{value}')
def enable_browser_authflow(value):
    """
    Admin login (browser) ke liye globally OTP enforce/disable karo.

    Path param: value = "true"|"false" (string).
    Response: Keycloak auth-flow-update result (raw, koi envelope nahi).
    """
    if value == 'true':
        value=True
    else:
        value=False
    return key_config.set_otp_for_browser_auth(value)

@router.put('/totp/enable-disable-guac/{value}')
def enable_browser_guacamole_authflow(value):
    """
    Guacamole (end-user) login ke liye globally OTP enforce/disable karo.

    Path param: value = "true"|"false" (string).
    Response: Keycloak auth-flow-update result (raw).
    """
    if value == 'true':
        value=True
    else:
        value=False
    return  key_config.set_otp_for_guacamole_browser(value)

@router.get('/totp/get-enable-disable-totp-browser')
def  get_enable_browser_authflow():
    """
    Admin-browser OTP flag ka current state lo.

    Response: bool (raw, koi envelope nahi) — True = OTP required.
    """
    return  key_config.get_Auth_flow_Value_browser()

@router.get('/totp/get-enable-disable-guac')
def get_enable_browser_guacamole_authflow():
    """
    Guacamole OTP flag ka current state lo.

    Response: bool (raw) — True = OTP required.
    """
    return  key_config.get_Auth_flow_Value_guacamole_browser()

@router.post('/totp/reset-guac-totp/{user_id}')
def reset_guac_totp(user_id: str):
    """
    Ek user ka TOTP/OTP setup reset karo (Keycloak se OTP credential remove
    — user ko dobara QR-scan karke setup karna padega).

    Response: Keycloak reset-result (raw).
    """
    return  key_config.reset_guac_totp(user_id)


class _TOTPVerifyBody(BaseModel):
    totp_code: str


def _extract_jwt_claims(request: Request) -> dict:
    """Extract sub + preferred_username from Bearer JWT without signature verification."""
    import jwt as _pyjwt
    token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    if not token:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    try:
        payload = _pyjwt.decode(token, options={"verify_signature": False})
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid JWT token")
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Cannot extract user id from token")
    return payload


@router.post('/totp/verify-totp')
def verify_totp(body: _TOTPVerifyBody, request: Request):
    """
    Verify TOTP via Keycloak directly.
    Keycloak internally validates the OTP using its own stored secret — no password needed.
    If user has no OTP configured in Keycloak, returns verified=True (OTP not required).

    Request body: {"totp_code": str}
    Header: Authorization (Bearer, required — user pehchanne ke liye).

    Response `data`: {"verified": true}. Verification 5 minute ke liye cache
    hoti hai (isi window mein delete jaisi sensitive actions bina dobara
    OTP maange chalti hain).
    Errors: 401 missing/invalid token, 401 invalid TOTP code.
    """
    payload  = _extract_jwt_claims(request)
    user_id  = payload.get("sub")
    username = payload.get("preferred_username") or user_id

    if not key_config.has_keycloak_otp(user_id):
        return response_format.success_response(200, "OTP not enabled for this user", {"verified": True})

    ok = key_config.verify_user_totp(user_id, body.totp_code, username=username)
    if not ok:
        raise HTTPException(status_code=401, detail="Invalid TOTP code")
    key_config.mark_totp_verified(user_id)   # cache for 5 min so delete needs no re-verification
    return response_format.success_response(200, "TOTP verified successfully", {"verified": True})


# ─────────────────────────────────────────────────────────────────────────────
# Temporal workflow monitoring
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/workflows")
async def list_workflows():
    """
    Fetch and return active workflows from Temporal.

    Sirf "Entity" search-attribute wale workflows dikhata hai (utility/cron
    workflows chhod deta hai). LLM-pool/Library/K8s-Harbor-deploy/Harbor-push
    workflow-types ke liye extra step-by-step progress bhi enrich hoti hai.

    Response `data` (raw list, koi APIResponse-wrapping ke andar bhi list
    hoti hai — envelope khud `success_response` se aata hai):
        [
          {
            "workflow_id": str, "run_id": str, "workflow_type": str,
            "task_name": str,     # search-attribute "Entity"
            "action": str,        # search-attribute "Action"
            "start_time": str,    # IST, "YYYY-MM-DDTHH:MM:SS.mmm"
            "close_time": str,    # IST ya "Ongoing"
            "execution_time": str|None,  # e.g. "1.23s" ya "450ms"
            "status": str,        # Temporal WorkflowExecutionStatus name
            "UserName": str,
            # LLM/Library/LXC workflow types ke liye extra:
            "steps": [ {..step info..}, ... ], "total_steps": int,
            "completed_steps": int, "current_step": str|None,
            # K8sHarborDeployWorkflow ke liye extra:
            "deploy_id", "cluster_id", "node_ip", "namespace", "deploy_status",
            "progress", "harbor_url", "error_message",
            # HarborPushWorkflow ke liye extra:
            "library_item_id", "library_name", "push_status", "harbor_image", "push_error",
          }, ...
        ]
    """
    client = await TemporalClientManager.get_temporal_client()
    time_format = "%Y-%m-%dT%H:%M:%S.%fZ"

    # Set IST timezone (Indian Standard Time)
    import pytz
    IST_TZ = pytz.timezone("Asia/Kolkata")


    workflows = []
    # Using a Temporal Query to filter workflows on the server side.
    # This significantly reduces the load on the persistence layer by not fetching
    # utility workflows that don't have the 'Entity' attribute.
    query = "Entity != ''"

    try:
        async for wf in client.list_workflows(query=query):
            # Convert UTC to IST (Indian Standard Time)
            start_time_ist = wf.start_time.astimezone(IST_TZ).strftime(time_format)[:-3]
            close_time_ist = wf.close_time.astimezone(IST_TZ).strftime(time_format)[:-3] if wf.close_time else "Ongoing"

            execution_time = None
            if wf.close_time:
                delta_ms = (wf.close_time - wf.start_time).total_seconds() * 1000
                if delta_ms < 1000:
                    execution_time = f"{int(delta_ms)}ms"
                else:
                    execution_time = f"{delta_ms / 1000:.2f}s"
            else:
                execution_time = None

            entity = wf.search_attributes.get("Entity", ["UnknownEntity"])[0]
            action = wf.search_attributes.get("Action", ["UnknownAction"])[0]
            UserName = wf.search_attributes.get("UserName", ["UnknownUserName"])[0]

            if entity == "UnknownEntity":
                continue

            workflows.append({
                "workflow_id": wf.id,
                "run_id": wf.run_id,
                "workflow_type": wf.workflow_type,
                "task_name": entity,
                "action": action,
                "start_time": start_time_ist,
                "close_time": close_time_ist,
                "execution_time": execution_time,
                "status": wf.status.name if wf.status else "RUNNING",
                "UserName": UserName
            })
    except Exception as e:
        logger.error(f"Error listing workflows with query: {e}", exc_info=True)
        # Fallback to listing all but with a strict limit to avoid hitting QPS again
        async for wf in client.list_workflows():
            if len(workflows) >= 50:
                break

            entity = wf.search_attributes.get("Entity", ["UnknownEntity"])[0]
            if entity == "UnknownEntity":
                continue

            start_time_ist = wf.start_time.astimezone(IST_TZ).strftime(time_format)[:-3]
            close_time_ist = wf.close_time.astimezone(IST_TZ).strftime(time_format)[:-3] if wf.close_time else "Ongoing"

            workflows.append({
                "workflow_id": wf.id,
                "run_id": wf.run_id,
                "workflow_type": wf.workflow_type,
                "task_name": entity,
                "action": wf.search_attributes.get("Action", ["UnknownAction"])[0],
                "start_time": start_time_ist,
                "close_time": close_time_ist,
                "status": wf.status.name if wf.status else "RUNNING",
                "UserName": wf.search_attributes.get("UserName", ["UnknownUserName"])[0]
            })

    # Enrich workflows with activity steps
    async def _add_steps(wf):
        wf_type = wf.get("workflow_type")

        if wf_type == _K8S_DEPLOY_WF_TYPE:
            return await _enrich_k8s_deploy(wf)

        if wf_type == _HARBOR_PUSH_WF_TYPE:
            return await _enrich_harbor_push(wf)

        if wf_type not in _LLM_WF_TYPES:
            return wf

        try:
            handle = client.get_workflow_handle(wf["workflow_id"])
            steps  = await _fetch_steps(handle)
            wf["steps"]           = steps
            wf["total_steps"]     = len(steps)
            wf["completed_steps"] = sum(1 for s in steps if s["status"] == "completed")
            running = [s for s in steps if s["status"] == "running"]
            wf["current_step"] = running[0]["display_name"] if running else None
        except Exception:
            wf["steps"] = []
        return wf

    workflows = list(await asyncio.gather(*[_add_steps(w) for w in workflows]))

    return response_format.success_response(200, "Workflows retrieved successfully.", workflows)




from service.pollingStatus import get_workflow_failure_message_simple
@router.get("/workflow-status/{workflow_id}")
async def workflow_status(workflow_id: str):
    """
    Ek workflow (agar wo fail ho gaya ho) ka simplified, human-readable
    failure-reason lo — raw Temporal stack-trace ki jagah ek short message.

    Response: {"failed": bool, "message": str|None, ...} (raw, koi APIResponse envelope nahi).
    """
    return await get_workflow_failure_message_simple(workflow_id)


@router.post("/pools/update-status")
def update_pool_status(
    pool_ids: List[int],
    status: str,
    db: Session = Depends(get_db)
):
    """
    Bulk — multiple pools ka status (enabled/disabled) ek saath set karo.

    Request body: pool_ids (list of int, JSON array body), query/body param `status`.
    Response (raw dict, koi APIResponse envelope nahi):
        {"message": "Status updated", "updated_pools": [int, ...]}
    Errors: 400 agar `status` "enabled"/"disabled" na ho, 404 agar koi bhi
    pool_id na mile.
    """
    if status not in ["enabled", "disabled"]:
        raise HTTPException(status_code=400, detail="Invalid status value")
    pools = db.query(models.Pool).filter(models.Pool.id.in_(pool_ids)).all()
    if not pools:
        raise HTTPException(status_code=404, detail="Pools not found")
    for pool in pools:
        pool.pool_status = status
    db.commit()
    return {"message": "Status updated", "updated_pools": [pool.id for pool in pools]}

@router.post('/add-tasks-data')
async def add_data(task,db: Session = Depends(get_db)):
    """
    Generic task-record create karo (internal/bookkeeping use).

    Response `data`: {"msg": str, "task": {...saved Task_DB record...}}
    """
    return controller.create_task_details(task,db)

from fastapi import APIRouter, Depends, Query, HTTPException, Request, Body
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Any, Optional

from controllers import llm_inference_controller
from db_configuration.config import get_db
from models.llm_inference_model import LLMInferenceJobCreate, LLMInferenceJobUpdate, PoolActionRequest
from models.API_Response_model import APIResponse

llm_inference_router = APIRouter(prefix="/v1/llm-inference", tags=["llm-inference"])


class DeleteRequest(BaseModel):
    totp_code: str = ""


@llm_inference_router.post("/create-private-llm", response_model=APIResponse[Any])
async def create_llm_inference_job(data: LLMInferenceJobCreate, db: Session = Depends(get_db)):
    """
    Create a new multi-node "private LLM" inference pool — provisions one or
    more Proxmox VMs, sets up GPU passthrough, and deploys a vLLM (or
    similar) server cluster on them (Temporal workflow, async).

    Request body: LLMInferenceJobCreate — clusterName, poolName, ipPools[],
    template, nodes[] ({node, gpu: [...]}), model/model_path, modelType, etc.

    Response 200 — `data`: {"job_id": int, "workflow_id": str, "status": "provisioning", ...}
    Errors: 400 if nodes/ipPools are empty or template is missing, 404 if
    clusterName/ipPool is not found, 409 if poolName already exists.
    """
    return await llm_inference_controller.create_llm_inference_job(data, db)


@llm_inference_router.get("/list-private-llm", response_model=APIResponse[Any])
def list_llm_inferences(
    page:      int = Query(1,  ge=1,          description="Page number"),
    page_size: int = Query(10, ge=1, le=100,  description="Items per page"),
    db: Session = Depends(get_db),
):
    """
    List all LLM inference jobs (pools) (paginated).

    Response 200 — `data`:
        {
          "items": [
            {
              "id": int, "name": str, "cluster_id": int, "cluster_name": str|None,
              "ip_pool_ids": [int], "ip_pool_names": [str|None],
              "template": str, "nodes": [...], "machines_name": [str],
              "storage": str, "vmids": [...], "ip_addresses": [...],
              "head_ip": str|None, "endpoint_url": str|None,
              "status": str, "workflow_id": str|None, "created_at": str
            }, ...
          ],
          "pagination": {"page": int, "page_size": int, "total": int, "total_pages": int, "has_next": bool, "has_prev": bool}
        }
    """
    return llm_inference_controller.list_llm_inferences(db, page, page_size)


@llm_inference_router.get("/deployed", response_model=APIResponse[Any])
def list_deployed_llm_jobs(
    page:      int = Query(1,  ge=1,         description="Page number"),
    page_size: int = Query(10, ge=1, le=100, description="Items per page"),
    db: Session = Depends(get_db),
):
    """
    List only successfully deployed private LLMs (status=running). Returns
    minimal fields for selection UI.

    Response 200 — `data`:
        {
          "items": [ {"id": int, "name": str, "machines_name": [str], "head_ip": str|None, "endpoint_url": str|None, "ip_addresses": [...]}, ... ],
          "pagination": {"page": int, "page_size": int, "total": int, "total_pages": int, "has_next": bool, "has_prev": bool}
        }
    """
    return llm_inference_controller.list_deployed_llm_jobs(db, page, page_size)


@llm_inference_router.get("/list-private-llm/{job_id}", response_model=APIResponse[Any])
def get_llm_inference_job(job_id: int, db: Session = Depends(get_db)):
    """
    Get the full detail of one LLM inference job (with per-machine enrichment).

    Response 200 — `data`: the same full record as `list_llm_inferences`, plus
    per-node machine details.
    Errors: 404 if job_id is not found.
    """
    return llm_inference_controller.get_llm_inference_job(job_id, db)


@llm_inference_router.put("/update-private-llm/{job_id}", response_model=APIResponse[Any])
def update_llm_inference_job(job_id: int, data: LLMInferenceJobUpdate, db: Session = Depends(get_db)):
    """
    Update a job's `model` and/or `status` field (a lightweight metadata
    update — doesn't touch the VMs/deployment).

    Request body: LLMInferenceJobUpdate = {"model": str|None, "status": str|None}
    Response 200 — `data`: the updated record.
    Errors: 404 if job_id is not found.
    """
    return llm_inference_controller.update_llm_inference_job(job_id, data, db)


@llm_inference_router.delete("/delete-private-llm/{job_id}", response_model=APIResponse[Any])
async def delete_llm_inference_job(
    job_id: int,
    request: Request,
    db: Session = Depends(get_db),
    body: Optional[DeleteRequest] = Body(default=None),
):
    """
    Delete an LLM inference job (VMs destroy + DB record remove).

    TOTP logic:
    - If the admin disabled OTP in Keycloak → delete directly, no OTP needed.
    - If the admin enabled OTP AND the user verified within the last 5 min → delete directly.
    - If the admin enabled OTP AND the user hasn't verified → pass totp_code in the body.

    Request body (conditional): DeleteRequest = {"totp_code": str}
    Header: Authorization (Bearer — required to identify the user).

    Response 200 — `data`: the deletion result.
    Errors: 401 missing/invalid token, 400 OTP required (totp_code not given),
    401 invalid TOTP, 404 job_id not found.
    """
    import jwt as _pyjwt
    from keycloak_configration import keycloak_config as key_config

    token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    if not token:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    try:
        jwt_payload = _pyjwt.decode(token, options={"verify_signature": False})
        user_id  = jwt_payload.get("sub")
        username = jwt_payload.get("preferred_username") or user_id
        if not user_id:
            raise HTTPException(status_code=401, detail="Cannot extract user id from token")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid JWT token")

    # Gate: only enforce OTP if admin has enabled it globally (same flag as /totp/get-enable-disable-guac)
    if key_config.get_Auth_flow_Value_browser():
        if not key_config.is_totp_recently_verified(user_id):
            totp_code = (body.totp_code if body else "") or ""
            if not totp_code:
                raise HTTPException(
                    status_code=400,
                    detail="OTP required. Call POST /v1/totp/verify-totp first, or send totp_code in request body.",
                )
            ok = key_config.verify_user_totp(user_id, totp_code, username=username)
            if not ok:
                raise HTTPException(status_code=401, detail="Invalid TOTP code — delete aborted")
            key_config.mark_totp_verified(user_id)

    return await llm_inference_controller.delete_llm_inference_job(job_id, db)


@llm_inference_router.post("/pool-action/{job_id}", response_model=APIResponse[Any])
async def pool_vm_action(job_id: int, data: PoolActionRequest, db: Session = Depends(get_db)):
    """
    Perform a power action on the whole pool (all nodes/VMs) (Temporal
    workflow, async — job.status immediately becomes "<action>ing").

    Request body: PoolActionRequest = {"action": "start"|"stop"|"shutdown"|"restart"}
    Response 200 — `data`: {"workflow_id": str, "status": str}
    Errors: 404 if job_id is not found, 400 if the pool has no VMs at all.
    """
    return await llm_inference_controller.pool_vm_action(job_id, data, db)

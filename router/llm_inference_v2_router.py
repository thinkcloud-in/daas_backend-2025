from fastapi import APIRouter, Depends, Query, HTTPException, Request, Body
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Any, Optional

from controllers import llm_inference_v2_controller
from db_configuration.config import get_db
from models.llm_inference_v2_model import LLMInferenceJobCreate, LLMInferenceJobUpdate, PoolActionRequest
from models.API_Response_model import APIResponse

llm_inference_v2_router = APIRouter(prefix="/v1/llm-inference-v2", tags=["llm-inference-v2"])


class DeleteRequest(BaseModel):
    totp_code: str = ""


@llm_inference_v2_router.post("/create-private-llm", response_model=APIResponse[Any])
async def create_llm_inference_job(data: LLMInferenceJobCreate, db: Session = Depends(get_db)):
    return await llm_inference_v2_controller.create_llm_inference_job(data, db)


@llm_inference_v2_router.get("/list-private-llm", response_model=APIResponse[Any])
def list_llm_inference_jobs(
    page:      int = Query(1,  ge=1,          description="Page number"),
    page_size: int = Query(10, ge=1, le=100,  description="Items per page"),
    db: Session = Depends(get_db),
):
    return llm_inference_v2_controller.list_llm_inference_jobs(db, page, page_size)


@llm_inference_v2_router.get("/deployed", response_model=APIResponse[Any])
def list_deployed_llm_jobs(
    page:      int = Query(1,  ge=1,         description="Page number"),
    page_size: int = Query(10, ge=1, le=100, description="Items per page"),
    db: Session = Depends(get_db),
):
    """List only successfully deployed private LLMs (status=running). Returns minimal fields for selection UI."""
    return llm_inference_v2_controller.list_deployed_llm_jobs(db, page, page_size)


@llm_inference_v2_router.get("/list-private-llm/{job_id}", response_model=APIResponse[Any])
def get_llm_inference_job(job_id: int, db: Session = Depends(get_db)):
    return llm_inference_v2_controller.get_llm_inference_job(job_id, db)


@llm_inference_v2_router.put("/update-private-llm/{job_id}", response_model=APIResponse[Any])
def update_llm_inference_job(job_id: int, data: LLMInferenceJobUpdate, db: Session = Depends(get_db)):
    return llm_inference_v2_controller.update_llm_inference_job(job_id, data, db)


@llm_inference_v2_router.delete("/delete-private-llm/{job_id}", response_model=APIResponse[Any])
async def delete_llm_inference_job(
    job_id: int,
    request: Request,
    db: Session = Depends(get_db),
    body: Optional[DeleteRequest] = Body(default=None),
):
    """
    Delete an LLM inference job.

    TOTP logic:
    - Admin ne Keycloak mein OTP disabled kiya hai → directly delete, no OTP needed.
    - Admin ne OTP enabled kiya hai AND user ne last 5 min mein verify kiya → directly delete.
    - Admin ne OTP enabled kiya hai AND user ne verify nahi kiya → totp_code body mein do.
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

    return await llm_inference_v2_controller.delete_llm_inference_job(job_id, db)


@llm_inference_v2_router.post("/pool-action/{job_id}", response_model=APIResponse[Any])
async def pool_vm_action(job_id: int, data: PoolActionRequest, db: Session = Depends(get_db)):
    return await llm_inference_v2_controller.pool_vm_action(job_id, data, db)



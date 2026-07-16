from fastapi import APIRouter, Depends, Form, Query, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Any, List, Optional

from controllers import library_controller
from controllers import lxc_restore_controller
from db_configuration.config import get_db
from models.API_Response_model import APIResponse

library_router = APIRouter(prefix="/v1/library", tags=["library"])


class LibraryUploadInit(BaseModel):
    name:      str
    file_name: str
    file_size: Optional[int] = None
    type:      Optional[str] = None
    version:   Optional[str] = None


@library_router.post("/upload", response_model=APIResponse[Any])
async def create_library_item(
    body:    LibraryUploadInit,
    request: Request,
    db:      Session = Depends(get_db),
):
    """
    Step 1 — send metadata as JSON, returns item_id within milliseconds.
    Then stream the actual file via PUT /v1/library/{item_id}/file.
    """
    return await library_controller.create_library_item(
        body.name, body.type, body.version, body.file_name, body.file_size, db, request
    )


@library_router.put("/{item_id}/file", response_model=APIResponse[Any])
async def upload_library_file(
    item_id: int,
    request: Request,
    db:      Session = Depends(get_db),
):
    """
    Step 2 — stream raw file bytes (Content-Type: application/octet-stream).
    DB progress_pct updates 0→99% in real-time as bytes arrive.
    Temporal workflow starts automatically once file is fully received (sets 100% + status=ready).
    """
    return await library_controller.upload_library_file(item_id, request, db)


@library_router.get("/list", response_model=APIResponse[Any])
def list_library_items(
    type:      Optional[str] = Query(None, description="Filter: base_os | devraq_agent | open_web_ui | lxc_backup | harbor_template | general"),
    page:      int           = Query(1,  ge=1),
    page_size: int           = Query(10, ge=1, le=100),
    db:        Session       = Depends(get_db),
):
    return library_controller.list_library_items(type, page, page_size, db)


# ── Deployment list/detail — static routes BEFORE /{item_id} so FastAPI
#    doesn't mistake "deployments" for an integer item_id ───────────────────

@library_router.get("/deployments", response_model=APIResponse[Any])
def list_deployments(
    page:      int = Query(1,  ge=1),
    page_size: int = Query(10, ge=1, le=100),
    db:        Session = Depends(get_db),
):
    return lxc_restore_controller.list_lxc_restore_jobs(db, page, page_size)


@library_router.get("/deployments/{job_id}", response_model=APIResponse[Any])
def get_deployment(job_id: int, db: Session = Depends(get_db)):
    return lxc_restore_controller.get_lxc_restore_job(job_id, db)


# ── Download ──────────────────────────────────────────────────────────────────

@library_router.get("/download/{item_id}")
def download_library_item(item_id: int, db: Session = Depends(get_db)):
    from fastapi.responses import RedirectResponse
    file_url, _ = library_controller.get_library_item_path(item_id, db)
    return RedirectResponse(url=file_url, status_code=302)


# ── Single item CRUD — parameterized routes last ──────────────────────────────

@library_router.get("/{item_id}", response_model=APIResponse[Any])
def get_library_item(item_id: int, db: Session = Depends(get_db)):
    return library_controller.get_library_item(item_id, db)


@library_router.put("/{item_id}", response_model=APIResponse[Any])
async def update_library_item(
    item_id: int,
    request: Request,
    name:    Optional[str] = Form(None, description="New display name"),
    version: Optional[str] = Form(None, description="New version string"),
    db:      Session       = Depends(get_db),
):
    return await library_controller.update_library_item(item_id, name, version, db, request)


@library_router.delete("/{item_id}", response_model=APIResponse[Any])
async def delete_library_item(item_id: int, request: Request, db: Session = Depends(get_db)):
    return await library_controller.delete_library_item(item_id, db, request)


# ── Deploy ────────────────────────────────────────────────────────────────────

class LibraryDeployBody(BaseModel):
    name:       str
    cluster_id: int
    ip_pools:   List[str]
    storage:    Optional[str] = "local-lvm"


@library_router.post("/{item_id}/deploy", response_model=APIResponse[Any])
async def deploy_library_item(
    item_id: int,
    body:    LibraryDeployBody,
    request: Request,
    db:      Session = Depends(get_db),
):
    """Deploy a library item (LXC backup) as a container on Proxmox."""
    return await library_controller.deploy_library_item(
        item_id, body.name, body.cluster_id, body.ip_pools, body.storage, db, request
    )



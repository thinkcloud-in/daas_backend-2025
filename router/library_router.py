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
    name:      Optional[str] = None   # container type: Harbor image name; required for the rest
    file_name: str
    file_size: Optional[int] = None
    type:      Optional[str] = None   # container | llm_model | llm_template | base_os | ...
    version:   Optional[str] = None   # for container: Harbor image tag
    # Harbor push — for container / llm_model / llm_template
    harbor_registry_id: Optional[int] = None  # kubernetes_deployments.id (Harbor instance)
    harbor_owner:       Optional[str] = None  # fallback owner segment (optional)
    owner_name:         Optional[str] = None  # for container: Harbor owner/org segment
    # Optional metadata JSON — pass it here so you don't need to embed it inside the ZIP
    # All fields end up as annotations in the Harbor Overview
    metadata: Optional[dict] = None


@library_router.post("/upload", response_model=APIResponse[Any])
async def create_library_item(
    body:    LibraryUploadInit,
    request: Request,
    db:      Session = Depends(get_db),
):
    """
    Step 1 — send the metadata JSON, get back an item_id within milliseconds.
    Then stream the file: PUT /v1/library/{item_id}/file

    For container / llm_model / llm_template:
      harbor_registry_id = kubernetes_deployments.id (Harbor instance)
      The backend automatically derives the K8s cluster from the Harbor record.
      name is optional — it'll be auto-set from the Docker image metadata.

    Request body: LibraryUploadInit (see above).

    Response 200 — `data`:
        {
          "id": int,            # new library-item ID, use it in the next step (file upload)
          "status": "uploading"
        }

    Errors: 400 if `type` is invalid or a required field is missing.
    """
    return await library_controller.create_library_item(
        body.name, body.type, body.version, body.file_name, body.file_size,
        db, request,
        harbor_registry_id=body.harbor_registry_id,
        harbor_owner=body.harbor_owner,
        owner_name=body.owner_name,
        metadata=body.metadata,
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

    Path param: item_id — the ID from Step 1 (`POST /upload`).
    Request body: raw binary stream (file bytes), not JSON.

    Response 200 — `data`: the full updated library-item dict (see `_item_to_dict`
    — id, name, type, version, file_name, file_size, progress_pct, status,
    workflow_id, harbor_* fields, created_at, updated_at).
    For container/llm_model/llm_template, the Harbor-push workflow is already
    triggered in the background before this response even comes back.

    Errors: 404 if item_id is not found, 409 if the item is already "ready"/"failed".
    """
    return await library_controller.upload_library_file(item_id, request, db)



@library_router.get("/list", response_model=APIResponse[Any])
def list_library_items(
    type:               Optional[str] = Query(None, description="Filter: base_os | container | llm_model | llm_template | openwebui | vectordb | postgresql | ..."),
    owner:              Optional[str] = Query(None, description="Filter by harbor_owner (partial match, case-insensitive)"),
    harbor_registry_id: Optional[int] = Query(None, description="Filter by harbor registry (kubernetes_deployments.id)"),
    page:               int           = Query(1,  ge=1),
    page_size:          int           = Query(10, ge=1, le=100),
    db:                 Session       = Depends(get_db),
):
    """
    List library items, filtered by type/owner/registry, grouped by directory.

    Response 200 — `data`:
        {
          "filters": [ {"type": str, "label": str, "count": int}, ... ],
          "directories": {
            "<dir_name>": [ <item>, ... ],   # e.g. "container", "llm_model", "harbor", "os", "general"
            ...
          },
          "total": int,          # count of all filtered items (independent of page)
          "pagination": {
            "page": int, "page_size": int, "total": int, "total_pages": int,
            "has_next": bool, "has_prev": bool
          }
        }

    Each `<item>` contains (LibraryItem + attached deployments):
        id, name, display_name, type, directory, version, description,
        category, tags, file_name, file_path, file_size, progress_pct,
        status, workflow_id, k8s_cluster_id, harbor_url, harbor_project,
        harbor_owner, harbor_registry_id, harbor_image, push_status,
        push_error, push_workflow_id, created_at, updated_at,
        deployments: [ {..LXC/K8s restore-job summary..}, ... ]
    """
    return library_controller.list_library_items(
        type, page, page_size, db,
        owner_filter=owner,
        harbor_registry_id=harbor_registry_id,
    )


# ── Deployment list/detail — static routes BEFORE /{item_id} so FastAPI
#    doesn't mistake "deployments" for an integer item_id ───────────────────

@library_router.get("/deployments", response_model=APIResponse[Any])
def list_deployments(
    page:      int = Query(1,  ge=1),
    page_size: int = Query(10, ge=1, le=100),
    db:        Session = Depends(get_db),
):
    """
    List LXC/Kubernetes jobs deployed from library items (paginated).

    Response 200 — `data`:
        {
          "items": [ {..deployment-job summary..}, ... ],
          "pagination": {"page": int, "page_size": int, "total": int, "total_pages": int, "has_next": bool, "has_prev": bool}
        }
    """
    return lxc_restore_controller.list_lxc_restore_jobs(db, page, page_size)


@library_router.get("/deployments/{job_id}", response_model=APIResponse[Any])
def get_deployment(
    job_id: int,
    type:   Optional[str] = Query(None, description="lxc | kubernetes — disambiguate when both tables share an ID"),
    db:     Session = Depends(get_db),
):
    """
    Get the full detail of one deployment job (LXC restore or K8s Harbor deploy).

    Response 200 — `data`: the deployment-job record (status, target cluster,
    IP/namespace, timestamps, error if any).

    Errors: 404 if job_id (with the given type) is not found.
    """
    return lxc_restore_controller.get_lxc_restore_job(job_id, db, deployment_type=type)


@library_router.delete("/deployments/{job_id}", response_model=APIResponse[Any])
def delete_deployment(job_id: int, db: Session = Depends(get_db)):
    """
    Delete a deployment job record (tracking record only — doesn't destroy
    the deployed LXC/K8s resource itself).

    Response 200 — `data`: {"id": job_id}
    Errors: 404 if job_id is not found.
    """
    return lxc_restore_controller.delete_deployment(job_id, db)


# ── Download ──────────────────────────────────────────────────────────────────

@library_router.get("/download/{item_id}")
def download_library_item(item_id: int, db: Session = Depends(get_db)):
    """
    Download a library item's file — issues a 302 redirect straight to the
    WebDAV storage URL (does not return a JSON response).

    Response: 302 Redirect → Location header has the actual file URL.
    Errors: 404 if item_id is not found, 409 if the item's status isn't
    "ready" yet (upload not complete).
    """
    from fastapi.responses import RedirectResponse
    file_url, _ = library_controller.get_library_item_path(item_id, db)
    return RedirectResponse(url=file_url, status_code=302)


# ── Harbor artifacts ─────────────────────────────────────────────────────────

@library_router.get("/harbor/{registry_id}/artifacts", response_model=APIResponse[Any])
def get_harbor_artifacts(
    registry_id:   int,
    project:       Optional[str] = Query(None, description="Harbor project name — specify to get the repositories list"),
    repository:    Optional[str] = Query(None, description="Repository name — specify with project to get the artifacts (tags) list"),
    owner:         Optional[str] = Query(None, description="Owner prefix filter — only show repositories for this owner (e.g. raqsoft)"),
    type_filter:   Optional[str] = Query(None, alias="type", description="Filter by ai.artifact.type (e.g. template, backup)"),
    hypervisor:    Optional[str] = Query(None, description="Filter by ai.artifact.hypervisor (e.g. proxmox, vmware, hyper-v)"),
    os_type:       Optional[str] = Query(None, description="Filter by os_type field in vm_template_details (e.g. linux, windows)"),
    os_name:       Optional[str] = Query(None, description="Filter by os_name field in vm_template_details (e.g. ubuntu, centos)"),
    page:          int           = Query(1,  ge=1),
    page_size:     int           = Query(20, ge=1, le=100),
    db:            Session       = Depends(get_db),
):
    """
    Fetch artifacts deployed on Harbor. This is a "drill-down" endpoint —
    the more query params you give, the deeper the response:

    - `registry_id` alone                                         → list of all projects
    - `registry_id` + `project`                                   → list of repositories in that project
    - `registry_id` + `project` + `owner`                         → only that owner's repositories
    - `registry_id` + `project` + `repository`                    → list of artifacts (tags) in that repo
    - `registry_id` + `project` + filter params                   → cross-repo filtered artifacts
      Filters: artifact_type, hypervisor, os_type, os_name (partial match, case-insensitive)

    Response 200 — `data` (shape depends on the mode above):
        Projects mode:     {"projects": [{"name": str}, ...]}
        Repositories mode: {"repositories": [{"name": str, "artifact_count": int}, ...]}
        Artifacts mode:    {
                              "items": [ {tag, digest, size, push_time, ...annotations} , ...],
                              "pagination": {page, page_size, total, total_pages, has_next, has_prev}
                            }

    Errors: 404 if registry_id is not found or Harbor is unreachable.
    """
    return library_controller.list_harbor_artifacts(
        registry_id=registry_id,
        db=db,
        project=project,
        repository=repository,
        owner=owner,
        type_filter=type_filter,
        hypervisor=hypervisor,
        os_type=os_type,
        os_name=os_name,
        page=page,
        page_size=page_size,
    )


# ── Single item CRUD — parameterized routes last ──────────────────────────────

@library_router.get("/{item_id}", response_model=APIResponse[Any])
def get_library_item(item_id: int, db: Session = Depends(get_db)):
    """
    Get the full detail of one library item.

    Response 200 — `data`: an `<item>` dict (see `list_library_items`
    docstring for the exact shape), plus the attached `deployments` list.

    Errors: 404 if item_id is not found.
    """
    return library_controller.get_library_item(item_id, db)


@library_router.put("/{item_id}", response_model=APIResponse[Any])
async def update_library_item(
    item_id: int,
    request: Request,
    name:    Optional[str] = Form(None, description="New display name"),
    version: Optional[str] = Form(None, description="New version string"),
    db:      Session       = Depends(get_db),
):
    """
    Update a library item's name/version (multipart form fields, not JSON).
    Doesn't touch the file/Harbor image — this only renames metadata
    (via a Temporal workflow, async).

    Response 200 — `data`: the updated `<item>` dict (returned immediately,
    at workflow-start time — the actual DB write happens a bit later in the background).

    Errors: 404 if item_id is not found, 409 if the item is still in "uploading" state.
    """
    return await library_controller.update_library_item(item_id, name, version, db, request)


@library_router.delete("/{item_id}", response_model=APIResponse[Any])
async def delete_library_item(item_id: int, request: Request, db: Session = Depends(get_db)):
    """
    Delete a library item.

    - status "uploading"/"failed"  → deleted immediately from DB + temp file (sync).
    - status "ready"               → deleted via a Temporal workflow: if it
      was pushed to Harbor, the image/artifact is removed from Harbor first
      (if that fails, the DB record is NOT deleted, it'll retry), then the
      pod file, then the DB record.

    Response 200 — `data`:
        {"id": item_id}                                   # sync path
        {"id": item_id, "workflow_id": str}                # async (Temporal) path

    Errors: 404 if item_id is not found.
    """
    return await library_controller.delete_library_item(item_id, db, request)


# ── Deploy ────────────────────────────────────────────────────────────────────

class LibraryDeployBody(BaseModel):
    deployment_type: str               # "lxc" or "kubernetes"
    name:            str               # Deployment name
    cluster_id:      int               # LXC: Proxmox cluster ID | K8s: kubernetes_clusters ID

    # For LXC (required when deployment_type="lxc")
    ip_pools:        Optional[List[str]] = None
    storage:         Optional[str]       = "local-lvm"

    # For K8s (optional, has defaults)
    namespace:       Optional[str] = "harbor"
    http_port:       Optional[int] = 80


@library_router.post("/{item_id}/deploy", response_model=APIResponse[Any])
async def deploy_library_item(
    item_id: int,
    body:    LibraryDeployBody,
    request: Request,
    db:      Session = Depends(get_db),
):
    """
    Deploy a library item:
    - deployment_type="lxc"        → deploy a Proxmox LXC machine (existing)
    - deployment_type="kubernetes" → deploy Harbor onto a K8s cluster (new)

    Request body: LibraryDeployBody (see above) — `ip_pools`/`storage` only
    needed for lxc, `namespace`/`http_port` only for kubernetes.

    Response 200 — `data`:
        lxc:        {"job_id": int, "workflow_id": str, "status": "started", ...}
        kubernetes: {"deployment_id": int, "workflow_id": str, "namespace": str, ...}

    Errors: 404 if item_id/cluster_id is not found, 409 if the item's status
    isn't "ready", 400 if none of the given ip_pools has a free IP (for lxc).
    """
    return await library_controller.deploy_library_item(
        item_id  = item_id,
        body     = body.model_dump(),
        db       = db,
        request  = request,
    )

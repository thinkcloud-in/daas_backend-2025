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
    name:      Optional[str] = None   # container type: Harbor image name; baaki ke liye required
    file_name: str
    file_size: Optional[int] = None
    type:      Optional[str] = None   # container | llm_model | llm_template | base_os | ...
    version:   Optional[str] = None   # container ke liye: Harbor image tag
    # Harbor push — container / llm_model / llm_template ke liye
    harbor_registry_id: Optional[int] = None  # kubernetes_deployments.id (Harbor instance)
    harbor_owner:       Optional[str] = None  # fallback owner segment (optional)
    owner_name:         Optional[str] = None  # container ke liye: Harbor owner/org segment
    # Optional metadata JSON — yahan pass karo to ZIP ke andar rakhne ki zaroorat nahi
    # Saare fields Harbor Overview mein annotations ke roop mein jayenge
    metadata: Optional[dict] = None


@library_router.post("/upload", response_model=APIResponse[Any])
async def create_library_item(
    body:    LibraryUploadInit,
    request: Request,
    db:      Session = Depends(get_db),
):
    """
    Step 1 — metadata JSON bhejo, milliseconds mein item_id milta hai.
    Phir file stream karo: PUT /v1/library/{item_id}/file

    container / llm_model / llm_template ke liye:
      harbor_registry_id = kubernetes_deployments.id (Harbor instance)
      Backend automatically K8s cluster derive karega Harbor record se.
      name optional hai — Docker image metadata se auto-set hoga.

    Request body: LibraryUploadInit (upar dekho).

    Response 200 — `data`:
        {
          "id": int,            # naya library-item ID, agle step (file upload) mein use karo
          "status": "uploading"
        }

    Errors: 400 agar `type` invalid ho ya required field missing ho.
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

    Path param: item_id — Step 1 (`POST /upload`) se mila ID.
    Request body: raw binary stream (file bytes), koi JSON nahi.

    Response 200 — `data`: poora updated library-item dict (dekho `_item_to_dict`
    — id, name, type, version, file_name, file_size, progress_pct, status,
    workflow_id, harbor_* fields, created_at, updated_at).
    container/llm_model/llm_template ke liye response mile-milne se pehle hi
    background mein Harbor-push workflow bhi trigger ho jaata hai.

    Errors: 404 agar item_id na mile, 409 agar item already "ready"/"failed" ho.
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
    Library items list karo, type/owner/registry se filter karke, directory ke
    hisaab se grouped.

    Response 200 — `data`:
        {
          "filters": [ {"type": str, "label": str, "count": int}, ... ],
          "directories": {
            "<dir_name>": [ <item>, ... ],   # e.g. "container", "llm_model", "harbor", "os", "general"
            ...
          },
          "total": int,          # saare filtered items ka count (page se independent)
          "pagination": {
            "page": int, "page_size": int, "total": int, "total_pages": int,
            "has_next": bool, "has_prev": bool
          }
        }

    Har `<item>` mein (LibraryItem + attached deployments):
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
    Library items se deploy kiye gaye LXC/Kubernetes jobs list karo (paginated).

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
    type:   Optional[str] = Query(None, description="lxc | kubernetes — dono tables mein same ID ho to disambiguate karo"),
    db:     Session = Depends(get_db),
):
    """
    Ek deployment job ki poori detail lo (LXC restore ya K8s Harbor deploy).

    Response 200 — `data`: deployment-job record (status, target cluster,
    IP/namespace, timestamps, error agar koi ho).

    Errors: 404 agar job_id (given type ke saath) na mile.
    """
    return lxc_restore_controller.get_lxc_restore_job(job_id, db, deployment_type=type)


@library_router.delete("/deployments/{job_id}", response_model=APIResponse[Any])
def delete_deployment(job_id: int, db: Session = Depends(get_db)):
    """
    Deployment job record delete karo (sirf tracking record — deployed
    LXC/K8s resource ko khud se destroy nahi karta).

    Response 200 — `data`: {"id": job_id}
    Errors: 404 agar job_id na mile.
    """
    return lxc_restore_controller.delete_deployment(job_id, db)


# ── Download ──────────────────────────────────────────────────────────────────

@library_router.get("/download/{item_id}")
def download_library_item(item_id: int, db: Session = Depends(get_db)):
    """
    Library item ki file download karo — seedha WebDAV storage URL pe
    302-redirect karta hai (JSON response nahi deta).

    Response: 302 Redirect → Location header mein actual file URL.
    Errors: 404 agar item_id na mile, 409 agar item ka status "ready" na ho
    (upload abhi complete nahi hua).
    """
    from fastapi.responses import RedirectResponse
    file_url, _ = library_controller.get_library_item_path(item_id, db)
    return RedirectResponse(url=file_url, status_code=302)


# ── Harbor artifacts ─────────────────────────────────────────────────────────

@library_router.get("/harbor/{registry_id}/artifacts", response_model=APIResponse[Any])
def get_harbor_artifacts(
    registry_id:   int,
    project:       Optional[str] = Query(None, description="Harbor project name — specify karo to repositories list mile"),
    repository:    Optional[str] = Query(None, description="Repository name — project ke saath specify karo to artifacts (tags) list mile"),
    owner:         Optional[str] = Query(None, description="Owner prefix filter — sirf is owner ki repositories dikhao (e.g. raqsoft)"),
    type_filter:   Optional[str] = Query(None, alias="type", description="Filter by ai.artifact.type (e.g. template, backup)"),
    hypervisor:    Optional[str] = Query(None, description="Filter by ai.artifact.hypervisor (e.g. proxmox, vmware, hyper-v)"),
    os_type:       Optional[str] = Query(None, description="Filter by os_type field in vm_template_details (e.g. linux, windows)"),
    os_name:       Optional[str] = Query(None, description="Filter by os_name field in vm_template_details (e.g. ubuntu, centos)"),
    page:          int           = Query(1,  ge=1),
    page_size:     int           = Query(20, ge=1, le=100),
    db:            Session       = Depends(get_db),
):
    """
    Harbor pe deploy hue artifacts fetch karo. Yeh ek "drill-down" endpoint
    hai — jitne query params doge, utna deep response milega:

    - `registry_id` alone                                         → sab projects list
    - `registry_id` + `project`                                   → us project ke repositories list
    - `registry_id` + `project` + `owner`                        → sirf us owner ki repositories
    - `registry_id` + `project` + `repository`                   → us repo ke artifacts (tags) list
    - `registry_id` + `project` + filter params                  → cross-repo filtered artifacts
      Filters: artifact_type, hypervisor, os_type, os_name (partial match, case-insensitive)

    Response 200 — `data` (mode ke hisaab se shape badalta hai):
        Projects mode:     {"projects": [{"name": str}, ...]}
        Repositories mode: {"repositories": [{"name": str, "artifact_count": int}, ...]}
        Artifacts mode:    {
                              "items": [ {tag, digest, size, push_time, ...annotations} , ...],
                              "pagination": {page, page_size, total, total_pages, has_next, has_prev}
                            }

    Errors: 404 agar registry_id na mile ya Harbor unreachable ho.
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
    Ek library item ki poori detail lo.

    Response 200 — `data`: ek `<item>` dict (dekho `list_library_items` ke
    docstring mein exact shape), plus attached `deployments` list.

    Errors: 404 agar item_id na mile.
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
    Library item ka naam/version update karo (multipart form fields, JSON nahi).
    File/Harbor-image ko touch nahi karta — sirf metadata rename hoti hai
    (Temporal workflow ke through, async).

    Response 200 — `data`: updated `<item>` dict (turant, workflow-start ke
    saath — asli DB-write background mein thodi der baad hoti hai).

    Errors: 404 agar item_id na mile, 409 agar item abhi "uploading" state mein ho.
    """
    return await library_controller.update_library_item(item_id, name, version, db, request)


@library_router.delete("/{item_id}", response_model=APIResponse[Any])
async def delete_library_item(item_id: int, request: Request, db: Session = Depends(get_db)):
    """
    Library item delete karo.

    - status "uploading"/"failed" ho    → turant DB + temp-file se delete (sync).
    - status "ready" ho                 → Temporal workflow se delete: agar
      Harbor pe push hua tha to pehle Harbor se image/artifact hataya jaata
      hai (fail ho to DB record delete nahi hota, retry hoga), phir pod file,
      phir DB record.

    Response 200 — `data`:
        {"id": item_id}                                   # sync path
        {"id": item_id, "workflow_id": str}                # async (Temporal) path

    Errors: 404 agar item_id na mile.
    """
    return await library_controller.delete_library_item(item_id, db, request)


# ── Deploy ────────────────────────────────────────────────────────────────────

class LibraryDeployBody(BaseModel):
    deployment_type: str               # "lxc" ya "kubernetes"
    name:            str               # Deployment ka naam
    cluster_id:      int               # LXC: Proxmox cluster ID | K8s: kubernetes_clusters ID

    # LXC ke liye (deployment_type="lxc" me required)
    ip_pools:        Optional[List[str]] = None
    storage:         Optional[str]       = "local-lvm"

    # K8s ke liye (optional, defaults hain)
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
    Library item deploy karo:
    - deployment_type="lxc"        → Proxmox LXC machine deploy (existing)
    - deployment_type="kubernetes" → K8s cluster pe Harbor deploy (new)

    Request body: LibraryDeployBody (upar dekho) — `ip_pools`/`storage` sirf
    lxc ke liye zaroori, `namespace`/`http_port` sirf kubernetes ke liye.

    Response 200 — `data`:
        lxc:        {"job_id": int, "workflow_id": str, "status": "started", ...}
        kubernetes: {"deployment_id": int, "workflow_id": str, "namespace": str, ...}

    Errors: 404 agar item_id/cluster_id na mile, 409 agar item ka status
    "ready" na ho, 400 agar lxc ke liye ip_pools mein koi free IP na ho.
    """
    return await library_controller.deploy_library_item(
        item_id  = item_id,
        body     = body.model_dump(),
        db       = db,
        request  = request,
    )

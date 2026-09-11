"""
Library (file/model/container/template repository) controller —
router/library_router.py ("/v1/library") delegates to this. It is the
largest and most central controller:

- 2-step upload flow: `create_library_item` (DB record + item_id) →
  `upload_library_file` (raw bytes stream — routed to WebDAV/local-temp per
  item.type, then triggers a type-specific push workflow:
  LibraryUploadWorkflow / HarborPushWorkflow (container) / LLMPushWorkflow
  (llm_model, llm_template)).
- Harbor artifacts browsing (`list_harbor_artifacts`) — from both the Harbor
  REST + OCI Distribution API, with drill-down (projects → repos → artifacts).
- Deploy — deploy a library item to an LXC (Proxmox) or Kubernetes (Harbor)
  target, via Temporal workflows.
- Update/Delete — see `update_library_item`/`delete_library_item` (delete also
  does Harbor cleanup — that logic is in activities_library.py).
"""
import asyncio
import logging
import os
import queue
import requests
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import List

from fastapi import HTTPException, Request
from sqlalchemy.orm import Session
from temporalio.common import SearchAttributeKey, SearchAttributePair, TypedSearchAttributes

from models.library_model import LibraryItem, LIBRARY_TYPES, TYPE_SUBDIR, POD_DIRS
from models.lxc_restore_model import LXCRestoreJob
from models.IPs_model import IPEntry, IPSModel
from models.models import Cluster
from service.temporalResource.workers.workers_library import TASK_QUEUE as LIBRARY_TASK_QUEUE
from service.temporalResource.workers.workers_lxc_restore import TASK_QUEUE as LXC_TASK_QUEUE
from service.temporalResource.workers.workers_harbor_push import TASK_QUEUE as HARBOR_PUSH_TASK_QUEUE
from service.temporalResource.workers.workers_llm_push import TASK_QUEUE as LLM_PUSH_TASK_QUEUE
from service.temporalResource.workflows.workflows_llm_push import LLMPushWorkflow
from service.temporalResource.workflows.workflows_library import (
    LibraryUploadWorkflow,
    LibraryDeleteWorkflow,
    LibraryUpdateWorkflow,
)
from service.temporalResource.workflows.workflows_lxc_restore import LXCRestoreWorkflow
from service.temporalResource.workflows.workflows_harbor_push import HarborPushWorkflow
from utils.temporal_client import TemporalClientManager
from utils import response_format

_ENTITY_KEY   = SearchAttributeKey.for_keyword("Entity")
_ACTION_KEY   = SearchAttributeKey.for_keyword("Action")
_USERNAME_KEY = SearchAttributeKey.for_keyword("UserName")


def _make_search_attrs(entity: str, action: str, username: str) -> TypedSearchAttributes:
    """Build Entity/Action/UserName search-attributes for a Temporal workflow (for the workflow-monitoring UI)."""
    return TypedSearchAttributes([
        SearchAttributePair(_ENTITY_KEY,   entity),
        SearchAttributePair(_ACTION_KEY,   action),
        SearchAttributePair(_USERNAME_KEY, username),
    ])

logger = logging.getLogger(__name__)

LIBRARY_BASE_PATH  = os.getenv("LIBRARY_BASE_PATH", "/data/library")
LIBRARY_TEMP_PATH  = os.getenv("LIBRARY_TEMP_PATH", "/tmp/library_uploads")
_HARBOR_PUSH_TYPES = {"container", "llm_model", "llm_template", "postgresql"}  # Harbor push types


def _extract_username(request: Request) -> str:
    """Extract preferred_username (or sub) from the Bearer JWT, for audit/search-attributes. Falls back to "system" if the token is missing/invalid."""
    try:
        import jwt as _pyjwt
        token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
        if token:
            payload = _pyjwt.decode(token, options={"verify_signature": False})
            return payload.get("preferred_username") or payload.get("sub") or "system"
    except Exception:
        pass
    return "system"


def _item_to_dict(item: LibraryItem) -> dict:
    """
    Convert a LibraryItem ORM row into an API dict — `harbor_user`/`harbor_pass`
    are NOT included here (already safe), only non-secret Harbor metadata like
    `harbor_image`/`harbor_url`/`push_status` goes out.
    """
    directory = TYPE_SUBDIR.get(item.type, "general")
    return {
        "id":              item.id,
        "name":            item.name,
        "display_name":    getattr(item, "display_name", None),
        "type":            item.type,
        "directory":       directory,
        "version":         item.version,
        "description":     getattr(item, "description", None),
        "category":        getattr(item, "category", None),
        "tags":            getattr(item, "tags", None),
        "file_name":       item.file_name,
        "file_path":       item.file_path,
        "file_size":       item.file_size,
        "progress_pct":    item.progress_pct,
        "status":          item.status,
        "workflow_id":     item.workflow_id,
        # Harbor push fields
        "k8s_cluster_id":  item.k8s_cluster_id,
        "harbor_url":      item.harbor_url,
        "harbor_project":  item.harbor_project,
        "harbor_owner":    item.harbor_owner,
        "harbor_registry_id": item.harbor_registry_id,
        "harbor_image":       item.harbor_image,
        "push_status":        item.push_status,
        "push_error":         item.push_error,
        "push_workflow_id":   item.push_workflow_id,
        "created_at":         item.created_at.isoformat() if item.created_at else None,
        "updated_at":         item.updated_at.isoformat() if item.updated_at else None,
    }


async def create_library_item(
    name:               str | None,
    type:               str | None,
    version:            str | None,
    file_name:          str,
    file_size:          int | None,
    db:                 Session,
    request:            Request,
    harbor_registry_id: int | None = None,
    harbor_owner:       str | None = None,
    owner_name:         str | None = None,
    metadata:           dict | None = None,
):
    """
    Step 1 — create the DB record; the item_id comes back in milliseconds.
    The frontend then streams the file via PUT /{item_id}/file.

    For container / llm_model / llm_template:
      harbor_registry_id = kubernetes_deployments.id (Harbor instance)
      The backend derives the K8s cluster automatically.
      name = optional — for container type it is auto-set from the Docker image metadata.
    """
    from models.kubernetes_deploy_model import KubernetesDeployment

    effective_type = (type or "general").strip()
    if effective_type not in LIBRARY_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid type '{effective_type}'. Valid: {', '.join(sorted(LIBRARY_TYPES))}",
        )

    # for container type the name is optional — use a placeholder, the activity will update it
    # Types where the name is auto-set from the ZIP's version_metadata.json
    _AUTO_NAME_TYPES = _HARBOR_PUSH_TYPES | {"harbor_template", "general", "base_os"}

    effective_name = (name or "").strip()
    if not effective_name:
        if effective_type in _AUTO_NAME_TYPES:
            # the name will be set from the ZIP/metadata after upload — file stem as placeholder
            effective_name = os.path.splitext(file_name)[0]
        else:
            raise HTTPException(status_code=400, detail="'name' field required for this type")

    subdir    = TYPE_SUBDIR[effective_type]
    dest_path = f"{LIBRARY_BASE_PATH}/{subdir}/{file_name}"
    temp_path = os.path.join(LIBRARY_TEMP_PATH, f"{uuid.uuid4().hex}_{file_name}")

    # harbor_registry_id is required for Harbor push types
    k8s_cluster_id = None
    if effective_type in _HARBOR_PUSH_TYPES:
        if not harbor_registry_id:
            raise HTTPException(
                status_code=400,
                detail=f"'harbor_registry_id' required for type '{effective_type}' — select a Harbor instance",
            )
    if effective_type in _HARBOR_PUSH_TYPES and harbor_registry_id:
        harbor_dep = db.query(KubernetesDeployment).filter(
            KubernetesDeployment.id == harbor_registry_id
        ).first()
        if not harbor_dep:
            raise HTTPException(
                status_code=404,
                detail=f"Harbor instance id={harbor_registry_id} was not found in kubernetes_deployments"
            )
        if not harbor_dep.harbor_url:
            raise HTTPException(
                status_code=409,
                detail=f"Harbor id={harbor_registry_id} has no harbor_url set — deploy it first"
            )
        k8s_cluster_id = harbor_dep.cluster_id   # the backend derives it
        logger.info(f"[Library] harbor_registry={harbor_registry_id} → k8s_cluster={k8s_cluster_id} derived")

    # for container: the owner_name field overrides harbor_owner
    if effective_type == "container" and owner_name:
        harbor_owner = owner_name.strip() or harbor_owner

    # for Harbor push types the version comes from the image inspect
    # Exception: if the user explicitly gave a version for container type, use it (it becomes the tag in Harbor)
    if effective_type == "container" and version:
        effective_version = version.strip() or None
    elif effective_type in _HARBOR_PUSH_TYPES and harbor_registry_id:
        effective_version = None
    else:
        effective_version = version or None

    import json as _json
    _meta_json = None
    if metadata:
        try:
            _meta_json = _json.dumps(metadata, ensure_ascii=False)
        except Exception:
            pass

    record = LibraryItem(
        name               = effective_name,
        type               = effective_type,
        version            = effective_version,
        file_name          = file_name,
        file_path          = dest_path,
        file_size          = file_size,
        progress_pct       = 0,
        status             = "uploading",
        harbor_registry_id = harbor_registry_id,
        k8s_cluster_id     = k8s_cluster_id,
        harbor_owner       = harbor_owner,
        push_status        = "pending" if harbor_registry_id else None,
        # metadata_json: the JSON passed from the upload API — used by the push activity for annotations
        display_name       = str(metadata.get("display_name", "")).strip() or None if metadata else None,
        description        = str(metadata.get("description", "")).strip() or None if metadata else None,
        category           = str(metadata.get("category", "")).strip() or None if metadata else None,
        metadata_json      = _meta_json,
    )
    db.add(record)
    db.commit()
    db.refresh(record)

    logger.info(f"[Library] item={record.id} created type={effective_type} harbor_reg={harbor_registry_id}")
    return response_format.success_response(
        202,
        "Library item created — stream file to PUT /v1/library/{item_id}/file",
        {**_item_to_dict(record), "temp_path": temp_path},
    )


def _update_progress_in_db(item_id: int, pct: int, db_session):
    """Update progress in the DB — not thread-safe, only call from the upload thread."""
    try:
        item = db_session.query(LibraryItem).filter(LibraryItem.id == item_id).first()
        if item:
            item.progress_pct = pct
            db_session.commit()
    except Exception as exc:
        logger.warning(f"[Library] progress DB update failed item={item_id}: {exc}")
        db_session.rollback()


def _put_file_to_webdav_with_progress(
    item_id:     int,
    temp_path:   str,
    url:         str,
    total_bytes: int,
    db_session,
    pct_start:   int = 50,
    pct_end:     int = 99,
    chunk_size:  int = 8 * 1024 * 1024,
):
    """
    Stream Phase 2 (backend temp file -> WebDAV storage) chunk-by-chunk, so
    that progress_pct updates granularly (1% steps) from pct_start to pct_end
    -- previously the whole file went in a single blocking PUT, during which
    progress showed stuck at 50% (for large files this looked "stuck" for a
    long time).

    This runs in a separate thread via asyncio.to_thread() (while the caller
    coroutine just awaits and does not touch the DB session concurrently
    elsewhere) — so it is safe to use db_session from here.
    """
    last_bucket = pct_start - 1
    sent = 0

    def _chunks():
        nonlocal sent, last_bucket
        with open(temp_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                sent += len(chunk)
                yield chunk
                if total_bytes > 0:
                    frac = min(sent / total_bytes, 1.0)
                    pct = pct_start + int(frac * (pct_end - pct_start))
                    if pct > last_bucket:
                        last_bucket = pct
                        _update_progress_in_db(item_id, pct, db_session)

    return requests.put(
        url,
        data=_chunks(),
        headers={"Content-Type": "application/octet-stream"},
        verify=False,
        timeout=None,
    )


async def _upload_harbor_direct_to_webdav(
    record,
    request:    Request,
    db:         Session,
    username:   str,
    total_size: int,
):
    """Container type: browser stream → WebDAV seedha (no /tmp disk write)."""
    import httpx as _httpx

    subdir      = TYPE_SUBDIR.get(record.type, "container")
    storage_url = (
        os.getenv("STORAGE_INTERNAL_URL", "").rstrip("/")
        or os.getenv("STORAGE_BASE_URL",  "").rstrip("/")
    )
    if not storage_url:
        raise HTTPException(
            status_code=500,
            detail="STORAGE_INTERNAL_URL / STORAGE_BASE_URL not configured",
        )

    webdav_url = f"{storage_url}/{subdir}/{record.file_name}"
    pod_path   = f"{LIBRARY_BASE_PATH}/{subdir}/{record.file_name}"
    bytes_written = 0
    last_bucket   = -1

    async def _stream_gen():
        nonlocal bytes_written, last_bucket
        async for chunk in request.stream():
            bytes_written += len(chunk)
            yield chunk
            if total_size > 0:
                pct = min(int(bytes_written / total_size * 100), 99)
                if pct > last_bucket:
                    last_bucket          = pct
                    record.progress_pct  = pct
                    db.commit()

    try:
        async with _httpx.AsyncClient(verify=False, timeout=None) as client:
            resp = await client.put(
                webdav_url,
                content=_stream_gen(),
                headers={"Content-Type": "application/octet-stream"},
            )
        if resp.status_code not in (200, 201, 204):
            raise RuntimeError(
                f"WebDAV PUT failed: {resp.status_code} {resp.text[:200]}"
            )
    except Exception as exc:
        logger.error(f"[Library] Container WebDAV upload failed item={record.id}: {exc}")
        try:
            async with _httpx.AsyncClient(verify=False, timeout=10) as c:
                await c.delete(webdav_url)
        except Exception:
            pass
        record.status     = "failed"
        record.push_error = str(exc)
        db.commit()
        raise HTTPException(status_code=500, detail=f"Storage upload failed: {exc}")

    logger.info(f"[Library] Container WebDAV OK: {webdav_url} ({bytes_written:,} bytes)")
    record.file_path    = pod_path
    record.file_size    = bytes_written or total_size or None
    record.progress_pct = 100
    record.status       = "ready"
    db.commit()

    push_workflow_id = f"harbor-push-{record.id}-{uuid.uuid4().hex[:8]}"
    try:
        temporal_client = await TemporalClientManager.get_temporal_client()
        await temporal_client.start_workflow(
            HarborPushWorkflow.run,
            args=[{
                "item_id":    record.id,
                "pod_path":   pod_path,
                "webdav_url": webdav_url,
            }],
            id=push_workflow_id,
            task_queue=HARBOR_PUSH_TASK_QUEUE,
            search_attributes=_make_search_attrs(record.name, "Harbor-Push", username),
        )
        record.push_workflow_id = push_workflow_id
        record.push_status      = "pushing"
        db.commit()
        logger.info(f"[Library] Harbor-Push triggered: wf={push_workflow_id}")
    except Exception as exc:
        logger.error(f"[Library] Harbor-Push workflow start failed: {exc}")
        record.push_status = "failed"
        record.push_error  = str(exc)
        db.commit()
        raise HTTPException(
            status_code=500,
            detail=f"Harbor-Push workflow start failed: {exc}",
        )

    return response_format.success_response(
        200, "File uploaded and Harbor-Push started", {
            **_item_to_dict(record),
            "push_workflow_id": push_workflow_id,
        }
    )


async def upload_library_file(
    item_id: int,
    request: Request,
    db:      Session,
):
    """
    Step 2 — stream the raw file bytes (Content-Type: application/octet-stream).
    There are 3 branches depending on `item.type`:

    - `container` → `_upload_harbor_direct_to_webdav()` (browser → WebDAV
      directly, no /tmp write), then trigger HarborPushWorkflow.
    - `llm_model`/`llm_template` → written to a local temp file
      (STORAGE_TEMP_DIR or the OS default); the file does not go to WebDAV —
      the temp_path is passed straight to LLMPushWorkflow (push-image-tool
      accesses this path from the shared PV — a known gotcha of this design is
      that if the backend runs outside the cluster / on a local dev machine,
      the STORAGE_TEMP_DIR local filesystem alias does not work).
    - all other types → local temp file → WebDAV (`_put_file_to_webdav_with_progress`,
      chunk-by-chunk so progress_pct updates granularly) → LibraryUploadWorkflow.

    In all branches, if the ZIP contains a `version_metadata.json`,
    name/version/owner are auto-updated.

    Used by: PUT /v1/library/{item_id}/file
    Returns: success_response with, in `data`, the updated `_item_to_dict()` +
    workflow_id (or push_workflow_id).
    Errors: a disk/WebDAV-write failure → 500 (the item is also delete/failed-marked).
    """
    import tempfile

    record = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
    if not record:
        raise HTTPException(status_code=404, detail=f"Library item {item_id} not found")
    if record.status != "uploading":
        raise HTTPException(status_code=409, detail=f"Item is not in uploading state (status={record.status})")

    username   = _extract_username(request)
    total_size = int(request.headers.get("content-length") or record.file_size or 0)
    _td        = os.getenv("STORAGE_TEMP_DIR", "")
    temp_dir   = _td if (_td and os.path.isdir(_td)) else None

    # ── Container: stream directly to WebDAV (no /tmp) ───────────────────────
    if record.type == "container":
        return await _upload_harbor_direct_to_webdav(
            record, request, db, username, total_size,
        )

    # ── Phase 1 (common): browser stream → backend temp file ─────────────────
    temp_fd, temp_path = tempfile.mkstemp(
        prefix=f"lib_{item_id}_", suffix=f"_{record.file_name}", dir=temp_dir
    )
    bytes_written = 0
    last_bucket   = -1

    try:
        with os.fdopen(temp_fd, "wb") as f:
            async for chunk in request.stream():
                f.write(chunk)
                bytes_written += len(chunk)
                if total_size > 0:
                    pct = min(int(bytes_written / total_size * 100), 49)
                    if pct > last_bucket:
                        last_bucket         = pct
                        record.progress_pct = pct
                        db.commit()
    except Exception as exc:
        logger.error(f"[Library] Disk write failed item={item_id}: {exc}")
        try:
            os.remove(temp_path)
        except OSError:
            pass
        db.delete(record)
        db.commit()
        raise HTTPException(status_code=500, detail=f"Upload failed during receive: {exc}")

    logger.info(f"[Library] received {bytes_written:,} bytes → {temp_path}")
    record.progress_pct = 50
    db.commit()

    # ── ZIP: update name + version in the DB from version_metadata.json ────────
    # For all types — container, llm_model, llm_template, harbor_template
    try:
        import zipfile as _zf, json as _zjson
        if _zf.is_zipfile(temp_path):
            with _zf.ZipFile(temp_path, "r") as _z:
                _mf = next(
                    (n for n in _z.namelist() if os.path.basename(n) == "version_metadata.json"),
                    None,
                )
                if _mf:
                    _zmeta = _zjson.loads(_z.open(_mf).read().decode("utf-8"))
                    if _zmeta.get("artifact_name"):
                        record.name = _zmeta["artifact_name"]
                    if _zmeta.get("version"):
                        record.version = _zmeta["version"]
                    _owner = _zmeta.get("owner") or _zmeta.get("owner_name")
                    if _owner and not record.harbor_owner:
                        record.harbor_owner = _owner
                    db.commit()
                    logger.info(
                        f"[Library] ZIP metadata → name={record.name} "
                        f"version={record.version} owner={record.harbor_owner}"
                    )
    except Exception as _ze:
        logger.warning(f"[Library] ZIP metadata read (non-fatal): {_ze}")

    # ── llm_model / llm_template: pass the temp path, direct OCI push ──────────
    if record.type in ("llm_model", "llm_template"):
        record.file_path    = temp_path
        record.file_size    = bytes_written
        record.progress_pct = 100
        record.status       = "ready"
        db.commit()

        push_workflow_id = f"llm-push-{item_id}-{uuid.uuid4().hex[:8]}"
        try:
            temporal_client = await TemporalClientManager.get_temporal_client()
            await temporal_client.start_workflow(
                LLMPushWorkflow.run,
                args=[{"item_id": item_id, "temp_path": temp_path}],
                id=push_workflow_id,
                task_queue=LLM_PUSH_TASK_QUEUE,
                search_attributes=_make_search_attrs(record.name, "LLM-Push", username),
            )
            record.push_workflow_id = push_workflow_id
            record.push_status      = "pushing"
            db.commit()
            logger.info(f"[Library] LLM-Push triggered: wf={push_workflow_id}")
        except Exception as exc:
            logger.error(f"[Library] LLM-Push workflow start failed: {exc}")
            try:
                os.remove(temp_path)
            except OSError:
                pass
            record.push_status = "failed"
            record.push_error  = str(exc)
            db.commit()
            raise HTTPException(status_code=500, detail=f"LLM-Push workflow start failed: {exc}")

        return response_format.success_response(200, "File received and LLM-Push started successfully", {
            **_item_to_dict(record),
            "push_workflow_id": push_workflow_id,
        })

    # ── All other types: temp file → WebDAV (internal URL) → PV ──────────────
    subdir       = TYPE_SUBDIR.get(record.type, "general")
    storage_base = os.getenv("STORAGE_BASE_URL", "https://devraq.dev.team/library").rstrip("/")
    # both the PUT and file_path use STORAGE_BASE_URL — client_max_body_size is raised on APISIX
    public_url = f"{storage_base}/{subdir}/{record.file_name}"

    try:
        resp = await asyncio.to_thread(
            _put_file_to_webdav_with_progress,
            item_id, temp_path, public_url, bytes_written, db,
        )
        if resp.status_code not in (200, 201, 204):
            raise RuntimeError(f"WebDAV PUT failed: {resp.status_code} {resp.text[:200]}")
    except Exception as exc:
        logger.error(f"[Library] WebDAV PUT failed item={item_id}: {exc}")
        record.status = "failed"
        db.commit()
        raise HTTPException(status_code=500, detail=f"Upload failed — WebDAV error: {exc}")
    finally:
        try:
            os.remove(temp_path)
        except OSError:
            pass

    logger.info(f"[Library] Phase 2 done: {public_url} ({bytes_written:,} bytes)")

    workflow_id         = f"library-upload-{item_id}-{uuid.uuid4().hex[:8]}"
    record.file_path    = public_url
    record.file_size    = bytes_written
    record.progress_pct = 100
    record.status       = "ready"
    record.workflow_id  = workflow_id
    db.commit()

    try:
        temporal_client = await TemporalClientManager.get_temporal_client()
        await temporal_client.start_workflow(
            LibraryUploadWorkflow.run,
            args=[{"item_id": item_id, "total_size": bytes_written}],
            id=workflow_id,
            task_queue=LIBRARY_TASK_QUEUE,
            search_attributes=_make_search_attrs(record.name, "Library-Upload", username),
        )
    except Exception as exc:
        logger.warning(f"[Library] Temporal workflow failed (upload done): {exc}")

    return response_format.success_response(200, "File uploaded successfully", {
        **_item_to_dict(record),
        "workflow_id": workflow_id,
    })


async def upload_library_direct(request: Request, db: Session):
    """
    Single-call upload: metadata in the X-Library-Metadata header, file in the raw body.

    Header example:
      X-Library-Metadata: {"file_name":"harbor.zip","type":"harbor_template","metadata":{...}}

    This does create_library_item + upload_library_file in a single request.

    ⚠ No router calls this (confirmed via grep — no reference anywhere in this
    repo) — it looks like dead/legacy code; `create_library_item` +
    `upload_library_file` (the 2-step flow) is the actual API surface.
    """
    import json as _json
    import tempfile

    username = _extract_username(request)

    # ── Parse the metadata header ────────────────────────────────────────────
    meta_header = request.headers.get("X-Library-Metadata", "").strip()
    if not meta_header:
        raise HTTPException(status_code=400, detail="X-Library-Metadata header required (JSON string)")
    try:
        params = _json.loads(meta_header)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"X-Library-Metadata JSON parse failed: {e}")

    file_name          = (params.get("file_name") or "").strip()
    item_type          = (params.get("type") or "general").strip()
    item_name          = (params.get("name") or "").strip()
    item_version       = params.get("version")
    harbor_registry_id = params.get("harbor_registry_id")
    harbor_owner       = params.get("harbor_owner")
    metadata           = params.get("metadata")  # nested dict — for annotations

    if not file_name:
        raise HTTPException(status_code=400, detail="file_name required in X-Library-Metadata")
    if item_type not in LIBRARY_TYPES:
        raise HTTPException(status_code=400, detail=f"Invalid type '{item_type}'. Valid: {', '.join(sorted(LIBRARY_TYPES))}")

    total_size = int(request.headers.get("content-length") or 0)

    # ── Create the DB record (same logic as create_library_item) ─────────────
    from models.kubernetes_deploy_model import KubernetesDeployment as _KDep

    _AUTO_NAME_TYPES = _HARBOR_PUSH_TYPES | {"harbor_template", "general", "base_os"}
    effective_name = item_name or os.path.splitext(file_name)[0]
    if not effective_name and item_type not in _AUTO_NAME_TYPES:
        raise HTTPException(status_code=400, detail="'name' required for this type")

    k8s_cluster_id = None
    if item_type in _HARBOR_PUSH_TYPES:
        if not harbor_registry_id:
            raise HTTPException(status_code=400, detail=f"harbor_registry_id required for type '{item_type}'")
        harbor_dep = db.query(_KDep).filter(_KDep.id == harbor_registry_id).first()
        if not harbor_dep:
            raise HTTPException(status_code=404, detail=f"Harbor registry id={harbor_registry_id} not found")
        k8s_cluster_id = harbor_dep.cluster_id

    effective_version = None if (item_type in _HARBOR_PUSH_TYPES and harbor_registry_id) else item_version

    _meta_json = _json.dumps(metadata, ensure_ascii=False) if metadata else None
    subdir   = TYPE_SUBDIR.get(item_type, "general")
    dest_path = f"{LIBRARY_BASE_PATH}/{subdir}/{file_name}"

    record = LibraryItem(
        name               = effective_name,
        type               = item_type,
        version            = effective_version,
        file_name          = file_name,
        file_path          = dest_path,
        file_size          = total_size or None,
        progress_pct       = 0,
        status             = "uploading",
        harbor_registry_id = harbor_registry_id,
        k8s_cluster_id     = k8s_cluster_id,
        harbor_owner       = harbor_owner,
        push_status        = "pending" if harbor_registry_id else None,
        display_name       = str(metadata.get("display_name", "")).strip() or None if metadata else None,
        description        = str(metadata.get("description", "")).strip() or None if metadata else None,
        category           = str(metadata.get("category", "")).strip() or None if metadata else None,
        metadata_json      = _meta_json,
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    item_id = record.id
    logger.info(f"[Library] Direct upload: item={item_id} type={item_type} file={file_name}")

    # ── File stream → temp ────────────────────────────────────────────────────
    _td = os.getenv("STORAGE_TEMP_DIR", "")
    temp_dir = _td if (_td and os.path.isdir(_td)) else None
    temp_fd, temp_path = tempfile.mkstemp(prefix=f"lib_{item_id}_", suffix=f"_{file_name}", dir=temp_dir)
    bytes_written = 0
    last_bucket   = -1

    try:
        with os.fdopen(temp_fd, "wb") as f:
            async for chunk in request.stream():
                f.write(chunk)
                bytes_written += len(chunk)
                if total_size > 0:
                    pct = min(int(bytes_written / total_size * 100), 49)
                    if pct > last_bucket:
                        last_bucket         = pct
                        record.progress_pct = pct
                        db.commit()
    except Exception as exc:
        logger.error(f"[Library] Direct upload disk write failed item={item_id}: {exc}")
        try:
            os.remove(temp_path)
        except OSError:
            pass
        db.delete(record)
        db.commit()
        raise HTTPException(status_code=500, detail=f"Upload failed: {exc}")

    logger.info(f"[Library] Direct upload received {bytes_written:,} bytes → {temp_path}")
    record.progress_pct = 50
    db.commit()

    # ── Parse the ZIP metadata (name/version/owner auto-set) ─────────────────
    try:
        import zipfile as _zf
        if _zf.is_zipfile(temp_path):
            with _zf.ZipFile(temp_path, "r") as _z:
                _mf = next(
                    (n for n in _z.namelist() if os.path.basename(n) == "version_metadata.json"),
                    None,
                )
                if _mf:
                    _zmeta = _json.loads(_z.open(_mf).read().decode("utf-8"))
                    if _zmeta.get("artifact_name"):
                        record.name = _zmeta["artifact_name"]
                    if _zmeta.get("version"):
                        record.version = _zmeta["version"]
                    _owner = _zmeta.get("owner") or _zmeta.get("owner_name")
                    if _owner and not record.harbor_owner:
                        record.harbor_owner = _owner
                    db.commit()
    except Exception as _ze:
        logger.warning(f"[Library] Direct upload ZIP metadata (non-fatal): {_ze}")

    # ── Phase 2: finalize the file per type ─────────────────────────────────
    # llm_model / llm_template → local temp path, direct OCI push
    if item_type in ("llm_model", "llm_template"):
        record.file_path    = temp_path
        record.file_size    = bytes_written
        record.progress_pct = 100
        record.status       = "ready"
        db.commit()
        push_wf_id = f"llm-push-{item_id}-{uuid.uuid4().hex[:8]}"
        try:
            tc = await TemporalClientManager.get_temporal_client()
            await tc.start_workflow(
                LLMPushWorkflow.run,
                args=[{"item_id": item_id, "temp_path": temp_path}],
                id=push_wf_id, task_queue=LLM_PUSH_TASK_QUEUE,
                search_attributes=_make_search_attrs(record.name, "LLM-Push", username),
            )
            record.push_workflow_id = push_wf_id
            record.push_status      = "pushing"
            db.commit()
        except Exception as exc:
            try: os.remove(temp_path)
            except OSError: pass
            record.push_status = "failed"; record.push_error = str(exc); db.commit()
            raise HTTPException(status_code=500, detail=f"LLM-Push start failed: {exc}")
        return response_format.success_response(200, "Direct upload completed and LLM-Push started successfully", _item_to_dict(record))

    # container → WebDAV (STORAGE_BASE_URL via APISIX), pod path
    if item_type == "container":
        storage_base = os.getenv("STORAGE_BASE_URL", "https://devraq.dev.team/library").rstrip("/")
        pod_path   = f"/data/library/{subdir}/{file_name}"
        webdav_url = f"{storage_base}/{subdir}/{file_name}"

        try:
            r = await asyncio.to_thread(
                _put_file_to_webdav_with_progress,
                item_id, temp_path, webdav_url, bytes_written, db,
            )
            if r.status_code not in (200, 201, 204):
                raise RuntimeError(f"WebDAV PUT {r.status_code}: {r.text[:200]}")
        except Exception as exc:
            record.status = "failed"
            db.commit()
            raise HTTPException(status_code=500, detail=f"WebDAV upload failed: {exc}")
        finally:
            try: os.remove(temp_path)
            except OSError: pass

        record.file_path = pod_path; record.file_size = bytes_written
        record.progress_pct = 100; record.status = "ready"; db.commit()

        push_wf_id = f"harbor-push-{item_id}-{uuid.uuid4().hex[:8]}"
        try:
            tc = await TemporalClientManager.get_temporal_client()
            await tc.start_workflow(
                HarborPushWorkflow.run,
                args=[{"item_id": item_id, "temp_path": pod_path}],
                id=push_wf_id, task_queue=HARBOR_PUSH_TASK_QUEUE,
                search_attributes=_make_search_attrs(record.name, "Harbor-Push", username),
            )
            record.push_workflow_id = push_wf_id; record.push_status = "pushing"; db.commit()
        except Exception as exc:
            record.push_status = "failed"; record.push_error = str(exc); db.commit()
            raise HTTPException(status_code=500, detail=f"Harbor-Push start failed: {exc}")
        return response_format.success_response(200, "Direct upload completed and Harbor-Push started successfully", _item_to_dict(record))

    # ── Baki sab types: WebDAV (STORAGE_BASE_URL) ────────────────────────────
    storage_base = os.getenv("STORAGE_BASE_URL", "https://devraq.dev.team/library").rstrip("/")
    public_url   = f"{storage_base}/{subdir}/{file_name}"

    try:
        resp = await asyncio.to_thread(
            _put_file_to_webdav_with_progress,
            item_id, temp_path, public_url, bytes_written, db,
        )
        if resp.status_code not in (200, 201, 204):
            raise RuntimeError(f"WebDAV PUT failed: {resp.status_code} {resp.text[:200]}")
    except Exception as exc:
        logger.error(f"[Library] Direct upload WebDAV failed item={item_id}: {exc}")
        record.status = "failed"
        db.commit()
        raise HTTPException(status_code=500, detail=f"WebDAV error: {exc}")
    finally:
        try: os.remove(temp_path)
        except OSError: pass

    workflow_id = f"library-upload-{item_id}-{uuid.uuid4().hex[:8]}"
    record.file_path = public_url; record.file_size = bytes_written
    record.progress_pct = 100; record.status = "ready"; record.workflow_id = workflow_id; db.commit()
    try:
        tc = await TemporalClientManager.get_temporal_client()
        await tc.start_workflow(
            LibraryUploadWorkflow.run,
            args=[{"item_id": item_id, "total_size": bytes_written}],
            id=workflow_id, task_queue=LIBRARY_TASK_QUEUE,
            search_attributes=_make_search_attrs(record.name, "Library-Upload", username),
        )
    except Exception as exc:
        logger.warning(f"[Library] Direct upload Temporal failed (non-fatal): {exc}")

    return response_format.success_response(200, "Direct upload completed successfully", _item_to_dict(record))


def _deployment_summary(job: LXCRestoreJob) -> dict:
    """Convert an LXCRestoreJob into a small summary dict (to embed alongside the library item)."""
    return {
        "job_id":     job.id,
        "name":       job.name,
        "status":     job.status,
        "ip_address": job.ip_address,
        "vmid":       job.vmid,
        "node":       job.node,
        "created_at": job.created_at.isoformat() if job.created_at else None,
    }


def _attach_deployments(items: list, db: Session) -> list:
    """Batch-fetch all LXCRestoreJob records for the given library items and attach them."""
    if not items:
        return []
    item_ids = [i["id"] for i in items]
    jobs = (
        db.query(LXCRestoreJob)
        .filter(LXCRestoreJob.library_item_id.in_(item_ids))
        .order_by(LXCRestoreJob.created_at.desc())
        .all()
    )
    # Group by library_item_id
    jobs_map: dict[int, list] = {}
    for j in jobs:
        jobs_map.setdefault(j.library_item_id, []).append(_deployment_summary(j))

    for item in items:
        deploys = jobs_map.get(item["id"], [])
        item["deployments"]        = deploys
        item["total_deployments"]  = len(deploys)
        item["latest_deployment"]  = deploys[0] if deploys else None
    return items


_TYPE_LABELS = {
    "harbor_template": "Harbor",
    "lxc_backup":      "LXC Backup",
    "base_os":         "Base OS",
    "container":       "Container",
    "llm_model":       "LLM Model",
    "llm_template":    "LLM Proxmox Template",
    "podman":          "Podman",
    "devraq_agent":    "Devraq Agent",
    "general":         "General",
    "openwebui":       "Open WebUI",
    "vectordb":        "Vector DB",
}

# Virtual types — not stored in the DB, they are query-time filters
# any of these keywords must match in the name or harbor_owner
# db_types: list of actual DB types to include in query (supports items uploaded as container OR postgresql)
_VIRTUAL_TYPE_MAP = {
    "openwebui": {
        "db_types": ["container"],
        "keywords": ["openwebui", "open-webui", "open_webui"],
    },
    "vectordb": {
        "db_types": ["container"],
        "keywords": ["vectordb", "vector-db", "vector_db", "pgvector", "chroma", "qdrant", "weaviate"],
    },
    "postgresql": {
        "db_types": ["container", "postgresql"],   # old container uploads + the new postgresql type
        "keywords": ["postgres", "postgresql"],
    },
}


def _build_filters(db: Session) -> list:
    """Get the distinct types + count from the DB and build the filter list for the frontend."""
    from sqlalchemy import func
    rows = (
        db.query(LibraryItem.type, func.count(LibraryItem.id).label("count"))
        .group_by(LibraryItem.type)
        .order_by(LibraryItem.type)
        .all()
    )
    return [
        {
            "type":  row.type,
            "label": _TYPE_LABELS.get(row.type, row.type.replace("_", " ").title()),
            "count": row.count,
        }
        for row in rows
    ]


def list_library_items(
    type_filter:        str | None,
    page:               int,
    page_size:          int,
    db:                 Session,
    owner_filter:       str | None = None,
    harbor_registry_id: int | None = None,
):
    """
    List library items — filtered by type/owner/harbor_registry, grouped by
    directory, with deployments (LXC/K8s) also attached.

    Used by: GET /v1/library/list
    Returns: success_response with, in `data`, {"filters": [...], "directories":
    {"<dir>": [<item>, ...], ...}, "total": int, "pagination": {...}}
    Errors: 400 if `type_filter` is not a valid type/virtual-type.
    """
    from sqlalchemy import func

    query = db.query(LibraryItem)

    if type_filter:
        if type_filter in _VIRTUAL_TYPE_MAP:
            from sqlalchemy import or_
            vt = _VIRTUAL_TYPE_MAP[type_filter]
            db_types = vt.get("db_types", [vt.get("db_type", "container")])
            query = query.filter(LibraryItem.type.in_(db_types))
            # match any keyword in the name or harbor_owner
            keyword_conditions = []
            for kw in vt["keywords"]:
                keyword_conditions.append(LibraryItem.name.ilike(f"%{kw}%"))
                keyword_conditions.append(LibraryItem.harbor_owner.ilike(f"%{kw}%"))
            query = query.filter(or_(*keyword_conditions))
        elif type_filter in LIBRARY_TYPES:
            query = query.filter(LibraryItem.type == type_filter)
        else:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid type '{type_filter}'. "
                    f"Valid: {', '.join(sorted(LIBRARY_TYPES | set(_VIRTUAL_TYPE_MAP)))}"
                ),
            )

    # owner_filter — partial case-insensitive match against harbor_owner
    if owner_filter:
        query = query.filter(LibraryItem.harbor_owner.ilike(f"%{owner_filter}%"))

    # harbor_registry_id filter — items of a specific harbor registry
    if harbor_registry_id is not None:
        query = query.filter(LibraryItem.harbor_registry_id == harbor_registry_id)

    total  = query.count()
    offset = (page - 1) * page_size
    items  = query.order_by(LibraryItem.created_at.desc()).offset(offset).limit(page_size).all()

    enriched = _attach_deployments([_item_to_dict(i) for i in items], db)

    # Override the directory when a virtual-type filter is applied
    # e.g. type=postgresql → container-type items also show in the "postgresql" directory
    if type_filter and type_filter in _VIRTUAL_TYPE_MAP:
        for item in enriched:
            item["directory"] = type_filter

    # Group by directory
    grouped: dict = {d: [] for d in sorted(POD_DIRS)}
    for item in enriched:
        d = item.get("directory", "general")
        grouped.setdefault(d, []).append(item)

    filters = _build_filters(db)

    total_pages = (total + page_size - 1) // page_size if page_size else 1
    return response_format.success_response(200, "Library items fetched", {
        "filters":     filters,
        "directories": grouped,
        "total":       total,
        "pagination": {
            "page":        page,
            "page_size":   page_size,
            "total":       total,
            "total_pages": total_pages,
            "has_next":    page < total_pages,
            "has_prev":    page > 1,
        },
    })


def get_library_item(item_id: int, db: Session):
    """
    Get a library item's full detail, with attached deployments.

    Used by: GET /v1/library/{item_id}
    Returns: success_response with `<item>` in `data` (see `list_library_items`).
    Errors: 404 if item_id is not found.
    """
    item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail=f"Library item {item_id} not found")
    data = _attach_deployments([_item_to_dict(item)], db)[0]
    return response_format.success_response(200, "Library item fetched", data)


def _parse_artifact_annotations(raw_ann: dict) -> dict:
    """
    Parse a Harbor artifact's OCI + ai.artifact.* annotations.
    The llm_model/llm_template push writes them with the "ai.artifact.<key>"
    prefix, the VM-template (Proxmox) push writes bare keys without a prefix --
    so try the prefixed key first, then fall back to the bare key.
    """
    import ast as _ast, json as _json

    def _ann(key: str) -> str:
        return raw_ann.get(f"ai.artifact.{key}") or raw_ann.get(key) or ""

    vm_details = {}
    raw_details = _ann("vm_template_details").strip()
    if raw_details:
        try:
            vm_details = _ast.literal_eval(raw_details)
        except Exception:
            try:
                vm_details = _json.loads(raw_details)
            except Exception:
                pass

    return {
        "artifact_type":       _ann("artifact_type"),
        "hypervisor":          _ann("hypervisor"),
        "size_bytes":          _ann("size_bytes"),
        "uploaded_by":         _ann("uploaded_by"),
        "type":                _ann("type"),
        "title":               raw_ann.get("org.opencontainers.image.title", ""),
        "description":         raw_ann.get("org.opencontainers.image.description", ""),
        "version":             raw_ann.get("org.opencontainers.image.version", ""),
        "vendor":              raw_ann.get("org.opencontainers.image.vendor", ""),
        "created":             raw_ann.get("org.opencontainers.image.created", ""),
        "url":                 raw_ann.get("org.opencontainers.image.url", ""),
        "vm_template_details": vm_details,
    }


def _artifact_matches(
    ann:         dict,
    type_filter: str | None,
    hypervisor:  str | None,
    os_type:     str | None,
    os_name:     str | None = None,
) -> bool:
    """Check the filters against the parsed annotation dict (partial, case-insensitive)."""
    if type_filter and type_filter.lower() not in ann.get("type", "").lower():
        return False
    if hypervisor and hypervisor.lower() not in ann.get("hypervisor", "").lower():
        return False
    details = ann.get("vm_template_details") or {}
    if os_type:
        if os_type.lower() not in (details.get("os_type") or "").lower():
            return False
    if os_name:
        if os_name.lower() not in (details.get("os_name") or "").lower():
            return False
    return True


def list_harbor_artifacts(
    registry_id: int,
    db:          Session,
    project:     str | None = None,
    repository:  str | None = None,
    owner:       str | None = None,
    type_filter: str | None = None,
    hypervisor:  str | None = None,
    os_type:     str | None = None,
    os_name:     str | None = None,
    page:        int        = 1,
    page_size:   int        = 20,
):
    """
    Fetch the list of artifacts deployed on Harbor — uses both the Harbor REST
    API (`/api/v2.0`, for the projects/repositories list) and the OCI
    Distribution API (`/v2/...`, for tags/manifests — literal slashes in the
    URL, so the %2F double-encoding issue does not apply here).

    Used by: GET /v1/library/harbor/{registry_id}/artifacts

    4 modes, depending on the params, each with a different `data` shape:
    - registry_id alone → {"registry_id","harbor_url","page","page_size","count","projects": [{"id","name","repo_count","chart_count","creation_time","update_time","public"}, ...]}
    - + project → {"registry_id","harbor_url","project","owner","page","page_size","count","repositories": [{"name","full_name","artifact_count","pull_count","update_time"}, ...]}
    - + project + repository → {"registry_id","harbor_url","project","repository","page","page_size","count","artifacts": [{"digest","tags","name","repository","size","full_image","annotations"}, ...]}
    - + project + (type/hypervisor/os_type/os_name) → cross-repo filtered
      search — {"registry_id","harbor_url","project","total","page","page_size",
      "total_pages","has_next","has_prev","artifacts": [...]} (every
      repository's tags are scanned, so this mode is the slowest)

    Errors: 404 if registry_id is not found, 409 if harbor_url is not set, 502 if Harbor
    is unreachable/auth-invalid/API-error.
    """
    import httpx as _httpx
    from models.kubernetes_deploy_model import KubernetesDeployment

    harbor = db.query(KubernetesDeployment).filter(KubernetesDeployment.id == registry_id).first()
    if not harbor:
        raise HTTPException(status_code=404, detail=f"Harbor registry id={registry_id} not found")
    if not harbor.harbor_url:
        raise HTTPException(status_code=409, detail=f"Harbor registry id={registry_id} has no harbor_url")

    base = harbor.harbor_url.rstrip("/")
    auth = (harbor.harbor_user or "admin", harbor.harbor_pass or "")
    host = base.replace("http://", "").replace("https://", "")

    _timeout = _httpx.Timeout(connect=5.0, read=30.0, write=5.0, pool=5.0)

    def _get(path: str, p: dict | None = None) -> list:
        url = f"{base}/api/v2.0{path}"
        try:
            r = _httpx.get(url, auth=auth, params=p or {},
                           timeout=_timeout, verify=False)
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Harbor API unreachable ({base}): {e}")
        if r.status_code == 401:
            raise HTTPException(status_code=502, detail="Harbor credentials invalid (401)")
        if r.status_code == 404:
            raise HTTPException(status_code=404, detail=f"Harbor resource not found: {path}")
        if not r.is_success:
            raise HTTPException(status_code=502, detail=f"Harbor API error {r.status_code}: {r.text[:200]}")
        return r.json() if r.text.strip() else []

    def _format_artifact(a: dict, repo_name: str) -> dict:
        tags    = [t["name"] for t in (a.get("tags") or []) if t.get("name")]
        ann     = _parse_artifact_annotations(a.get("annotations") or {})
        return {
            "digest":       a.get("digest", ""),
            "tags":         tags,
            "repository":   repo_name,
            "size":         a.get("size", 0),
            "push_time":    a.get("push_time", ""),
            "annotations":  ann,
            "pull_command": f"docker pull {host}/{project}/{repo_name}:{tags[0]}" if tags else "",
        }

    _OCI_MANIFEST_ACCEPT = (
        "application/vnd.oci.image.manifest.v1+json,"
        "application/vnd.docker.distribution.manifest.v2+json"
    )
    _TEMPLATE_ANN_EXCLUDE = frozenset({
        "org.opencontainers.image.description",
        "org.opencontainers.image.url",
        "org.opencontainers.image.vendor",
        "org.opencontainers.image.version",
    })

    def _oci_tags(full_name: str) -> list:
        """OCI /v2/ API se tags list — literal slashes in path, no %2F issue."""
        url = f"{base}/v2/{full_name}/tags/list"
        try:
            r = _httpx.get(url, auth=auth, timeout=_timeout, verify=False)
        except Exception:
            return []
        return r.json().get("tags") or [] if r.is_success else []

    def _oci_manifest(full_name: str, ref: str) -> dict:
        """Fetch a manifest (with annotations) from the OCI /v2/ API."""
        url = f"{base}/v2/{full_name}/manifests/{ref}"
        try:
            r = _httpx.get(url, auth=auth, timeout=_timeout, verify=False,
                           headers={"Accept": _OCI_MANIFEST_ACCEPT})
        except Exception:
            return {}
        return r.json() if r.is_success else {}

    def _oci_artifact(full_name: str, repo_name: str, tag: str) -> dict | None:
        """OCI manifest → artifact dict."""
        manifest = _oci_manifest(full_name, tag)
        if not manifest:
            return None
        size    = sum(lyr.get("size", 0) for lyr in (manifest.get("layers") or []))
        digest  = manifest.get("config", {}).get("digest", "")
        raw_ann = manifest.get("annotations") or {}
        # model: all annotations; template: exclude the OCI standard keys
        if type_filter == "model":
            annotations = raw_ann
        else:
            annotations = {k: v for k, v in raw_ann.items() if k not in _TEMPLATE_ANN_EXCLUDE}
        return {
            "digest":      digest,
            "tags":        [tag],
            "name":        repo_name.split("/")[-1],
            "repository":  repo_name,
            "size":        size,
            "full_image":  f"{host}/{full_name}:{tag}",
            "annotations": annotations,
        }

    # ── Filter mode: project + any filter → cross-repo search ───────────────
    _filters_set = any([type_filter, hypervisor, os_type, os_name])
    if project and _filters_set:
        # 1. Fetch all repos (up to 200)
        all_repos_raw = _get(f"/projects/{project}/repositories", {"page": 1, "page_size": 100})
        if len(all_repos_raw) == 100:
            all_repos_raw += _get(f"/projects/{project}/repositories", {"page": 2, "page_size": 100})

        matched: list = []
        for repo_raw in (all_repos_raw if isinstance(all_repos_raw, list) else []):
            full_name = repo_raw.get("name", "")       # "library/raqsoft/proxmox-template"
            repo_name = full_name.split("/", 1)[-1]    # "raqsoft/proxmox-template"
            if owner and not repo_name.startswith(f"{owner}/"):
                continue
            # OCI /v2/ API — the repo name's slashes go directly in the URL, no %2F needed
            for tag in _oci_tags(full_name):
                raw_ann = _oci_manifest(full_name, tag).get("annotations") or {}
                ann     = _parse_artifact_annotations(raw_ann)
                if _artifact_matches(ann, type_filter, hypervisor, os_type, os_name):
                    art = _oci_artifact(full_name, repo_name, tag)
                    if art:
                        matched.append(art)

        total       = len(matched)
        offset      = (page - 1) * page_size
        paged       = matched[offset: offset + page_size]
        total_pages = (total + page_size - 1) // page_size if page_size else 1
        return response_format.success_response(200, "Filtered artifacts", {
            "registry_id": registry_id,
            "harbor_url":  base,
            "project":     project,
            "total":       total,
            "page":        page,
            "page_size":   page_size,
            "total_pages": total_pages,
            "has_next":    page < total_pages,
            "has_prev":    page > 1,
            "artifacts":   paged,
        })

    # ── Level 3: project + repository → artifacts list ──────────────────────
    if project and repository:
        full_name = f"{project}/{repository}"   # "library/raqsoft/proxmox-template"
        tags      = _oci_tags(full_name)
        artifacts = []
        for tag in tags:
            art = _oci_artifact(full_name, repository, tag)
            if art:
                artifacts.append(art)
        return response_format.success_response(200, f"Artifacts in {project}/{repository}", {
            "registry_id": registry_id,
            "harbor_url":  base,
            "project":     project,
            "repository":  repository,
            "page":        page,
            "page_size":   page_size,
            "count":       len(artifacts),
            "artifacts":   artifacts,
        })

    # ── Level 2: project → repositories list ────────────────────────────────
    if project:
        raw   = _get(f"/projects/{project}/repositories", {"page": page, "page_size": page_size})
        repos = []
        for r in (raw if isinstance(raw, list) else []):
            full_name = r.get("name", "")
            repo_name = full_name.split("/", 1)[-1]
            if owner and not repo_name.startswith(f"{owner}/"):
                continue
            repos.append({
                "name":           repo_name,
                "full_name":      full_name,
                "artifact_count": r.get("artifact_count", 0),
                "pull_count":     r.get("pull_count", 0),
                "update_time":    r.get("update_time", ""),
            })
        return response_format.success_response(200, f"Repositories in project '{project}'", {
            "registry_id":  registry_id,
            "harbor_url":   base,
            "project":      project,
            "owner":        owner,
            "page":         page,
            "page_size":    page_size,
            "count":        len(repos),
            "repositories": repos,
        })

    # ── Level 1: projects list ───────────────────────────────────────────────
    raw      = _get("/projects", {"page": page, "page_size": page_size})
    projects = []
    for p in (raw if isinstance(raw, list) else []):
        projects.append({
            "id":            p.get("id"),
            "name":          p.get("name", ""),
            "repo_count":    p.get("repo_count", 0),
            "chart_count":   p.get("chart_count", 0),
            "creation_time": p.get("creation_time", ""),
            "update_time":   p.get("update_time", ""),
            "public":        p.get("metadata", {}).get("public", "false") == "true",
        })
    return response_format.success_response(200, "Harbor projects list", {
        "registry_id": registry_id,
        "harbor_url":  base,
        "page":        page,
        "page_size":   page_size,
        "count":       len(projects),
        "projects":    projects,
    })


def get_library_item_path(item_id: int, db: Session) -> tuple[str, str]:
    """
    Get the item's (file_path, file_name) tuple — for building a download redirect.

    Used by: GET /v1/library/download/{item_id} (via RedirectResponse)
    Errors: 404 if item_id is not found, 409 if the status is not "ready".
    """
    item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail=f"Library item {item_id} not found")
    if item.status != "ready":
        raise HTTPException(
            status_code=409,
            detail=f"File is not ready yet (status={item.status}). Wait for upload to complete.",
        )
    return item.file_path, item.file_name


async def update_library_item(
    item_id: int,
    name:    str | None,
    version: str | None,
    db:      Session,
    request: Request,
):
    """
    Update the item's name/version (LibraryUpdateWorkflow, Temporal — async,
    the file/Harbor image is not touched).

    Used by: PUT /v1/library/{item_id}
    Returns: success_response with the updated `<item>` in `data` (right after
    the workflow start — the real DB write happens a bit later in the background).
    Errors: 404 if item_id is not found, 409 if the status is "uploading".
    """
    item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail=f"Library item {item_id} not found")
    if item.status == "uploading":
        raise HTTPException(status_code=409, detail="Cannot update while upload is in progress")

    username = _extract_username(request)

    try:
        client      = await TemporalClientManager.get_temporal_client()
        workflow_id = f"library-update-{item_id}-{uuid.uuid4().hex[:8]}"
        await client.start_workflow(
            LibraryUpdateWorkflow.run,
            args=[{"item_id": item_id, "name": name, "version": version}],
            id=workflow_id,
            task_queue=LIBRARY_TASK_QUEUE,
            search_attributes=_make_search_attrs(item.name, "Library-Update", username),
        )
        item.workflow_id = workflow_id
        db.commit()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Workflow start failed: {exc}")

    return response_format.success_response(200, "Library item update started successfully", _item_to_dict(item))


async def delete_library_item(item_id: int, db: Session, request: Request):
    """
    Delete a library item.

    - status "uploading"/"failed" → immediate sync delete (DB record + temp
      file if it exists), no Temporal/Harbor involvement.
    - status "ready" → LibraryDeleteWorkflow (Temporal, async) — if it was
      pushed to Harbor (`_HARBOR_PUSH_TYPES`: container, llm_model,
      llm_template, postgresql), Harbor is cleaned up first, then the DB
      record (if the Harbor delete fails, the DB record is NOT deleted — see
      activities_library.py's `delete_library_file_activity`).

    Used by: DELETE /v1/library/{item_id}
    Returns: sync path → {"id": item_id}; async path → {"id": item_id, "workflow_id": str}
    Errors: 404 if item_id is not found.
    """
    item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail=f"Library item {item_id} not found")

    username = _extract_username(request)

    # in the uploading/failed state — delete straight from the DB, no Temporal
    if item.status in ("uploading", "failed"):
        file_path = item.file_path
        db.delete(item)
        db.commit()
        # Remove the file too if it exists
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except OSError:
                pass
        logger.info(f"[Library] item={item_id} force-deleted (status was {item.status})")
        return response_format.success_response(200, "Library item deleted successfully", {"id": item_id})

    try:
        client      = await TemporalClientManager.get_temporal_client()
        workflow_id = f"library-delete-{item_id}-{uuid.uuid4().hex[:8]}"
        await client.start_workflow(
            LibraryDeleteWorkflow.run,
            args=[{"item_id": item_id}],
            id=workflow_id,
            task_queue=LIBRARY_TASK_QUEUE,
            search_attributes=_make_search_attrs(item.name, "Library-Delete", username),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Workflow start failed: {exc}")

    return response_format.success_response(200, "Library item deletion started successfully", {"id": item_id, "workflow_id": workflow_id})


_LXC_BRIDGE   = os.getenv("LXC_BRIDGE",   "vmbr0")
_LXC_SSH_USER = os.getenv("LXC_SSH_USER", "root")
_LXC_SSH_PASS = os.getenv("LXC_SSH_PASS", "")


async def deploy_library_item(
    item_id: int,
    body:    dict,
    db:      Session,
    request: Request,
):
    """
    Deploy a library item — dispatch by `deployment_type`:
    "lxc" (default) → Proxmox LXC restore, "kubernetes" → K8s Harbor deploy
    (see kubernetes_controller.deploy_harbor_to_k8s).

    Used by: POST /v1/library/{item_id}/deploy
    Args: body = LibraryDeployBody (deployment_type, name, cluster_id, +
    lxc-only: ip_pools/storage, + k8s-only: namespace/http_port).
    Returns: see `_deploy_library_lxc` / `kubernetes_controller.deploy_harbor_to_k8s`.
    """
    deployment_type = body.get("deployment_type", "lxc").lower()

    if deployment_type == "kubernetes":
        return await _deploy_library_k8s(item_id, body, db)

    # ── LXC path (original flow) ──────────────────────────────────────────────
    return await _deploy_library_lxc(
        item_id    = item_id,
        name       = body["name"],
        cluster_id = body["cluster_id"],
        ip_pools   = body.get("ip_pools") or [],
        storage    = body.get("storage", "local-lvm"),
        db         = db,
        request    = request,
    )


async def _deploy_library_k8s(item_id: int, body: dict, db: Session):
    """Deploy a library item onto a K8s cluster as Harbor."""
    from controllers.kubernetes_controller import deploy_harbor_to_k8s

    cluster_id = body["cluster_id"]
    return await deploy_harbor_to_k8s(
        cluster_id = cluster_id,
        body       = {
            "library_item_id": item_id,
            "name":            body["name"],
            "namespace":       body.get("namespace", "harbor"),
            "http_port":       body.get("http_port", 80),
        },
        db = db,
    )


async def _deploy_library_lxc(
    item_id:    int,
    name:       str,
    cluster_id: int,
    ip_pools:   List[str],
    storage:    str,
    db:         Session,
    request:    Request,
):
    """
    Restore a library item (an LXC backup) onto Proxmox as a new LXC container
    — validate the item/cluster, reserve the first available IP from the given
    `ip_pools`, create an LXCRestoreJob record, then start LXCRestoreWorkflow
    (Temporal). The bridge/SSH creds come from env, not the request body
    (security).

    Returns: success_response(202) with, in `data`, {"job_id", "name",
    "ip_address", "cluster", "library_item", "workflow_id", "status": "provisioning"}.
    Errors: 404 if item_id/cluster_id is not found, 409 if the item is not ready, 400 if
    no pool has a free IP.
    """
    # Validate library item
    item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail=f"Library item {item_id} not found")
    if item.status != "ready":
        raise HTTPException(
            status_code=409,
            detail=f"Library item '{item.name}' is not ready (status={item.status}). Wait for upload to complete.",
        )

    # Validate cluster by ID
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail=f"Cluster id={cluster_id} not found")

    # Find first available IP from the given pools in order
    ip_entry   = None
    used_pool  = None
    for pool_name in ip_pools:
        pool = db.query(IPSModel).filter(IPSModel.Pool_name == pool_name).first()
        if not pool:
            continue
        entry = (
            db.query(IPEntry)
            .filter(IPEntry.pool_id == pool.id, IPEntry.status == "unused")
            .order_by(IPEntry.id.asc())
            .first()
        )
        if entry:
            ip_entry  = entry
            used_pool = pool
            break

    if not ip_entry:
        raise HTTPException(
            status_code=400,
            detail=f"No available IPs in pools: {ip_pools}",
        )

    ip_entry.status = "used"
    db.flush()

    # Create deploy job record
    record = LXCRestoreJob(
        name=name,
        cluster_id=cluster.id,
        ip_pool_id=used_pool.id,
        library_item_id=item_id,
        ip_address=ip_entry.ip,
        storage=storage or "local-lvm",
        status="provisioning",
    )
    db.add(record)
    db.commit()
    db.refresh(record)

    # Start LXC Restore Temporal workflow
    username = _extract_username(request)
    wf_payload = {
        "job_id":          record.id,
        "cluster_id":      cluster.id,
        "library_item_id": item_id,
        "ip_address":      ip_entry.ip,
        "ip_pool_id":      used_pool.id,
        "name":            name,
        "storage":         storage or "local-lvm",
        "bridge":          _LXC_BRIDGE,
        "ssh_user":        _LXC_SSH_USER,
        "ssh_pass":        _LXC_SSH_PASS,
    }

    try:
        client = await TemporalClientManager.get_temporal_client()
        wf_id  = f"lxc-deploy-{record.id}-{uuid.uuid4().hex[:8]}"
        await client.start_workflow(
            LXCRestoreWorkflow.run,
            wf_payload,
            id=wf_id,
            task_queue=LXC_TASK_QUEUE,
            search_attributes=_make_search_attrs(name, "LXC-Deploy", username),
        )
        record.workflow_id = wf_id
        db.commit()
    except Exception as exc:
        logger.error(f"deploy_library_item workflow start failed: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Workflow start failed: {exc}")

    return response_format.success_response(202, "LXC deploy started", {
        "job_id":      record.id,
        "name":        name,
        "ip_address":  ip_entry.ip,
        "cluster":     cluster.name,
        "library_item": item.name,
        "workflow_id": wf_id,
        "status":      "provisioning",
    })

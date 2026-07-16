import asyncio
import logging
import os
import queue
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
from service.temporalResource.workflows.workflows_library import (
    LibraryUploadWorkflow,
    LibraryDeleteWorkflow,
    LibraryUpdateWorkflow,
)
from service.temporalResource.workflows.workflows_lxc_restore import LXCRestoreWorkflow
from utils.temporal_client import TemporalClientManager
from utils import response_format

_ENTITY_KEY   = SearchAttributeKey.for_keyword("Entity")
_ACTION_KEY   = SearchAttributeKey.for_keyword("Action")
_USERNAME_KEY = SearchAttributeKey.for_keyword("UserName")


def _make_search_attrs(entity: str, action: str, username: str) -> TypedSearchAttributes:
    return TypedSearchAttributes([
        SearchAttributePair(_ENTITY_KEY,   entity),
        SearchAttributePair(_ACTION_KEY,   action),
        SearchAttributePair(_USERNAME_KEY, username),
    ])

logger = logging.getLogger(__name__)

LIBRARY_BASE_PATH = os.getenv("LIBRARY_BASE_PATH", "/data/library")
LIBRARY_TEMP_PATH = os.getenv("LIBRARY_TEMP_PATH", "/tmp/library_uploads")


def _extract_username(request: Request) -> str:
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
    directory = TYPE_SUBDIR.get(item.type, "general")
    return {
        "id":           item.id,
        "name":         item.name,
        "type":         item.type,
        "directory":    directory,
        "version":      item.version,
        "file_name":    item.file_name,
        "file_path":    item.file_path,
        "file_size":    item.file_size,
        "progress_pct": item.progress_pct,
        "status":       item.status,
        "workflow_id":  item.workflow_id,
        "created_at":   item.created_at.isoformat() if item.created_at else None,
        "updated_at":   item.updated_at.isoformat() if item.updated_at else None,
    }


async def create_library_item(
    name:      str,
    type:      str | None,
    version:   str | None,
    file_name: str,
    file_size: int | None,
    db:        Session,
    request:   Request,
):
    """
    Step 1 — create DB record immediately (no file, returns in milliseconds).
    Frontend gets item_id and then starts streaming file via PUT /{item_id}/file.
    """
    effective_type = (type or "general").strip()
    if effective_type not in LIBRARY_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid type '{effective_type}'. Must be one of: {', '.join(sorted(LIBRARY_TYPES))}",
        )

    subdir    = TYPE_SUBDIR[effective_type]
    dest_path = f"{LIBRARY_BASE_PATH}/{subdir}/{file_name}"

    temp_name = f"{uuid.uuid4().hex}_{file_name}"
    temp_path = os.path.join(LIBRARY_TEMP_PATH, temp_name)

    record = LibraryItem(
        name=name,
        type=effective_type,
        version=version or None,
        file_name=file_name,
        file_path=dest_path,
        file_size=file_size,
        progress_pct=0,
        status="uploading",
    )
    db.add(record)
    db.commit()
    db.refresh(record)

    logger.info(f"[Library] item={record.id} created — waiting for file stream")
    return response_format.success_response(
        202,
        "Library item created — stream file to PUT /v1/library/{item_id}/file",
        {**_item_to_dict(record), "temp_path": temp_path},
    )


def _update_progress_in_db(item_id: int, pct: int, db_session):
    """Progress DB mein update karo — thread-safe nahi, sirf upload thread se call karo."""
    try:
        item = db_session.query(LibraryItem).filter(LibraryItem.id == item_id).first()
        if item:
            item.progress_pct = pct
            db_session.commit()
    except Exception as exc:
        logger.warning(f"[Library] progress DB update failed item={item_id}: {exc}")
        db_session.rollback()


async def upload_library_file(
    item_id:   int,
    request:   Request,
    db:        Session,
):
    """
    Two-phase upload:
      Phase 1 — browser stream → temp file on disk  (decoupled from WebDAV)
      Phase 2 — temp file → WebDAV PUT via requests  (stable internal LAN, no browser timeout)

    Direct streaming (browser→FastAPI→WebDAV) fail hota hai kyunki nginx client_body_timeout
    browser chunks ke beech trigger ho jaata hai. Temp file se dono operations decouple hote hain.
    """
    import asyncio
    import requests as _req
    import tempfile

    record = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
    if not record:
        raise HTTPException(status_code=404, detail=f"Library item {item_id} not found")
    if record.status != "uploading":
        raise HTTPException(status_code=409, detail=f"Item is not in uploading state (status={record.status})")

    username     = _extract_username(request)
    total_size   = int(request.headers.get("content-length") or record.file_size or 0)
    subdir       = TYPE_SUBDIR.get(record.type, "general")
    storage_base = os.getenv("STORAGE_BASE_URL",     "https://devraq.dev.team/library").rstrip("/")
    public_url   = f"{storage_base}/{subdir}/{record.file_name}"
    # Phase 2 (disk→WebDAV) ke liye hamesha external URL use karo —
    # internal nginx pe client_max_body_size limit hoti hai, APISIX pe nahi.
    # Disk se upload hai isliye browser-streaming ka ReadError issue nahi hoga.
    webdav_url   = public_url
    _td = os.getenv("STORAGE_TEMP_DIR", "")
    temp_dir = _td if (_td and os.path.isdir(_td)) else None  # None = OS default

    # ── Phase 1: Browser → temp file ─────────────────────────────────────────
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
                    pct    = min(int(bytes_written / total_size * 100), 49)  # 0–49% = disk phase
                    bucket = (pct // 5) * 5
                    if bucket > last_bucket:
                        last_bucket         = bucket
                        record.progress_pct = bucket
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

    logger.info(f"[Library] Phase 1 done: {bytes_written:,} bytes → {temp_path}")
    record.progress_pct = 50
    db.commit()

    # ── Phase 2: Temp file → WebDAV (requests, blocking — run in thread) ────
    def _put_to_webdav():
        with open(temp_path, "rb") as f:
            return _req.put(
                webdav_url,
                data=f,
                headers={
                    "Content-Length":   str(bytes_written),
                    "Content-Type":     "application/octet-stream",
                },
                verify=False,
                timeout=None,   # badi file ke liye koi timeout nahi
            )

    try:
        resp = await asyncio.to_thread(_put_to_webdav)
        if resp.status_code not in (200, 201, 204):
            raise RuntimeError(f"WebDAV PUT failed: {resp.status_code} {resp.text[:200]}")
    except Exception as exc:
        logger.error(f"[Library] WebDAV PUT failed item={item_id} url={webdav_url}: {exc}")
        try:
            os.remove(temp_path)
        except OSError:
            pass
        db.delete(record)
        db.commit()
        raise HTTPException(status_code=500, detail=f"Upload failed — WebDAV error: {exc}")
    finally:
        try:
            os.remove(temp_path)
        except OSError:
            pass

    logger.info(f"[Library] Phase 2 done: {webdav_url} ({bytes_written:,} bytes)")

    workflow_id         = f"library-upload-{item_id}-{uuid.uuid4().hex[:8]}"
    record.file_path    = public_url   # external URL store karo — deploy activity isi se download karega
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

    logger.info(f"[Library] item={item_id} → {webdav_url} ({bytes_written:,} bytes)")
    return response_format.success_response(200, "File uploaded successfully", {
        **_item_to_dict(record),
        "workflow_id": workflow_id,
    })


def _deployment_summary(job: LXCRestoreJob) -> dict:
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


def list_library_items(type_filter: str | None, page: int, page_size: int, db: Session):
    query = db.query(LibraryItem)
    if type_filter:
        if type_filter not in LIBRARY_TYPES:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid type '{type_filter}'. Must be one of: {', '.join(sorted(LIBRARY_TYPES))}",
            )
        query = query.filter(LibraryItem.type == type_filter)

    total  = query.count()
    offset = (page - 1) * page_size
    items  = query.order_by(LibraryItem.created_at.desc()).offset(offset).limit(page_size).all()

    enriched = _attach_deployments([_item_to_dict(i) for i in items], db)

    # Group by directory — saari directories dikhao, chahe empty ho
    grouped: dict = {d: [] for d in sorted(POD_DIRS)}
    for item in enriched:
        d = item.get("directory", "general")
        grouped.setdefault(d, []).append(item)

    total_pages = (total + page_size - 1) // page_size if page_size else 1
    return response_format.success_response(200, "Library items fetched", {
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
    item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail=f"Library item {item_id} not found")
    data = _attach_deployments([_item_to_dict(item)], db)[0]
    return response_format.success_response(200, "Library item fetched", data)


def get_library_item_path(item_id: int, db: Session) -> tuple[str, str]:
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

    return response_format.success_response(200, "Library item update started", _item_to_dict(item))


async def delete_library_item(item_id: int, db: Session, request: Request):
    item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail=f"Library item {item_id} not found")

    username = _extract_username(request)

    # uploading/failed state mein — seedha DB se delete karo, koi Temporal nahi
    if item.status in ("uploading", "failed"):
        file_path = item.file_path
        db.delete(item)
        db.commit()
        # File bhi hata do agar exist kare
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except OSError:
                pass
        logger.info(f"[Library] item={item_id} force-deleted (status was {item.status})")
        return response_format.success_response(200, "Library item deleted", {"id": item_id})

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

    return response_format.success_response(200, "Library item deletion started", {"id": item_id, "workflow_id": workflow_id})


_LXC_BRIDGE   = os.getenv("LXC_BRIDGE",   "vmbr0")
_LXC_SSH_USER = os.getenv("LXC_SSH_USER", "root")
_LXC_SSH_PASS = os.getenv("LXC_SSH_PASS", "")


async def deploy_library_item(
    item_id:    int,
    name:       str,
    cluster_id: int,
    ip_pools:   List[str],
    storage:    str,
    db:         Session,
    request:    Request,
):
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

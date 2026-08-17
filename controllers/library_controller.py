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
    Step 1 — DB record create karo, milliseconds mein item_id milta hai.
    Frontend PUT /{item_id}/file se file stream karta hai.

    container / llm_model / llm_template ke liye:
      harbor_registry_id = kubernetes_deployments.id (Harbor instance)
      Backend K8s cluster automatically derive karta hai.
      name = optional — container type ke liye Docker image metadata se auto-set hoga.
    """
    from models.kubernetes_deploy_model import KubernetesDeployment

    effective_type = (type or "general").strip()
    if effective_type not in LIBRARY_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid type '{effective_type}'. Valid: {', '.join(sorted(LIBRARY_TYPES))}",
        )

    # container type ke liye name optional hai — placeholder use karo, activity update karega
    # Types jahan name ZIP version_metadata.json se auto-set hoga
    _AUTO_NAME_TYPES = _HARBOR_PUSH_TYPES | {"harbor_template", "general", "base_os"}

    effective_name = (name or "").strip()
    if not effective_name:
        if effective_type in _AUTO_NAME_TYPES:
            # name ZIP/metadata se upload ke baad set hoga — file stem placeholder
            effective_name = os.path.splitext(file_name)[0]
        else:
            raise HTTPException(status_code=400, detail="'name' field required for this type")

    subdir    = TYPE_SUBDIR[effective_type]
    dest_path = f"{LIBRARY_BASE_PATH}/{subdir}/{file_name}"
    temp_path = os.path.join(LIBRARY_TEMP_PATH, f"{uuid.uuid4().hex}_{file_name}")

    # Harbor push types ke liye harbor_registry_id required hai
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
        k8s_cluster_id = harbor_dep.cluster_id   # backend derive karta hai
        logger.info(f"[Library] harbor_registry={harbor_registry_id} → k8s_cluster={k8s_cluster_id} derived")

    # container ke liye: owner_name field harbor_owner se override karta hai
    if effective_type == "container" and owner_name:
        harbor_owner = owner_name.strip() or harbor_owner

    # Harbor push types ke liye version image inspect se aayega
    # Exception: container type mein user ne explicitly version diya to use karo (tag banega Harbor mein)
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
        # metadata_json: upload API se pass hua JSON — push activity mein use hoga annotations ke liye
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
    item_id: int,
    request: Request,
    db:      Session,
):
    import tempfile
    import requests as _req

    record = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
    if not record:
        raise HTTPException(status_code=404, detail=f"Library item {item_id} not found")
    if record.status != "uploading":
        raise HTTPException(status_code=409, detail=f"Item is not in uploading state (status={record.status})")

    username   = _extract_username(request)
    total_size = int(request.headers.get("content-length") or record.file_size or 0)
    _td        = os.getenv("STORAGE_TEMP_DIR", "")
    temp_dir   = _td if (_td and os.path.isdir(_td)) else None

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
                    pct    = min(int(bytes_written / total_size * 100), 49)
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

    logger.info(f"[Library] received {bytes_written:,} bytes → {temp_path}")
    record.progress_pct = 50
    db.commit()

    # ── ZIP: version_metadata.json se name + version DB mein update karo ────────
    # Sab types ke liye — container, llm_model, llm_template, harbor_template
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

    # ── llm_model / llm_template: temp path pass karo, direct OCI push ──────────
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

    # ── container: WebDAV (STORAGE_BASE_URL via APISIX) pe upload, pod path set karo ──
    if record.type == "container":
        # Docker TAR se harbor_owner + name extract karo (agar pehle set nahi hua)
        if not record.harbor_owner:
            try:
                import tarfile as _tf, json as _tjson
                if _tf.is_tarfile(temp_path):
                    _REGS = ("docker.io/", "ghcr.io/", "quay.io/",
                             "registry-1.docker.io/", "index.docker.io/")
                    def _strip_r(s):
                        for _r in _REGS:
                            if s.startswith(_r): return s[len(_r):]
                        return s
                    _owner, _iname = None, None
                    with _tf.open(temp_path, "r:*") as _tar:
                        for _en in ("repositories", "manifest.json"):
                            try:
                                _f = _tar.extractfile(_en)
                                if not _f: continue
                                _d = _tjson.loads(_f.read())
                                if _en == "repositories":
                                    for _rk in _d:
                                        _comps = _strip_r(_rk).split("/")
                                        if len(_comps) >= 2: _owner = _comps[-2]
                                        if _comps: _iname = _comps[-1]
                                        break
                                else:
                                    for _mf in _d:
                                        for _rt in (_mf.get("RepoTags") or []):
                                            _path = _strip_r(_rt.partition(":")[0])
                                            _comps = _path.split("/")
                                            if len(_comps) >= 2 and not _owner: _owner = _comps[-2]
                                            if _comps and not _iname: _iname = _comps[-1]
                                            break
                                        if _owner: break
                                if _owner: break
                            except KeyError:
                                continue
                    if _owner:
                        record.harbor_owner = _owner
                    _placeholder = os.path.splitext(record.file_name)[0]
                    if _iname and (not record.name or record.name == _placeholder):
                        record.name = _iname
                    if _owner or _iname:
                        db.commit()
                        logger.info(f"[Library] Container TAR → owner={record.harbor_owner} name={record.name}")
            except Exception as _te:
                logger.warning(f"[Library] Container TAR metadata (non-fatal): {_te}")

        # Container: temp file seedha activity ko dete hain
        # Activity STORAGE_INTERNAL_URL pe temp upload karega → push-image wrapper access karega
        # (WebDAV intermediate step nahi — APISIX backend != push-image accessible storage)
        logger.info(f"[Library] Container received → {temp_path} ({bytes_written:,} bytes)")
        record.file_path    = temp_path
        record.file_size    = bytes_written
        record.progress_pct = 100
        record.status       = "ready"
        db.commit()

        push_workflow_id = f"harbor-push-{item_id}-{uuid.uuid4().hex[:8]}"
        try:
            temporal_client = await TemporalClientManager.get_temporal_client()
            await temporal_client.start_workflow(
                HarborPushWorkflow.run,
                args=[{"item_id": item_id, "temp_path": temp_path}],
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
            try:
                os.remove(temp_path)
            except OSError:
                pass
            record.push_status = "failed"
            record.push_error  = str(exc)
            db.commit()
            raise HTTPException(status_code=500, detail=f"Harbor-Push workflow start failed: {exc}")

        return response_format.success_response(200, "File received and Harbor-Push started successfully", {
            **_item_to_dict(record),
            "push_workflow_id": push_workflow_id,
        })

    # ── Baki sab types: temp file → WebDAV (internal URL) → PV ──────────────
    subdir       = TYPE_SUBDIR.get(record.type, "general")
    storage_base = os.getenv("STORAGE_BASE_URL", "https://devraq.dev.team/library").rstrip("/")
    # PUT + file_path dono STORAGE_BASE_URL se — APISIX pe client_max_body_size badha rakha hai
    public_url = f"{storage_base}/{subdir}/{record.file_name}"

    def _put_to_webdav():
        with open(temp_path, "rb") as f:
            return _req.put(
                public_url,
                data=f,
                headers={"Content-Length": str(bytes_written), "Content-Type": "application/octet-stream"},
                verify=False,
                timeout=None,
            )

    try:
        resp = await asyncio.to_thread(_put_to_webdav)
        if resp.status_code not in (200, 201, 204):
            raise RuntimeError(f"WebDAV PUT failed: {resp.status_code} {resp.text[:200]}")
    except Exception as exc:
        logger.error(f"[Library] WebDAV PUT failed item={item_id}: {exc}")
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
    Single-call upload: metadata X-Library-Metadata header mein, file raw body mein.

    Header example:
      X-Library-Metadata: {"file_name":"harbor.zip","type":"harbor_template","metadata":{...}}

    Ye create_library_item + upload_library_file ko ek hi request mein karta hai.
    """
    import json as _json
    import tempfile
    import requests as _req

    username = _extract_username(request)

    # ── Metadata header parse karo ────────────────────────────────────────────
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
    metadata           = params.get("metadata")  # nested dict — annotations ke liye

    if not file_name:
        raise HTTPException(status_code=400, detail="file_name required in X-Library-Metadata")
    if item_type not in LIBRARY_TYPES:
        raise HTTPException(status_code=400, detail=f"Invalid type '{item_type}'. Valid: {', '.join(sorted(LIBRARY_TYPES))}")

    total_size = int(request.headers.get("content-length") or 0)

    # ── DB record banao (create_library_item logic same) ─────────────────────
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
                    pct    = min(int(bytes_written / total_size * 100), 49)
                    bucket = (pct // 5) * 5
                    if bucket > last_bucket:
                        last_bucket         = bucket
                        record.progress_pct = bucket
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

    # ── ZIP metadata parse karo (name/version/owner auto-set) ────────────────
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

    # ── Phase 2: type ke hisab se file finalize karo ─────────────────────────
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

        def _put_c():
            with open(temp_path, "rb") as f:
                return _req.put(webdav_url, data=f,
                    headers={"Content-Length": str(bytes_written), "Content-Type": "application/octet-stream"},
                    verify=False, timeout=None)
        try:
            r = await asyncio.to_thread(_put_c)
            if r.status_code not in (200, 201, 204):
                raise RuntimeError(f"WebDAV PUT {r.status_code}: {r.text[:200]}")
        except Exception as exc:
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

    def _put_to_webdav():
        with open(temp_path, "rb") as f:
            return _req.put(public_url, data=f,
                headers={"Content-Length": str(bytes_written), "Content-Type": "application/octet-stream"},
                verify=False, timeout=None)
    try:
        resp = await asyncio.to_thread(_put_to_webdav)
        if resp.status_code not in (200, 201, 204):
            raise RuntimeError(f"WebDAV PUT failed: {resp.status_code} {resp.text[:200]}")
    except Exception as exc:
        logger.error(f"[Library] Direct upload WebDAV failed item={item_id}: {exc}")
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

# Virtual types — DB mein stored nahi, query-time filter hain
# name ya harbor_owner mein in keywords mein se koi bhi match hona chahiye
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
        "db_types": ["container", "postgresql"],   # purane container uploads + naye postgresql type
        "keywords": ["postgres", "postgresql"],
    },
}


def _build_filters(db: Session) -> list:
    """DB se distinct types + count nikalo, frontend ke liye filter list banao."""
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
    from sqlalchemy import func

    query = db.query(LibraryItem)

    if type_filter:
        if type_filter in _VIRTUAL_TYPE_MAP:
            from sqlalchemy import or_
            vt = _VIRTUAL_TYPE_MAP[type_filter]
            db_types = vt.get("db_types", [vt.get("db_type", "container")])
            query = query.filter(LibraryItem.type.in_(db_types))
            # name ya harbor_owner mein se koi bhi keyword match kare
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

    # owner_filter — harbor_owner se partial case-insensitive match
    if owner_filter:
        query = query.filter(LibraryItem.harbor_owner.ilike(f"%{owner_filter}%"))

    # harbor_registry_id filter — specific harbor registry ke items
    if harbor_registry_id is not None:
        query = query.filter(LibraryItem.harbor_registry_id == harbor_registry_id)

    total  = query.count()
    offset = (page - 1) * page_size
    items  = query.order_by(LibraryItem.created_at.desc()).offset(offset).limit(page_size).all()

    enriched = _attach_deployments([_item_to_dict(i) for i in items], db)

    # Virtual type filter hone par directory override karo
    # e.g. type=postgresql → container type items bhi "postgresql" directory mein dikhenge
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

    return response_format.success_response(200, "Library item update started successfully", _item_to_dict(item))


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
    """Library item ko K8s cluster pe Harbor ke roop me deploy karo."""
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

import logging
import os
import uuid

from fastapi import HTTPException, Request, UploadFile
from sqlalchemy.orm import Session

from models.library_model import LibraryItem, LIBRARY_TYPES, TYPE_SUBDIR
from service.temporalResource.workers.workers_library import TASK_QUEUE
from service.temporalResource.workflows.workflows_library import (
    LibraryUploadWorkflow,
    LibraryDeleteWorkflow,
    LibraryUpdateWorkflow,
)
from utils.temporal_client import TemporalClientManager
from utils import response_format

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
    return {
        "id":           item.id,
        "name":         item.name,
        "type":         item.type,
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


async def upload_library_item(
    name:    str,
    type:    str,
    version: str | None,
    file:    UploadFile,
    db:      Session,
    request: Request,
):
    if type not in LIBRARY_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid type '{type}'. Must be one of: {', '.join(sorted(LIBRARY_TYPES))}",
        )

    username = _extract_username(request)

    filename  = file.filename or "upload"
    temp_name = f"{uuid.uuid4().hex}_{filename}"
    temp_path = os.path.join(LIBRARY_TEMP_PATH, temp_name)
    subdir    = TYPE_SUBDIR[type]
    dest_path = os.path.join(LIBRARY_BASE_PATH, subdir, filename)

    # ── Step 1: Create DB record FIRST so it shows in list immediately at 0% ──
    record = LibraryItem(
        name=name,
        type=type,
        version=version or None,
        file_name=filename,
        file_path=dest_path,
        file_size=None,
        progress_pct=0,
        status="uploading",
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    item_id = record.id

    # ── Step 2: Stream file to temp staging dir ──────────────────────────────
    os.makedirs(LIBRARY_TEMP_PATH, exist_ok=True)
    try:
        with open(temp_path, "wb") as out:
            while chunk := await file.read(1024 * 1024):
                out.write(chunk)
    except Exception as exc:
        logger.error(f"Temp file write failed: {exc}")
        record.status = "failed"
        db.commit()
        try:
            os.remove(temp_path)
        except OSError:
            pass
        raise HTTPException(status_code=500, detail=f"File staging failed: {exc}")

    # ── Step 3: Start Temporal workflow ──────────────────────────────────────
    try:
        client      = await TemporalClientManager.get_temporal_client()
        workflow_id = f"library-upload-{item_id}-{uuid.uuid4().hex[:8]}"
        await client.start_workflow(
            LibraryUploadWorkflow.run,
            args=[{"item_id": item_id, "temp_path": temp_path, "dest_path": dest_path}],
            id=workflow_id,
            task_queue=TASK_QUEUE,
            search_attributes={
                "Entity":   [name],
                "Action":   ["Library-Upload"],
                "UserName": [username],
            },
        )
        record.workflow_id = workflow_id
        db.commit()
    except Exception as exc:
        logger.error(f"Workflow start failed for library item {item_id}: {exc}")
        record.status = "failed"
        db.commit()
        raise HTTPException(status_code=500, detail=f"Workflow start failed: {exc}")

    return response_format.success_response(
        202,
        "File upload started — transfer running in background",
        {**_item_to_dict(record), "workflow_id": workflow_id},
    )


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

    return response_format.paginated_success_response(
        200, "Library items fetched", [_item_to_dict(i) for i in items], total, page_size, offset
    )


def get_library_item(item_id: int, db: Session):
    item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail=f"Library item {item_id} not found")
    return response_format.success_response(200, "Library item fetched", _item_to_dict(item))


def get_library_item_path(item_id: int, db: Session) -> tuple[str, str]:
    item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail=f"Library item {item_id} not found")
    if item.status != "ready":
        raise HTTPException(
            status_code=409,
            detail=f"File is not ready yet (status={item.status}). Wait for upload to complete.",
        )
    if not os.path.exists(item.file_path):
        raise HTTPException(status_code=410, detail="File no longer exists on storage")
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
            task_queue=TASK_QUEUE,
            search_attributes={
                "Entity":   [item.name],
                "Action":   ["Library-Update"],
                "UserName": [username],
            },
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
    if item.status == "uploading":
        raise HTTPException(status_code=409, detail="Cannot delete while upload is in progress")

    username = _extract_username(request)

    try:
        client      = await TemporalClientManager.get_temporal_client()
        workflow_id = f"library-delete-{item_id}-{uuid.uuid4().hex[:8]}"
        await client.start_workflow(
            LibraryDeleteWorkflow.run,
            args=[{"item_id": item_id}],
            id=workflow_id,
            task_queue=TASK_QUEUE,
            search_attributes={
                "Entity":   [item.name],
                "Action":   ["Library-Delete"],
                "UserName": [username],
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Workflow start failed: {exc}")

    return response_format.success_response(200, "Library item deletion started", {"id": item_id, "workflow_id": workflow_id})

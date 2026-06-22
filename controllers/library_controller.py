import logging
import os
import uuid

from fastapi import HTTPException, UploadFile
from sqlalchemy.orm import Session

from models.library_model import LibraryItem, LIBRARY_TYPES, TYPE_SUBDIR
from service.temporalResource.workers.workers_library import TASK_QUEUE
from service.temporalResource.workflows.workflows_library import LibraryUploadWorkflow
from utils.temporal_client import TemporalClientManager
from utils import response_format

logger = logging.getLogger(__name__)

LIBRARY_BASE_PATH = os.getenv("LIBRARY_BASE_PATH", "/data/library")
LIBRARY_TEMP_PATH = os.getenv("LIBRARY_TEMP_PATH", "/tmp/library_uploads")


def _item_to_dict(item: LibraryItem) -> dict:
    return {
        "id":          item.id,
        "name":        item.name,
        "type":        item.type,
        "version":     item.version,
        "file_name":   item.file_name,
        "file_path":   item.file_path,
        "file_size":   item.file_size,
        "status":      item.status,
        "workflow_id": item.workflow_id,
        "created_at":  item.created_at.isoformat() if item.created_at else None,
        "updated_at":  item.updated_at.isoformat() if item.updated_at else None,
    }


async def upload_library_item(
    name:    str,
    type:    str,
    version: str | None,
    file:    UploadFile,
    db:      Session,
):
    if type not in LIBRARY_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid type '{type}'. Must be one of: {', '.join(sorted(LIBRARY_TYPES))}",
        )

    # ── Step 1: Stream file to temp staging dir ──────────────────────────────
    os.makedirs(LIBRARY_TEMP_PATH, exist_ok=True)
    filename  = file.filename or "upload"
    temp_name = f"{uuid.uuid4().hex}_{filename}"
    temp_path = os.path.join(LIBRARY_TEMP_PATH, temp_name)

    try:
        with open(temp_path, "wb") as out:
            while chunk := await file.read(1024 * 1024):  # 1 MB read chunks
                out.write(chunk)
    except Exception as exc:
        logger.error(f"Temp file write failed: {exc}")
        try:
            os.remove(temp_path)
        except OSError:
            pass
        raise HTTPException(status_code=500, detail=f"File staging failed: {exc}")

    # ── Step 2: Create DB record with status=uploading ────────────────────────
    subdir    = TYPE_SUBDIR[type]
    dest_path = os.path.join(LIBRARY_BASE_PATH, subdir, filename)

    record = LibraryItem(
        name=name,
        type=type,
        version=version or None,
        file_name=filename,
        file_path=dest_path,   # final path — updated by workflow on completion
        file_size=None,
        status="uploading",
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    item_id = record.id

    # ── Step 3: Start Temporal workflow ──────────────────────────────────────
    try:
        client      = await TemporalClientManager.get_temporal_client()
        workflow_id = f"library-upload-{item_id}-{uuid.uuid4().hex[:8]}"
        await client.start_workflow(
            LibraryUploadWorkflow.run,
            args=[{
                "item_id":   item_id,
                "temp_path": temp_path,
                "dest_path": dest_path,
            }],
            id=workflow_id,
            task_queue=TASK_QUEUE,
        )
        # Save workflow_id to DB for tracking
        record.workflow_id = workflow_id
        db.commit()
    except Exception as exc:
        logger.error(f"Failed to start Temporal workflow for library item {item_id}: {exc}")
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


def delete_library_item(item_id: int, db: Session):
    item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail=f"Library item {item_id} not found")

    for path in (item.file_path,):
        try:
            if path and os.path.exists(path):
                os.remove(path)
        except OSError as exc:
            logger.warning(f"Could not remove file {path}: {exc}")

    db.delete(item)
    db.commit()
    return response_format.success_response(200, "Library item deleted", {"id": item_id})

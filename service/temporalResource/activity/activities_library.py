import logging
import os
import shutil

from temporalio import activity

from db_configuration.config import SessionLocal
from models.library_model import LibraryItem

logger = logging.getLogger(__name__)

_CHUNK_BYTES = 5 * 1024 * 1024  # 5 MB per chunk — heartbeat sent after each


@activity.defn
def transfer_file_activity(payload: dict) -> dict:
    """
    Copies the uploaded temp file to the PV destination path.
    Sends a Temporal heartbeat after every 5 MB chunk so Temporal UI shows live progress.
    """
    item_id   = payload["item_id"]
    temp_path = payload["temp_path"]
    dest_path = payload["dest_path"]

    dest_dir = os.path.dirname(dest_path)
    os.makedirs(dest_dir, exist_ok=True)

    file_size   = os.path.getsize(temp_path)
    bytes_done  = 0

    logger.info(f"[LibraryUpload] item={item_id} | {temp_path} → {dest_path} | size={file_size:,} bytes")

    with open(temp_path, "rb") as src, open(dest_path, "wb") as dst:
        while True:
            chunk = src.read(_CHUNK_BYTES)
            if not chunk:
                break
            dst.write(chunk)
            bytes_done += len(chunk)
            pct = round(bytes_done / file_size * 100, 1) if file_size else 100
            activity.heartbeat({
                "progress_pct": pct,
                "bytes_done":   bytes_done,
                "bytes_total":  file_size,
                "stage":        "transferring",
            })

    # Remove temp file after successful copy
    try:
        os.remove(temp_path)
    except OSError:
        pass

    logger.info(f"[LibraryUpload] item={item_id} transfer complete — {bytes_done:,} bytes written")
    return {"file_path": dest_path, "file_size": bytes_done}


@activity.defn
def finalize_library_record_activity(payload: dict) -> dict:
    """Updates the DB record to status=ready with final file_path and file_size."""
    item_id   = payload["item_id"]
    file_path = payload["file_path"]
    file_size = payload["file_size"]
    workflow_id = payload.get("workflow_id", "")

    db = SessionLocal()
    try:
        item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
        if item:
            item.file_path  = file_path
            item.file_size  = file_size
            item.status     = "ready"
            item.workflow_id = workflow_id
            db.commit()
        logger.info(f"[LibraryUpload] item={item_id} finalized, status=ready")
    finally:
        db.close()

    return {"item_id": item_id, "status": "ready"}


@activity.defn
def mark_upload_failed_activity(payload: dict) -> dict:
    """Marks the DB record as failed and cleans up temp + dest files."""
    item_id   = payload["item_id"]
    temp_path = payload.get("temp_path", "")
    dest_path = payload.get("dest_path", "")

    for path in (temp_path, dest_path):
        if path:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except OSError:
                pass

    db = SessionLocal()
    try:
        item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
        if item:
            item.status = "failed"
            db.commit()
        logger.warning(f"[LibraryUpload] item={item_id} marked as failed")
    finally:
        db.close()

    return {"item_id": item_id, "status": "failed"}

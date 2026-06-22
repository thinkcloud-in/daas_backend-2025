import logging
import os

from temporalio import activity

from db_configuration.config import SessionLocal
from models.library_model import LibraryItem

logger = logging.getLogger(__name__)

_CHUNK_BYTES = 5 * 1024 * 1024  # 5 MB — heartbeat sent after each chunk


def _update_progress_in_db(item_id: int, pct: int):
    """Write progress_pct to DB — called every 5% increment during transfer."""
    db = SessionLocal()
    try:
        item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
        if item:
            item.progress_pct = pct
            db.commit()
    except Exception as exc:
        logger.warning(f"[Library] progress DB update failed item={item_id}: {exc}")
    finally:
        db.close()


@activity.defn
def transfer_file_activity(payload: dict) -> dict:
    """
    Copies the uploaded temp file to the PV destination path.
    - Sends Temporal heartbeat every 5 MB (visible in Temporal UI)
    - Updates progress_pct in DB every 5% increment (visible in list API)
    """
    item_id   = payload["item_id"]
    temp_path = payload["temp_path"]
    dest_path = payload["dest_path"]

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    file_size     = os.path.getsize(temp_path)
    bytes_done    = 0
    last_db_pct   = -1   # track last DB-written percent (write every 5%)

    logger.info(f"[Library] upload item={item_id} | {temp_path} → {dest_path} | size={file_size:,} bytes")

    with open(temp_path, "rb") as src, open(dest_path, "wb") as dst:
        while True:
            chunk = src.read(_CHUNK_BYTES)
            if not chunk:
                break
            dst.write(chunk)
            bytes_done += len(chunk)
            pct = int(bytes_done / file_size * 100) if file_size else 100

            # Temporal heartbeat every chunk (5 MB) — shows in Temporal UI
            activity.heartbeat({
                "progress_pct": pct,
                "bytes_done":   bytes_done,
                "bytes_total":  file_size,
                "stage":        "transferring",
            })

            # DB update every 5% — capped at 95 inside loop.
            # 100% is set ONLY by finalize_library_record_activity (with status=ready).
            # This prevents the state: progress_pct=100 but status=uploading.
            bucket = min((pct // 5) * 5, 95)
            if bucket > last_db_pct:
                _update_progress_in_db(item_id, bucket)
                last_db_pct = bucket

    # Set 99% after transfer completes — finalize_activity will set 100 + status=ready
    _update_progress_in_db(item_id, 99)

    try:
        os.remove(temp_path)
    except OSError:
        pass

    logger.info(f"[Library] upload item={item_id} complete — {bytes_done:,} bytes")
    return {"file_path": dest_path, "file_size": bytes_done}


@activity.defn
def finalize_library_record_activity(payload: dict) -> dict:
    """Updates DB record to status=ready with final file_path and file_size."""
    item_id     = payload["item_id"]
    file_path   = payload["file_path"]
    file_size   = payload["file_size"]
    workflow_id = payload.get("workflow_id", "")

    db = SessionLocal()
    try:
        item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
        if item:
            item.file_path    = file_path
            item.file_size    = file_size
            item.progress_pct = 100
            item.status       = "ready"
            item.workflow_id  = workflow_id
            db.commit()
        logger.info(f"[Library] item={item_id} finalized → status=ready")
    finally:
        db.close()

    return {"item_id": item_id, "status": "ready"}


@activity.defn
def mark_upload_failed_activity(payload: dict) -> dict:
    """Marks DB record as failed and removes temp + dest files."""
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
        logger.warning(f"[Library] item={item_id} marked failed")
    finally:
        db.close()

    return {"item_id": item_id, "status": "failed"}


@activity.defn
def delete_library_file_activity(payload: dict) -> dict:
    """Deletes the file from PV storage and removes the DB record."""
    item_id = payload["item_id"]

    db = SessionLocal()
    try:
        item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
        if not item:
            logger.warning(f"[Library] delete item={item_id} not found in DB — already deleted?")
            return {"item_id": item_id, "status": "deleted"}

        file_path = item.file_path
        try:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"[Library] deleted file: {file_path}")
        except OSError as exc:
            logger.warning(f"[Library] could not remove file {file_path}: {exc}")

        db.delete(item)
        db.commit()
        logger.info(f"[Library] item={item_id} deleted from DB")
    finally:
        db.close()

    return {"item_id": item_id, "status": "deleted"}


@activity.defn
def update_library_metadata_activity(payload: dict) -> dict:
    """Updates name and/or version of a library item in DB (file itself stays unchanged)."""
    item_id = payload["item_id"]
    name    = payload.get("name")
    version = payload.get("version")

    db = SessionLocal()
    try:
        item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
        if not item:
            raise RuntimeError(f"Library item {item_id} not found")
        if name is not None:
            item.name = name
        if version is not None:
            item.version = version
        db.commit()
        logger.info(f"[Library] item={item_id} updated — name={name} version={version}")
    finally:
        db.close()

    return {"item_id": item_id, "status": "updated"}

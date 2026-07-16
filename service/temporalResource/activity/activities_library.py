import logging
import os
import time

from temporalio import activity

from db_configuration.config import SessionLocal
from models.library_model import LibraryItem
from utils.k8s_pod_exec import delete_file_from_pod

logger = logging.getLogger(__name__)


_MAX_POLL_SECONDS = 6 * 3600  # 6 hours max — iske baad timeout

@activity.defn(name="Library-Track-Upload-Progress")
def transfer_file_activity(payload: dict) -> dict:
    """
    HTTP handler seedha pod mein stream karta hai.
    Ye activity sirf DB poll karke Temporal mein progress dikhata hai.
    Jab DB status=ready/failed ho jaaye, ya max 6h baad, activity complete.
    """
    item_id    = payload["item_id"]
    total_size = payload.get("total_size", 0)
    deadline   = time.time() + _MAX_POLL_SECONDS

    db = SessionLocal()
    try:
        while time.time() < deadline:
            item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()

            # Item DB se delete ho gaya (force delete) — quietly complete
            if not item:
                logger.info(f"[Library] item={item_id} deleted — stopping tracker")
                return {"item_id": item_id, "status": "deleted", "progress_pct": 0}

            pct    = item.progress_pct
            status = item.status

            activity.heartbeat({
                "progress_pct": pct,
                "bytes_done":   int(pct / 100 * total_size) if total_size else 0,
                "bytes_total":  total_size,
                "status":       status,
            })

            if status in ("ready", "failed"):
                logger.info(f"[Library] item={item_id} status={status} pct={pct}")
                return {"item_id": item_id, "status": status, "progress_pct": pct}

            time.sleep(2)

        raise RuntimeError(f"Upload tracking timed out after 6h for item={item_id}")
    finally:
        db.close()


@activity.defn(name="Library-Finalize-Record-status-ready")
def finalize_library_record_activity(payload: dict) -> dict:
    """DB record status=ready update karo — file_path aur file_size set karo."""
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


@activity.defn(name="Library-Mark-Upload-Failed-and-Cleanup")
def mark_upload_failed_activity(payload: dict) -> dict:
    """DB record failed mark karo aur temp file hata do."""
    item_id   = payload["item_id"]
    temp_path = payload.get("temp_path", "")

    if temp_path:
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
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


@activity.defn(name="Library-Delete-File-from-Pod-and-DB")
def delete_library_file_activity(payload: dict) -> dict:
    """Pod se file delete karo aur DB record remove karo."""
    item_id = payload["item_id"]

    db = SessionLocal()
    try:
        item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
        if not item:
            logger.warning(f"[Library] delete item={item_id} not found — already deleted?")
            return {"item_id": item_id, "status": "deleted"}

        file_path = item.file_path
        logger.info(f"[Library] delete item={item_id} file={file_path}")

        if file_path:
            try:
                delete_file_from_pod(file_path)
                logger.info(f"[Library] pod file deleted: {file_path}")
            except Exception as exc:
                logger.warning(f"[Library] pod delete failed {file_path}: {exc} — removing DB record anyway")

        db.delete(item)
        db.commit()
        logger.info(f"[Library] item={item_id} deleted from DB")
    finally:
        db.close()

    return {"item_id": item_id, "status": "deleted"}


@activity.defn(name="Library-Update-Name-and-Version-in-DB")
def update_library_metadata_activity(payload: dict) -> dict:
    """Name aur version update karo DB mein — file untouched."""
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

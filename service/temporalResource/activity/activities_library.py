import os
import time
from urllib.parse import quote as _url_quote

import requests as _req
from temporalio import activity

from db_configuration.config import SessionLocal
from models.library_model import LibraryItem
from utils.k8s_pod_exec import delete_file_from_pod

logger = activity.logger
# container: activities_harbor_push.py (skopeo) se push hote hain.
# llm_model/llm_template: activities_llm_push.py (oras) se push hote hain.
# Dono cases mein harbor_image column "host/project/owner/name:tag" format mein
# bharta hai, isliye ek hi _harbor_delete_image() dono ke liye kaam karta hai.
_HARBOR_PUSH_TYPES = {"container", "llm_model", "llm_template"}


def _harbor_delete_image(harbor_image: str, harbor_user: str, harbor_pass: str):
    """
    Harbor REST API se image delete karo.
    harbor_image format: "172.16.4.41:30080/library/openwebui/openwebui:0.6.5"
    """
    # Parse: host / project / repo_path : tag
    # "172.16.4.41:30080/library/openwebui/openwebui:0.6.5"
    parts = harbor_image.split("/", 1)          # ["172.16.4.41:30080", "library/openwebui/openwebui:0.6.5"]
    host  = parts[0]
    rest  = parts[1] if len(parts) > 1 else ""  # "library/openwebui/openwebui:0.6.5"

    # tag alag karo
    if ":" in rest.split("/")[-1]:
        last_slash = rest.rfind(":")
        tag  = rest[last_slash + 1:]            # "0.6.5"
        path = rest[:last_slash]                # "library/openwebui/openwebui"
    else:
        tag  = "latest"
        path = rest

    path_parts = path.split("/")               # ["library", "openwebui", "openwebui"]
    project    = path_parts[0]                 # "library"
    repo_path  = "/".join(path_parts[1:])      # "openwebui/openwebui"
    # Harbor ka API gateway ek layer % -decode kar deta hai isse pahle ki
    # request Harbor core tak pahunche — single-encoded "%2F" isliye ek "/"
    # ban jaata hai aur router route hi match nahi kar paata (404 "path not
    # found"). Double-encoding se yeh survive karta hai. Verified live.
    repo_enc   = _url_quote(_url_quote(repo_path, safe=""), safe="")  # "openwebui%252Fopenwebui"

    base_url = f"http://{host}"
    tag_url  = f"{base_url}/api/v2.0/projects/{project}/repositories/{repo_enc}/artifacts/{tag}"

    # Tag se digest resolve karo — tag se seedha delete karne par sirf tag
    # hatta tha, artifact (manifest+blobs) orphan reh jaata tha. Digest se
    # delete karne par asli artifact bhi hat jaata hai.
    get_resp = _req.get(tag_url, auth=(harbor_user, harbor_pass), verify=False, timeout=30)
    if get_resp.status_code == 404:
        logger.info(f"[LibraryDelete] artifact already gone (tag={tag})")
        return
    get_resp.raise_for_status()
    digest  = get_resp.json()["digest"]
    api_url = f"{base_url}/api/v2.0/projects/{project}/repositories/{repo_enc}/artifacts/{digest}"

    logger.info(f"[LibraryDelete] Harbor API DELETE: {api_url}")
    resp = _req.delete(
        api_url,
        auth=(harbor_user, harbor_pass),
        verify=False,
        timeout=30,
    )
    if resp.status_code in (200, 202, 404):
        # 404 = already deleted — theek hai
        logger.info(f"[LibraryDelete] Harbor delete status={resp.status_code}")
        return
    raise RuntimeError(f"Harbor delete failed: {resp.status_code} {resp.text[:200]}")


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
    """
    Harbor image (agar pushed hai) delete karo, pod se file delete karo, phir
    DB record remove karo — is order mein, taaki Harbor cleanup na ho paaye to
    DB record bhi na hate (warna orphan image Harbor mein reh jaata, jisका
    trace karne ka koi record hi nahi bachta). Harbor delete fail hone par
    activity exception raise karti hai — workflow-level retry policy (5
    attempts) khud dobara try karegi.
    """
    item_id = payload["item_id"]

    db = SessionLocal()
    try:
        item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
        if not item:
            logger.warning(f"[Library] delete item={item_id} not found — already deleted?")
            return {"item_id": item_id, "status": "deleted"}

        file_path = item.file_path
        logger.info(f"[Library] delete item={item_id} type={item.type} file={file_path}")

        if item.type in _HARBOR_PUSH_TYPES and item.harbor_image:
            # Harbor se image delete karo — yahan koi try/except nahi: fail hone
            # par exception upar propagate hogi, DB record delete hi nahi hoga,
            # aur Temporal poori activity retry karega.
            _harbor_delete_image(
                harbor_image=item.harbor_image,
                harbor_user=item.harbor_user or "admin",
                harbor_pass=item.harbor_pass or "Harbor12345",
            )
            logger.info(f"[Library] Harbor image deleted: {item.harbor_image}")

        if file_path:
            # Storage (WebDAV/pod) se file delete karo — Harbor-push types ke
            # liye bhi try karo (agar koi original upload file abhi bhi pod pe
            # padi ho); `rm -f` idempotent hai, missing file pe error nahi deta.
            try:
                delete_file_from_pod(file_path)
                logger.info(f"[Library] pod file deleted: {file_path}")
            except Exception as exc:
                logger.warning(f"[Library] pod delete failed {file_path}: {exc} — continuing")

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

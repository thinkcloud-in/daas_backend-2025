import os
import time
from urllib.parse import quote as _url_quote

import requests as _req
from temporalio import activity

from db_configuration.config import SessionLocal
from models.library_model import LibraryItem
from utils.k8s_pod_exec import delete_file_from_pod

logger = activity.logger
# container: pushed via activities_harbor_push.py (skopeo).
# llm_model/llm_template: pushed via activities_llm_push.py (oras).
# In both cases the harbor_image column is filled in "host/project/owner/name:tag"
# format, so a single _harbor_delete_image() works for both.
_HARBOR_PUSH_TYPES = {"container", "llm_model", "llm_template"}


def _harbor_delete_image(harbor_image: str, harbor_user: str, harbor_pass: str):
    """
    Delete an image via the Harbor REST API.
    harbor_image format: "172.16.4.41:30080/library/openwebui/openwebui:0.6.5"
    """
    # Parse: host / project / repo_path : tag
    # "172.16.4.41:30080/library/openwebui/openwebui:0.6.5"
    parts = harbor_image.split("/", 1)          # ["172.16.4.41:30080", "library/openwebui/openwebui:0.6.5"]
    host  = parts[0]
    rest  = parts[1] if len(parts) > 1 else ""  # "library/openwebui/openwebui:0.6.5"

    # Separate out the tag
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
    # Harbor's API gateway does one layer of %-decoding before the request
    # reaches Harbor core — so a single-encoded "%2F" becomes a "/" and the
    # router cannot match the route at all (404 "path not found").
    # Double-encoding lets it survive. Verified live.
    repo_enc   = _url_quote(_url_quote(repo_path, safe=""), safe="")  # "openwebui%252Fopenwebui"

    base_url = f"http://{host}"
    tag_url  = f"{base_url}/api/v2.0/projects/{project}/repositories/{repo_enc}/artifacts/{tag}"

    # Resolve the digest from the tag — deleting directly by tag only removed
    # the tag, leaving the artifact (manifest+blobs) orphaned. Deleting by
    # digest removes the actual artifact too.
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
        # 404 = already deleted — that's fine
        logger.info(f"[LibraryDelete] Harbor delete status={resp.status_code}")
        _harbor_delete_repo_if_empty(base_url, project, repo_enc, harbor_user, harbor_pass)
        return
    raise RuntimeError(f"Harbor delete failed: {resp.status_code} {resp.text[:200]}")


def _harbor_delete_repo_if_empty(base_url, project, repo_enc, harbor_user, harbor_pass):
    """
    After deleting an artifact, the repository itself stays in Harbor as an
    empty entry. Check whether any artifact remains in the repo (if other
    versions/tags are pushed in the same repo, don't touch them) -- only
    delete the repo when it is completely empty.
    Non-fatal: on failure just warn, because the actual artifact has already
    been deleted.
    """
    list_url = f"{base_url}/api/v2.0/projects/{project}/repositories/{repo_enc}/artifacts"
    try:
        resp = _req.get(list_url, auth=(harbor_user, harbor_pass), verify=False, timeout=30)
        if resp.status_code == 404:
            return
        resp.raise_for_status()
        if resp.json():
            return  # artifacts still remain -- don't touch the repo

        repo_url = f"{base_url}/api/v2.0/projects/{project}/repositories/{repo_enc}"
        del_resp = _req.delete(repo_url, auth=(harbor_user, harbor_pass), verify=False, timeout=30)
        if del_resp.status_code in (200, 202, 404):
            logger.info(f"[LibraryDelete] empty Harbor repository removed: {project}/{repo_enc}")
        else:
            logger.warning(f"[LibraryDelete] repo cleanup failed (non-fatal): {del_resp.status_code} {del_resp.text[:200]}")
    except Exception as exc:
        logger.warning(f"[LibraryDelete] repo cleanup failed (non-fatal): {exc}")


_MAX_POLL_SECONDS = 6 * 3600  # 6 hours max — timeout after this

@activity.defn(name="Library-Track-Upload-Progress")
def transfer_file_activity(payload: dict) -> dict:
    """
    The HTTP handler streams straight into the pod.
    This activity only polls the DB and surfaces the progress in Temporal.
    The activity completes when the DB status becomes ready/failed, or after 6h max.
    """
    item_id    = payload["item_id"]
    total_size = payload.get("total_size", 0)
    deadline   = time.time() + _MAX_POLL_SECONDS

    db = SessionLocal()
    try:
        while time.time() < deadline:
            item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()

            # The item was deleted from the DB (force delete) — quietly complete
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
    """Update the DB record to status=ready — set file_path and file_size."""
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
    """Mark the DB record as failed and remove the temp file."""
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
    Delete the Harbor image (if pushed), delete the file from the pod, then
    remove the DB record — in this order, so that if Harbor cleanup fails the
    DB record is not removed either (otherwise an orphan image stays in Harbor
    with no record left to trace it). If the Harbor delete fails, the activity
    raises an exception — the workflow-level retry policy (5 attempts) will
    retry it.
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
            # Delete the image from Harbor — no try/except here: on failure the
            # exception propagates up, the DB record is not deleted, and
            # Temporal retries the whole activity.
            _harbor_delete_image(
                harbor_image=item.harbor_image,
                harbor_user=item.harbor_user or "admin",
                harbor_pass=item.harbor_pass or "Harbor12345",
            )
            logger.info(f"[Library] Harbor image deleted: {item.harbor_image}")

        if file_path:
            # Delete the file from storage (WebDAV/pod) — try this for
            # Harbor-push types too (in case an original upload file is still
            # on the pod); `rm -f` is idempotent and does not error on a
            # missing file.
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
    """Update name and version in the DB — file untouched."""
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

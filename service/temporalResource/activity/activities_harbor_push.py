"""
Harbor Push Activity  (WebDAV + push-image API → skopeo → Harbor)

Flow:
  1. DB → library item + kubernetes_deployments
  2. Harbor URL + creds — DB only
  3. Local metadata extraction (tar/zip directly)
  4. File → WebDAV (STORAGE_BASE_URL) → shared /data/library PV
  5. POST push-image/push/image → push-image-tool pod → skopeo → Harbor
  6. WebDAV cleanup + DB update
"""

import json
import logging
import os
import re
import shutil
import tarfile
import tempfile
import uuid
import zipfile

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from temporalio import activity

from db_configuration.config import SessionLocal
from models.library_model import LibraryItem
from models.kubernetes_deploy_model import KubernetesDeployment

logger = logging.getLogger(__name__)

HARBOR_PUSH_TASK_QUEUE = "harbor-push-queue"

_PUSH_IMAGE_BASE_URL  = os.getenv("PUSH_IMAGE_BASE_URL",  "").rstrip("/")
_STORAGE_BASE_URL     = os.getenv("STORAGE_BASE_URL",     "").rstrip("/")
_STORAGE_INTERNAL_URL = os.getenv("STORAGE_INTERNAL_URL", "").rstrip("/")
_POD_LIBRARY_PATH     = "/data/library"  # Pod ka WebDAV root mount path

_REGISTRIES = (
    "docker.io/", "ghcr.io/", "quay.io/",
    "registry-1.docker.io/", "index.docker.io/",
)


# ── helpers ───────────────────────────────────────────────────────────────────

def _sanitize(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "_", name).lower()


def _strip_reg(s: str) -> str:
    for r in _REGISTRIES:
        if s.startswith(r):
            return s[len(r):]
    return s


def _webdav_upload_file(local_path: str, uid: str) -> tuple:
    """
    Single file ko WebDAV pe upload karo.
    STORAGE_BASE_URL (APISIX) use karo — large files ke liye no size limit.
    Returns: (pod_path, cleanup_url)
    """
    storage_url = _STORAGE_BASE_URL or _STORAGE_INTERNAL_URL
    if not storage_url:
        raise RuntimeError("STORAGE_BASE_URL env var set nahi hai")
    fname       = os.path.basename(local_path)
    remote_name = f"harbor-push-{uid}-{fname}"
    upload_url  = f"{storage_url}/{remote_name}"
    with open(local_path, "rb") as fh:
        r = requests.put(
            upload_url, data=fh,
            headers={"Content-Type": "application/octet-stream"},
            timeout=3600, verify=False,
        )
    if not r.ok:
        raise RuntimeError(f"WebDAV upload failed: {r.status_code} {r.text[:200]}")
    pod_path = f"{_POD_LIBRARY_PATH}/{remote_name}"
    logger.info(f"[HarborPush] WebDAV upload OK → {upload_url} (pod: {pod_path})")
    return pod_path, upload_url


def _webdav_upload_dir(local_dir: str, uid: str) -> tuple:
    """
    OCI dir ke saare files WebDAV pe recursively upload karo.
    Returns: (pod_dir_path, cleanup_url)
    """
    if not _STORAGE_INTERNAL_URL:
        raise RuntimeError("STORAGE_INTERNAL_URL env var set nahi hai")
    dir_name = f"harbor-push-{uid}"
    base_url = f"{_STORAGE_INTERNAL_URL}/{dir_name}"
    requests.request("MKCOL", base_url, timeout=30, verify=False)
    for root, dirs, files in os.walk(local_dir):
        rel = os.path.relpath(root, local_dir).replace(os.sep, "/")
        if rel != ".":
            requests.request("MKCOL", f"{base_url}/{rel}", timeout=30, verify=False)
        for fname in files:
            fpath    = os.path.join(root, fname)
            rel_path = f"{rel}/{fname}" if rel != "." else fname
            put_url  = f"{base_url}/{rel_path}"
            with open(fpath, "rb") as fh:
                r = requests.put(
                    put_url, data=fh,
                    headers={"Content-Type": "application/octet-stream"},
                    timeout=3600, verify=False,
                )
            if not r.ok:
                logger.warning(f"[HarborPush] WebDAV {rel_path}: HTTP {r.status_code}")
    pod_path = f"{_POD_LIBRARY_PATH}/{dir_name}"
    logger.info(f"[HarborPush] WebDAV dir upload OK → {base_url}")
    return pod_path, base_url


def _webdav_delete(url: str) -> None:
    try:
        requests.delete(url, timeout=30, verify=False)
        logger.info(f"[HarborPush] WebDAV cleanup OK: {url}")
    except Exception as e:
        logger.warning(f"[HarborPush] WebDAV cleanup failed ({url}): {e}")


def _extract_image_metadata_local(local_path: str, item) -> dict:
    """
    Image metadata nikalo.
    - File locally accessible hai → ZIP/TAR parse karo
    - Pod path hai (file locally nahi) → DB values + extension se derive karo
    Returns: image_owner, image_name, image_version, source_type, is_oci_dir_zip
    """
    file_ext      = os.path.splitext(local_path)[1].lower()
    is_zip        = file_ext == ".zip"
    image_owner   = None
    image_name    = None
    image_version = "latest"
    source_type   = "docker-archive"
    is_oci_dir_zip = False

    # File pod pe hai, backend pe locally nahi → DB values use karo
    if not os.path.exists(local_path):
        image_owner   = _sanitize(item.harbor_owner or "")
        image_name    = _sanitize(item.name or "")
        image_version = _sanitize(item.version or "latest") if item.version else "latest"
        if is_zip:
            source_type    = "oci"
            is_oci_dir_zip = True
        logger.info(
            f"[HarborPush] File pod pe hai, DB metadata use kar rahe hain: "
            f"owner={image_owner} name={image_name} version={image_version} zip={is_oci_dir_zip}"
        )
        # item fallbacks
        if not image_owner:
            image_owner = _sanitize(item.harbor_owner or "")
        if not image_name:
            image_name = _sanitize(item.name or "")
        if item.version and item.version not in ("", "latest"):
            image_version = _sanitize(item.version)
        return {
            "image_owner":    image_owner,
            "image_name":     image_name,
            "image_version":  image_version,
            "source_type":    source_type,
            "is_oci_dir_zip": is_oci_dir_zip,
        }

    if is_zip:
        try:
            with zipfile.ZipFile(local_path, "r") as z:
                meta_entry = next(
                    (n for n in z.namelist() if os.path.basename(n) == "version_metadata.json"),
                    None,
                )
                if meta_entry:
                    meta_data  = json.loads(z.open(meta_entry).read())
                    zip_format = meta_data.get("format", "")
                    image_owner   = _sanitize(meta_data.get("owner_name", "") or "")
                    image_name    = _sanitize(
                        meta_data.get("artifact_name", "") or meta_data.get("name", "") or ""
                    )
                    image_version = _sanitize(meta_data.get("version", "latest") or "latest")
                    if zip_format == "OCI-Skopeo-Dir":
                        source_type    = "oci"
                        is_oci_dir_zip = True
        except Exception as e:
            logger.warning(f"[HarborPush] ZIP metadata read failed: {e}")
    else:
        try:
            with tarfile.open(local_path, "r:*") as tar:
                # repositories
                try:
                    f = tar.extractfile("repositories")
                    if f:
                        repos = json.loads(f.read())
                        for repo_key, tags_dict in repos.items():
                            stripped = _strip_reg(repo_key)
                            comps    = stripped.split("/")
                            if len(comps) >= 2:
                                image_owner = _sanitize(comps[-2])
                            if comps:
                                image_name = _sanitize(comps[-1])
                            if isinstance(tags_dict, dict):
                                first_tag = next(iter(tags_dict), "")
                                if first_tag and first_tag != "latest":
                                    image_version = _sanitize(first_tag)
                            break
                except KeyError:
                    pass

                # manifest.json fallback
                if not image_owner or not image_name:
                    try:
                        f = tar.extractfile("manifest.json")
                        if f:
                            for mf in json.loads(f.read()):
                                for repo_tag in (mf.get("RepoTags") or []):
                                    stripped = _strip_reg(repo_tag)
                                    path, _, tag_p = stripped.partition(":")
                                    comps = path.split("/")
                                    if len(comps) >= 2 and not image_owner:
                                        image_owner = _sanitize(comps[-2])
                                    if comps and not image_name:
                                        image_name = _sanitize(comps[-1])
                                    if tag_p and tag_p != "latest":
                                        image_version = _sanitize(tag_p)
                                    break
                                if image_owner:
                                    break
                    except KeyError:
                        pass
        except Exception as e:
            logger.warning(f"[HarborPush] TAR metadata read failed: {e}")

    # item fallbacks
    if not image_owner:
        image_owner = _sanitize(item.harbor_owner or "")
    if not image_name:
        image_name = _sanitize(item.name or "")
    # item.version overrides only if it's set and not placeholder
    if item.version and item.version not in ("", "latest"):
        image_version = _sanitize(item.version)

    return {
        "image_owner":    image_owner,
        "image_name":     image_name,
        "image_version":  image_version,
        "source_type":    source_type,
        "is_oci_dir_zip": is_oci_dir_zip,
    }


def _push_image_via_api(
    harbor_host: str, harbor_user: str, harbor_pass: str,
    project: str, image_name: str, tag: str,
    source_path: str, source_type: str,
) -> None:
    """POST → push-image wrapper → skopeo → Harbor."""
    if not _PUSH_IMAGE_BASE_URL:
        raise RuntimeError("PUSH_IMAGE_BASE_URL env var set nahi hai")
    payload = {
        "harbor_url":  harbor_host,
        "username":    harbor_user,
        "password":    harbor_pass,
        "project":     project,
        "image_name":  image_name,
        "tag":         tag,
        "source_path": source_path,
        "source_type": source_type,
        "tls_verify":  False,
    }
    logger.info(f"[HarborPush] POST {_PUSH_IMAGE_BASE_URL}/push/image → {harbor_host}/{project}/{image_name}:{tag}")
    resp = requests.post(
        f"{_PUSH_IMAGE_BASE_URL}/push/image",
        json=payload, timeout=900, verify=False,
    )
    if not resp.ok:
        raise RuntimeError(f"push/image API failed (HTTP {resp.status_code}): {resp.text[:1000]}")
    logger.info(f"[HarborPush] push/image OK: {resp.text[:200]}")


# ── main activity ─────────────────────────────────────────────────────────────

@activity.defn(name="harbor_push_activity")
def harbor_push_activity(params: dict) -> dict:
    item_id   = params["item_id"]
    temp_path = params.get("temp_path")
    db        = SessionLocal()

    def _update(status: str, error: str = None, image: str = None,
                version: str = None, owner: str = None,
                h_user: str = None, h_pass: str = None, name: str = None):
        try:
            it = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
            if it:
                it.push_status = status
                if error   is not None: it.push_error   = error
                if image   is not None: it.harbor_image = image
                if version is not None: it.version      = version
                if owner   is not None: it.harbor_owner = owner
                if h_user  is not None: it.harbor_user  = h_user
                if h_pass  is not None: it.harbor_pass  = h_pass
                if name    is not None: it.name         = name
                db.commit()
        except Exception as exc:
            logger.warning(f"[HarborPush] DB update failed: {exc}")
            try:
                db.rollback()
            except Exception:
                pass

    try:
        # ── 1. Load records ──────────────────────────────────────────────────
        item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
        if not item:
            raise RuntimeError(f"Library item {item_id} not found")
        if not temp_path:
            temp_path = item.file_path
        if not temp_path:
            raise RuntimeError("file_path not set in DB")
        if not item.harbor_registry_id:
            raise RuntimeError("harbor_registry_id not set — Harbor instance select karo")

        harbor_dep = db.query(KubernetesDeployment).filter(
            KubernetesDeployment.id == item.harbor_registry_id
        ).first()
        if not harbor_dep:
            raise RuntimeError(f"Harbor registry id={item.harbor_registry_id} not found")
        if not harbor_dep.harbor_url:
            raise RuntimeError(f"Harbor id={item.harbor_registry_id} ka harbor_url not set")

        # ── 2. Harbor creds (DB only — no env vars) ─────────────────────────
        harbor_url  = harbor_dep.harbor_url.strip().rstrip("/")
        harbor_host = harbor_url.split("://", 1)[1] if "://" in harbor_url else harbor_url
        harbor_user = harbor_dep.harbor_user or item.harbor_user or "admin"
        harbor_pass = harbor_dep.harbor_pass or item.harbor_pass or "Harbor12345"
        project = (item.harbor_project or "library").strip("/")

        _update("pushing")
        logger.info(f"[HarborPush] item={item_id} harbor={harbor_host}")

        # ── 3. Local metadata extraction ─────────────────────────────────────
        meta = _extract_image_metadata_local(temp_path, item)
        if not meta["image_owner"]:
            meta["image_owner"] = _sanitize(item.harbor_project or "library")
            logger.warning(
                f"[HarborPush] image_owner nahi mila — defaulting to '{meta['image_owner']}'. "
                "Docker tar mein proper RepoTag rakho ya upload request mein harbor_owner pass karo."
            )

        image_owner    = meta["image_owner"]
        image_name     = meta["image_name"]
        image_version  = meta["image_version"]
        source_type    = meta["source_type"]
        is_oci_dir_zip = meta["is_oci_dir_zip"]

        logger.info(
            f"[HarborPush] meta → owner={image_owner} name={image_name} "
            f"version={image_version} source_type={source_type}"
        )

        # ── 4. WebDAV upload → push-image API (skopeo) → Harbor ────────────────
        # docker-archive (.tar) → WebDAV → push/image (source_type=docker-archive)
        # OCI dir ZIP   (.zip)  → extract → WebDAV dir → push/image (source_type=oci)
        dest_image  = f"{harbor_host}/{project}/{image_owner}/{image_name}:{image_version}"
        extract_tmp = None
        webdav_url  = None
        try:
            uid = uuid.uuid4().hex[:8]
            if is_oci_dir_zip:
                extract_tmp = tempfile.mkdtemp(prefix="harbor-oci-")
                with zipfile.ZipFile(temp_path, "r") as z:
                    z.extractall(extract_tmp)
                oci_dir = extract_tmp
                for root, dirs, files in os.walk(extract_tmp):
                    if "oci-layout" in files:
                        oci_dir = root
                        break
                source_path, webdav_url = _webdav_upload_dir(oci_dir, uid)
                source_type = "oci"
            else:
                source_path, webdav_url = _webdav_upload_file(temp_path, uid)
                source_type = "docker-archive"

            logger.info(f"[HarborPush] pushing → {dest_image}")
            _push_image_via_api(
                harbor_host, harbor_user, harbor_pass,
                project, f"{image_owner}/{image_name}", image_version,
                source_path, source_type,
            )
            logger.info(f"[HarborPush] push OK → {dest_image}")

        finally:
            if webdav_url:
                _webdav_delete(webdav_url)
            if extract_tmp:
                shutil.rmtree(extract_tmp, ignore_errors=True)
            if temp_path and os.path.isfile(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass

        # ── 6. DB update ──────────────────────────────────────────────────────
        _update(
            "pushed",
            image=dest_image, version=image_version,
            owner=image_owner, h_user=harbor_user, h_pass=harbor_pass,
            name=image_name,
        )
        logger.info(f"[HarborPush] item={item_id} done → {dest_image}")
        return {
            "status":       "pushed",
            "harbor_image": dest_image,
            "version":      image_version,
            "owner":        image_owner,
            "name":         image_name,
        }

    except Exception as exc:
        logger.error(f"[HarborPush] item={item_id} failed: {exc}", exc_info=True)
        _update("failed", error=str(exc))
        if temp_path:
            try:
                os.remove(temp_path)
            except OSError:
                pass
        raise

    finally:
        try:
            db.close()
        except Exception:
            pass

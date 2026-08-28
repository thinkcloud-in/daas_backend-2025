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

logger = activity.logger
HARBOR_PUSH_TASK_QUEUE = "harbor-push-queue"


def _clean_env_url(value: str | None) -> str:
    if value is None:
        return ""
    return value.split("#", 1)[0].strip().rstrip("/")


_PUSH_IMAGE_BASE_URL  = _clean_env_url(os.getenv("PUSH_IMAGE_BASE_URL",  ""))
_STORAGE_BASE_URL     = _clean_env_url(os.getenv("STORAGE_BASE_URL",     ""))
_STORAGE_INTERNAL_URL = _clean_env_url(os.getenv("STORAGE_INTERNAL_URL", ""))
_POD_LIBRARY_PATH     = os.getenv("POD_LIBRARY_PATH", "/data/library")


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
    Push-image wrapper pod ke liye internal storage path prefer karo,
    kyunki woh /data/library mount ke same shared PV se source file dekhta hai.
    Returns: (pod_path, cleanup_url)
    """
    storage_url = _STORAGE_INTERNAL_URL or _STORAGE_BASE_URL
    if not storage_url:
        raise RuntimeError("STORAGE_INTERNAL_URL / STORAGE_BASE_URL env vars are not set")
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

    # pod_path: WebDAV root (_POD_LIBRARY_PATH) + URL subpath + filename
    # e.g. STORAGE_BASE_URL=http://host:port/library → URL subpath=/library
    #      pod_path = /data/library/library/{remote_name}
    from urllib.parse import urlparse as _urlparse
    _url_subpath = _urlparse(storage_url).path.rstrip("/")  # e.g. "/library"
    pod_path = f"{_POD_LIBRARY_PATH}{_url_subpath}/{remote_name}"
    logger.info(f"[HarborPush] WebDAV upload OK → {upload_url} (pod: {pod_path})")
    return pod_path, upload_url


def _webdav_upload_dir(local_dir: str, uid: str) -> tuple:
    """
    OCI dir ke saare files WebDAV pe recursively upload karo.
    Returns: (pod_dir_path, cleanup_url)
    """
    if not _STORAGE_INTERNAL_URL:
        raise RuntimeError("STORAGE_INTERNAL_URL env var is not set")
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
    from urllib.parse import urlparse as _urlparse
    _url_subpath = _urlparse(_STORAGE_INTERNAL_URL).path.rstrip("/")
    pod_path = f"{_POD_LIBRARY_PATH}{_url_subpath}/{dir_name}"
    logger.info(f"[HarborPush] WebDAV dir upload OK → {base_url}")
    return pod_path, base_url


def _webdav_delete(url: str) -> None:
    try:
        requests.delete(url, timeout=30, verify=False)
        logger.info(f"[HarborPush] WebDAV cleanup OK: {url}")
    except Exception as e:
        logger.warning(f"[HarborPush] WebDAV cleanup failed ({url}): {e}")


def _detect_tar_source_type(tar_path: str) -> str:
    """
    Tar ke andar peek karo: manifest.json → docker-archive, index.json → oci-archive.
    `docker save` tars mein manifest.json hoti hai; `skopeo copy --format oci` tars mein index.json.
    """
    try:
        with tarfile.open(tar_path, "r:*") as tf:
            names = tf.getnames()
        if "manifest.json" in names:
            return "docker-archive"
        if "index.json" in names:
            return "oci-archive"
        logger.warning(f"[HarborPush] Tar mein na manifest.json na index.json — defaulting docker-archive. Names: {names[:10]}")
    except Exception as e:
        logger.warning(f"[HarborPush] tar format detection failed ({tar_path}): {e}")
    return "docker-archive"


def _read_docker_archive_labels(tar_path: str) -> dict:
    """
    docker-archive image config se OCI labels padho.
    manifest.json → Config blob path → config JSON → Labels dict.
    """
    try:
        with tarfile.open(tar_path, "r:*") as tf:
            with tf.extractfile("manifest.json") as f:
                manifests = json.loads(f.read())
            config_path = manifests[0].get("Config", "")
            if config_path:
                with tf.extractfile(config_path) as f:
                    config = json.loads(f.read())
                return config.get("config", {}).get("Labels") or {}
    except Exception as e:
        logger.warning(f"[HarborPush] docker-archive labels read failed ({tar_path}): {e}")
    return {}


def _owner_from_url(url: str) -> str:
    """
    org.opencontainers.image.url se owner naam nikalo.
    https://hub.docker.com/_/postgres → "postgres"
    https://github.com/open-webui/open-webui → "open-webui"
    """
    last = url.rstrip("/").split("/")[-1]
    return _sanitize(last) if last else ""


def _read_docker_archive_ref(tar_path: str) -> tuple[str, str] | None:
    """
    docker-archive tar ke manifest.json se actual RepoTag read karo.
    `docker save image:tag -o file.tar` ke baad manifest.json mein RepoTags hota hai.
    Returns: (image_without_tag, tag) — e.g. ("ghcr.io/open-webui/open-webui", "v0.11.0")
    None agar koi RepoTag nahi mila.
    """
    try:
        with tarfile.open(tar_path, "r:*") as tf:
            with tf.extractfile("manifest.json") as f:
                data = json.loads(f.read())
        for entry in data:
            repo_tags = entry.get("RepoTags") or []
            for tag in repo_tags:
                if tag and ":" in tag:
                    img, ver = tag.rsplit(":", 1)
                    logger.info(f"[HarborPush] docker-archive RepoTag: {img}:{ver}")
                    return img, ver
    except Exception as e:
        logger.warning(f"[HarborPush] manifest.json RepoTag read failed ({tar_path}): {e}")
    return None


def _patch_docker_archive_ref(tar_path: str, new_ref: str) -> str:
    """
    docker-archive tar ke manifest.json mein new_ref add karo.
    Disk extraction se bachta hai (Windows mein sha256:... colon path issue hota hai).
    Source tar se stream mein copy karta hai — sirf manifest.json memory mein modify hota hai.
    """
    import io as _io

    new_tar = tar_path + ".patched.tar"
    with tarfile.open(tar_path, "r:*") as src:
        mf_member = src.getmember("manifest.json")
        mf_bytes  = src.extractfile(mf_member).read()
        manifests = json.loads(mf_bytes)

        if manifests:
            repo_tags = list(manifests[0].get("RepoTags") or [])
            if new_ref not in repo_tags:
                repo_tags.insert(0, new_ref)
                manifests[0]["RepoTags"] = repo_tags

        new_mf_bytes = json.dumps(manifests).encode()

        # Rewind source tar to stream members
        src_members = src.getmembers()

    # Second pass: copy with manifest.json replaced
    with tarfile.open(tar_path, "r:*") as src, tarfile.open(new_tar, "w:") as dst:
        for member in src.getmembers():
            if member.name == "manifest.json":
                info      = tarfile.TarInfo(name="manifest.json")
                info.size = len(new_mf_bytes)
                dst.addfile(info, _io.BytesIO(new_mf_bytes))
            else:
                fobj = src.extractfile(member)
                if fobj is not None:
                    dst.addfile(member, fobj)
                else:
                    dst.addfile(member)  # directory / symlink

    logger.info(f"[HarborPush] docker-archive patched: RepoTag '{new_ref}' added → {new_tar}")
    return new_tar


def _patch_oci_index_ref(oci_dir: str, image_ref: str) -> None:
    """
    OCI directory ke index.json mein har manifest pe
    org.opencontainers.image.ref.name annotation set karo.
    Push-image API source command = oci:{path}:{image_ref} —
    skopeo isi annotation se match karta hai.
    """
    idx_path = os.path.join(oci_dir, "index.json")
    with open(idx_path) as f:
        data = json.load(f)
    for manifest in data.get("manifests", []):
        manifest.setdefault("annotations", {})["org.opencontainers.image.ref.name"] = image_ref
    with open(idx_path, "w") as f:
        json.dump(data, f)
    logger.info(f"[HarborPush] OCI index.json patched → ref={image_ref}")


def _read_oci_archive_ref(tar_path: str) -> tuple[str, str] | None:
    """
    OCI archive ke index.json se actual manifest reference nikalo.
    1. org.opencontainers.image.ref.name → direct use
    2. org.opencontainers.image.url → last path segment as owner
       e.g. https://hub.docker.com/_/postgres → "postgres"
    Returns (image_ref_without_tag, tag) or None.
    """
    try:
        with tarfile.open(tar_path, "r:*") as tf:
            with tf.extractfile("index.json") as f:
                data = json.loads(f.read())
        for manifest in data.get("manifests", []):
            ann = manifest.get("annotations", {})

            # 1. Direct ref annotation
            ref = ann.get("org.opencontainers.image.ref.name", "")
            if ref:
                if ":" in ref.rsplit("/", 1)[-1]:
                    img, tag = ref.rsplit(":", 1)
                else:
                    img, tag = ref, "latest"
                logger.info(f"[HarborPush] OCI ref.name: {img}:{tag}")
                return img, tag

            # 2. URL annotation → last segment as owner name
            url = ann.get("org.opencontainers.image.url", "")
            if url:
                last = url.rstrip("/").split("/")[-1]  # e.g. "postgres" from ".../_/postgres"
                if last:
                    logger.info(f"[HarborPush] OCI image.url={url} → owner='{last}'")
                    return last, "latest"   # tag baad mein version se override hoga

    except Exception as e:
        logger.warning(f"[HarborPush] OCI index.json ref read failed ({tar_path}): {e}")
    return None


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
    source_type: str,
    local_file: str | None = None,    # local file on this machine → multipart (no PV needed)
    webdav_path: str | None = None,   # path on push-image pod's PV → JSON fallback
) -> None:
    """POST → push-image wrapper → skopeo → Harbor.

    Priority:
    1. local_file exists → multipart upload (no PV required — push-image receives file directly)
    2. webdav_path set   → JSON payload with source_path (push-image reads from its PV mount)
    """
    if not _PUSH_IMAGE_BASE_URL:
        raise RuntimeError("PUSH_IMAGE_BASE_URL env var is not set")

    payload = {
        "harbor_url":  harbor_host,
        "username":    harbor_user,
        "password":    harbor_pass,
        "project":     project,
        "image_name":  image_name,
        "tag":         tag,
        "source_type": source_type,
        "tls_verify":  False,
    }
    if webdav_path:
        payload["source_path"] = webdav_path

    endpoint = f"{_PUSH_IMAGE_BASE_URL}/push/image"
    logger.info(f"[HarborPush] POST {endpoint} → {harbor_host}/{project}/{image_name}:{tag}")

    resp = None
    file_handle = None
    try:
        if local_file and os.path.exists(local_file):
            # Multipart: file sent directly → no shared PV required
            logger.info(f"[HarborPush] multipart upload: {local_file}")
            file_handle = open(local_file, "rb")
            resp = requests.post(
                endpoint,
                data=payload,
                files={"file": (os.path.basename(local_file), file_handle, "application/octet-stream")},
                timeout=900,
                verify=False,
            )
        else:
            # JSON only — push-image reads file from its own PV via webdav_path
            logger.info(f"[HarborPush] JSON push (source_path={webdav_path})")
            resp = requests.post(
                endpoint,
                json=payload,
                timeout=900,
                verify=False,
            )
    except Exception as _req_err:
        raise RuntimeError(f"push/image request failed: {_req_err}") from _req_err
    finally:
        if file_handle is not None:
            file_handle.close()

    # resp is always set here (exception raised above if not)
    if not resp.ok:
        raise RuntimeError(
            f"push/image API failed (HTTP {resp.status_code}): {resp.text[:1000]}"
        )

    logger.info(f"[HarborPush] push/image OK: {resp.text[:200]}")




# ── main activity ─────────────────────────────────────────────────────────────

@activity.defn(name="harbor_push_activity")
def harbor_push_activity(params: dict) -> dict:
    item_id   = params["item_id"]
    temp_path          = params.get("temp_path")
    pod_path           = params.get("pod_path")
    webdav_cleanup_url = params.get("webdav_url")
    db                 = SessionLocal()

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
            raise RuntimeError("harbor_registry_id not set — please select a Harbor instance")

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

        # OCI archive ke liye: annotation se actual owner/name try karo pehle
        if not meta["image_owner"] and os.path.isfile(temp_path):
            oci_annotation = _read_oci_archive_ref(temp_path)
            if oci_annotation:
                full_img = _strip_reg(oci_annotation[0])  # registry prefix hatao
                comps = [c for c in full_img.split("/") if c]
                if len(comps) >= 2:
                    # e.g. "library/postgres" → owner=library, name=postgres
                    meta["image_owner"] = _sanitize(comps[-2])
                    meta["image_name"]  = meta["image_name"] or _sanitize(comps[-1])
                elif len(comps) == 1:
                    # e.g. URL se mila "postgres" → directly owner set karo
                    meta["image_owner"] = _sanitize(comps[0])
                if oci_annotation[1] and oci_annotation[1] != "latest":
                    meta["image_version"] = oci_annotation[1]
                logger.info(
                    f"[HarborPush] OCI annotation → owner={meta['image_owner']} "
                    f"name={meta['image_name']} version={meta['image_version']}"
                )

        # image_owner last resort: item.harbor_owner (user ne upload mein diya)
        # harbor_project mat copy karo — same hone par double prefix banta hai
        if not meta["image_owner"]:
            meta["image_owner"] = _sanitize(item.harbor_owner or "")
            if meta["image_owner"]:
                logger.info(f"[HarborPush] image_owner: item.harbor_owner se liya → '{meta['image_owner']}'")
            else:
                logger.warning("[HarborPush] image_owner nahi mila — harbor path mein owner nahi hoga")

        image_owner    = meta["image_owner"]
        image_name     = meta["image_name"]
        image_version  = meta["image_version"]
        source_type    = meta["source_type"]
        is_oci_dir_zip = meta["is_oci_dir_zip"]

        # ── Explicit user overrides (container upload mein name/owner_name/version diya) ──
        # User-provided values always win over auto-derived tar/zip metadata
        _file_stem = os.path.splitext(os.path.basename(item.file_name or ""))[0]
        if item.harbor_owner:
            image_owner = _sanitize(item.harbor_owner)
            logger.info(f"[HarborPush] owner override from upload → '{image_owner}'")
        if item.name and item.name != _file_stem:
            image_name = _sanitize(item.name)
            logger.info(f"[HarborPush] image_name override from upload → '{image_name}'")
        if item.version and item.version not in ("", "latest"):
            image_version = _sanitize(item.version)
            logger.info(f"[HarborPush] version override from upload → '{image_version}'")

        # Agar owner == harbor project → double prefix bachao
        if image_owner and image_owner == project:
            logger.warning(
                f"[HarborPush] image_owner='{image_owner}' == project='{project}' → "
                "double prefix hoga. Upload mein alag harbor_owner specify karo."
            )

        logger.info(
            f"[HarborPush] meta → owner={image_owner} name={image_name} "
            f"version={image_version} source_type={source_type}"
        )

        # ── 4. Push to Harbor ─────────────────────────────────────────────────
        _owner_prefix = f"{image_owner}/" if (image_owner and image_owner != project) else ""
        dest_image    = f"{harbor_host}/{project}/{_owner_prefix}{image_name}:{image_version}"
        api_image     = f"{_owner_prefix}{image_name}"
        api_tag       = image_version

        if pod_path:
            # File already on PV via direct WebDAV upload — skip local processing
            file_ext  = os.path.splitext(pod_path)[1].lower()
            _src_type = "oci" if file_ext == ".zip" else "docker-archive"
            logger.info(f"[HarborPush] pod_path mode → {pod_path} ({_src_type})")
            _push_image_via_api(
                harbor_host, harbor_user, harbor_pass,
                project, api_image, api_tag,
                source_type=_src_type,
                webdav_path=pod_path,
            )
            logger.info(f"[HarborPush] push OK → {dest_image}")
            _update("pushed", image=dest_image, version=image_version,
                    owner=image_owner, h_user=harbor_user, h_pass=harbor_pass,
                    name=image_name)
            logger.info(f"[HarborPush] item={item_id} done → {dest_image}")
            return {
                "status":       "pushed",
                "harbor_image": dest_image,
                "version":      image_version,
                "owner":        image_owner,
                "name":         image_name,
            }

        extract_tmp   = None
        webdav_url    = None
        patched_tar   = None   # docker-archive patch → temp file, cleanup needed
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
                _patch_oci_index_ref(oci_dir, f"{api_image}:{image_version}")
                source_path, webdav_url = _webdav_upload_dir(oci_dir, uid)
                source_type = "oci"
            else:
                detected_type = _detect_tar_source_type(temp_path)
                if detected_type == "oci-archive":
                    if extract_tmp is None:
                        extract_tmp = tempfile.mkdtemp(prefix="harbor-oci-")
                    with tarfile.open(temp_path, "r:*") as tf:
                        tf.extractall(extract_tmp)
                    oci_dir = extract_tmp
                    for root, dirs, files in os.walk(extract_tmp):
                        if "oci-layout" in files:
                            oci_dir = root
                            break
                    _patch_oci_index_ref(oci_dir, f"{api_image}:{image_version}")
                    source_path, webdav_url = _webdav_upload_dir(oci_dir, uid)
                    source_type = "oci"
                    logger.info(f"[HarborPush] oci-archive extracted + patched → oci dir")
                else:
                    source_type = detected_type  # "docker-archive"

                    # ── docker-archive owner/name derivation ─────────────────
                    # Step 1: RepoTag se registry strip karke owner/name nikalo
                    # Step 2: Agar owner Docker Hub official namespace ("library","_")
                    #         ya harbor project hi hai → image config labels se real
                    #         owner/name nikalo (org.opencontainers.image.url).
                    # Step 3: Agar reference badal gaya → tar patch karke desired
                    #         RepoTag add karo taaki skopeo source match ho sake.
                    _DOCKER_OFFICIAL_NS = {"library", "_"}
                    docker_ref = _read_docker_archive_ref(temp_path)
                    push_tar = temp_path  # default: original tar upload karo
                    if docker_ref:
                        stripped  = _strip_reg(docker_ref[0])
                        comps     = [c for c in stripped.split("/") if c]
                        raw_owner = comps[-2] if len(comps) >= 2 else ""
                        raw_name  = comps[-1] if comps else image_name

                        # Official namespace ya harbor project → real owner labels se nikalo
                        if not raw_owner or raw_owner in _DOCKER_OFFICIAL_NS or raw_owner == project:
                            # Reset karo — official NS ya project naam real owner nahi hai
                            raw_owner = ""
                            labels    = _read_docker_archive_labels(temp_path)
                            # url ya source dono check karo (ghcr.io images mein source hota hai)
                            url = (
                                labels.get("org.opencontainers.image.url", "")
                                or labels.get("org.opencontainers.image.source", "")
                            )
                            if url:
                                from urllib.parse import urlparse as _up
                                path_parts = [c for c in _up(url).path.split("/") if c]
                                if len(path_parts) >= 2 and path_parts[0] not in _DOCKER_OFFICIAL_NS:
                                    # e.g. github.com/open-webui/open-webui → owner=open-webui name=open-webui
                                    raw_owner = path_parts[-2]
                                    raw_name  = path_parts[-1]
                                elif path_parts:
                                    # e.g. hub.docker.com/_/postgres → name=postgres (no real org)
                                    raw_owner = path_parts[-1]
                                logger.info(f"[HarborPush] docker-archive label URL='{url}' → owner={raw_owner} name={raw_name}")
                            if not raw_owner:
                                raw_owner = _sanitize(item.harbor_owner or "")

                        final_owner = _sanitize(raw_owner) if raw_owner else ""
                        final_name  = _sanitize(raw_name)  if raw_name  else image_name
                        final_tag   = image_version  # item.version ko priority do

                        # Agar owner/name/tag change hua → tar patch karo taaki skopeo ref match ho
                        desired_ref  = f"{final_owner}/{final_name}:{final_tag}" if final_owner else f"{final_name}:{final_tag}"
                        existing_ref = f"{stripped}:{docker_ref[1]}"
                        need_patch   = desired_ref != existing_ref and final_owner not in _DOCKER_OFFICIAL_NS
                        if need_patch:
                            try:
                                patched_tar = _patch_docker_archive_ref(temp_path, desired_ref)
                                push_tar    = patched_tar
                            except Exception as _pe:
                                logger.warning(f"[HarborPush] docker-archive patch failed: {_pe} — original use")

                        _owner_prefix = f"{final_owner}/" if (final_owner and final_owner != project) else ""
                        api_image    = f"{_owner_prefix}{final_name}"
                        api_tag      = final_tag
                        dest_image   = f"{harbor_host}/{project}/{api_image}:{api_tag}"
                        # DB update ke liye bhi sync karo
                        image_owner  = final_owner
                        image_name   = final_name
                        image_version = final_tag
                        logger.info(f"[HarborPush] docker-archive → owner={final_owner} name={final_name} tag={api_tag} dest={dest_image}")

                    if not docker_ref:
                        # RepoTags empty → config blob ke labels se owner/name/tag nikalo
                        # (yahi data Harbor Overview mein dikhta hai)
                        labels  = _read_docker_archive_labels(temp_path)
                        url     = (
                            labels.get("org.opencontainers.image.url", "")
                            or labels.get("org.opencontainers.image.source", "")
                        )
                        title   = labels.get("org.opencontainers.image.title", "")
                        ver_lbl = labels.get("org.opencontainers.image.version", "")

                        final_owner = ""
                        final_name  = ""

                        if url:
                            from urllib.parse import urlparse as _up2
                            _parts = [c for c in _up2(url).path.split("/") if c]
                            if len(_parts) >= 2 and _parts[0] not in _DOCKER_OFFICIAL_NS:
                                # e.g. github.com/open-webui/open-webui → owner=open-webui name=open-webui
                                final_owner = _sanitize(_parts[-2])
                                final_name  = _sanitize(_parts[-1])
                            elif _parts:
                                final_owner = _sanitize(_parts[-1])

                        # title label naam se zyada accurate hota hai
                        if title:
                            final_name = _sanitize(title) or final_name

                        # version label se tag
                        final_tag = _sanitize(ver_lbl) if ver_lbl else image_version

                        # Fallbacks: agar labels se nahi mila
                        if not final_owner:
                            final_owner = _sanitize(item.harbor_owner or "")
                        if not final_name:
                            final_name = image_name or _sanitize(item.name or "")

                        logger.info(f"[HarborPush] docker-archive (no RepoTags) labels → owner={final_owner} name={final_name} tag={final_tag}")

                        desired_ref = f"{final_owner}/{final_name}:{final_tag}" if final_owner else f"{final_name}:{final_tag}"
                        try:
                            patched_tar = _patch_docker_archive_ref(temp_path, desired_ref)
                            push_tar    = patched_tar
                        except Exception as _pe:
                            logger.warning(f"[HarborPush] no-RepoTag tar patch failed: {_pe} — original use")
                        _owner_prefix  = f"{final_owner}/" if (final_owner and final_owner != project) else ""
                        api_image      = f"{_owner_prefix}{final_name}"
                        api_tag        = final_tag
                        dest_image     = f"{harbor_host}/{project}/{api_image}:{api_tag}"
                        image_owner    = final_owner
                        image_name     = final_name
                        image_version  = final_tag
                        logger.info(f"[HarborPush] docker-archive (no RepoTags) → ref='{desired_ref}' dest={dest_image}")

                    source_path, webdav_url = _webdav_upload_file(push_tar, uid)

            logger.info(f"[HarborPush] pushing → {dest_image}")
            _push_image_via_api(
                harbor_host, harbor_user, harbor_pass,
                project, api_image, api_tag,
                source_type=source_type,
                webdav_path=source_path,
            )
            logger.info(f"[HarborPush] push OK → {dest_image}")

        finally:
            if webdav_url:
                _webdav_delete(webdav_url)
            if extract_tmp:
                shutil.rmtree(extract_tmp, ignore_errors=True)
            if patched_tar and os.path.isfile(patched_tar):
                try:
                    os.remove(patched_tar)
                except OSError:
                    pass
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
        if webdav_cleanup_url:
            _webdav_delete(webdav_cleanup_url)
        try:
            db.close()
        except Exception:
            pass

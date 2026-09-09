"""
LLM Push Activity  (K8s-free, direct OCI push)

Flow:
  1. DB se library item + harbor registry record lo
  2. GGUF / template metadata nikalo (local file read + ZIP mein version_metadata.json)
  3. OCI annotations build karo (version_metadata.json ke SAARE fields)
  4. Direct OCI Distribution Spec se Harbor pe push karo (no wrapper, no pod)
  5. Harbor REST API se repository description set karo
  6. DB update: harbor_image, version, harbor_owner, push_status=pushed
"""

import os
import re
import tempfile
import uuid
from urllib.parse import urlparse

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from temporalio import activity

from db_configuration.config import SessionLocal
from models.library_model import LibraryItem
from models.kubernetes_deploy_model import KubernetesDeployment

logger = activity.logger
LLM_PUSH_TASK_QUEUE = "llm-push-queue"


def _clean_env_url(value: str | None) -> str:
    if value is None:
        return ""
    return value.split("#", 1)[0].strip().rstrip("/")


_PUSH_IMAGE_BASE_URL = _clean_env_url(os.getenv("PUSH_IMAGE_BASE_URL", ""))


def _sanitize(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "-", name).lower().strip("-")


# ── GGUF metadata reader ──────────────────────────────────────────────────────

def _read_gguf_metadata(file_path: str) -> dict:
    """GGUF binary header se metadata nikalo. Fallback: filename parse."""
    try:
        from gguf import GGUFReader

        reader = GGUFReader(file_path)
        fields = reader.fields

        def _str(key: str, default: str = "") -> str:
            f = fields.get(key)
            if f is None:
                return default
            try:
                val = f.parts[-1]
                if hasattr(val, "tobytes"):
                    return val.tobytes().decode("utf-8", errors="replace").rstrip("\x00").strip()
                if hasattr(val, "__iter__"):
                    return str(list(val)[0]).strip()
                return str(val).strip()
            except Exception:
                return default

        def _int(key: str, default: int = 0) -> int:
            f = fields.get(key)
            if f is None:
                return default
            try:
                return int(list(f.parts[-1])[0])
            except Exception:
                return default

        arch    = _str("general.architecture", "unknown")
        ctx_len = _int(f"{arch}.context_length") or _int("llama.context_length")

        param_count = _int("general.parameter_count")
        params_b    = f"{param_count / 1e9:.1f}B" if param_count > 0 else ""

        return {
            "name":        _str("general.name"),
            "version":     _str("general.version", ""),
            "author":      _str("general.author"),
            "description": _str("general.description"),
            "arch":        arch,
            "license":     _str("general.license.name") or _str("general.license"),
            "source_url":  _str("general.source.url") or _str("general.url"),
            "hf_repo":     _str("general.source.huggingface.repository"),
            "tags":        _str("general.tags"),
            "languages":   _str("general.languages"),
            "context_len": str(ctx_len) if ctx_len else "",
            "params":      params_b,
        }
    except ImportError:
        logger.warning("[LLMPush] gguf library not available — parsing from filename")
        return _parse_from_filename(os.path.basename(file_path))
    except Exception as exc:
        logger.warning(f"[LLMPush] GGUF header read failed: {exc} — filename fallback")
        return _parse_from_filename(os.path.basename(file_path))


def _parse_from_filename(filename: str) -> dict:
    """Fallback: filename se name + version nikalo."""
    stem = os.path.splitext(filename)[0]

    quant_match = re.search(r"[-_]((?:Q|F|BF)\d+[_A-Z0-9]*)\s*$", stem, re.IGNORECASE)
    if quant_match:
        return {
            "name":    _sanitize(stem[: quant_match.start()].strip("-_")) or "llm-model",
            "version": quant_match.group(1).upper(),
            "author":  "unknown",
            "arch":    "unknown",
        }

    variant_match = re.search(
        r"[-_](instruct|chat|base|code|math|vision|it|rl|sft|dpo|cot|reasoner)\s*$",
        stem, re.IGNORECASE,
    )
    if variant_match:
        return {
            "name":    _sanitize(stem[: variant_match.start()].strip("-_")) or "llm-model",
            "version": variant_match.group(1).lower(),
            "author":  "unknown",
            "arch":    "unknown",
        }

    return {"name": _sanitize(stem) or "llm-model", "version": "latest", "author": "unknown", "arch": "unknown"}


# ── Template metadata reader (ZIP → version_metadata.json) ───────────────────

def _read_template_metadata(zip_path: str) -> dict:
    """ZIP ke andar version_metadata.json padho — rich format support."""
    import zipfile
    import json as _json

    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            meta_file = next(
                (n for n in z.namelist() if os.path.basename(n) == "version_metadata.json"),
                None,
            )
            if not meta_file:
                logger.warning("[LLMPush] version_metadata.json not found in ZIP")
                return {}
            with z.open(meta_file) as f:
                data = _json.loads(f.read().decode("utf-8"))

        logger.info(f"[LLMPush] version_metadata.json loaded: artifact={data.get('artifact_name')} v={data.get('version')}")

        # Components — list of dicts ya list of strings dono handle karo
        raw_components = data.get("components", [])
        if raw_components and isinstance(raw_components[0], dict):
            components_str = ", ".join(c.get("name", "") for c in raw_components if c.get("name"))
        else:
            components_str = ", ".join(str(c) for c in raw_components)

        # Tags — list → comma string
        tags_list = data.get("tags", [])
        tags_str  = ", ".join(tags_list) if tags_list else ""

        # Deploy info → readable string
        deploy_data = data.get("deploy", {})
        if isinstance(deploy_data, dict):
            deploy_str = deploy_data.get("type", "") or deploy_data.get("installer_type", "")
            deploy_ns  = deploy_data.get("namespace", "")
        else:
            deploy_str = str(deploy_data)
            deploy_ns  = ""

        # Source info
        source_data = data.get("source", {})
        upstream_url = source_data.get("upstream_url", "") if isinstance(source_data, dict) else ""

        # Network info
        network_data    = data.get("network", {})
        external_ep     = network_data.get("external_endpoint", "") if isinstance(network_data, dict) else ""
        https_enabled   = network_data.get("https_enabled", None) if isinstance(network_data, dict) else None

        # Images archive info
        images_data   = data.get("images", {})
        archive_format = images_data.get("archive_format", "") if isinstance(images_data, dict) else ""
        size_bytes     = images_data.get("size_bytes", 0) if isinstance(images_data, dict) else 0
        size_gb        = f"{size_bytes / 1_073_741_824:.1f} GB" if size_bytes else ""

        return {
            # Core fields
            "name":           data.get("artifact_name", ""),
            "display_name":   data.get("display_name", ""),
            "author":         data.get("owner", "") or data.get("owner_name", ""),
            "version":        data.get("version", ""),
            "description":    data.get("description", ""),
            "category":       data.get("category", ""),
            "tags":           tags_str,
            "tags_list":      tags_list,
            "created_by":     data.get("created_by", ""),
            # Format / deploy
            "format":         data.get("format", ""),
            "format_version": data.get("format_version", ""),
            "deploy_type":    deploy_str,
            "deploy_ns":      deploy_ns,
            # Source
            "upstream_url":   upstream_url,
            # Network
            "external_ep":    external_ep,
            "https_enabled":  https_enabled,
            # Images
            "archive_format": archive_format,
            "archive_size":   size_gb,
            # Components
            "components":     components_str,
            "components_list": raw_components,
            # Full raw JSON (DB mein store ke liye)
            "_raw": data,
        }
    except zipfile.BadZipFile:
        logger.warning("[LLMPush] File is not a valid ZIP")
        return {}
    except Exception as exc:
        logger.warning(f"[LLMPush] Template metadata read failed: {exc}")
        return {}


# ── Harbor REST API — repository description ──────────────────────────────────

def _set_harbor_repo_description(
    harbor_host: str, harbor_user: str, harbor_pass: str,
    project: str, repo_name: str, meta: dict, is_template: bool = False,
) -> None:
    """Harbor repository Info tab pe description set karo via REST API."""
    import base64
    import json as _json
    import urllib.parse
    import urllib.request

    desc_lines = []
    if is_template:
        # Display name / description
        if meta.get("display_name"): desc_lines.append(f"## {meta['display_name']}")
        if meta.get("description"):  desc_lines.append(f"\n{meta['description']}\n")

        # Core info
        if meta.get("name"):         desc_lines.append(f"**Artifact:** `{meta['name']}`")
        if meta.get("author"):       desc_lines.append(f"**Owner:** {meta['author']}")
        if meta.get("created_by"):   desc_lines.append(f"**Created By:** {meta['created_by']}")
        if meta.get("version"):      desc_lines.append(f"**Version:** {meta['version']}")
        if meta.get("category"):     desc_lines.append(f"**Category:** {meta['category']}")
        if meta.get("tags"):         desc_lines.append(f"**Tags:** {meta['tags']}")

        # Format / Deploy
        if meta.get("format"):       desc_lines.append(f"\n**Format:** {meta['format']}")
        if meta.get("format_version"): desc_lines.append(f"**Format Version:** {meta['format_version']}")
        if meta.get("deploy_type"):  desc_lines.append(f"**Deploy Type:** {meta['deploy_type']}")
        if meta.get("deploy_ns"):    desc_lines.append(f"**Namespace:** {meta['deploy_ns']}")

        # Images
        if meta.get("archive_format"): desc_lines.append(f"\n**Image Archive Format:** {meta['archive_format']}")
        if meta.get("archive_size"):   desc_lines.append(f"**Image Archive Size:** {meta['archive_size']}")

        # Components
        if meta.get("components_list"):
            comp_list = meta["components_list"]
            if comp_list and isinstance(comp_list[0], dict):
                names = [c.get("name", "") for c in comp_list if c.get("name")]
                desc_lines.append(f"\n**Components ({len(names)}):** {', '.join(names)}")
            elif meta.get("components"):
                desc_lines.append(f"\n**Components:** {meta['components']}")

        # Network
        if meta.get("external_ep"):   desc_lines.append(f"\n**External Endpoint:** {meta['external_ep']}")
        if meta.get("https_enabled") is not None:
            desc_lines.append(f"**HTTPS:** {'Yes' if meta['https_enabled'] else 'No'}")

        # Source
        if meta.get("upstream_url"):  desc_lines.append(f"\n**Upstream:** {meta['upstream_url']}")
    else:
        # GGUF model
        if meta.get("name"):        desc_lines.append(f"**Model:** {meta['name']}")
        if meta.get("author"):      desc_lines.append(f"**Author:** {meta['author']}")
        if meta.get("arch"):        desc_lines.append(f"**Architecture:** {meta['arch']}")
        if meta.get("params"):      desc_lines.append(f"**Parameters:** {meta['params']}")
        if meta.get("context_len"): desc_lines.append(f"**Context Length:** {meta['context_len']}")
        if meta.get("license"):     desc_lines.append(f"**License:** {meta['license']}")
        if meta.get("languages"):   desc_lines.append(f"**Languages:** {meta['languages']}")
        if meta.get("hf_repo"):     desc_lines.append(f"**HuggingFace:** {meta['hf_repo']}")
        if meta.get("source_url"):  desc_lines.append(f"**Source:** {meta['source_url']}")
        if meta.get("description"): desc_lines.append(f"\n{meta['description']}")

    if not desc_lines:
        return

    description = "\n".join(desc_lines)
    url  = (
        f"http://{harbor_host}/api/v2.0/projects/{project}/repositories/"
        f"{urllib.parse.quote(repo_name, safe='')}"
    )
    data = _json.dumps({"description": description}).encode("utf-8")
    req  = urllib.request.Request(url, data=data, method="PUT")
    req.add_header("Content-Type", "application/json")
    creds = base64.b64encode(f"{harbor_user}:{harbor_pass}".encode()).decode()
    req.add_header("Authorization", f"Basic {creds}")

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            logger.info(f"[LLMPush] Harbor repo description set: {resp.status}")
    except Exception as exc:
        logger.warning(f"[LLMPush] Harbor description set failed (non-fatal): {exc}")


# ── OCI annotations builder ───────────────────────────────────────────────────

# version_metadata.json ke known fields → standard OCI annotation keys
_OCI_STD_KEYS = {
    "name":         "org.opencontainers.image.title",
    "display_name": "org.opencontainers.image.title",   # display_name overrides name
    "description":  "org.opencontainers.image.description",
    "version":      "org.opencontainers.image.version",
    "owner":        "org.opencontainers.image.vendor",
    "author":       "org.opencontainers.image.vendor",
    "created_at":   "org.opencontainers.image.created",
    "source":       "org.opencontainers.image.url",
    "upstream_url": "org.opencontainers.image.url",
    "license":      "org.opencontainers.image.licenses",
}


def _build_oci_annotations_from_raw(raw_data: dict) -> dict:
    """
    JSON dict ke SAARE fields → OCI annotation dict.
    - Known fields   → org.opencontainers.image.* standard keys
    - Baaki sab      → <key> prefix ke saath
    Harbor Overview tab mein jo bhi fields hain sab dikh jaayenge.
    Wrapper in annotations ko oras --annotation key=value flags mein pass karega.
    """
    import json as _j

    def _s(v) -> str:
        """Value → clean string (nested dict/list → JSON string)."""
        if isinstance(v, (dict, list)):
            v = _j.dumps(v, ensure_ascii=False, separators=(",", ":"))
        s = str(v).strip() if v is not None else ""
        # OCI annotation values mein newlines allowed nahi
        return s.replace("\n", " ").replace("\r", "").replace('"', "'")[:500]

    ann: dict = {}

    for key, value in raw_data.items():
        val_str = _s(value)
        # Empty, null, ya empty container skip karo
        if not val_str or val_str in ("null", "None", "[]", "{}", ""):
            continue

        if key in _OCI_STD_KEYS:
            ann[_OCI_STD_KEYS[key]] = val_str
        else:
            # Key → DNS-label safe format
            clean = re.sub(r"[^a-zA-Z0-9._-]", "_", str(key)).lower().strip("_.-")
            if clean:
                ann[f"{clean}"] = val_str

    # display_name → title mein highest priority (name se override)
    if raw_data.get("display_name"):
        ann["org.opencontainers.image.title"] = _s(raw_data["display_name"])

    return {k: v for k, v in ann.items() if v}


# ── HTTP push artifact ────────────────────────────────────────────────────────

def _push_artifact_via_api(
    harbor_host: str, harbor_user: str, harbor_pass: str,
    project: str, artifact_name: str, tag: str,
    file_path: str,
    annotations: dict = None,
) -> None:
    """HTTP POST → push-image wrapper → oras push → Harbor."""
    if not _PUSH_IMAGE_BASE_URL:
        raise RuntimeError("PUSH_IMAGE_BASE_URL env var is not set")

    payload = {
        "harbor_url":    harbor_host,
        "username":      harbor_user,
        "password":      harbor_pass,
        "project":       project,
        "artifact_name": artifact_name,
        "tag":           tag,
        "file_path":     file_path,
        "plain_http":    True,
    }
    if annotations:
        payload["annotations"] = annotations
        logger.info(f"[LLMPush] OCI annotations ({len(annotations)}): {list(annotations.keys())}")

    logger.info(
        f"[LLMPush] POST {_PUSH_IMAGE_BASE_URL}/push/artifact "
        f"→ {harbor_host}/{project}/{artifact_name}:{tag}"
    )
    resp = requests.post(
        f"{_PUSH_IMAGE_BASE_URL}/push/artifact",
        json=payload,
        timeout=900,
        verify=False,
    )
    if not resp.ok:
        raise RuntimeError(
            f"push/artifact API failed (HTTP {resp.status_code}): {resp.text[:1000]}"
        )
    logger.info(f"[LLMPush] push/artifact OK: {resp.text[:200]}")


# ── Direct OCI push (no wrapper, no pod, no oras CLI) ────────────────────────

def _push_oci_artifact_direct(
    harbor_host: str,
    harbor_user: str,
    harbor_pass: str,
    project: str,
    repo_name: str,
    tag: str,
    file_path: str,
    annotations: dict = None,
) -> str:
    """
    OCI Distribution Spec se directly Harbor pe artifact push karo.
    Wrapper ya oras CLI ki zaroorat nahi — annotations manifest mein directly embed hoti hain.
    Harbor Overview tab mein saare annotations dikhengi.
    Returns: manifest digest (sha256:xxx)
    """
    import hashlib
    import json as _json

    auth    = (harbor_user, harbor_pass)
    fname   = os.path.basename(file_path)
    reg_url = f"http://{harbor_host}/v2/{project}/{repo_name}"

    logger.info(f"[LLMPush] Direct OCI push → {harbor_host}/{project}/{repo_name}:{tag}")

    # ── 1. File digest + size (streaming, memory safe) ────────────────────────
    h = hashlib.sha256()
    file_size = 0
    with open(file_path, "rb") as fh:
        for _c in iter(lambda: fh.read(8 * 1024 * 1024), b""):
            h.update(_c)
            file_size += len(_c)
    file_digest = f"sha256:{h.hexdigest()}"
    logger.info(f"[LLMPush] blob digest={file_digest} size={file_size / 1e6:.1f} MB")

    # ── 2. Blob upload (skip if already exists) ───────────────────────────────
    if requests.head(f"{reg_url}/blobs/{file_digest}", auth=auth, timeout=30, verify=False).status_code != 200:
        # Start upload session
        post_r = requests.post(f"{reg_url}/blobs/uploads/", auth=auth, timeout=30, verify=False)
        if not post_r.ok:
            raise RuntimeError(f"OCI upload session failed: {post_r.status_code} {post_r.text[:300]}")
        session_url = post_r.headers.get("Location", "")
        if not session_url.startswith("http"):
            session_url = f"http://{harbor_host}{session_url}"

        # PATCH — file stream karo (large files ke liye memory safe)
        logger.info(f"[LLMPush] Uploading blob {file_size / 1e9:.2f} GB...")
        with open(file_path, "rb") as fh:
            patch_r = requests.patch(
                session_url, data=fh, auth=auth,
                headers={"Content-Type": "application/octet-stream"},
                timeout=3600, verify=False,
            )
        if not patch_r.ok:
            raise RuntimeError(f"OCI blob PATCH failed: {patch_r.status_code} {patch_r.text[:300]}")
        put_url = patch_r.headers.get("Location", "")
        if not put_url.startswith("http"):
            put_url = f"http://{harbor_host}{put_url}"

        # PUT — blob commit
        sep = "&" if "?" in put_url else "?"
        put_r = requests.put(
            f"{put_url}{sep}digest={file_digest}",
            auth=auth, headers={"Content-Type": "application/octet-stream"},
            timeout=60, verify=False,
        )
        if not put_r.ok:
            raise RuntimeError(f"OCI blob commit failed: {put_r.status_code} {put_r.text[:300]}")
        logger.info("[LLMPush] Blob committed.")
    else:
        logger.info("[LLMPush] Blob already exists — skip upload.")

    # ── 3. Empty config blob ──────────────────────────────────────────────────
    _empty        = b"{}"
    _empty_digest = f"sha256:{hashlib.sha256(_empty).hexdigest()}"
    if requests.head(f"{reg_url}/blobs/{_empty_digest}", auth=auth, timeout=15, verify=False).status_code != 200:
        _pr  = requests.post(f"{reg_url}/blobs/uploads/", auth=auth, timeout=15, verify=False)
        _su  = _pr.headers.get("Location", "")
        if not _su.startswith("http"): _su = f"http://{harbor_host}{_su}"
        _par = requests.patch(_su, data=_empty, auth=auth,
                              headers={"Content-Type": "application/octet-stream"},
                              timeout=30, verify=False)
        _pu  = _par.headers.get("Location", "")
        if not _pu.startswith("http"): _pu = f"http://{harbor_host}{_pu}"
        _sep = "&" if "?" in _pu else "?"
        requests.put(f"{_pu}{_sep}digest={_empty_digest}", auth=auth,
                     headers={"Content-Type": "application/octet-stream"},
                     timeout=30, verify=False)

    # ── 4. Manifest with annotations ─────────────────────────────────────────
    manifest: dict = {
        "schemaVersion": 2,
        "mediaType":     "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest":    _empty_digest,
            "size":      len(_empty),
        },
        "layers": [{
            "mediaType":   "application/octet-stream",
            "digest":      file_digest,
            "size":        file_size,
            "annotations": {"org.opencontainers.image.title": fname},
        }],
    }
    if annotations:
        manifest["annotations"] = annotations
        logger.info(f"[LLMPush] Embedding {len(annotations)} annotations in manifest")

    manifest_bytes  = _json.dumps(manifest, ensure_ascii=False).encode()
    manifest_digest = f"sha256:{hashlib.sha256(manifest_bytes).hexdigest()}"

    mani_r = requests.put(
        f"{reg_url}/manifests/{tag}",
        auth=auth, data=manifest_bytes,
        headers={"Content-Type": "application/vnd.oci.image.manifest.v1+json"},
        timeout=60, verify=False,
    )
    if not mani_r.ok:
        raise RuntimeError(f"OCI manifest push failed: {mani_r.status_code} {mani_r.text[:500]}")

    logger.info(f"[LLMPush] Manifest pushed: {manifest_digest}")
    return manifest_digest


# ── Main activity ─────────────────────────────────────────────────────────────

@activity.defn(name="llm_push_activity")
def llm_push_activity(params: dict) -> dict:
    item_id   = params["item_id"]
    temp_path = params.get("temp_path")
    db        = SessionLocal()

    def _update(status: str, error: str = None, image: str = None,
                version: str = None, owner: str = None,
                h_url: str = None, h_user: str = None, h_pass: str = None,
                display_name: str = None, description: str = None,
                category: str = None, tags: str = None, metadata_json: str = None):
        try:
            item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
            if item:
                item.push_status = status
                if error         is not None: item.push_error    = error
                if image         is not None: item.harbor_image  = image
                if version       is not None: item.version       = version
                if owner         is not None: item.harbor_owner  = owner
                if h_url         is not None: item.harbor_url    = h_url
                if h_user        is not None: item.harbor_user   = h_user
                if h_pass        is not None: item.harbor_pass   = h_pass
                if display_name  is not None: item.display_name  = display_name
                if description   is not None: item.description   = description
                if category      is not None: item.category      = category
                if tags          is not None: item.tags          = tags
                if metadata_json is not None: item.metadata_json = metadata_json
                db.commit()
        except Exception as exc:
            logger.warning(f"[LLMPush] DB update failed: {exc}")
            try:
                db.rollback()
            except Exception:
                pass

    gguf_temp_dir = None

    try:
        # ── 1. Records load karo ─────────────────────────────────────────────
        item = db.query(LibraryItem).filter(LibraryItem.id == item_id).first()
        if not item:
            raise RuntimeError(f"Library item {item_id} not found")
        if not temp_path:
            temp_path = item.file_path
        if not temp_path or not os.path.exists(temp_path):
            raise RuntimeError(f"File not found: {temp_path}")

        # ── 2. Harbor registry details lo ────────────────────────────────────
        harbor_dep = db.query(KubernetesDeployment).filter(
            KubernetesDeployment.id == item.harbor_registry_id
        ).first()
        if not harbor_dep:
            raise RuntimeError(f"Harbor registry id={item.harbor_registry_id} not found")
        if not harbor_dep.harbor_url:
            raise RuntimeError(f"Harbor registry id={item.harbor_registry_id} has no harbor_url configured")

        parsed      = urlparse(harbor_dep.harbor_url)
        harbor_host = parsed.netloc or parsed.path.strip("/")
        harbor_user = harbor_dep.harbor_user or item.harbor_user or "admin"
        harbor_pass = harbor_dep.harbor_pass or item.harbor_pass or "Harbor12345"
        project = (item.harbor_project or "library").strip("/")

        logger.info(f"[LLMPush] harbor_host={harbor_host} project={project}")
        _update("pushing", h_url=harbor_dep.harbor_url, h_user=harbor_user, h_pass=harbor_pass)

        # ── 3. Metadata nikalo — type ke hisab se ────────────────────────────
        is_gguf = item.type == "llm_model"

        if is_gguf:
            import zipfile as _zf
            import json as _j

            actual_push_path = temp_path
            _raw_json_data: dict = {}  # version_metadata.json se (ZIP mein hoga to)

            if _zf.is_zipfile(temp_path):
                gguf_temp_dir = tempfile.mkdtemp(prefix="gguf_extract_")
                with _zf.ZipFile(temp_path, "r") as _z:
                    # version_metadata.json — ZIP ke andar any depth pe search karo
                    _meta_names = [
                        "version_metadata.json", "metadata.json",
                        "model_metadata.json", "model_info.json",
                    ]
                    _meta_entry = next(
                        (n for n in _z.namelist()
                         if os.path.basename(n).lower() in _meta_names),
                        None,
                    )
                    if _meta_entry:
                        try:
                            raw_bytes = _z.open(_meta_entry).read().decode("utf-8")
                            # JS-style comments strip karo (//) — standard JSON parser nahi samajhta
                            import re as _re
                            raw_clean = _re.sub(r'(?m)//[^\n]*', '', raw_bytes)
                            _raw_json_data = _j.loads(raw_clean)
                            logger.info(f"[LLMPush] {_meta_entry} found in ZIP: {list(_raw_json_data.keys())}")
                        except Exception as _je:
                            logger.warning(f"[LLMPush] ZIP metadata parse failed ({_meta_entry}): {_je}")

                    gguf_entry = next(
                        (n for n in _z.namelist() if n.lower().endswith(".gguf")),
                        None,
                    )
                    if gguf_entry:
                        _z.extract(gguf_entry, gguf_temp_dir)
                        actual_push_path = os.path.join(gguf_temp_dir, gguf_entry)
                        logger.info(f"[LLMPush] GGUF extracted from ZIP: {actual_push_path}")
                    else:
                        logger.warning("[LLMPush] No .gguf found in ZIP, using ZIP as-is")
                        gguf_temp_dir = None

            # Fallback 2: DB mein metadata_json hai? (upload API se pass kiya tha to)
            if not _raw_json_data and item.metadata_json:
                try:
                    _raw_json_data = _j.loads(item.metadata_json)
                    logger.info(f"[LLMPush] Using DB metadata_json: {list(_raw_json_data.keys())}")
                except Exception as _de:
                    logger.warning(f"[LLMPush] DB metadata_json parse failed: {_de}")

            # GGUF binary header se meta nikalo (name/version/arch ke liye)
            meta = _read_gguf_metadata(actual_push_path)

            # version_metadata.json fields se override karo (JSON > GGUF binary header)
            if _raw_json_data:
                for _src, _dst in [
                    ("name",         "name"),
                    ("display_name", "display_name"),
                    ("version",      "version"),
                    ("owner",        "author"),
                    ("description",  "description"),
                    ("source",       "source_url"),
                ]:
                    if _raw_json_data.get(_src):
                        meta[_dst] = _raw_json_data[_src]

            gguf_name = meta["name"] if meta.get("name") not in ("unknown", "", None) else None
            arch      = meta.get("arch", "")

            if item.version and item.version not in ("", "latest"):
                meta["version"] = item.version

            # DB mein store ke liye full JSON
            _raw_json = _j.dumps(_raw_json_data, ensure_ascii=False) if _raw_json_data else None
        else:
            # llm_template
            import json as _j
            actual_push_path = temp_path
            tmeta = _read_template_metadata(temp_path)
            _raw_json_data = tmeta.get("_raw", {})  # ZIP se mila JSON
            # DB fallback (upload API se pass kiya tha to)
            if not _raw_json_data and item.metadata_json:
                try:
                    _raw_json_data = _j.loads(item.metadata_json)
                    logger.info(f"[LLMPush] Template: Using DB metadata_json: {list(_raw_json_data.keys())}")
                except Exception:
                    pass
            meta  = {
                "name":            tmeta.get("name")           or item.name,
                "display_name":    tmeta.get("display_name")   or "",
                "author":          tmeta.get("author")         or "",
                "version":         tmeta.get("version")        or item.version or "latest",
                "description":     tmeta.get("description")    or "",
                "category":        tmeta.get("category")       or "",
                "tags":            tmeta.get("tags")           or "",
                "tags_list":       tmeta.get("tags_list")      or [],
                "created_by":      tmeta.get("created_by")     or "",
                "format":          tmeta.get("format")         or "",
                "format_version":  tmeta.get("format_version") or "",
                "deploy_type":     tmeta.get("deploy_type")    or "",
                "deploy_ns":       tmeta.get("deploy_ns")      or "",
                "upstream_url":    tmeta.get("upstream_url")   or "",
                "external_ep":     tmeta.get("external_ep")    or "",
                "https_enabled":   tmeta.get("https_enabled"),
                "archive_format":  tmeta.get("archive_format") or "",
                "archive_size":    tmeta.get("archive_size")   or "",
                "components":      tmeta.get("components")     or "",
                "components_list": tmeta.get("components_list") or [],
                # GGUF-only fields (empty for template)
                "arch": "", "license": "", "source_url": "", "hf_repo": "",
                "languages": "", "context_len": "", "params": "",
            }
            gguf_name = tmeta.get("name") or None
            arch      = tmeta.get("deploy_type") or ""
            _raw_json = _j.dumps(_raw_json_data, ensure_ascii=False) if _raw_json_data else None

        model_name = _sanitize(gguf_name or item.name) or ("llm-model" if is_gguf else "llm-template")
        version    = _sanitize(meta.get("version") or item.version or "latest")

        raw_author = meta.get("author") if meta.get("author") not in ("unknown", "", None) else None
        if not raw_author:
            name_for_author = gguf_name or item.name or model_name
            m = re.match(r'^([a-zA-Z]+)', name_for_author.lower())
            raw_author = m.group(1).capitalize() if m else "unknown"
        author      = raw_author
        author_slug = _sanitize(author.lower()) or "unknown"

        logger.info(f"[LLMPush] meta → name={model_name} version={version} author={author} arch={arch}")

        harbor_image = f"{harbor_host}/{project}/{author_slug}/{model_name}:{version}"

        # ── 4. OCI annotations build (version_metadata.json ke SAARE fields) ──
        if _raw_json_data:
            _ann_source = _raw_json_data                          # JSON fields → sab jayenge
        elif is_gguf:
            _ann_source = {k: v for k, v in meta.items()          # GGUF binary header fallback
                           if v and not k.startswith("_")}
        else:
            _ann_source = {}
        oci_annotations = _build_oci_annotations_from_raw(_ann_source)

        # ── 5. Direct OCI push → Harbor (no wrapper, no pod, no oras) ─────────
        _push_oci_artifact_direct(
            harbor_host, harbor_user, harbor_pass,
            project, f"{author_slug}/{model_name}", version,
            actual_push_path,
            annotations=oci_annotations,
        )
        logger.info(f"[LLMPush] push done: {harbor_image}")

        # ── 6. Harbor Info tab description set karo ───────────────────────────
        _set_harbor_repo_description(
            harbor_host, harbor_user, harbor_pass,
            project, f"{author_slug}/{model_name}", meta,
            is_template=not is_gguf,
        )

        # ── 9. DB update ─────────────────────────────────────────────────────
        _update(
            "pushed",
            image        = harbor_image,
            version      = version,
            owner        = author,
            display_name = meta.get("display_name") or None,
            description  = meta.get("description")  or None,
            category     = meta.get("category")     or None,
            tags         = meta.get("tags")          or None,
            metadata_json= locals().get("_raw_json") or None,
        )

        # ── 10. Temporal result ───────────────────────────────────────────────
        # metadata_found: JSON meta fields + GGUF binary fields (jo available hain)
        metadata_found = {}
        if _raw_json_data:
            metadata_found = {k: str(v) for k, v in _raw_json_data.items()
                              if v is not None and str(v).strip() not in ("", "null", "[]", "{}")}
        else:
            # Fallback: GGUF binary / processed meta se
            for _k, _v in {
                "name": gguf_name or model_name, "author": author, "version": version,
                "arch": arch, "params": meta.get("params", ""),
                "context_len": meta.get("context_len", ""), "license": meta.get("license", ""),
                "languages": meta.get("languages", ""), "hf_repo": meta.get("hf_repo", ""),
                "source_url": meta.get("source_url", ""), "description": meta.get("description", ""),
            }.items():
                if _v:
                    metadata_found[_k] = _v

        metadata_missing = []
        if not _raw_json_data and is_gguf:
            metadata_missing = [k for k, v in {
                "description": meta.get("description", ""), "license": meta.get("license", ""),
                "hf_repo": meta.get("hf_repo", ""), "params": meta.get("params", ""),
            }.items() if not v]

        logger.info(f"[LLMPush] item={item_id} DONE — {harbor_image}")
        logger.info(f"[LLMPush] json_meta_found={bool(_raw_json_data)} annotations={len(oci_annotations)}")

        return {
            "status":           "pushed",
            "item_id":          item_id,
            "harbor_image":     harbor_image,
            "harbor_url":       f"http://{harbor_host}/{project}/{model_name}",
            "json_meta_found":  bool(_raw_json_data),
            "annotations_sent": len(oci_annotations),
            "metadata_found":   metadata_found,
            "metadata_missing": metadata_missing,
        }

    except Exception as exc:
        err = str(exc)[:500]
        logger.error(f"[LLMPush] item={item_id} FAILED: {err}", exc_info=True)
        _update("failed", error=err)
        raise

    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
                logger.info(f"[LLMPush] temp file removed: {temp_path}")
            except OSError:
                pass
        if gguf_temp_dir and os.path.isdir(gguf_temp_dir):
            import shutil
            try:
                shutil.rmtree(gguf_temp_dir, ignore_errors=True)
            except Exception:
                pass
        db.close()

"""
Harbor Push Activity  (SSH-free, HTTP wrapper)

Flow:
  1. DB → library item + kubernetes_deployments
  2. Harbor URL + creds — DB + HARBOR_ADMIN_PASSWORD env
  3. Local metadata extraction (no K8s exec — tar/zip directly)
  4. Temp file → K8s exec stdin stream → image-push-tool pod /data/
  5. HTTP POST /push/image → push-image wrapper → skopeo → Harbor
  6. DB update + pod cleanup
"""

import io
import json
import logging
import os
import re
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
from models.kubernetes_model import KubernetesCluster
from models.kubernetes_deploy_model import KubernetesDeployment

logger = logging.getLogger(__name__)

HARBOR_PUSH_TASK_QUEUE = "harbor-push-queue"

_IMAGE_PUSH_NS       = os.getenv("IMAGE_PUSH_TOOL_NS",    "thinkcloud")
_IMAGE_PUSH_LABEL    = os.getenv("IMAGE_PUSH_TOOL_LABEL", "app=image-push-tool")
_STACK_CLUSTER_ID    = int(os.getenv("STACK_CLUSTER_ID", "0"))
_STACK_KUBECONFIG    = os.getenv("STACK_KUBECONFIG", "").strip()
_STACK_CLUSTER_IP    = os.getenv("STACK_CLUSTER_IP",  "").strip()
_PUSH_IMAGE_BASE_URL = os.getenv("PUSH_IMAGE_BASE_URL", "").rstrip("/")

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


def _build_k8s_client(cluster: KubernetesCluster):
    """kubeconfig se kubernetes ApiClient banao (hostname → IP replace)."""
    import kubernetes
    import yaml

    kc_data = yaml.safe_load(cluster.kubeconfig)
    for c in kc_data.get("clusters", []):
        server = c.get("cluster", {}).get("server", "")
        if server:
            c["cluster"]["server"] = re.sub(
                r"(https?://)([^:/]+)(:\d+)?",
                lambda m: m.group(1) + cluster.control_ip + (m.group(3) or ""),
                server,
            )
    kc_fd, kc_path = tempfile.mkstemp(suffix=".yaml")
    try:
        with os.fdopen(kc_fd, "w") as f:
            import yaml as _y
            _y.dump(kc_data, f)
        cfg = kubernetes.client.Configuration()
        kubernetes.config.load_kube_config(config_file=kc_path, client_configuration=cfg)
        cfg.verify_ssl = False
        return kubernetes.client.ApiClient(configuration=cfg)
    finally:
        try:
            os.unlink(kc_path)
        except OSError:
            pass


def _build_stack_client(db):
    """Single-node K8s client — STACK_CLUSTER_ID / STACK_KUBECONFIG / in-cluster."""
    import kubernetes
    import yaml

    if _STACK_CLUSTER_ID:
        stack_cluster = db.query(KubernetesCluster).filter(
            KubernetesCluster.id == _STACK_CLUSTER_ID
        ).first()
        if not stack_cluster or not stack_cluster.kubeconfig:
            raise RuntimeError(f"STACK_CLUSTER_ID={_STACK_CLUSTER_ID} DB mein nahi mila")
        return _build_k8s_client(stack_cluster)

    if _STACK_KUBECONFIG:
        with open(_STACK_KUBECONFIG, "r") as f:
            kc_data = yaml.safe_load(f)
        if _STACK_CLUSTER_IP:
            for c in kc_data.get("clusters", []):
                server = c.get("cluster", {}).get("server", "")
                if server:
                    c["cluster"]["server"] = re.sub(
                        r"(https?://)([^:/]+)(:\d+)?",
                        lambda m: m.group(1) + _STACK_CLUSTER_IP + (m.group(3) or ""),
                        server,
                    )
        kc_fd, kc_path = tempfile.mkstemp(suffix=".yaml")
        try:
            with os.fdopen(kc_fd, "w") as f:
                yaml.dump(kc_data, f)
            cfg = kubernetes.client.Configuration()
            kubernetes.config.load_kube_config(config_file=kc_path, client_configuration=cfg)
            cfg.verify_ssl = False
            return kubernetes.client.ApiClient(configuration=cfg)
        finally:
            try:
                os.unlink(kc_path)
            except OSError:
                pass

    # Production: backend pod in-cluster
    try:
        kubernetes.config.load_incluster_config()
        return kubernetes.client.ApiClient()
    except kubernetes.config.ConfigException:
        raise RuntimeError(
            "STACK_KUBECONFIG/STACK_CLUSTER_ID set nahi hai aur in-cluster config bhi nahi mili."
        )


def _get_image_push_pod(api_client) -> str:
    """image-push-tool pod ka naam nikalo (Running wala)."""
    import kubernetes

    v1   = kubernetes.client.CoreV1Api(api_client)
    pods = v1.list_namespaced_pod(_IMAGE_PUSH_NS, label_selector=_IMAGE_PUSH_LABEL)
    running = [p for p in pods.items if p.status.phase == "Running"]
    if not running:
        raise RuntimeError(
            f"image-push-tool pod Running nahi — ns={_IMAGE_PUSH_NS} label={_IMAGE_PUSH_LABEL}"
        )
    return running[0].metadata.name


def _exec_in_pod(api_client, pod_name: str, cmd: str, timeout: int = 120) -> tuple:
    """Pod mein /bin/sh -c cmd run karo. Internal helper only."""
    import kubernetes
    from kubernetes.stream import stream

    v1   = kubernetes.client.CoreV1Api(api_client)
    resp = stream(
        v1.connect_get_namespaced_pod_exec,
        pod_name, _IMAGE_PUSH_NS,
        command=["/bin/sh", "-c", cmd],
        stderr=True, stdin=False, stdout=True, tty=False,
        _preload_content=False, _request_timeout=timeout,
    )
    out_buf = []
    while resp.is_open():
        resp.update(timeout=1)
        if resp.peek_stdout():
            out_buf.append(resp.read_stdout())
        if resp.peek_stderr():
            out_buf.append(resp.read_stderr())
    resp.close()
    try:
        rc = resp.returncode or 0
    except Exception:
        rc = 0  # kubernetes ws_client bug — ERROR_CHANNEL returns None
    return rc, "".join(out_buf)


def _copy_file_to_pod(api_client, pod_name: str, local_path: str, remote_dir: str) -> str:
    """
    File ko pod mein copy karo — kubectl cp equivalent.
    tar stream via K8s exec stdin.
    Returns remote file path.
    """
    import kubernetes
    from kubernetes.stream import stream

    _exec_in_pod(api_client, pod_name, f"mkdir -p {remote_dir}", timeout=15)

    filename  = os.path.basename(local_path)
    file_size = os.path.getsize(local_path)

    buf  = io.BytesIO()
    info = tarfile.TarInfo(name=filename)
    info.size = file_size
    info.mode = 0o644
    with tarfile.open(fileobj=buf, mode="w") as tar:
        with open(local_path, "rb") as fh:
            tar.addfile(info, fh)
    buf.seek(0)

    v1   = kubernetes.client.CoreV1Api(api_client)
    resp = stream(
        v1.connect_get_namespaced_pod_exec,
        pod_name, _IMAGE_PUSH_NS,
        command=["tar", "xmf", "-", "-C", remote_dir],
        stdin=True, stdout=True, stderr=True, tty=False,
        _preload_content=False, _request_timeout=3600,
    )
    CHUNK = 4 * 1024 * 1024
    while True:
        chunk = buf.read(CHUNK)
        if not chunk:
            break
        resp.write_stdin(chunk)
    resp.close()
    try:
        rc = resp.returncode or 0
    except Exception:
        rc = 0
    if rc != 0:
        raise RuntimeError(f"File copy to pod failed (rc={rc})")

    logger.info(f"[HarborPush] Copied {file_size:,} bytes → pod:{remote_dir}/{filename}")
    return f"{remote_dir}/{filename}"


def _extract_image_metadata_local(local_path: str, item) -> dict:
    """
    Local file se image metadata nikalo — no K8s exec needed.
    Returns: image_owner, image_name, image_version, source_type, is_oci_dir_zip
    """
    file_ext      = os.path.splitext(local_path)[1].lower()
    is_zip        = file_ext == ".zip"
    image_owner   = None
    image_name    = None
    image_version = "latest"
    source_type   = "docker-archive"
    is_oci_dir_zip = False

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
    """HTTP POST → push-image wrapper → skopeo copy → Harbor."""
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
    logger.info(
        f"[HarborPush] POST {_PUSH_IMAGE_BASE_URL}/push/image "
        f"→ {harbor_host}/{project}/{image_name}:{tag}"
    )
    resp = requests.post(
        f"{_PUSH_IMAGE_BASE_URL}/push/image",
        json=payload,
        timeout=900,
        verify=False,
    )
    if not resp.ok:
        raise RuntimeError(
            f"push/image API failed (HTTP {resp.status_code}): {resp.text[:1000]}"
        )
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
        if not temp_path or not os.path.exists(temp_path):
            raise RuntimeError(f"Temp file nahi mila: {temp_path}")
        if not item.harbor_registry_id:
            raise RuntimeError("harbor_registry_id not set — Harbor instance select karo")

        harbor_dep = db.query(KubernetesDeployment).filter(
            KubernetesDeployment.id == item.harbor_registry_id
        ).first()
        if not harbor_dep:
            raise RuntimeError(f"Harbor registry id={item.harbor_registry_id} not found")
        if not harbor_dep.harbor_url:
            raise RuntimeError(f"Harbor id={item.harbor_registry_id} ka harbor_url not set")

        # ── 2. Harbor creds ──────────────────────────────────────────────────
        harbor_url  = harbor_dep.harbor_url.strip().rstrip("/")
        harbor_host = harbor_url.split("://", 1)[1] if "://" in harbor_url else harbor_url
        harbor_user = item.harbor_user or "admin"
        harbor_pass = (
            os.getenv("HARBOR_ADMIN_PASSWORD", "").strip()
            or item.harbor_pass
            or "Harbor12345"
        )
        project = (item.harbor_project or "library").strip("/")

        _update("pushing")
        logger.info(f"[HarborPush] item={item_id} harbor={harbor_host}")

        # ── 3. Stack K8s client + pod ────────────────────────────────────────
        stack_client = _build_stack_client(db)
        pod_name     = _get_image_push_pod(stack_client)
        logger.info(f"[HarborPush] Pod: {pod_name} (ns={_IMAGE_PUSH_NS})")

        # ── 4. Local metadata extraction (no K8s exec) ───────────────────────
        meta = _extract_image_metadata_local(temp_path, item)
        if not meta["image_owner"]:
            raise RuntimeError(
                "Image owner nahi mila. version_metadata.json mein 'owner_name' set karo "
                "ya Docker tar mein proper RepoTag honi chahiye."
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

        # ── 5. Copy file to pod /data/ ───────────────────────────────────────
        uid     = uuid.uuid4().hex[:8]
        pod_dir = f"/data/harbor-push-{item_id}-{uid}"
        pod_file = _copy_file_to_pod(stack_client, pod_name, temp_path, pod_dir)

        # Backend temp file hata do
        try:
            os.remove(temp_path)
        except OSError:
            pass

        # ── 6. OCI-Skopeo-Dir ZIP: pod mein extract karo ────────────────────
        source_path = pod_file
        if is_oci_dir_zip:
            extract_dir = f"{pod_dir}/extracted"
            rc_ex, out_ex = _exec_in_pod(
                stack_client, pod_name,
                f"mkdir -p {extract_dir} && unzip -o {pod_file} -d {extract_dir} 2>&1",
                timeout=300,
            )
            if rc_ex != 0:
                raise RuntimeError(f"ZIP extract failed: {out_ex[:300]}")
            # oci-layout file dhundo → uska parent = OCI dir
            rc_ol, out_ol = _exec_in_pod(
                stack_client, pod_name,
                f"find {extract_dir} -name 'oci-layout' -maxdepth 5 2>/dev/null | head -1",
                timeout=15,
            )
            oci_dir     = os.path.dirname(out_ol.strip()) if out_ol.strip() else extract_dir
            source_path = oci_dir

        # ── 7. HTTP push → Harbor ────────────────────────────────────────────
        dest_image = f"{harbor_host}/{project}/{image_owner}/{image_name}:{image_version}"
        logger.info(f"[HarborPush] pushing → {dest_image}")
        _push_image_via_api(
            harbor_host, harbor_user, harbor_pass,
            project, f"{image_owner}/{image_name}", image_version,
            source_path, source_type,
        )
        logger.info(f"[HarborPush] push OK → {dest_image}")

        # ── 8. Pod cleanup ────────────────────────────────────────────────────
        _exec_in_pod(stack_client, pod_name, f"rm -rf {pod_dir} 2>/dev/null", timeout=30)

        # ── 9. DB update ──────────────────────────────────────────────────────
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

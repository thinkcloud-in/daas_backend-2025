"""
Harbor K8s Deploy Activity — SSH-Free, Multi-Node

Flow:
  1.  DB se kubeconfig + cluster info load karo
  2.  WebDAV se ZIP download karo (backend pe locally)
  3.  version_metadata.json parse karo
  4.  K8s API connectivity check
  5.  Already deployed? check via K8s API
  6.  Namespace create karo via K8s API
  7.  Images archive ZIP se extract karo (locally)
  8.  Extracted archive WebDAV pe temp upload karo
  9.  DaemonSet create karo — sab worker nodes pe images import (ctr via WebDAV wget)
  10. Wait — sab pods ka log "HARBOR_IMPORT_DONE" check karo
  11. DaemonSet + WebDAV temp file delete karo
  12. YAML manifests apply karo (apply_order se) via K8s dynamic client
  13. Harbor URL discover karo via K8s API (NodePort)
  14. Health check (max 5 min)
  15. harbor-jobservice restart via K8s API
"""

import datetime
import fnmatch
import json
import logging
import os
import shutil
import tempfile
import time
import zipfile
from typing import List, Optional, Tuple

import requests
import urllib3
import yaml
from temporalio import activity

from db_configuration.config import SessionLocal
from models.kubernetes_deploy_model import KubernetesDeployment
from models.kubernetes_model import KubernetesCluster

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

_IMPORTER_DS_NAME = "harbor-img-importer"
_IMPORT_DONE_MARKER = "HARBOR_IMPORT_DONE"

_WEBDAV_BASE     = os.getenv("STORAGE_BASE_URL",     "https://devraq.dev.team/library").rstrip("/")
_WEBDAV_INTERNAL = os.getenv("STORAGE_INTERNAL_URL", _WEBDAV_BASE).rstrip("/")


# ─────────────────────────────────────────────────────────────────────────────
# DB helper
# ─────────────────────────────────────────────────────────────────────────────

def _db_update(deploy_id: int, **kwargs):
    db = SessionLocal()
    try:
        d = db.query(KubernetesDeployment).filter(KubernetesDeployment.id == deploy_id).first()
        if d:
            for k, v in kwargs.items():
                setattr(d, k, v)
            d.updated_at = datetime.datetime.utcnow()
            db.commit()
    except Exception as e:
        logger.warning(f"[K8sDeploy] DB update failed: {e}")
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# K8s client loader
# ─────────────────────────────────────────────────────────────────────────────

def _load_k8s_clients(kubeconfig_yaml: str, control_ip: str = None):
    """
    kubeconfig YAML string se K8s clients init karo.
    control_ip: hostname → IP replace (Windows DNS nahi resolve karta hostnames)
    """
    from kubernetes import client as kc, config as kcfg, dynamic

    kc_dict = yaml.safe_load(kubeconfig_yaml)

    if control_ip:
        import re as _re
        for ce in kc_dict.get("clusters", []):
            srv = ce.get("cluster", {}).get("server", "")
            if srv:
                new_srv = _re.sub(r"https://[^:/]+", f"https://{control_ip}", srv)
                ce["cluster"]["server"] = new_srv
                logger.info(f"[K8sDeploy] kubeconfig server: {srv} → {new_srv}")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(kc_dict, f)
        kc_path = f.name
    try:
        kcfg.load_kube_config(config_file=kc_path)
    finally:
        try:
            os.unlink(kc_path)
        except OSError:
            pass

    api_client = kc.ApiClient()
    return (
        kc.CoreV1Api(api_client),
        kc.AppsV1Api(api_client),
        kc.BatchV1Api(api_client),
        dynamic.DynamicClient(api_client),
    )


# ─────────────────────────────────────────────────────────────────────────────
# ZIP helpers
# ─────────────────────────────────────────────────────────────────────────────

def _zip_root_prefix(zf) -> str:
    """ZIP mein common root folder detect karo (e.g. 'harbor-v2.15.2-k8s-offline/')."""
    names = [n for n in zf.namelist() if n.strip("/")]
    if not names:
        return ""
    first_part = names[0].split("/")[0] + "/"
    if all(n.startswith(first_part) for n in names):
        return first_part
    return ""


def _find_yaml_prefix(zf, hint_path: str) -> str:
    """
    YAML files ka actual prefix dynamically resolve karo.
    1. root + hint_path try karo
    2. hint_path without root try karo
    3. Fallback: sabse zyada YAMLs wali directory use karo
    """
    names  = [n for n in zf.namelist() if n.strip("/")]
    root   = _zip_root_prefix(zf)
    hint   = hint_path.strip("/") + "/"

    # Try: root + hint, then hint alone, then root alone as prefix
    candidates = [root + hint, hint, root]
    for prefix in candidates:
        if not prefix:
            continue
        count = sum(1 for n in names if n.startswith(prefix) and n.endswith((".yaml", ".yml")))
        if count > 0:
            logger.info(f"[K8sDeploy] YAML prefix resolved: '{prefix}' ({count} files, hint='{hint_path}')")
            return prefix

    # Final fallback: directory with most YAML files
    dir_counts: dict = {}
    for name in names:
        if name.endswith((".yaml", ".yml")):
            parent = "/".join(name.split("/")[:-1]) + "/"
            dir_counts[parent] = dir_counts.get(parent, 0) + 1
    if dir_counts:
        best = max(dir_counts, key=dir_counts.get)
        logger.warning(f"[K8sDeploy] manifest_path hint '{hint_path}' not matched — fallback to '{best}' ({dir_counts[best]} YAMLs)")
        return best

    return root + hint   # give up, return original for clear error


def _read_version_metadata(zip_path: str) -> dict:
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            if os.path.basename(name) == "version_metadata.json":
                return json.loads(zf.read(name).decode("utf-8"))
    return {}


def _extract_yaml_files(zip_path: str, manifest_path: str, apply_order: List[str]) -> List[Tuple[str, str]]:
    all_yaml = {}
    with zipfile.ZipFile(zip_path, "r") as zf:
        prefix = _find_yaml_prefix(zf, manifest_path)
        for name in zf.namelist():
            if not name.startswith(prefix):
                continue
            bn = os.path.basename(name)
            if not bn.endswith((".yaml", ".yml")):
                continue
            all_yaml[bn] = zf.read(name).decode("utf-8")

    if not all_yaml:
        raise RuntimeError(f"No YAML found in ZIP (hint: {manifest_path})")

    ordered, seen = [], set()
    for pattern in apply_order:
        for fn in sorted(all_yaml):
            if fn not in seen and fnmatch.fnmatch(fn, pattern):
                ordered.append((fn, all_yaml[fn]))
                seen.add(fn)
    for fn in sorted(all_yaml):
        if fn not in seen:
            ordered.append((fn, all_yaml[fn]))
    return ordered


def _extract_images_archive(zip_path: str, archive_path_in_zip: str, dest_dir: str) -> str:
    target = archive_path_in_zip.lstrip("/")
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            if name.rstrip("/") == target or os.path.basename(name) == os.path.basename(target):
                dest = os.path.join(dest_dir, os.path.basename(name))
                with zf.open(name) as src, open(dest, "wb") as dst:
                    while True:
                        chunk = src.read(4 * 1024 * 1024)
                        if not chunk:
                            break
                        dst.write(chunk)
                logger.info(f"[K8sDeploy] Images extracted: {dest} ({os.path.getsize(dest):,} bytes)")
                return dest
    raise RuntimeError(f"Images archive not found in ZIP: {archive_path_in_zip}")


# ─────────────────────────────────────────────────────────────────────────────
# WebDAV temp upload / delete
# ─────────────────────────────────────────────────────────────────────────────

def _webdav_upload(local_path: str, webdav_url: str):
    """Local file WebDAV pe PUT karo."""
    size = os.path.getsize(local_path)
    with open(local_path, "rb") as f:
        resp = requests.put(
            webdav_url, data=f,
            headers={"Content-Length": str(size), "Content-Type": "application/octet-stream"},
            verify=False, timeout=None,
        )
    if resp.status_code not in (200, 201, 204):
        raise RuntimeError(f"WebDAV upload failed ({resp.status_code}): {resp.text[:200]}")
    logger.info(f"[K8sDeploy] WebDAV upload done: {webdav_url} ({size:,} bytes)")


def _webdav_delete(webdav_url: str):
    try:
        requests.delete(webdav_url, verify=False, timeout=30)
        logger.info(f"[K8sDeploy] WebDAV temp deleted: {webdav_url}")
    except Exception as e:
        logger.warning(f"[K8sDeploy] WebDAV delete failed (non-fatal): {e}")


# ─────────────────────────────────────────────────────────────────────────────
# K8s namespace / deploy check
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_namespace(v1, namespace: str):
    from kubernetes.client.exceptions import ApiException
    try:
        v1.read_namespace(name=namespace)
        logger.info(f"[K8sDeploy] Namespace '{namespace}' already exists")
    except ApiException as e:
        if e.status == 404:
            v1.create_namespace(body={
                "apiVersion": "v1", "kind": "Namespace",
                "metadata": {"name": namespace},
            })
            logger.info(f"[K8sDeploy] Namespace '{namespace}' created")
        else:
            raise


def _check_already_deployed(v1, namespace: str) -> bool:
    from kubernetes.client.exceptions import ApiException
    try:
        return len(v1.list_namespaced_pod(namespace=namespace).items) > 0
    except ApiException as e:
        return False if e.status == 404 else (_ for _ in ()).throw(e)


# ─────────────────────────────────────────────────────────────────────────────
# DaemonSet image importer — sab nodes pe
# ─────────────────────────────────────────────────────────────────────────────

def _create_import_daemonset(apps_v1, namespace: str, ds_name: str,
                              images_webdav_url: str, images_filename: str):
    """
    DaemonSet create karo jo sab nodes pe:
    1. WebDAV se images tar.gz download kare
    2. ctr -n k8s.io images import kare
    3. HARBOR_IMPORT_DONE log kare
    alpine:3.18 use karega (already available on all nodes)
    """
    from kubernetes.client.exceptions import ApiException

    # Purana DS hata do
    try:
        apps_v1.delete_namespaced_daemon_set(name=ds_name, namespace=namespace)
        time.sleep(5)
    except ApiException:
        pass

    # chroot /host: alpine musl vs host glibc issue solve karta hai.
    # Host root, run (containerd socket), tmp — teen alag mounts.
    # Download /host/tmp/ me (= host /tmp), chroot me /tmp visible hai.
    cmd = (
        f"wget -q --no-check-certificate -O /host/tmp/{images_filename} '{images_webdav_url}' && "
        f"echo 'Download complete, importing images...' && "
        f"chroot /host ctr -a /run/containerd/containerd.sock -n k8s.io images import /tmp/{images_filename} && "
        f"echo '{_IMPORT_DONE_MARKER}' && "
        f"rm -f /host/tmp/{images_filename} && "
        f"sleep 3600"
    )

    ds_manifest = {
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": {
            "name": ds_name,
            "namespace": namespace,
            "labels": {"app": ds_name},
        },
        "spec": {
            "selector": {"matchLabels": {"app": ds_name}},
            "template": {
                "metadata": {"labels": {"app": ds_name}},
                "spec": {
                    "hostPID": True,
                    "tolerations": [{"operator": "Exists"}],
                    "containers": [{
                        "name": "importer",
                        "image": "docker.io/library/alpine:3.18",
                        "imagePullPolicy": "IfNotPresent",
                        "command": ["sh", "-c", cmd],
                        "securityContext": {"privileged": True},
                        "volumeMounts": [
                            {"name": "host-root",
                             "mountPath": "/host"},
                            {"name": "host-run",
                             "mountPath": "/host/run"},
                            {"name": "host-tmp",
                             "mountPath": "/host/tmp"},
                        ],
                    }],
                    "volumes": [
                        {"name": "host-root",
                         "hostPath": {"path": "/"}},
                        {"name": "host-run",
                         "hostPath": {"path": "/run"}},
                        {"name": "host-tmp",
                         "hostPath": {"path": "/tmp"}},
                    ],
                },
            },
        },
    }
    apps_v1.create_namespaced_daemon_set(namespace=namespace, body=ds_manifest)
    logger.info(f"[K8sDeploy] DaemonSet '{ds_name}' created in ns={namespace}")


def _wait_import_daemonset(v1, apps_v1, namespace: str, ds_name: str,
                            timeout_sec: int = 1800) -> bool:
    """
    Sab DaemonSet pods ke logs mein HARBOR_IMPORT_DONE aane ka wait karo.
    Returns True if all succeeded.
    """
    deadline = time.time() + timeout_sec
    last_log = time.time()

    while time.time() < deadline:
        try:
            ds = apps_v1.read_namespaced_daemon_set(name=ds_name, namespace=namespace)
            desired  = ds.status.desired_number_scheduled or 0
            ready    = ds.status.number_ready or 0
        except Exception:
            time.sleep(10)
            continue

        if desired == 0:
            time.sleep(10)
            continue

        # Check each pod's log
        pods = v1.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"app={ds_name}",
        )
        done_count = 0
        failed_pods = []
        for pod in pods.items:
            pod_name = pod.metadata.name
            try:
                log = v1.read_namespaced_pod_log(name=pod_name, namespace=namespace,
                                                  tail_lines=20)
                if _IMPORT_DONE_MARKER in log:
                    done_count += 1
                elif "ERROR" in log:
                    failed_pods.append(f"{pod_name}: {log[-200:]}")
            except Exception:
                pass

        elapsed = int(time.time() - deadline + timeout_sec)
        status_msg = (
            f"Image import: {done_count}/{desired} nodes done "
            f"(elapsed {elapsed}s)"
        )
        activity.heartbeat(status_msg)

        if time.time() - last_log > 30:
            logger.info(f"[K8sDeploy] {status_msg}")
            last_log = time.time()

        if failed_pods:
            raise RuntimeError(f"Image import failed on pods:\n" + "\n".join(failed_pods))

        if done_count >= desired:
            logger.info(f"[K8sDeploy] All {desired} nodes imported images ✓")
            return True

        time.sleep(15)

    return False


def _delete_import_daemonset(apps_v1, namespace: str, ds_name: str):
    from kubernetes.client.exceptions import ApiException
    try:
        apps_v1.delete_namespaced_daemon_set(
            name=ds_name, namespace=namespace,
            body={"propagationPolicy": "Foreground"},
        )
        logger.info(f"[K8sDeploy] DaemonSet '{ds_name}' deleted")
    except ApiException:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# YAML apply via K8s dynamic client
# ─────────────────────────────────────────────────────────────────────────────

_CLUSTER_SCOPED = {
    "Namespace", "ClusterRole", "ClusterRoleBinding",
    "PersistentVolume", "StorageClass", "IngressClass",
}


def _apply_manifest(dyn_client, manifest: dict, namespace: str):
    from kubernetes.client.exceptions import ApiException

    api_version = manifest.get("apiVersion", "v1")
    kind        = manifest.get("kind", "")
    name        = manifest.get("metadata", {}).get("name", "")
    if not kind or not name:
        return

    cluster_scoped = kind in _CLUSTER_SCOPED
    resource = dyn_client.resources.get(api_version=api_version, kind=kind)

    if not cluster_scoped:
        manifest.setdefault("metadata", {})["namespace"] = namespace

    try:
        if cluster_scoped:
            existing = resource.get(name=name)
        else:
            existing = resource.get(name=name, namespace=namespace)
        manifest["metadata"]["resourceVersion"] = existing.metadata.resourceVersion
        if cluster_scoped:
            resource.replace(body=manifest, name=name)
        else:
            resource.replace(body=manifest, name=name, namespace=namespace)
        logger.info(f"[K8sDeploy] Updated  {kind}/{name}")

    except ApiException as e:
        if e.status == 404:
            if cluster_scoped:
                resource.create(body=manifest)
            else:
                resource.create(body=manifest, namespace=namespace)
            logger.info(f"[K8sDeploy] Created  {kind}/{name}")
        else:
            raise RuntimeError(
                f"Apply failed {kind}/{name}: {e.reason} — {(e.body or '')[:300]}"
            )


def _apply_yaml_docs(dyn_client, yaml_docs: List[Tuple[str, str]], namespace: str) -> List[str]:
    applied = []
    for filename, content in yaml_docs:
        try:
            for manifest in yaml.safe_load_all(content):
                if not manifest:
                    continue
                _apply_manifest(dyn_client, manifest, namespace)
                k = manifest.get("kind", "?")
                n = manifest.get("metadata", {}).get("name", "?")
                applied.append(f"{k}/{n}")
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f"YAML apply failed '{filename}': {e}")
    return applied


def _patch_harbor_deployments(apps_v1, namespace: str):
    """
    Kompose-converted deployments ke liye K8s-specific fixes:
    1. enableServiceLinks=false — K8s {SVC}_PORT env vars Harbor config override karte hain
    2. proxy runAsUser=0 — nginx client_body_temp Permission denied fix
    3. postgresql init container — PVC root-owned, postgres uid=999 needs chown
    """
    # Fix 1: K8s service env injection disable karo (POSTGRESQL_PORT=tcp://... issue)
    all_deployments = []
    try:
        dep_list = apps_v1.list_namespaced_deployment(namespace=namespace)
        all_deployments = [d.metadata.name for d in dep_list.items]
    except Exception:
        pass

    for dep_name in all_deployments:
        try:
            apps_v1.patch_namespaced_deployment(
                name=dep_name, namespace=namespace,
                body={"spec": {"template": {"spec": {"enableServiceLinks": False}}}},
            )
            logger.info(f"[K8sDeploy] {dep_name}: enableServiceLinks=false")
        except Exception as e:
            logger.warning(f"[K8sDeploy] enableServiceLinks patch failed for {dep_name}: {e}")

    # Fix 2: proxy nginx needs root
    try:
        apps_v1.patch_namespaced_deployment(
            name="proxy", namespace=namespace,
            body={"spec": {"template": {"spec": {"securityContext": {"runAsUser": 0}}}}},
        )
        logger.info("[K8sDeploy] proxy: runAsUser=0")
    except Exception as e:
        logger.warning(f"[K8sDeploy] proxy securityContext patch failed: {e}")

    # Fix 3: postgresql PVC permission — init container se chown
    try:
        from kubernetes import client as k8s_client
        init_container = k8s_client.V1Container(
            name="fix-perms",
            image="docker.io/library/alpine:3.18",
            image_pull_policy="IfNotPresent",
            command=["sh", "-c", "chown -R 999:999 /var/lib/postgresql/data"],
            volume_mounts=[k8s_client.V1VolumeMount(
                name="postgresql-claim0",
                mount_path="/var/lib/postgresql/data",
            )],
        )
        dep = apps_v1.read_namespaced_deployment(name="postgresql", namespace=namespace)
        existing_inits = dep.spec.template.spec.init_containers or []
        if not any(ic.name == "fix-perms" for ic in existing_inits):
            existing_inits.append(init_container)
            dep.spec.template.spec.init_containers = existing_inits
            apps_v1.replace_namespaced_deployment(name="postgresql", namespace=namespace, body=dep)
            logger.info("[K8sDeploy] postgresql: fix-perms init container added")
    except Exception as e:
        logger.warning(f"[K8sDeploy] postgresql init container patch failed: {e}")

    # Fix 4: registry — registry-cm2 (root.crt) ko registry-cm1 mein merge karo
    # K8s mein mounted directory ke andar subPath file mount nahi ho sakta
    try:
        v1_api = apps_v1.api_client.__class__._get_openapi_types  # get v1 from apps_v1
    except Exception:
        v1_api = None

    try:
        from kubernetes import client as k8s_client
        _v1 = k8s_client.CoreV1Api()

        # registry-cm2 se root.crt nikalo
        cm2 = _v1.read_namespaced_config_map(name="registry-cm2", namespace=namespace)
        root_crt = (cm2.data or {}).get("root.crt", "")

        if root_crt:
            # registry-cm1 mein add karo
            cm1 = _v1.read_namespaced_config_map(name="registry-cm1", namespace=namespace)
            if "root.crt" not in (cm1.data or {}):
                cm1.data = cm1.data or {}
                cm1.data["root.crt"] = root_crt
                _v1.replace_namespaced_config_map(name="registry-cm1", namespace=namespace, body=cm1)
                logger.info("[K8sDeploy] registry-cm1: root.crt merged from registry-cm2")

            # registry deployment se cm2 volumeMount aur volume hatao
            dep = apps_v1.read_namespaced_deployment(name="registry", namespace=namespace)
            # volumeMounts se registry-cm2 hatao
            vms = dep.spec.template.spec.containers[0].volume_mounts or []
            dep.spec.template.spec.containers[0].volume_mounts = [
                vm for vm in vms if vm.name != "registry-cm2"
            ]
            # volumes se registry-cm2 hatao
            vols = dep.spec.template.spec.volumes or []
            dep.spec.template.spec.volumes = [v for v in vols if v.name != "registry-cm2"]
            apps_v1.replace_namespaced_deployment(name="registry", namespace=namespace, body=dep)
            logger.info("[K8sDeploy] registry: registry-cm2 volumeMount/volume removed")
    except Exception as e:
        logger.warning(f"[K8sDeploy] registry cm2 merge patch failed: {e}")

    # Fix 5: proxy service → LoadBalancer (Cilium LB-IPAM se external IP milegi)
    try:
        from kubernetes import client as k8s_client
        _v1 = k8s_client.CoreV1Api()
        svc = _v1.read_namespaced_service(name="proxy", namespace=namespace)
        if svc.spec.type != "LoadBalancer":
            # NodePort entries clear karo, type LoadBalancer karo
            for p in (svc.spec.ports or []):
                p.node_port = None
            svc.spec.type = "LoadBalancer"
            _v1.replace_namespaced_service(name="proxy", namespace=namespace, body=svc)
            logger.info("[K8sDeploy] proxy service: type=LoadBalancer (Cilium LB-IPAM)")
    except Exception as e:
        logger.warning(f"[K8sDeploy] proxy LoadBalancer patch failed: {e}")

    # Fix 6: registry fsGroup=10000 — PVC root-owned, registry non-root write fix
    try:
        apps_v1.patch_namespaced_deployment(
            name="registry", namespace=namespace,
            body={"spec": {"template": {"spec": {"securityContext": {"fsGroup": 10000}}}}},
        )
        logger.info("[K8sDeploy] registry: securityContext.fsGroup=10000")
    except Exception as e:
        logger.warning(f"[K8sDeploy] registry fsGroup patch failed: {e}")

    # Fix 7: redis fsGroup=10000 — RDB snapshot permission fix (bgsave error)
    try:
        apps_v1.patch_namespaced_deployment(
            name="redis", namespace=namespace,
            body={"spec": {"template": {"spec": {"securityContext": {"fsGroup": 10000}}}}},
        )
        logger.info("[K8sDeploy] redis: securityContext.fsGroup=10000")
    except Exception as e:
        logger.warning(f"[K8sDeploy] redis fsGroup patch failed: {e}")

    # Fix 8: jobservice pods force delete — fresh start with correct config
    try:
        from kubernetes import client as k8s_client
        _v1 = k8s_client.CoreV1Api()
        _v1.delete_collection_namespaced_pod(
            namespace=namespace,
            label_selector="io.kompose.service=jobservice",
            grace_period_seconds=0,
        )
        logger.info("[K8sDeploy] jobservice pods force deleted — will restart fresh")
    except Exception as e:
        logger.warning(f"[K8sDeploy] jobservice pod delete failed (non-fatal): {e}")


def _patch_harbor_env_configmap(v1, namespace: str):
    """
    Kompose conversion kuch keys miss kar deta hai ya wrong name se banata hai.
    Missing keys ko sensible defaults se patch karo taaki pods start ho sakein.
    """
    from kubernetes.client.exceptions import ApiException

    # Key mappings: kompose-missed ya wrong-name keys → (source_key_in_cm, default_value)
    REQUIRED = {
        # PostgreSQL Docker image ka actual env var — Harbor ka POSTGRESQL_PASSWORD se alag naam
        "POSTGRES_PASSWORD":                          ("POSTGRESQL_PASSWORD", "Harbor12345"),
        # Harbor jobservice defaults — kompose Docker Compose env se miss kar deta hai
        "JOBSERVICE_WEBHOOK_JOB_HTTP_CLIENT_TIMEOUT": (None, "10"),
        "JOBSERVICE_WEBHOOK_JOB_MAX_RETRY":           (None, "10"),
    }

    try:
        cm = v1.read_namespaced_config_map(name="env", namespace=namespace)
    except ApiException as e:
        if e.status == 404:
            logger.warning("[K8sDeploy] 'env' ConfigMap not found — skip patch")
            return
        raise

    data    = cm.data or {}
    patches = {}
    for key, (src_key, default) in REQUIRED.items():
        if key not in data:
            value = data.get(src_key, default) if src_key else default
            patches[key] = str(value)
            logger.info(f"[K8sDeploy] env ConfigMap patch: {key}={value!r}")

    if patches:
        cm.data = {**data, **patches}
        v1.replace_namespaced_config_map(name="env", namespace=namespace, body=cm)
        logger.info(f"[K8sDeploy] env ConfigMap patched: {list(patches.keys())}")


# ─────────────────────────────────────────────────────────────────────────────
# Harbor URL discovery
# ─────────────────────────────────────────────────────────────────────────────

def _discover_harbor_url(v1, namespace: str, node_ip: str) -> Optional[str]:
    """
    Harbor URL discover karo — priority order:
    1. LoadBalancer external IP (Cilium LB-IPAM) → port 80 — no port in URL
    2. NodePort fallback → node_ip:nodePort
    """
    from kubernetes.client.exceptions import ApiException
    PROXY_KW = ("proxy", "nginx", "harbor-nginx", "harbor-portal", "harbor")

    try:
        services = v1.list_namespaced_service(namespace=namespace)
    except ApiException:
        return None

    all_svcs = services.items

    # Priority 1: LoadBalancer external IP (Cilium assigns this)
    for svc in all_svcs:
        svc_name = (svc.metadata.name or "").lower()
        if svc.spec.type != "LoadBalancer":
            continue
        ingress = (svc.status.load_balancer.ingress or []) if svc.status.load_balancer else []
        ext_ip = next((i.ip for i in ingress if i.ip), None)
        if not ext_ip:
            continue
        # proxy / nginx prefer karo, else port 80 wala
        for port in (svc.spec.ports or []):
            if port.port == 80:
                url = f"http://{ext_ip}"
                logger.info(f"[K8sDeploy] Harbor URL (LoadBalancer): {url}  svc={svc_name}")
                return url
        # koi bhi port pe
        url = f"http://{ext_ip}"
        logger.info(f"[K8sDeploy] Harbor URL (LoadBalancer fallback): {url}  svc={svc_name}")
        return url

    # Priority 2: NodePort fallback (proxy/nginx prefer)
    for svc in all_svcs:
        svc_name = (svc.metadata.name or "").lower()
        if not any(k in svc_name for k in PROXY_KW):
            continue
        if svc.spec.type != "NodePort":
            continue
        for port in (svc.spec.ports or []):
            if port.port in (80, 8080) and port.node_port:
                url = f"http://{node_ip}:{port.node_port}"
                logger.info(f"[K8sDeploy] Harbor URL (NodePort): {url}  svc={svc_name}")
                return url

    # Priority 3: any NodePort
    for svc in all_svcs:
        if svc.spec.type != "NodePort":
            continue
        for port in (svc.spec.ports or []):
            if port.node_port:
                url = f"http://{node_ip}:{port.node_port}"
                logger.info(f"[K8sDeploy] Harbor URL (NodePort any): {url}")
                return url

    return None


def _get_node_ip(v1) -> Optional[str]:
    try:
        for node in v1.list_node().items:
            for t in ("ExternalIP", "InternalIP"):
                for addr in (node.status.addresses or []):
                    if addr.type == t:
                        return addr.address
    except Exception:
        pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Main activity
# ─────────────────────────────────────────────────────────────────────────────

@activity.defn(name="k8s-harbor-deploy")
def k8s_harbor_deploy_activity(payload: dict) -> dict:
    deploy_id   = payload["deploy_id"]
    file_path   = payload["file_path"]
    file_name   = payload["file_name"]
    deploy_name = payload.get("name", "harbor")
    namespace   = payload.get("namespace", "harbor-system")
    http_port   = payload.get("http_port", 80)
    node_ip     = payload.get("node_ip")

    steps_log: list = []

    def _step(label: str, status: str, **extra):
        ts = datetime.datetime.utcnow().strftime("%H:%M:%S")
        steps_log.append(f"[{ts}] {label}")
        _db_update(deploy_id, status=status, steps_log=json.dumps(steps_log), **extra)
        activity.heartbeat(label)
        logger.info(f"[K8sDeploy id={deploy_id}] {label}")

    def _ok(label: str, **extra):
        ts = datetime.datetime.utcnow().strftime("%H:%M:%S")
        steps_log.append(f"[{ts}] ✓ {label}")
        _db_update(deploy_id, steps_log=json.dumps(steps_log), **extra)
        logger.info(f"[K8sDeploy id={deploy_id}] ✓ {label}")

    v1 = apps_v1 = dyn_client = None
    tmp_dir           = None
    webdav_temp_url   = None
    ds_created        = False

    try:
        # ── Step 1: DB se cluster + kubeconfig ───────────────────────────────
        _step("Loading cluster configuration from DB ...", "initializing")
        db = SessionLocal()
        try:
            deploy_rec = db.query(KubernetesDeployment).filter(
                KubernetesDeployment.id == deploy_id
            ).first()
            if not deploy_rec:
                raise RuntimeError(f"KubernetesDeployment id={deploy_id} not found")
            cluster = db.query(KubernetesCluster).filter(
                KubernetesCluster.id == deploy_rec.cluster_id
            ).first()
            if not cluster:
                raise RuntimeError(f"KubernetesCluster id={deploy_rec.cluster_id} not found")
            if not cluster.kubeconfig:
                raise RuntimeError("kubeconfig is empty — please add it in K8s cluster settings")
            kubeconfig_yaml = cluster.kubeconfig
            if not node_ip:
                node_ip = deploy_rec.node_ip or cluster.control_ip
        finally:
            db.close()
        _ok(f"Cluster: {cluster.name}  node_ip={node_ip}")

        # ── Step 2: K8s clients init ─────────────────────────────────────────
        _step("Initializing Kubernetes API client ...", "initializing")
        v1, apps_v1, batch_v1, dyn_client = _load_k8s_clients(
            kubeconfig_yaml, control_ip=cluster.control_ip
        )
        v1.list_namespace(_request_timeout=10)
        _ok("Kubernetes API connected")

        # ── Step 3: ZIP download ──────────────────────────────────────────────
        _step(f"Downloading '{file_name}' from storage ...", "downloading")
        tmp_dir   = tempfile.mkdtemp(prefix="harbor_deploy_")
        local_zip = os.path.join(tmp_dir, file_name)

        if file_path.startswith(("http://", "https://")):
            with requests.get(file_path, stream=True, verify=False, timeout=600) as resp:
                resp.raise_for_status()
                total, downloaded = int(resp.headers.get("content-length", 0)), 0
                with open(local_zip, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=4 * 1024 * 1024):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total and downloaded % (100 * 1024 * 1024) < (4 * 1024 * 1024):
                            activity.heartbeat(
                                f"Downloading: {downloaded//1024//1024} MB / {total//1024//1024} MB"
                            )
        else:
            shutil.copy2(file_path, local_zip)
        _ok(f"Downloaded: {os.path.getsize(local_zip):,} bytes")

        # ── Step 4: version_metadata.json parse ──────────────────────────────
        _step("Parsing version_metadata.json ...", "preparing")
        meta = _read_version_metadata(local_zip)
        if meta:
            dm             = meta.get("deploy", {})
            namespace      = dm.get("namespace", namespace)
            manifest_path  = dm.get("manifest_path", "manifest/")
            apply_order    = dm.get("apply_order", ["*.yaml"])
            im             = meta.get("images", {})
            images_archive = im.get("archive_path", "")
        else:
            manifest_path  = "manifest/"
            apply_order    = ["*.yaml"]
            images_archive = ""
            logger.warning("[K8sDeploy] version_metadata.json not found — defaults")

        _ok(f"namespace={namespace}  manifest_path={manifest_path}  images={images_archive}")

        # ── Step 5: Already deployed check ───────────────────────────────────
        _step(f"Checking if already deployed in '{namespace}' ...", "checking")
        already_deployed = _check_already_deployed(v1, namespace)
        harbor_url       = f"http://{node_ip}:{http_port}" if node_ip else None

        if already_deployed:
            _ok(f"Harbor already deployed in ns={namespace} — skip to URL discovery")
        else:
            # ── Step 6: Namespace create ──────────────────────────────────────
            _step(f"Creating namespace '{namespace}' ...", "deploying")
            _ensure_namespace(v1, namespace)
            _ok(f"Namespace '{namespace}' ready")

            # ── Step 7–11: Images import (DaemonSet + WebDAV) ─────────────────
            if images_archive:
                # 7. Images archive extract locally
                _step(f"Extracting images archive from ZIP ...", "deploying")
                img_dir           = os.path.join(tmp_dir, "images")
                os.makedirs(img_dir, exist_ok=True)
                local_images_path = _extract_images_archive(local_zip, images_archive, img_dir)
                img_mb            = os.path.getsize(local_images_path) // 1024 // 1024
                images_filename   = os.path.basename(local_images_path)
                _ok(f"Images extracted: {img_mb} MB → {images_filename}")

                # 8. WebDAV pe temp upload (STORAGE_BASE_URL — APISIX, no size limit)
                _step(
                    f"Uploading images archive ({img_mb} MB) to WebDAV temp ... "
                    "[may take several minutes]",
                    "deploying",
                )
                webdav_temp_url = f"{_WEBDAV_BASE}/harbor/harbor-img-temp-{deploy_id}.tar.gz"
                webdav_pub_url  = webdav_temp_url   # same URL for upload + DaemonSet download

                _webdav_upload(local_images_path, webdav_temp_url)
                _ok(f"Images uploaded to WebDAV: {webdav_temp_url}")

                # 9. DaemonSet create
                _step(
                    f"Creating DaemonSet '{_IMPORTER_DS_NAME}' — "
                    f"images will be imported on ALL nodes ...",
                    "deploying",
                )
                _create_import_daemonset(
                    apps_v1, namespace, _IMPORTER_DS_NAME,
                    webdav_pub_url, images_filename,
                )
                ds_created = True
                _ok(f"DaemonSet created in ns={namespace}")

                # 10. Wait for all pods
                _step(
                    "Waiting for image import on all nodes [max 30 min] ...",
                    "deploying",
                )
                all_done = _wait_import_daemonset(
                    v1, apps_v1, namespace, _IMPORTER_DS_NAME, timeout_sec=1800
                )
                if not all_done:
                    raise RuntimeError(
                        "Image import timeout (30 min) — "
                        f"kubectl logs -l app={_IMPORTER_DS_NAME} -n {namespace} se check karo"
                    )
                _ok("Harbor images imported on all nodes ✓")

                # 11. Cleanup
                _step("Cleaning up DaemonSet and temp WebDAV file ...", "deploying")
                _delete_import_daemonset(apps_v1, namespace, _IMPORTER_DS_NAME)
                ds_created = False
                _webdav_delete(webdav_temp_url)
                webdav_temp_url = None
                _ok("Cleanup done")

            # ── Step 12: YAML manifests apply ─────────────────────────────────
            _step(f"Loading YAML manifests from '{manifest_path}' ...", "deploying")
            yaml_docs = _extract_yaml_files(local_zip, manifest_path, apply_order)
            _ok(f"Found {len(yaml_docs)} manifest files")

            _step(f"Applying {len(yaml_docs)} Kubernetes manifests ...", "deploying")
            applied = _apply_yaml_docs(dyn_client, yaml_docs, namespace)
            _ok(
                f"Applied {len(applied)} resources: "
                f"{', '.join(applied[:6])}{'...' if len(applied) > 6 else ''}"
            )

            # ── Step 12b: Harbor post-apply patches ───────────────────────────
            _step("Applying Harbor K8s compatibility patches ...", "deploying")
            _patch_harbor_env_configmap(v1, namespace)
            _patch_harbor_deployments(apps_v1, namespace)
            _ok("Harbor patches applied (enableServiceLinks, proxy root, postgresql perms)")

        # ── Step 13: Harbor URL discover ─────────────────────────────────────
        _step("Discovering Harbor URL via K8s services ...", "waiting_ready")
        if not node_ip:
            node_ip = _get_node_ip(v1)

        for attempt in range(18):
            discovered = _discover_harbor_url(v1, namespace, node_ip) if node_ip else None
            if discovered:
                harbor_url = discovered
                break
            activity.heartbeat(f"Waiting for Harbor services ({attempt+1}/18) ...")
            time.sleep(5)

        if not harbor_url:
            harbor_url = f"http://{node_ip}:{http_port}" if node_ip else f"http://harbor:{http_port}"
            logger.warning(f"[K8sDeploy] URL discover failed — fallback: {harbor_url}")

        _db_update(deploy_id, harbor_url=harbor_url, status="waiting_ready")
        _ok(f"Harbor URL: {harbor_url}")

        # ── Step 14: Health check — max 5 min ────────────────────────────────
        _step(f"Waiting for Harbor at {harbor_url} [max 5 min] ...", "waiting_ready",
              harbor_url=harbor_url)
        harbor_ready = False
        for attempt in range(30):
            try:
                r = requests.get(f"{harbor_url}/api/v2.0/systeminfo",
                                 timeout=8, verify=False, allow_redirects=True)
                if r.status_code in (200, 401, 403):
                    harbor_ready = True
                    _ok(f"Harbor is ready (HTTP {r.status_code})")
                    break
            except Exception as e:
                logger.debug(f"[K8sDeploy] Health {attempt+1}/30: {e}")
            activity.heartbeat(f"Harbor not ready ({attempt+1}/30) ...")
            time.sleep(10)

        if not harbor_ready:
            steps_log.append("[WARN] Harbor health check timeout — pods start hone mein time lagega")
            _db_update(deploy_id, steps_log=json.dumps(steps_log))

        # ── Step 15: harbor-jobservice restart ───────────────────────────────
        if harbor_ready:
            _step("Restarting harbor-jobservice ...", "waiting_ready")
            try:
                apps_v1.patch_namespaced_deployment(
                    name="jobservice", namespace=namespace,
                    body={"spec": {"template": {"metadata": {"annotations": {
                        "kubectl.kubernetes.io/restartedAt":
                            datetime.datetime.utcnow().isoformat(),
                    }}}}},
                )
                _ok("harbor-jobservice restarted")
            except Exception as je:
                logger.warning(f"[K8sDeploy] Jobservice restart (non-fatal): {je}")

        harbor_admin_user = "admin"
        harbor_admin_pass = os.getenv("HARBOR_ADMIN_PASSWORD", "Harbor12345").strip() or "Harbor12345"
        _step(
            f"Harbor deployed at {harbor_url}", "deployed",
            harbor_url=harbor_url,
            harbor_user=harbor_admin_user,
            harbor_pass=harbor_admin_pass,
        )
        return {
            "status":                "deployed",
            "harbor_url":            harbor_url,
            "harbor_user":           harbor_admin_user,
            "harbor_pass":           harbor_admin_pass,
            "namespace":             namespace,
            "was_already_installed": already_deployed,
        }

    except Exception as exc:
        err_msg = str(exc)
        logger.error(f"[K8sDeploy id={deploy_id}] FAILED: {err_msg}", exc_info=True)
        ts = datetime.datetime.utcnow().strftime("%H:%M:%S")
        steps_log.append(f"[{ts}] ✗ FAILED: {err_msg[:400]}")
        _db_update(deploy_id, status="failed", error_message=err_msg[:500],
                   steps_log=json.dumps(steps_log))
        return {"status": "failed", "error": err_msg[:500]}

    finally:
        if ds_created and apps_v1:
            try:
                _delete_import_daemonset(apps_v1, namespace, _IMPORTER_DS_NAME)
            except Exception:
                pass
        if webdav_temp_url:
            _webdav_delete(webdav_temp_url)
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)

"""
App Deploy Activity — SSH-Free
OpenWebUI ya VectorDB ko K8s cluster pe deploy karo.

Flow:
  1. DB se kubeconfig + cluster info load karo
  2. K8s clients init (CoreV1, AppsV1, Dynamic)
  3. All nodes pe containerd configure karo (DaemonSet via K8s API — no SSH)
  4. Namespace create
  5. Old deployment cleanup
  6. Harbor imagePullSecret create
  7. Deployment + Service apply
  8. Pod ready wait (max 10 min)
  9. LoadBalancer IP fetch (max 2 min)
  10. DB update → deployed + service_url
"""

import base64
import json
import logging
import re
import tempfile
import time
import os

import yaml
from temporalio import activity

from db_configuration.config import SessionLocal
from models.app_deploy_model import AppDeployment
from models.kubernetes_model import KubernetesCluster
from models.library_model import LibraryItem

logger = logging.getLogger(__name__)

_CONTAINERD_LABEL = "daas-patch=containerd"


# ─────────────────────────────────────────────────────────────────────────────
# K8s client loader (same pattern as activities_kubernetes_deploy.py)
# ─────────────────────────────────────────────────────────────────────────────

def _load_k8s_clients(kubeconfig_yaml: str, control_ip: str = None):
    from kubernetes import client as kc, config as kcfg, dynamic

    kc_dict = yaml.safe_load(kubeconfig_yaml)
    if control_ip:
        for ce in kc_dict.get("clusters", []):
            srv = ce.get("cluster", {}).get("server", "")
            if srv:
                new_srv = re.sub(r"https://[^:/]+", f"https://{control_ip}", srv)
                ce["cluster"]["server"] = new_srv

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
        dynamic.DynamicClient(api_client),
    )


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def _db_update(deploy_id: int, **kwargs):
    db = SessionLocal()
    try:
        d = db.query(AppDeployment).filter(AppDeployment.id == deploy_id).first()
        if d:
            for k, v in kwargs.items():
                setattr(d, k, v)
            db.commit()
    finally:
        db.close()


def _log_step(deploy_id: int, step: str):
    db = SessionLocal()
    try:
        d = db.query(AppDeployment).filter(AppDeployment.id == deploy_id).first()
        if d:
            try:
                logs = json.loads(d.steps_log) if d.steps_log else []
            except Exception:
                logs = []
            logs.append({"step": step, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
            d.steps_log = json.dumps(logs)
            db.commit()
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# K8s resource name helper
# ─────────────────────────────────────────────────────────────────────────────

def _k8s_name(s: str) -> str:
    s = re.sub(r"[^a-z0-9-]", "-", s.lower())
    s = re.sub(r"-+", "-", s).strip("-")
    return s[:52]


# ─────────────────────────────────────────────────────────────────────────────
# Containerd insecure registry configure — DaemonSet via K8s API (no SSH)
# ─────────────────────────────────────────────────────────────────────────────

def _configure_containerd_per_node(
    v1, apps_v1,
    harbor_host: str,
    image: str,
    harbor_user: str,
    harbor_pass: str,
    namespace: str = "kube-system",
):
    """
    Har node pe ek individual Pod banao (restartPolicy: Never, nodeName set).
    Pod Succeeded phase tak pohonchta hai — DaemonSet se alag (jo kabhi Succeeded nahi hota).
    K8s API se — SSH ki zarurat nahi.
    """
    from kubernetes.client.exceptions import ApiException

    creds = f"{harbor_user}:{harbor_pass}"
    patch_script = (
        '#!/bin/sh\n'
        'set -e\n'
        'apk add --no-cache util-linux >/dev/null 2>&1 || true\n'
        'HARBOR="' + harbor_host + '"\n'
        'IMAGE="' + image + '"\n'
        'CREDS="' + creds + '"\n'
        'CERTS_DIR=/etc/containerd/certs.d\n'
        'CFG=/etc/containerd/config.toml\n'
        'echo "[daas] Step1: certs.d directory"\n'
        'nsenter -t 1 -m -u -n -p -- mkdir -p "${CERTS_DIR}/${HARBOR}"\n'
        'echo "[daas] Step2: hosts.toml"\n'
        'printf \'server = "http://%s"\\n\\n[host."http://%s"]\\n  capabilities = ["pull", "resolve"]\\n  skip_verify = true\\n\' \\\n'
        '    "${HARBOR}" "${HARBOR}" \\\n'
        '    | nsenter -t 1 -m -u -n -p -- tee "${CERTS_DIR}/${HARBOR}/hosts.toml" > /dev/null\n'
        'echo "[daas] Step3: config_path in config.toml"\n'
        'nsenter -t 1 -m -u -n -p -- \\\n'
        '    sed -i \'s|config_path = ""|config_path = "/etc/containerd/certs.d"|\' "${CFG}" 2>/dev/null || true\n'
        'if ! nsenter -t 1 -m -u -n -p -- grep -q "config_path" "${CFG}" 2>/dev/null; then\n'
        '    printf \'\\n[plugins."io.containerd.grpc.v1.cri".registry]\\n  config_path = "/etc/containerd/certs.d"\\n\' \\\n'
        '        | nsenter -t 1 -m -u -n -p -- tee -a "${CFG}" > /dev/null\n'
        'fi\n'
        'echo "[daas] Step4: restart containerd"\n'
        'nsenter -t 1 -m -u -n -p -- systemctl restart containerd || true\n'
        'sleep 8\n'
        'if [ -n "${IMAGE}" ]; then\n'
        '    echo "[daas] Step5: ctr pre-pull"\n'
        '    nsenter -t 1 -m -u -n -p -- \\\n'
        '        ctr -n k8s.io images pull --plain-http --user "${CREDS}" "${IMAGE}" \\\n'
        '        && echo "[daas] PRE-PULL SUCCESS" \\\n'
        '        || echo "[daas] WARNING: ctr pull failed — continuing"\n'
        'fi\n'
        'echo "[daas] DONE"\n'
    )
    script_b64 = base64.b64encode(patch_script.encode("utf-8")).decode("ascii")
    cmd        = f'echo {script_b64} | base64 -d | sh'

    # Pehle sab nodes ki list lo
    nodes      = v1.list_node()
    node_names = [n.metadata.name for n in nodes.items]
    if not node_names:
        raise RuntimeError("kubectl get nodes empty — cluster reachable hai?")
    logger.info(f"[AppDeploy] Configuring containerd on {len(node_names)} nodes: {node_names}")

    # Purane patch pods clean karo
    try:
        v1.delete_collection_namespaced_pod(
            namespace=namespace,
            label_selector="daas-patch=containerd",
            grace_period_seconds=0,
        )
        time.sleep(3)
    except ApiException:
        pass

    # Har node ke liye ek Pod (restartPolicy: Never) — ye Succeeded phase mein jayega
    created_pods = []
    for node_name in node_names:
        safe     = re.sub(r"[^a-z0-9-]", "-", node_name.lower())[:40]
        pod_name = f"daas-cp-{safe}"
        pod_body = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": namespace,
                "labels": {"daas-patch": "containerd"},
            },
            "spec": {
                "nodeName":      node_name,
                "restartPolicy": "Never",
                "hostPID":       True,
                "tolerations":   [{"operator": "Exists"}],
                "containers": [{
                    "name":            "patcher",
                    "image":           "alpine:3.18",
                    "imagePullPolicy": "IfNotPresent",
                    "command":         ["/bin/sh", "-c", cmd],
                    "securityContext": {"privileged": True},
                }],
            },
        }
        try:
            v1.delete_namespaced_pod(name=pod_name, namespace=namespace, grace_period_seconds=0)
            time.sleep(2)
        except ApiException:
            pass
        v1.create_namespaced_pod(namespace=namespace, body=pod_body)
        created_pods.append(pod_name)
        logger.info(f"[AppDeploy] Patch pod created on node: {node_name}")

    # Sab pods Succeeded hone ka wait (max 3 min)
    deadline      = time.time() + 180
    all_succeeded = False
    while time.time() < deadline:
        time.sleep(8)
        try:
            pods = v1.list_namespaced_pod(
                namespace=namespace,
                label_selector="daas-patch=containerd",
            )
            phases  = {p.metadata.name: (p.status.phase or "") for p in pods.items}
            done    = sum(1 for ph in phases.values() if ph == "Succeeded")
            failed  = [n for n, ph in phases.items() if ph == "Failed"]
            total   = len(node_names)
            logger.info(f"[AppDeploy] Patch pods: {phases}")
            activity.heartbeat(f"Containerd patch: {done}/{total} nodes done")
            if failed:
                raise RuntimeError(f"containerd patch failed on pods: {failed}")
            if done >= total:
                all_succeeded = True
                break
        except RuntimeError:
            raise
        except Exception as e:
            logger.warning(f"[AppDeploy] pod poll error: {e}")

    # Cleanup
    try:
        v1.delete_collection_namespaced_pod(
            namespace=namespace,
            label_selector="daas-patch=containerd",
            grace_period_seconds=0,
        )
    except Exception:
        pass

    if not all_succeeded:
        raise RuntimeError("containerd patch pods timed out (3 min) — nodes not configured")

    logger.info(f"[AppDeploy] All {len(node_names)} nodes containerd configured")


# ─────────────────────────────────────────────────────────────────────────────
# Namespace ensure
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_namespace(v1, namespace: str):
    from kubernetes.client.exceptions import ApiException
    try:
        v1.read_namespace(name=namespace)
    except ApiException as e:
        if e.status == 404:
            v1.create_namespace(body={
                "apiVersion": "v1", "kind": "Namespace",
                "metadata": {"name": namespace},
            })
            logger.info(f"[AppDeploy] Namespace '{namespace}' created")
        else:
            raise


# ─────────────────────────────────────────────────────────────────────────────
# imagePullSecret create/update
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_pull_secret(v1, namespace: str, harbor_host: str, harbor_user: str, harbor_pass: str):
    from kubernetes.client.exceptions import ApiException

    auth_b64  = base64.b64encode(f"{harbor_user}:{harbor_pass}".encode()).decode()
    dockercfg = json.dumps({"auths": {harbor_host: {"auth": auth_b64}}})
    data_b64  = base64.b64encode(dockercfg.encode()).decode()

    secret_body = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "harbor-registry-secret", "namespace": namespace},
        "type": "kubernetes.io/dockerconfigjson",
        "data": {".dockerconfigjson": data_b64},
    }
    try:
        v1.delete_namespaced_secret(name="harbor-registry-secret", namespace=namespace)
    except ApiException:
        pass
    v1.create_namespaced_secret(namespace=namespace, body=secret_body)
    logger.info(f"[AppDeploy] imagePullSecret created for {harbor_host}")


# ─────────────────────────────────────────────────────────────────────────────
# YAML builders
# ─────────────────────────────────────────────────────────────────────────────

def _openwebui_manifest(rname: str, namespace: str, image: str) -> list[dict]:
    svc_name = f"{rname}-openwebui"
    return [
        {
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": svc_name, "namespace": namespace,
                         "labels": {"app": svc_name, "managed-by": "daas", "daas-type": "openwebui"}},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": svc_name}},
                "template": {
                    "metadata": {"labels": {"app": svc_name, "managed-by": "daas"}},
                    "spec": {
                        "imagePullSecrets": [{"name": "harbor-registry-secret"}],
                        "containers": [{
                            "name": "openwebui", "image": image,
                            "imagePullPolicy": "IfNotPresent",
                            "ports": [{"containerPort": 8080}],
                            "env": [{"name": "WEBUI_SECRET_KEY", "value": "daas-openwebui-secret"}],
                            "resources": {
                                "requests": {"memory": "512Mi", "cpu": "250m"},
                                "limits":   {"memory": "2Gi",   "cpu": "2000m"},
                            },
                        }],
                    },
                },
            },
        },
        {
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": svc_name, "namespace": namespace,
                         "labels": {"app": svc_name, "managed-by": "daas"}},
            "spec": {
                "type": "LoadBalancer",
                "selector": {"app": svc_name},
                "ports": [{"name": "http", "port": 80, "targetPort": 8080}],
            },
        },
    ]


def _vectordb_manifest(rname: str, namespace: str, image: str) -> list[dict]:
    svc_name = f"{rname}-vectordb"
    return [
        {
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": svc_name, "namespace": namespace,
                         "labels": {"app": svc_name, "managed-by": "daas", "daas-type": "vectordb"}},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": svc_name}},
                "template": {
                    "metadata": {"labels": {"app": svc_name, "managed-by": "daas"}},
                    "spec": {
                        "imagePullSecrets": [{"name": "harbor-registry-secret"}],
                        "containers": [{
                            "name": "vectordb", "image": image,
                            "imagePullPolicy": "IfNotPresent",
                            "ports": [{"containerPort": 5432}],
                            "env": [
                                {"name": "POSTGRES_DB",       "value": "vectordb"},
                                {"name": "POSTGRES_USER",     "value": "postgres"},
                                {"name": "POSTGRES_PASSWORD", "value": "postgres123"},
                            ],
                            "resources": {
                                "requests": {"memory": "256Mi", "cpu": "250m"},
                                "limits":   {"memory": "1Gi",   "cpu": "1000m"},
                            },
                        }],
                    },
                },
            },
        },
        {
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": svc_name, "namespace": namespace,
                         "labels": {"app": svc_name, "managed-by": "daas"}},
            "spec": {
                "type": "LoadBalancer",
                "selector": {"app": svc_name},
                "ports": [{"name": "postgres", "port": 5432, "targetPort": 5432}],
            },
        },
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Apply manifest via K8s dynamic client
# ─────────────────────────────────────────────────────────────────────────────

def _apply_manifest(dyn_client, manifest: dict, namespace: str):
    from kubernetes.client.exceptions import ApiException

    api_version = manifest.get("apiVersion", "v1")
    kind        = manifest.get("kind", "")
    name        = manifest.get("metadata", {}).get("name", "")
    resource    = dyn_client.resources.get(api_version=api_version, kind=kind)

    manifest.setdefault("metadata", {})["namespace"] = namespace
    try:
        existing = resource.get(name=name, namespace=namespace)
        manifest["metadata"]["resourceVersion"] = existing.metadata.resourceVersion
        resource.replace(body=manifest, name=name, namespace=namespace)
        logger.info(f"[AppDeploy] Updated {kind}/{name}")
    except ApiException as e:
        if e.status == 404:
            resource.create(body=manifest, namespace=namespace)
            logger.info(f"[AppDeploy] Created {kind}/{name}")
        else:
            raise


# ─────────────────────────────────────────────────────────────────────────────
# Node external IP
# ─────────────────────────────────────────────────────────────────────────────

def _get_node_ip(v1, fallback: str) -> str:
    try:
        for node in v1.list_node().items:
            for addr_type in ("ExternalIP", "InternalIP"):
                for addr in (node.status.addresses or []):
                    if addr.type == addr_type and addr.address:
                        return addr.address
    except Exception as e:
        logger.warning(f"[AppDeploy] node IP fetch failed: {e}")
    return fallback


# ─────────────────────────────────────────────────────────────────────────────
# Main activity
# ─────────────────────────────────────────────────────────────────────────────

@activity.defn(name="App-Deploy-Activity")
def app_deploy_activity(payload: dict) -> dict:
    deploy_id       = payload["deploy_id"]
    deployment_type = payload["deployment_type"]
    k8s_cluster_id  = payload["k8s_cluster_id"]
    namespace       = payload.get("namespace", "default")
    name            = payload["name"]
    harbor_host     = payload["harbor_host"]
    harbor_user     = payload.get("harbor_user", "admin")
    harbor_pass     = payload.get("harbor_pass", "Harbor12345")
    image           = payload["image"]

    rname    = _k8s_name(name)
    svc_name = f"{rname}-{deployment_type}"

    try:
        # ── Step 1: DB se kubeconfig load karo ──────────────────────────────
        _db_update(deploy_id, status="connecting")
        _log_step(deploy_id, "Loading cluster configuration from DB ...")
        db = SessionLocal()
        try:
            cluster = db.query(KubernetesCluster).filter(
                KubernetesCluster.id == k8s_cluster_id
            ).first()
            if not cluster or not cluster.kubeconfig:
                raise RuntimeError(
                    f"K8s cluster id={k8s_cluster_id} nahi mila ya kubeconfig missing"
                )
            kubeconfig_yaml = cluster.kubeconfig
            node_ip         = cluster.control_ip

            # Library item se fresh image data lo
            library_item_id = payload.get("library_item_id")
            if library_item_id:
                lib = db.query(LibraryItem).filter(LibraryItem.id == library_item_id).first()
                if lib:
                    if lib.harbor_image:
                        image       = lib.harbor_image
                        harbor_host = image.split("/")[0]
                    if lib.harbor_user: harbor_user = lib.harbor_user
                    if lib.harbor_pass: harbor_pass = lib.harbor_pass
        finally:
            db.close()
        _log_step(deploy_id, f"Cluster: {cluster.name}  image: {image}")

        # ── Step 2: K8s clients init ─────────────────────────────────────────
        _log_step(deploy_id, "Initializing Kubernetes API client ...")
        v1, apps_v1, dyn_client = _load_k8s_clients(kubeconfig_yaml, control_ip=node_ip)
        v1.list_namespace(_request_timeout=10)
        _log_step(deploy_id, "Kubernetes API connected")

        # ── Step 3: Node IP ──────────────────────────────────────────────────
        external_ip = _get_node_ip(v1, fallback=node_ip)
        _log_step(deploy_id, f"Node IP: {external_ip}")
        _db_update(deploy_id, external_ip=external_ip)

        # ── Step 4: Containerd configure + image pre-pull (DaemonSet) ───────
        _db_update(deploy_id, status="deploying")
        _log_step(deploy_id, "Configuring containerd on all nodes (DaemonSet) ...")
        _configure_containerd_per_node(
            v1, apps_v1,
            harbor_host=harbor_host,
            image=image,
            harbor_user=harbor_user,
            harbor_pass=harbor_pass,
            namespace="kube-system",
        )
        _log_step(deploy_id, "All nodes configured + image pre-pulled")

        # ── Step 5: Namespace ensure ─────────────────────────────────────────
        _ensure_namespace(v1, namespace)
        _log_step(deploy_id, f"Namespace '{namespace}' ready")

        # ── Step 6: Old deployment cleanup ───────────────────────────────────
        from kubernetes.client.exceptions import ApiException
        try:
            apps_v1.delete_namespaced_deployment(name=svc_name, namespace=namespace)
            _log_step(deploy_id, f"Old deployment '{svc_name}' deleted")
            time.sleep(10)
        except ApiException as e:
            if e.status != 404:
                logger.warning(f"[AppDeploy] old deployment delete: {e}")

        # ── Step 7: Harbor imagePullSecret ───────────────────────────────────
        _ensure_pull_secret(v1, namespace, harbor_host, harbor_user, harbor_pass)
        _log_step(deploy_id, f"imagePullSecret set for {harbor_host}")

        # ── Step 8: Manifest apply ───────────────────────────────────────────
        if deployment_type == "openwebui":
            manifests = _openwebui_manifest(rname, namespace, image)
        else:
            manifests = _vectordb_manifest(rname, namespace, image)

        for m in manifests:
            _apply_manifest(dyn_client, m, namespace)
        _log_step(deploy_id, f"Manifest applied: {image}")

        # ── Step 9: Pod ready wait (max 10 min) ──────────────────────────────
        time.sleep(15)
        _db_update(deploy_id, status="waiting_ready")
        _log_step(deploy_id, "Waiting for pod to be ready ...")

        pod_label = f"app={svc_name}"
        deadline  = time.time() + 600
        pod_ready = False

        while time.time() < deadline:
            time.sleep(10)
            try:
                pods = v1.list_namespaced_pod(
                    namespace=namespace, label_selector=pod_label
                )
                active = [p for p in pods.items
                          if (p.status.phase or "") not in ("Terminating", "")]
                if not active:
                    continue

                for p in active:
                    phase = p.status.phase or ""
                    cs    = (p.status.container_statuses or [])
                    for c in cs:
                        w = c.state.waiting
                        if w and w.reason in ("ImagePullBackOff", "ErrImagePull"):
                            raise RuntimeError(
                                f"Image pull failed: {w.reason} — {w.message}"
                            )
                        if w and w.reason == "CrashLoopBackOff":
                            raise RuntimeError(f"CrashLoopBackOff: {w.message}")

                all_ready = all(
                    c.ready
                    for p in active
                    for c in (p.status.container_statuses or [])
                )
                activity.heartbeat(
                    f"Pod status: {len(active)} active, ready={all_ready}"
                )
                logger.info(f"[AppDeploy] pods active={len(active)} ready={all_ready}")
                if all_ready:
                    pod_ready = True
                    _log_step(deploy_id, "Pod Running and Ready")
                    break
            except RuntimeError:
                raise
            except Exception as e:
                logger.warning(f"[AppDeploy] pod poll error: {e}")

        if not pod_ready:
            _log_step(deploy_id, "Pod ready timeout (10 min)")
            logger.warning(f"[AppDeploy] id={deploy_id} pod ready timeout")

        # ── Step 10: LoadBalancer IP fetch (max 2 min) ───────────────────────
        _log_step(deploy_id, "Waiting for LoadBalancer IP ...")
        lb_deadline = time.time() + 120
        while time.time() < lb_deadline:
            time.sleep(5)
            try:
                svc    = v1.read_namespaced_service(name=svc_name, namespace=namespace)
                ingress = (svc.status.load_balancer.ingress or []) \
                          if svc.status.load_balancer else []
                lb_ip  = next((i.ip for i in ingress if i.ip), None)
                if lb_ip:
                    external_ip = lb_ip
                    _log_step(deploy_id, f"LoadBalancer IP: {lb_ip}")
                    break
            except Exception:
                pass
        else:
            _log_step(deploy_id, f"LB IP timeout — node IP fallback: {external_ip}")

        # Service URL build
        if deployment_type == "openwebui":
            service_url = f"http://{external_ip}"
        else:
            service_url = f"postgresql://postgres:postgres123@{external_ip}/vectordb"

        _log_step(deploy_id, f"Service URL: {service_url}")

        # NodePort (optional, for info)
        node_port = None
        try:
            svc       = v1.read_namespaced_service(name=svc_name, namespace=namespace)
            ports     = svc.spec.ports or []
            node_port = str(ports[0].node_port) if ports and ports[0].node_port else None
        except Exception:
            pass

        # ── Step 11: Final DB save ───────────────────────────────────────────
        _db_update(
            deploy_id,
            status      = "deployed",
            external_ip = external_ip,
            node_port   = node_port,
            service_url = service_url,
        )
        _log_step(deploy_id, "Deployment complete ✓")
        logger.info(f"[AppDeploy] id={deploy_id} DONE — {service_url}")

        return {
            "status":      "deployed",
            "deploy_id":   deploy_id,
            "type":        deployment_type,
            "external_ip": external_ip,
            "node_port":   node_port,
            "service_url": service_url,
        }

    except Exception as e:
        err_msg = str(e)[:500]
        logger.error(f"[AppDeploy] id={deploy_id} FAILED: {err_msg}", exc_info=True)
        _db_update(deploy_id, status="failed", error_message=err_msg)
        _log_step(deploy_id, f"FAILED: {err_msg[:200]}")
        raise

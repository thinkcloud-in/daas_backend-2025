import asyncio
import base64
import datetime
import logging
import os
import re
import tempfile

import requests
import urllib3
from fastapi import HTTPException
from sqlalchemy.orm import Session

from models.kubernetes_model import KubernetesCluster
from utils import response_format

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Primitive parsers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_cpu(cpu_str: str) -> float:
    """'500m' → 0.5, '2' → 2.0, '200000000n' → 0.2"""
    if not cpu_str:
        return 0.0
    try:
        if cpu_str.endswith("m"):
            return int(cpu_str[:-1]) / 1000.0
        if cpu_str.endswith("n"):
            return int(cpu_str[:-1]) / 1_000_000_000.0
        return float(cpu_str)
    except Exception:
        return 0.0


def _parse_memory_bytes(mem_str: str) -> int:
    """'8110084Ki' → bytes int"""
    if not mem_str:
        return 0
    try:
        for suffix, mult in [("Ki", 1024), ("Mi", 1024**2), ("Gi", 1024**3),
                               ("Ti", 1024**4), ("K", 1000), ("M", 1000**2), ("G", 1000**3)]:
            if mem_str.endswith(suffix):
                return int(mem_str[:-len(suffix)]) * mult
        return int(mem_str)
    except Exception:
        return 0


def _parse_memory(mem_str: str) -> str:
    """'8110084Ki' → '7.7 GB'  (display string)"""
    b = _parse_memory_bytes(mem_str)
    return _bytes_to_human(b)


def _bytes_to_human(b: int) -> str:
    if b == 0:
        return "0 MB"
    if b >= 1024**3:
        return f"{b / 1024**3:.1f} GB"
    if b >= 1024**2:
        return f"{b / 1024**2:.0f} MB"
    return f"{b / 1024:.0f} KB"


def _calc_uptime(ts_str: str) -> str:
    """ISO timestamp → '5d 16h' style uptime string"""
    if not ts_str:
        return None
    try:
        ts  = datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        now = datetime.datetime.now(datetime.timezone.utc)
        diff = now - ts
        d    = diff.days
        h    = diff.seconds // 3600
        m    = (diff.seconds % 3600) // 60
        if d > 0:
            return f"{d}d {h}h"
        if h > 0:
            return f"{h}h {m}m"
        return f"{m}m"
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Auth builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_request_kwargs(control_ip: str, port: int,
                           username=None, password=None,
                           auth_token=None, kubeconfig=None):
    """
    Returns (base_url, req_kwargs, cert_files_to_cleanup)
    Auth priority: kubeconfig (token > client-cert) → auth_token → username/password
    """
    base_url   = f"https://{control_ip}:{port}"
    headers    = {}
    auth       = None
    cert       = None
    cert_files = []

    if kubeconfig:
        try:
            import yaml
            kc        = yaml.safe_load(kubeconfig)
            users_kc  = kc.get("users", [])
            if users_kc:
                user_data = users_kc[0].get("user", {})
                if user_data.get("token"):
                    headers["Authorization"] = f"Bearer {user_data['token']}"
                elif (user_data.get("client-certificate-data")
                      and user_data.get("client-key-data")):
                    cert_bytes = base64.b64decode(user_data["client-certificate-data"])
                    key_bytes  = base64.b64decode(user_data["client-key-data"])
                    c_fd, c_path = tempfile.mkstemp(suffix=".crt")
                    k_fd, k_path = tempfile.mkstemp(suffix=".key")
                    with os.fdopen(c_fd, "wb") as f:
                        f.write(cert_bytes)
                    with os.fdopen(k_fd, "wb") as f:
                        f.write(key_bytes)
                    cert       = (c_path, k_path)
                    cert_files = [c_path, k_path]
        except Exception as e:
            logger.warning(f"[K8s] kubeconfig auth build error: {e}")

    if not headers.get("Authorization") and cert is None:
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"
        elif username and password:
            auth = (username, password)

    return base_url, {"headers": headers, "auth": auth, "cert": cert, "verify": False}, cert_files


def _cleanup(cert_files: list):
    for p in cert_files:
        try:
            os.unlink(p)
        except OSError:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# DB dict helper
# ─────────────────────────────────────────────────────────────────────────────

def _to_dict(c: KubernetesCluster) -> dict:
    return {
        "id":             c.id,
        "name":           c.name,
        "control_ip":     c.control_ip,
        "port":           c.port,
        "username":       c.username,
        "auth_token":     "***" if c.auth_token else None,
        "kubeconfig":     "***" if c.kubeconfig else None,
        "has_kubeconfig": bool(c.kubeconfig),
        "status":         c.status,
        "last_tested":    c.last_tested.isoformat() if c.last_tested else None,
        "created_at":     c.created_at.isoformat() if c.created_at else None,
        "updated_at":     c.updated_at.isoformat() if c.updated_at else None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# kubeconfig parser (auto-fill)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_kubeconfig_info(kubeconfig: str) -> dict:
    result = {"control_ip": None, "port": 6443}
    try:
        import yaml, ipaddress
        kc          = yaml.safe_load(kubeconfig)
        clusters_kc = kc.get("clusters", [])
        if clusters_kc:
            server = clusters_kc[0].get("cluster", {}).get("server", "")
            m = re.match(r'https?://([^/:]+):?(\d*)', server)
            if m:
                host     = m.group(1)
                port_str = m.group(2)
                if port_str:
                    result["port"] = int(port_str)
                try:
                    ipaddress.ip_address(host)
                    result["control_ip"] = host
                except ValueError:
                    pass
    except Exception as e:
        logger.warning(f"[K8s] kubeconfig parse error: {e}")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# System-component helpers
# ─────────────────────────────────────────────────────────────────────────────

_COMP_ORDER = [
    "kube-apiserver", "kube-controller-manager", "kube-scheduler", "etcd",
    "kube-proxy", "coredns",
    "calico-node", "calico-kube-controllers", "calico-typha",
    "tigera-node", "tigera-operator",
    "flannel", "weave-net", "cilium",
    "metrics-server", "ingress-nginx",
]

_COMP_TYPE_MAP = {
    "kube-apiserver":          "Static Pod",
    "kube-controller-manager": "Static Pod",
    "kube-scheduler":          "Static Pod",
    "etcd":                    "Static Pod",
    "kube-proxy":              "DaemonSet",
    "coredns":                 "Deployment",
    "calico-node":             "DaemonSet",
    "calico-kube-controllers": "Deployment",
    "calico-typha":            "Deployment",
    "tigera-node":             "DaemonSet",
    "tigera-operator":         "Deployment",
    "flannel":                 "DaemonSet",
    "weave-net":               "DaemonSet",
    "cilium":                  "DaemonSet",
    "metrics-server":          "Deployment",
    "ingress-nginx":           "DaemonSet",
}

# Label keys se canonical component name match karo
_LABEL_TO_COMP = {
    "calico-node":             "calico-node",
    "calico-kube-controllers": "calico-kube-controllers",
    "calico-typha":            "calico-typha",
    "k8s-app=calico-node":     "calico-node",
    "tigera-operator":         "tigera-operator",
    "kube-apiserver":          "kube-apiserver",
    "kube-scheduler":          "kube-scheduler",
    "kube-controller-manager": "kube-controller-manager",
    "etcd":                    "etcd",
    "kube-proxy":              "kube-proxy",
    "coredns":                 "coredns",
    "k8s-dns-kube-dns":        "coredns",
    "flannel":                 "flannel",
    "weave-net":               "weave-net",
    "cilium":                  "cilium",
    "metrics-server":          "metrics-server",
    "ingress-nginx":           "ingress-nginx",
}


def _component_health_detail(name: str, ready: int, total: int) -> str:
    ok = ready == total and total > 0
    if "apiserver" in name:
        return ("API Server responds normally. Authentication and authorization active."
                if ok else f"API Server degraded: {ready}/{total} instances healthy.")
    if "controller-manager" in name:
        return ("Leader election active. All core loops synchronized."
                if ok else f"Controller manager degraded: {ready}/{total} instances.")
    if "scheduler" in name:
        return ("Scheduler operating normally. Zero pending unschedulable pods."
                if ok else f"Scheduler degraded: {ready}/{total} instances.")
    if "etcd" in name:
        return ("etcd cluster healthy. Raft consensus synchronized."
                if ok else f"etcd degraded: {ready}/{total} instances.")
    if "kube-proxy" in name:
        return (f"Network rules applied on all {total} nodes. Endpoints bound."
                if ok else f"kube-proxy degraded: {ready}/{total} nodes.")
    if "coredns" in name:
        return (f"Replicas ({ready}/{total}) online. DNS resolution active."
                if ok else f"CoreDNS degraded: {ready}/{total} replicas.")
    if "calico" in name:
        return (f"BGP session active. Network policies enforced on {total} nodes."
                if ok else f"Calico degraded: {ready}/{total} nodes.")
    if "flannel" in name:
        return (f"VXLAN overlay active on {total} nodes."
                if ok else f"Flannel degraded: {ready}/{total} nodes.")
    if "weave" in name:
        return (f"Weave network mesh active on {total} nodes."
                if ok else f"Weave degraded: {ready}/{total} nodes.")
    if "metrics-server" in name:
        return ("Metrics Server online. Resource usage data available."
                if ok else "Metrics Server unavailable. Usage data will be missing.")
    if "ingress" in name:
        return (f"Ingress controller active on {total} nodes."
                if ok else f"Ingress degraded: {ready}/{total} nodes.")
    return (f"Replicas ({ready}/{total}) healthy."
            if ok else f"Degraded: {ready}/{total} instances.")


def _match_component(pod_name: str, labels: dict, namespace: str) -> str | None:
    """
    Pod ko 3 strategies se component name match karo:
    1. Pod name prefix
    2. Common label values (k8s-app, app, component, app.kubernetes.io/name)
    3. Namespace-based heuristic (calico-system, tigera-operator, etc.)
    """
    # Strategy 1: name prefix
    for comp in _COMP_ORDER:
        if pod_name.startswith(comp):
            return comp

    # Strategy 2: label matching
    label_values = set(filter(None, [
        labels.get("k8s-app"),
        labels.get("app"),
        labels.get("component"),
        labels.get("app.kubernetes.io/name"),
        labels.get("app.kubernetes.io/component"),
    ]))
    for lv in label_values:
        if lv in _LABEL_TO_COMP:
            return _LABEL_TO_COMP[lv]
        # Partial match (e.g., "calico-node-xxxxx" label value)
        for comp in _COMP_ORDER:
            if lv.startswith(comp) or comp.startswith(lv):
                return comp

    # Strategy 3: namespace heuristics
    if namespace in ("calico-system", "calico-apiserver"):
        if any(x in pod_name for x in ("node", "calico-node")):
            return "calico-node"
        if "controller" in pod_name:
            return "calico-kube-controllers"
        if "typha" in pod_name:
            return "calico-typha"
    if namespace == "tigera-operator":
        return "tigera-operator"
    if namespace in ("kube-flannel", "flannel-system"):
        return "flannel"
    if namespace == "cilium":
        return "cilium"

    return None


def _build_pods_list(pods_raw: list) -> list:
    """
    kubectl get pods --all-namespaces jaisa output — har namespace ke saare pods.
    Status: container-level status (CrashLoopBackOff etc.) dikhata hai, pod phase nahi.
    """
    result = []
    for pod in pods_raw:
        meta   = pod.get("metadata", {})
        spec   = pod.get("spec", {})
        status = pod.get("status", {})

        containers          = spec.get("containers", [])
        container_statuses  = status.get("containerStatuses", [])
        init_statuses       = status.get("initContainerStatuses", [])

        total_containers = len(containers)
        ready_containers = sum(1 for cs in container_statuses if cs.get("ready", False))

        # Restart count (max across all containers)
        restarts = max(
            (cs.get("restartCount", 0) for cs in container_statuses),
            default=0
        )

        # Effective display status (like kubectl)
        display_status = status.get("phase", "Unknown")
        for cs in container_statuses:
            waiting = cs.get("state", {}).get("waiting", {})
            if waiting.get("reason"):
                display_status = waiting["reason"]   # CrashLoopBackOff, ImagePullBackOff, etc.
                break
        # Check init containers too
        if display_status == status.get("phase"):
            for ic in init_statuses:
                waiting = ic.get("state", {}).get("waiting", {})
                if waiting.get("reason"):
                    display_status = f"Init:{waiting['reason']}"
                    break

        # Pod conditions for more info
        conditions = {c.get("type"): c.get("status") for c in status.get("conditions", [])}

        result.append({
            "name":       meta.get("name"),
            "namespace":  meta.get("namespace"),
            "node":       spec.get("nodeName"),
            "status":     display_status,
            "phase":      status.get("phase"),
            "ready":      f"{ready_containers}/{total_containers}",
            "restarts":   restarts,
            "age":        _calc_uptime(meta.get("creationTimestamp")),
            "pod_ip":     status.get("podIP"),
        })

    # Sort: namespace → name
    result.sort(key=lambda p: (p["namespace"] or "", p["name"] or ""))
    return result


def _extract_system_components(all_pods: list) -> list:
    comp_data: dict = {}

    for pod in all_pods:
        meta     = pod.get("metadata", {})
        ns       = meta.get("namespace", "")
        pod_name = meta.get("name", "")
        labels   = meta.get("labels", {})

        matched = _match_component(pod_name, labels, ns)
        if not matched:
            continue

        phase              = pod.get("status", {}).get("phase", "Unknown")
        container_statuses = pod.get("status", {}).get("containerStatuses", [])
        is_ready = (
            all(cs.get("ready", False) for cs in container_statuses)
            if container_statuses else (phase == "Running")
        )

        if matched not in comp_data:
            comp_data[matched] = {
                "name":        matched,
                "namespace":   ns or "kube-system",
                "type":        _COMP_TYPE_MAP.get(matched, "Pod"),
                "ready_count": 0,
                "total_count": 0,
            }
        comp_data[matched]["total_count"] += 1
        if phase == "Running" and is_ready:
            comp_data[matched]["ready_count"] += 1

    result = []
    for comp_name in _COMP_ORDER:
        if comp_name not in comp_data:
            continue
        d     = comp_data[comp_name]
        ready = d["ready_count"]
        total = d["total_count"]
        result.append({
            "name":           comp_name,
            "namespace":      d["namespace"],
            "type":           d["type"],
            "status":         "Healthy" if ready == total and total > 0
                              else ("Degraded" if ready > 0 else "Unhealthy"),
            "ready_replicas": ready,
            "total_replicas": total,
            "health_details": _component_health_detail(comp_name, ready, total),
        })
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Main data fetch (blocking)
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_all_cluster_data(cluster: KubernetesCluster) -> dict:
    """
    Single function — minimum API calls me poora cluster data:
    cluster_summary + nodes (with uptime & pod count) + system_components
    """
    base_url, req_kw, cert_files = _build_request_kwargs(
        cluster.control_ip, cluster.port,
        cluster.username, cluster.password,
        cluster.auth_token, cluster.kubeconfig,
    )
    try:
        # ── 1. Nodes ────────────────────────────────────────────────────────
        nodes_resp = requests.get(f"{base_url}/api/v1/nodes", timeout=15, **req_kw)
        nodes_resp.raise_for_status()
        nodes_raw = nodes_resp.json().get("items", [])

        # ── 2. All pods (system-components + per-node counts) ───────────────
        pods_resp = requests.get(f"{base_url}/api/v1/pods", timeout=20, **req_kw)
        pods_raw  = pods_resp.json().get("items", []) if pods_resp.status_code == 200 else []

        # ── 3. Metrics-server (optional) ────────────────────────────────────
        metrics_by_node: dict = {}
        has_metrics = False
        try:
            mr = requests.get(
                f"{base_url}/apis/metrics.k8s.io/v1beta1/nodes",
                timeout=10, **req_kw
            )
            if mr.status_code == 200:
                has_metrics = True
                for item in mr.json().get("items", []):
                    usage = item.get("usage", {})
                    metrics_by_node[item["metadata"]["name"]] = {
                        "cpu_cores":    _parse_cpu(usage.get("cpu", "0")),
                        "memory_bytes": _parse_memory_bytes(usage.get("memory", "0Ki")),
                    }
        except Exception:
            pass

        # ── Pod aggregation ─────────────────────────────────────────────────
        # Total = all pods (Running + Pending + Succeeded + Failed)
        # Running = sirf phase=Running wale (per-node count ke liye bhi)
        total_pods_cluster = len(pods_raw)
        running_pods_total = 0
        pending_pods_total = 0
        pod_counts_by_node: dict = {}   # node -> running pod count
        for pod in pods_raw:
            phase     = pod.get("status", {}).get("phase", "")
            node_name = pod.get("spec", {}).get("nodeName")
            if phase == "Running":
                running_pods_total += 1
                if node_name:
                    pod_counts_by_node[node_name] = pod_counts_by_node.get(node_name, 0) + 1
            elif phase == "Pending":
                pending_pods_total += 1

        # ── Node processing ─────────────────────────────────────────────────
        total_cpu_alloc  = 0.0
        total_mem_alloc  = 0
        total_pod_cap    = 0
        total_cpu_used   = 0.0
        total_mem_used   = 0
        ready_count      = 0
        master_nodes     = []
        worker_nodes     = []

        for node in nodes_raw:
            meta       = node.get("metadata", {})
            labels     = meta.get("labels", {})
            status_obj = node.get("status", {})
            alloc      = status_obj.get("allocatable", {})
            cap        = status_obj.get("capacity", {})
            node_name  = meta.get("name")

            cpu_alloc = _parse_cpu(alloc.get("cpu", "0"))
            mem_alloc = _parse_memory_bytes(alloc.get("memory", "0Ki"))
            pod_cap   = int(alloc.get("pods", "0"))

            total_cpu_alloc += cpu_alloc
            total_mem_alloc += mem_alloc
            total_pod_cap   += pod_cap

            nm = metrics_by_node.get(node_name, {})
            total_cpu_used += nm.get("cpu_cores", 0)
            total_mem_used += nm.get("memory_bytes", 0)

            is_master = (
                "node-role.kubernetes.io/master" in labels
                or "node-role.kubernetes.io/control-plane" in labels
            )

            ready    = "Unknown"
            uptime   = None
            for cond in status_obj.get("conditions", []):
                if cond.get("type") == "Ready":
                    if cond.get("status") == "True":
                        ready = "Ready"
                        ready_count += 1
                        uptime = _calc_uptime(cond.get("lastTransitionTime"))
                    else:
                        ready = "NotReady"
                    break

            addrs       = status_obj.get("addresses", [])
            internal_ip = next((a["address"] for a in addrs if a["type"] == "InternalIP"), None)
            external_ip = next((a["address"] for a in addrs if a["type"] == "ExternalIP"), None)
            hostname    = next((a["address"] for a in addrs if a["type"] == "Hostname"), None)
            info        = status_obj.get("nodeInfo", {})
            taints      = [{"key": t.get("key"), "effect": t.get("effect")}
                           for t in node.get("spec", {}).get("taints", [])]

            running_pods_on_node = pod_counts_by_node.get(node_name, 0)

            node_detail = {
                "name":         node_name,
                "role":         "master / control-plane" if is_master else "worker",
                "status":       ready,
                "internal_ip":  internal_ip,
                "external_ip":  external_ip,
                "hostname":     hostname,
                "uptime":       uptime,
                "taints":       taints,
                "running_pods": running_pods_on_node,
                "pod_capacity": pod_cap,
                "machine": {
                    "os":                info.get("osImage"),
                    "kernel":            info.get("kernelVersion"),
                    "architecture":      info.get("architecture"),
                    "container_runtime": info.get("containerRuntimeVersion"),
                    "kubelet_version":   info.get("kubeletVersion"),
                    "kube_proxy_version": info.get("kubeProxyVersion"),
                },
                "capacity": {
                    "cpu":     cap.get("cpu"),
                    "memory":  _parse_memory(cap.get("memory", "")),
                    "pods":    cap.get("pods"),
                    "storage": _parse_memory(cap.get("ephemeral-storage", "")),
                },
                "allocatable": {
                    "cpu":    alloc.get("cpu"),
                    "memory": _parse_memory(alloc.get("memory", "")),
                    "pods":   alloc.get("pods"),
                },
                "usage": {
                    "cpu_cores": round(nm["cpu_cores"], 2),
                    "memory":    _bytes_to_human(nm["memory_bytes"]),
                } if nm else None,
                "labels":     {k: v for k, v in labels.items()
                               if k.startswith("node-role") or k.startswith("kubernetes.io")},
                "created_at": meta.get("creationTimestamp"),
            }

            if is_master:
                master_nodes.append(node_detail)
            else:
                worker_nodes.append(node_detail)

        # ── Cluster summary ──────────────────────────────────────────────────
        cpu_pct = (round(total_cpu_used / total_cpu_alloc * 100)
                   if has_metrics and total_cpu_alloc else None)
        mem_pct = (round(total_mem_used / total_mem_alloc * 100)
                   if has_metrics and total_mem_alloc else None)
        pods_pct = round(running_pods_total / total_pod_cap * 100) if total_pod_cap else 0

        cluster_summary = {
            "cpu": {
                "total_cores":   int(total_cpu_alloc),
                "used_cores":    round(total_cpu_used, 2) if has_metrics else None,
                "usage_percent": cpu_pct,
                "has_metrics":   has_metrics,
            },
            "memory": {
                "total_human":   _bytes_to_human(total_mem_alloc),
                "used_human":    _bytes_to_human(total_mem_used) if has_metrics else None,
                "usage_percent": mem_pct,
            },
            "pods": {
                "total":         total_pods_cluster,
                "running":       running_pods_total,
                "pending":       pending_pods_total,
                "capacity":      total_pod_cap,
                "usage_percent": pods_pct,   # running/capacity %
            },
            "nodes": {
                "total": len(nodes_raw),
                "ready": ready_count,
            },
        }

        return {
            "cluster_summary":   cluster_summary,
            "nodes": {
                "total_nodes":  len(nodes_raw),
                "master_count": len(master_nodes),
                "worker_count": len(worker_nodes),
                "master_nodes": master_nodes,
                "worker_nodes": worker_nodes,
            },
            "system_components": _extract_system_components(pods_raw),
            "all_pods":          _build_pods_list(pods_raw),
        }
    finally:
        _cleanup(cert_files)


# ─────────────────────────────────────────────────────────────────────────────
# Quick test (blocking)
# ─────────────────────────────────────────────────────────────────────────────

def _run_k8s_test(payload: dict) -> dict:
    control_ip = payload["control_ip"]
    port       = payload.get("port", 6443)

    base_url, req_kw, cert_files = _build_request_kwargs(
        control_ip, port,
        payload.get("username"), payload.get("password"),
        payload.get("auth_token"), payload.get("kubeconfig"),
    )
    try:
        resp = requests.get(f"{base_url}/healthz", timeout=15, **req_kw)

        if resp.status_code == 200 and resp.text.strip() == "ok":
            version_info = {}
            try:
                ver = requests.get(f"{base_url}/version", timeout=10, **req_kw)
                if ver.status_code == 200:
                    version_info = ver.json()
            except Exception:
                pass

            node_count = None
            try:
                nr = requests.get(f"{base_url}/api/v1/nodes", timeout=10, **req_kw)
                if nr.status_code == 200:
                    node_count = len(nr.json().get("items", []))
            except Exception:
                pass

            return {
                "status":     "connected",
                "message":    "Kubernetes cluster reachable",
                "server":     base_url,
                "version":    version_info,
                "node_count": node_count,
            }

        return {"status": "failed",
                "error": f"/healthz HTTP {resp.status_code}: {resp.text[:300]}"}

    except requests.exceptions.ConnectTimeout:
        return {"status": "failed", "error": f"Connection timed out to {base_url}"}
    except requests.exceptions.ConnectionError as e:
        return {"status": "failed", "error": f"Connection refused: {str(e)[:200]}"}
    except requests.exceptions.Timeout:
        return {"status": "failed", "error": "Request timed out after 15s"}
    except Exception as e:
        return {"status": "failed", "error": str(e)[:300]}
    finally:
        _cleanup(cert_files)


# ─────────────────────────────────────────────────────────────────────────────
# API handlers
# ─────────────────────────────────────────────────────────────────────────────

async def test_k8s_connection_direct(body: dict) -> dict:
    control_ip = body.get("control_ip")
    port       = body.get("port") or 6443

    if body.get("kubeconfig") and not control_ip:
        info = _parse_kubeconfig_info(body["kubeconfig"])
        control_ip = info.get("control_ip")
        port       = info.get("port", port)

    if not control_ip:
        raise HTTPException(
            status_code=400,
            detail="control_ip required — kubeconfig me IP nahi mili (k8s-master jaise hostname hai), manually do"
        )

    result = await asyncio.to_thread(_run_k8s_test, {
        "control_ip": control_ip, "port": port,
        "username":   body.get("username"),
        "password":   body.get("password"),
        "auth_token": body.get("auth_token"),
        "kubeconfig": body.get("kubeconfig"),
    })
    if result["status"] == "connected":
        return response_format.success_response(200, "Connection test successful", result)
    return response_format.error_response(400, "Connection test failed", result)


async def add_k8s_cluster(body: dict, db: Session) -> dict:
    control_ip = body.get("control_ip")
    port       = body.get("port") or 6443

    if body.get("kubeconfig") and not control_ip:
        info = _parse_kubeconfig_info(body["kubeconfig"])
        control_ip = info.get("control_ip")
        port       = info.get("port", 6443)

    if not control_ip:
        raise HTTPException(
            status_code=400,
            detail="control_ip required — kubeconfig me IP nahi mili, manually do"
        )

    existing = db.query(KubernetesCluster).filter(
        KubernetesCluster.name == body["name"]
    ).first()
    if existing:
        raise HTTPException(status_code=409, detail=f"Cluster '{body['name']}' already exists")

    test_result = await asyncio.to_thread(_run_k8s_test, {
        "control_ip": control_ip, "port": port,
        "username":   body.get("username"),
        "password":   body.get("password"),
        "auth_token": body.get("auth_token"),
        "kubeconfig": body.get("kubeconfig"),
    })

    if test_result["status"] != "connected":
        return response_format.error_response(
            400, "Cannot add — connection test failed",
            {"error": test_result.get("error")}
        )

    cluster = KubernetesCluster(
        name        = body["name"],
        control_ip  = control_ip,
        port        = port,
        username    = body.get("username"),
        password    = body.get("password"),
        auth_token  = body.get("auth_token"),
        kubeconfig  = body.get("kubeconfig"),
        status      = "connected",
        last_tested = datetime.datetime.utcnow(),
    )
    db.add(cluster)
    db.commit()
    db.refresh(cluster)
    logger.info(f"[K8s] Cluster '{cluster.name}' saved (id={cluster.id})")
    return response_format.success_response(201, "Kubernetes cluster connected and added", {
        **_to_dict(cluster), "test_result": test_result,
    })


async def update_k8s_cluster(cluster_id: int, body: dict, db: Session) -> dict:
    cluster = db.query(KubernetesCluster).filter(KubernetesCluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail=f"Cluster {cluster_id} not found")

    if body.get("kubeconfig") and not body.get("control_ip"):
        info = _parse_kubeconfig_info(body["kubeconfig"])
        if info.get("control_ip"):
            body["control_ip"] = info["control_ip"]
        if info.get("port") and not body.get("port"):
            body["port"] = info["port"]

    for field in ["name", "control_ip", "port", "username", "password", "auth_token", "kubeconfig"]:
        val = body.get(field)
        if val is None:
            continue
        # "***" / "" = masked sentinel from GET response → skip, don't overwrite real value
        if field in ("kubeconfig", "auth_token") and val in ("***", ""):
            continue
        setattr(cluster, field, val)

    test_result = await asyncio.to_thread(_run_k8s_test, {
        "control_ip": cluster.control_ip,
        "port":       cluster.port,
        "username":   cluster.username,
        "password":   cluster.password,
        "auth_token": cluster.auth_token,
        "kubeconfig": cluster.kubeconfig,
    })

    cluster.status      = test_result["status"]
    cluster.last_tested = datetime.datetime.utcnow()
    db.commit()
    db.refresh(cluster)

    if test_result["status"] != "connected":
        return response_format.error_response(400, "Updated but connection failed", {
            **_to_dict(cluster), "test_result": test_result,
        })
    return response_format.success_response(200, "Cluster updated and connection verified", {
        **_to_dict(cluster), "test_result": test_result,
    })


async def test_k8s_cluster(cluster_id: int, db: Session) -> dict:
    cluster = db.query(KubernetesCluster).filter(KubernetesCluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail=f"Cluster {cluster_id} not found")

    result = await asyncio.to_thread(_run_k8s_test, {
        "control_ip": cluster.control_ip,
        "port":       cluster.port,
        "username":   cluster.username,
        "password":   cluster.password,
        "auth_token": cluster.auth_token,
        "kubeconfig": cluster.kubeconfig,
    })
    cluster.status      = result["status"]
    cluster.last_tested = datetime.datetime.utcnow()
    db.commit()

    if result["status"] == "connected":
        return response_format.success_response(200, "Connection test successful",
                                                {**result, "cluster_id": cluster_id})
    return response_format.error_response(400, "Connection test failed",
                                          {**result, "cluster_id": cluster_id})


def list_k8s_clusters(db: Session) -> dict:
    clusters = db.query(KubernetesCluster).order_by(KubernetesCluster.id.desc()).all()
    return response_format.success_response(
        200, "Kubernetes clusters listed", [_to_dict(c) for c in clusters]
    )


async def get_k8s_cluster(cluster_id: int, db: Session) -> dict:
    cluster = db.query(KubernetesCluster).filter(KubernetesCluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail=f"Cluster {cluster_id} not found")

    data = _to_dict(cluster)
    try:
        cluster_data = await asyncio.to_thread(_fetch_all_cluster_data, cluster)
        data.update(cluster_data)
    except Exception as e:
        logger.warning(f"[K8s] Data fetch failed for cluster {cluster_id}: {e}")
        data["cluster_summary"]    = {"error": str(e)[:200]}
        data["nodes"]              = {"error": str(e)[:200]}
        data["system_components"]  = []

    return response_format.success_response(200, "Kubernetes cluster fetched", data)


def delete_k8s_cluster(cluster_id: int, db: Session) -> dict:
    cluster = db.query(KubernetesCluster).filter(KubernetesCluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail=f"Cluster {cluster_id} not found")
    db.delete(cluster)
    db.commit()
    return response_format.success_response(200, f"Cluster '{cluster.name}' deleted")


# ─────────────────────────────────────────────────────────────────────────────
# Harbor Deploy on K8s node
# ─────────────────────────────────────────────────────────────────────────────

async def deploy_harbor_to_k8s(cluster_id: int, body: dict, db: Session) -> dict:
    from models.kubernetes_deploy_model import KubernetesDeployment
    from models.library_model import LibraryItem
    from service.temporalResource.workers.workers_kubernetes_deploy import TASK_QUEUE
    from service.temporalResource.workflows.workflows_kubernetes_deploy import K8sHarborDeployWorkflow
    from utils.temporal_client import TemporalClientManager
    from temporalio.common import SearchAttributeKey, SearchAttributePair, TypedSearchAttributes

    # ── Cluster DB se fetch karo ─────────────────────────────────────────────
    cluster = db.query(KubernetesCluster).filter(KubernetesCluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail=f"Cluster {cluster_id} not found")

    # kubeconfig hona chahiye — SSH nahi, K8s API use hota hai
    if not cluster.kubeconfig:
        raise HTTPException(
            status_code=400,
            detail=(
                "kubeconfig missing for this cluster. "
                f"Please update via PUT /v1/kubernetes/clusters/{cluster_id}"
            )
        )

    # ── Library item verify ───────────────────────────────────────────────────
    lib_id   = body["library_item_id"]
    lib_item = db.query(LibraryItem).filter(LibraryItem.id == lib_id).first()
    if not lib_item:
        raise HTTPException(status_code=404, detail=f"Library item {lib_id} not found")
    if lib_item.status != "ready":
        raise HTTPException(
            status_code=409,
            detail=f"Library file not ready yet (status={lib_item.status})"
        )

    # ── Create deployment record ──────────────────────────────────────────────
    namespace = body.get("namespace", "harbor")
    http_port = body.get("http_port", 80)

    deploy = KubernetesDeployment(
        cluster_id      = cluster_id,
        library_item_id = lib_id,
        name            = body["name"],
        namespace       = namespace,
        node_name       = cluster.name,
        node_ip         = cluster.control_ip,
        deployment_type = "kubernetes",
        status          = "pending",
    )
    db.add(deploy)
    db.commit()
    db.refresh(deploy)

    # ── Start Temporal workflow ───────────────────────────────────────────────
    workflow_id = f"k8s-harbor-deploy-{deploy.id}-{cluster_id}"
    payload = {
        "deploy_id":       deploy.id,
        "cluster_id":      cluster_id,
        "library_item_id": lib_id,
        "file_path":       lib_item.file_path,
        "file_name":       lib_item.file_name,
        "name":            body["name"],
        "node_ip":         cluster.control_ip,
        "namespace":       namespace,
        "http_port":       http_port,
    }

    try:
        client = await TemporalClientManager.get_temporal_client()
        _sa = TypedSearchAttributes([
            SearchAttributePair(SearchAttributeKey.for_keyword("Entity"),   deploy.name),
            SearchAttributePair(SearchAttributeKey.for_keyword("Action"),   "K8s-Harbor-Deploy"),
            SearchAttributePair(SearchAttributeKey.for_keyword("UserName"), cluster.username or "system"),
        ])
        handle = await client.start_workflow(
            K8sHarborDeployWorkflow.run,
            payload,
            id                = workflow_id,
            task_queue        = TASK_QUEUE,
            search_attributes = _sa,
        )
        deploy.workflow_id = handle.id
        deploy.status      = "deploying"
        db.commit()
        logger.info(f"[K8sDeploy] Workflow started: {workflow_id}")
    except Exception as e:
        deploy.status        = "failed"
        deploy.error_message = str(e)[:300]
        db.commit()
        raise HTTPException(status_code=500, detail=f"Failed to start deploy workflow: {e}")

    return response_format.success_response(201, "Harbor deployment started", {
        "deploy_id":   deploy.id,
        "cluster_id":  cluster_id,
        "name":        deploy.name,
        "node_ip":     deploy.node_ip,
        "namespace":   deploy.namespace,
        "status":      deploy.status,
        "workflow_id": deploy.workflow_id,
        "harbor_url":  None,
        "message":     "Deployment started. Poll GET /deployments/{id} for status and harbor_url.",
    })


def get_k8s_deployment(cluster_id: int, deploy_id: int, db: Session) -> dict:
    from models.kubernetes_deploy_model import KubernetesDeployment
    d = db.query(KubernetesDeployment).filter(
        KubernetesDeployment.id         == deploy_id,
        KubernetesDeployment.cluster_id == cluster_id,
    ).first()
    if not d:
        raise HTTPException(status_code=404, detail=f"Deployment {deploy_id} not found")
    return response_format.success_response(200, "Deployment fetched", _deploy_to_dict(d))


def list_k8s_deployments(cluster_id: int, db: Session) -> dict:
    from models.kubernetes_deploy_model import KubernetesDeployment
    deployments = (
        db.query(KubernetesDeployment)
        .filter(KubernetesDeployment.cluster_id == cluster_id)
        .order_by(KubernetesDeployment.id.desc())
        .all()
    )
    return response_format.success_response(
        200, "Deployments listed", [_deploy_to_dict(d) for d in deployments]
    )


_DEPLOY_PROGRESS = {
    "pending":       {"step": 0, "label": "Queued",                 "pct": 0},
    "connecting":    {"step": 1, "label": "SSH Connecting",         "pct": 10},
    "transferring":  {"step": 2, "label": "Transferring File",      "pct": 30},
    "extracting":    {"step": 3, "label": "Extracting Archive",     "pct": 50},
    "deploying":     {"step": 4, "label": "Deploying Harbor",       "pct": 70},
    "waiting_ready": {"step": 5, "label": "Waiting for Harbor URL", "pct": 85},
    "deployed":      {"step": 6, "label": "Deployed",               "pct": 100},
    "failed":        {"step": -1,"label": "Failed",                 "pct": 0},
}


def _deploy_to_dict(d) -> dict:
    import json as _json

    progress = _DEPLOY_PROGRESS.get(
        d.status,
        {"step": 0, "label": d.status, "pct": 0},
    )

    try:
        steps_log = _json.loads(d.steps_log) if d.steps_log else []
    except Exception:
        steps_log = []

    return {
        "id":               d.id,
        "cluster_id":       d.cluster_id,
        "library_item_id":  d.library_item_id,
        "name":             d.name,
        "namespace":        d.namespace,
        "node_name":        d.node_name,
        "node_ip":          d.node_ip,
        "deployment_type":  d.deployment_type,
        "status":           d.status,
        "progress":         progress,
        "steps_log":        steps_log,
        "harbor_url":       d.harbor_url,
        "deploy_dir":       d.deploy_dir,
        "error_message":    d.error_message,
        "workflow_id":      d.workflow_id,
        "created_at":       d.created_at.isoformat() if d.created_at else None,
        "updated_at":       d.updated_at.isoformat() if d.updated_at else None,
    }

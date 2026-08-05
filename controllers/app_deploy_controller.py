import logging
import uuid
from urllib.parse import urlparse

from fastapi import HTTPException
from sqlalchemy.orm import Session

from models.app_deploy_model import AppDeployment, VALID_DEPLOY_TYPES
from models.kubernetes_model import KubernetesCluster
from models.kubernetes_deploy_model import KubernetesDeployment
from models.library_model import LibraryItem
from utils import response_format

logger = logging.getLogger(__name__)

# library.type → deployment_type map
_LIB_TYPE_MAP = {
    "open_web_ui": "openwebui",
    "vectordb":    "vectordb",
    "container":   "openwebui",   # generic container bhi accept
}
# acceptable library.types for each deployment_type (set)
_EXPECTED_LIB_TYPE = {
    "openwebui": {"open_web_ui", "container"},
    "vectordb":  {"vectordb",    "container"},
}


# ─────────────────────────────────────────────────────────────────────────────
# Progress map
# ─────────────────────────────────────────────────────────────────────────────

_PROGRESS = {
    "pending":            {"step": 0,  "label": "Queued",               "pct": 0},
    "connecting":         {"step": 1,  "label": "SSH Connecting",        "pct": 10},
    "deploying":          {"step": 2,  "label": "Applying Manifests",    "pct": 40},
    "waiting_ready":      {"step": 3,  "label": "Waiting for Pods",      "pct": 70},
    "deployed":           {"step": 4,  "label": "Deployed",              "pct": 100},
    "failed":             {"step": -1, "label": "Failed",                "pct": 0},
    "connecting_llm":     {"step": 5,  "label": "Connecting LLM",        "pct": 95},
    "disconnecting_llm":  {"step": 6,  "label": "Disconnecting LLM",     "pct": 95},
    "llm_connect_failed": {"step": -2, "label": "LLM Connection Failed", "pct": 100},
}


def _to_dict(d: AppDeployment, db: Session = None) -> dict:
    import json as _json
    progress = _PROGRESS.get(d.status, {"step": 0, "label": d.status, "pct": 0})
    try:
        steps_log = _json.loads(d.steps_log) if d.steps_log else []
    except Exception:
        steps_log = []

    # Parse linked_llm_ids JSON array
    try:
        llm_id_list = _json.loads(d.linked_llm_ids) if d.linked_llm_ids else []
        if not isinstance(llm_id_list, list):
            llm_id_list = []
    except Exception:
        llm_id_list = []

    # Fetch each connected LLM's details
    linked_llms = []
    if llm_id_list and db:
        from models.llm_inference_v2_model import LLMInferenceJob
        for llm_id in llm_id_list:
            try:
                llm = db.query(LLMInferenceJob).filter(LLMInferenceJob.id == llm_id).first()
                if llm:
                    linked_llms.append({
                        "id":           llm.id,
                        "name":         llm.name,
                        "machine_name": llm.machine_name,
                        "endpoint_url": llm.endpoint_url,
                        "head_ip":      llm.head_ip,
                    })
                else:
                    linked_llms.append({"id": llm_id})
            except Exception:
                linked_llms.append({"id": llm_id})

    return {
        "id":                 d.id,
        "name":               d.name,
        "deployment_type":    d.deployment_type,
        "k8s_cluster_id":     d.k8s_cluster_id,
        "harbor_registry_id": d.harbor_registry_id,
        "library_item_id":    d.library_item_id,
        "namespace":          d.namespace,
        "harbor_url":         d.harbor_url,
        "image":              d.image,
        "external_ip":        d.external_ip,
        "node_port":          d.node_port,
        "service_url":        d.service_url,
        "status":             d.status,
        "progress":           progress,
        "steps_log":          steps_log,
        "error_message":      d.error_message,
        "workflow_id":        d.workflow_id,
        "linked_vectordb_id": d.linked_vectordb_id,
        "linked_llm_ids":     llm_id_list,
        "linked_llms":        linked_llms,
        "created_at":         d.created_at.isoformat() if d.created_at else None,
        "updated_at":         d.updated_at.isoformat() if d.updated_at else None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Create
# ─────────────────────────────────────────────────────────────────────────────

async def create_app_deployment(body: dict, db: Session) -> dict:
    from service.temporalResource.workers.workers_app_deploy import TASK_QUEUE
    from service.temporalResource.workflows.workflows_app_deploy import AppDeployWorkflow
    from utils.temporal_client import TemporalClientManager
    from temporalio.common import SearchAttributeKey, SearchAttributePair, TypedSearchAttributes

    name              = body["name"]
    deployment_type   = body["deployment_type"]
    k8s_cluster_id    = body["k8s_cluster_id"]
    harbor_registry_id = body.get("harbor_registry_id")
    version_id        = body["version_id"]
    namespace         = body.get("namespace", "default")

    # ── Validate deployment_type ─────────────────────────────────────────────
    if deployment_type not in VALID_DEPLOY_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"deployment_type '{deployment_type}' invalid. Allowed: {sorted(VALID_DEPLOY_TYPES)}"
        )

    # ── K8s cluster validate ─────────────────────────────────────────────────
    cluster = db.query(KubernetesCluster).filter(KubernetesCluster.id == k8s_cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail=f"K8s cluster id={k8s_cluster_id} not found")
    if not cluster.kubeconfig:
        raise HTTPException(
            status_code=400,
            detail=(
                f"K8s cluster id={k8s_cluster_id} has no kubeconfig. "
                f"Set it using PUT /v1/kubernetes/clusters/{k8s_cluster_id}."
            )
        )

    # ── Library item validate ─────────────────────────────────────────────────
    lib = db.query(LibraryItem).filter(LibraryItem.id == version_id).first()
    if not lib:
        raise HTTPException(status_code=404, detail=f"Library item id={version_id} not found")

    accepted_lib_types = _EXPECTED_LIB_TYPE[deployment_type]
    if lib.type not in accepted_lib_types:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Library item id={version_id} has type '{lib.type}', "
                f"but deployment_type='{deployment_type}' requires one of {sorted(accepted_lib_types)}."
            )
        )
    if not lib.harbor_image:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Library item id={version_id} has no harbor_image set — "
                "upload it first and allow it to be pushed to Harbor (push_status=pushed)."
            )
        )

    image = lib.harbor_image

    # ── Harbor registry validate + get harbor_url ────────────────────────────
    harbor_url  = lib.harbor_url or None
    harbor_user = lib.harbor_user or "admin"
    harbor_pass = lib.harbor_pass or "Harbor12345"

    if harbor_registry_id:
        harbor_dep = db.query(KubernetesDeployment).filter(
            KubernetesDeployment.id == harbor_registry_id
        ).first()
        if not harbor_dep:
            raise HTTPException(
                status_code=404,
                detail=f"Harbor registry id={harbor_registry_id} was not found in kubernetes_deployments"
            )
        if not harbor_dep.harbor_url:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Harbor registry id={harbor_registry_id} has no harbor_url set yet — "
                    "the Harbor deployment may not be complete."
                )
            )
        harbor_url = harbor_dep.harbor_url   # registry record se override

    # ── Harbor host for imagePullSecret ──────────────────────────────────────
    # image format: "172.16.4.41:30080/library/open-webui/open-webui:0.6.5"
    harbor_host = image.split("/")[0]

    # ── Save deployment record ────────────────────────────────────────────────
    deploy = AppDeployment(
        name               = name,
        deployment_type    = deployment_type,
        k8s_cluster_id     = k8s_cluster_id,
        harbor_registry_id = harbor_registry_id,
        library_item_id    = version_id,
        namespace          = namespace,
        harbor_url         = harbor_url,
        image              = image,
        status             = "pending",
    )
    db.add(deploy)
    db.commit()
    db.refresh(deploy)

    # ── Temporal workflow start ───────────────────────────────────────────────
    workflow_id = f"app-deploy-{deploy.id}-{uuid.uuid4().hex[:8]}"
    payload = {
        "deploy_id":        deploy.id,
        "deployment_type":  deployment_type,
        "k8s_cluster_id":   k8s_cluster_id,
        "library_item_id":  version_id,
        "namespace":        namespace,
        "name":             name,
        "harbor_host":      harbor_host,
        "harbor_user":      harbor_user,
        "harbor_pass":      harbor_pass,
        "image":            image,
    }

    try:
        client = await TemporalClientManager.get_temporal_client()
        _sa = TypedSearchAttributes([
            SearchAttributePair(SearchAttributeKey.for_keyword("Entity"),   name),
            SearchAttributePair(SearchAttributeKey.for_keyword("Action"),   f"AppDeploy-{deployment_type}"),
            SearchAttributePair(SearchAttributeKey.for_keyword("UserName"), cluster.username or "system"),
        ])
        handle = await client.start_workflow(
            AppDeployWorkflow.run,
            payload,
            id                = workflow_id,
            task_queue        = TASK_QUEUE,
            search_attributes = _sa,
        )
        deploy.workflow_id = handle.id
        deploy.status      = "connecting"
        db.commit()
        logger.info(f"[AppDeploy] Workflow started: {workflow_id} type={deployment_type}")
    except Exception as e:
        deploy.status        = "failed"
        deploy.error_message = str(e)[:300]
        db.commit()
        raise HTTPException(status_code=500, detail=f"Workflow failed to start: {e}")

    return response_format.success_response(201, f"{deployment_type} deployment started successfully", {
        **_to_dict(deploy),
        "message": f"Deployment started. Use GET /v1/app-deploy/{deploy.id} to check the status.",
    })


# ─────────────────────────────────────────────────────────────────────────────
# List
# ─────────────────────────────────────────────────────────────────────────────

def list_app_deployments(
    db: Session,
    page: int = 1,
    page_size: int = 10,
    deployment_type: str | None = None,
) -> dict:
    try:
        page      = max(1, page)
        page_size = max(1, min(page_size, 100))
        offset    = (page - 1) * page_size

        q = db.query(AppDeployment)
        if deployment_type:
            q = q.filter(AppDeployment.deployment_type == deployment_type)

        total = q.count()
        items = q.order_by(AppDeployment.id.desc()).offset(offset).limit(page_size).all()
        total_pages = (total + page_size - 1) // page_size if page_size else 1

        return response_format.success_response(200, "App deployments retrieved successfully", {
            "items": [_to_dict(d, db=db) for d in items],
            "pagination": {
                "page":        page,
                "page_size":   page_size,
                "total":       total,
                "total_pages": total_pages,
                "has_next":    page < total_pages,
                "has_prev":    page > 1,
            },
        })
    except Exception as e:
        logger.error(f"list_app_deployments error: {e}", exc_info=True)
        return response_format.error_response(500, "Failed to list app deployments", str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Get single
# ─────────────────────────────────────────────────────────────────────────────

def get_app_deployment(deploy_id: int, db: Session) -> dict:
    d = db.query(AppDeployment).filter(AppDeployment.id == deploy_id).first()
    if not d:
        raise HTTPException(status_code=404, detail=f"App deployment id={deploy_id} not found")
    return response_format.success_response(200, "App deployment details retrieved successfully", _to_dict(d, db=db))


# ─────────────────────────────────────────────────────────────────────────────
# Delete
# ─────────────────────────────────────────────────────────────────────────────

def delete_app_deployment(deploy_id: int, db: Session) -> dict:
    import re
    import shlex
    import paramiko

    d = db.query(AppDeployment).filter(AppDeployment.id == deploy_id).first()
    if not d:
        raise HTTPException(status_code=404, detail=f"App deployment id={deploy_id} not found")

    # ── K8s resources delete karo ────────────────────────────────────────────
    k8s_cleaned = False
    k8s_warning = None

    if d.k8s_cluster_id:
        cluster = db.query(KubernetesCluster).filter(
            KubernetesCluster.id == d.k8s_cluster_id
        ).first()

        if cluster and cluster.control_ip and cluster.username and cluster.password:
            try:
                # svc_name reconstruct karo (same logic as activity)
                rname    = re.sub(r"[^a-z0-9-]", "-", d.name.lower())
                rname    = re.sub(r"-+", "-", rname).strip("-")[:52]
                svc_name = f"{rname}-{d.deployment_type}"
                ns_q     = shlex.quote(d.namespace or "default")

                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(
                    hostname=cluster.control_ip,
                    port=22,
                    username=cluster.username,
                    password=cluster.password,
                    timeout=30,
                )

                def _run(cmd: str, timeout: int = 60):
                    _, out, err = ssh.exec_command(cmd, timeout=timeout)
                    out.channel.recv_exit_status()
                    return out.read().decode(errors="replace").strip()

                _run(
                    f"kubectl delete deployment {shlex.quote(svc_name)} "
                    f"-n {ns_q} --ignore-not-found=true 2>&1"
                )
                _run(
                    f"kubectl delete service {shlex.quote(svc_name)} "
                    f"-n {ns_q} --ignore-not-found=true 2>&1"
                )
                ssh.close()

                k8s_cleaned = True
                logger.info(f"[AppDeploy] K8s resources deleted: {svc_name} ns={d.namespace}")

            except Exception as e:
                k8s_warning = str(e)[:300]
                logger.warning(f"[AppDeploy] K8s cleanup failed id={deploy_id}: {e}")

    # ── DB se delete karo ────────────────────────────────────────────────────
    db.delete(d)
    db.commit()
    logger.info(f"[AppDeploy] id={deploy_id} deleted from DB")

    result = {"id": deploy_id, "k8s_cleaned": k8s_cleaned}
    if k8s_warning:
        result["k8s_warning"] = f"DB deleted but K8s cleanup failed: {k8s_warning}"

    return response_format.success_response(200, "App deployment deleted successfully", result)


# ─────────────────────────────────────────────────────────────────────────────
# Connect VectorDB to OpenWebUI
# ─────────────────────────────────────────────────────────────────────────────

def connect_vectordb(openwebui_id: int, vectordb_deploy_id: int, db: Session) -> dict:
    import re
    import shlex
    import paramiko

    # ── Validate OpenWebUI deployment ────────────────────────────────────────
    ow = db.query(AppDeployment).filter(AppDeployment.id == openwebui_id).first()
    if not ow:
        raise HTTPException(status_code=404, detail=f"OpenWebUI deployment id={openwebui_id} not found")
    if ow.deployment_type != "openwebui":
        raise HTTPException(status_code=400, detail=f"id={openwebui_id} is not an OpenWebUI deployment")
    if ow.status != "deployed":
        raise HTTPException(status_code=409, detail=f"OpenWebUI deployment is currently in '{ow.status}' state — connect after it is deployed")

    # ── Validate VectorDB deployment ─────────────────────────────────────────
    vdb = db.query(AppDeployment).filter(AppDeployment.id == vectordb_deploy_id).first()
    if not vdb:
        raise HTTPException(status_code=404, detail=f"VectorDB deployment id={vectordb_deploy_id} not found")
    if vdb.deployment_type != "vectordb":
        raise HTTPException(status_code=400, detail=f"id={vectordb_deploy_id} is not a VectorDB deployment")
    if vdb.status != "deployed":
        raise HTTPException(status_code=409, detail=f"VectorDB deployment is currently in '{vdb.status}' state")
    if not vdb.external_ip:
        raise HTTPException(status_code=409, detail="VectorDB external_ip is not set — deploy it first")

    # ── K8s env inject ───────────────────────────────────────────────────────
    cluster = db.query(KubernetesCluster).filter(KubernetesCluster.id == ow.k8s_cluster_id).first()
    if not cluster or not cluster.control_ip or not cluster.username or not cluster.password:
        raise HTTPException(status_code=409, detail="OpenWebUI cluster SSH credentials are missing")

    rname    = re.sub(r"[^a-z0-9-]", "-", ow.name.lower())
    rname    = re.sub(r"-+", "-", rname).strip("-")[:52]
    dep_name = f"{rname}-openwebui"
    ns_q     = shlex.quote(ow.namespace or "default")
    pgurl    = f"postgresql://postgres:postgres123@{vdb.external_ip}:5432/vectordb"

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            hostname=cluster.control_ip,
            port=22,
            username=cluster.username,
            password=cluster.password,
            timeout=30,
        )

        def _run(cmd: str, timeout: int = 180):
            _, out, err = ssh.exec_command(cmd, timeout=timeout)
            out.channel.recv_exit_status()
            return out.read().decode(errors="replace").strip()

        # Inject env vars into openwebui deployment
        env_cmd = (
            f"kubectl set env deployment/{shlex.quote(dep_name)} "
            f"VECTOR_DB=pgvector "
            f"PGVECTOR_DB_URL={shlex.quote(pgurl)} "
            f"-n {ns_q} 2>&1"
        )
        set_out = _run(env_cmd)
        logger.info(f"[AppDeploy] kubectl set env: {set_out}")

        # Wait for rollout to complete
        rollout_out = _run(
            f"kubectl rollout status deployment/{shlex.quote(dep_name)} "
            f"-n {ns_q} --timeout=180s 2>&1",
            timeout=200,
        )
        logger.info(f"[AppDeploy] rollout status: {rollout_out}")
        ssh.close()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"K8s env inject failed: {str(e)[:300]}")

    # ── DB save ──────────────────────────────────────────────────────────────
    ow.linked_vectordb_id = vectordb_deploy_id
    db.commit()
    db.refresh(ow)

    logger.info(f"[AppDeploy] OpenWebUI id={openwebui_id} linked to VectorDB id={vectordb_deploy_id}")
    return response_format.success_response(200, "VectorDB connected to OpenWebUI successfully", {
        "openwebui_id":      openwebui_id,
        "vectordb_id":       vectordb_deploy_id,
        "pgvector_url":      pgurl,
        "rollout_output":    rollout_out,
    })


# ─────────────────────────────────────────────────────────────────────────────
# Disconnect VectorDB from OpenWebUI
# ─────────────────────────────────────────────────────────────────────────────

def disconnect_vectordb(openwebui_id: int, db: Session) -> dict:
    import re
    import shlex
    import paramiko

    ow = db.query(AppDeployment).filter(AppDeployment.id == openwebui_id).first()
    if not ow:
        raise HTTPException(status_code=404, detail=f"OpenWebUI deployment id={openwebui_id} not found")
    if ow.deployment_type != "openwebui":
        raise HTTPException(status_code=400, detail=f"id={openwebui_id} is not an OpenWebUI deployment")

    cluster = db.query(KubernetesCluster).filter(KubernetesCluster.id == ow.k8s_cluster_id).first()
    if not cluster or not cluster.control_ip:
        raise HTTPException(status_code=409, detail="Cluster credentials are missing")

    rname    = re.sub(r"[^a-z0-9-]", "-", ow.name.lower())
    rname    = re.sub(r"-+", "-", rname).strip("-")[:52]
    dep_name = f"{rname}-openwebui"
    ns_q     = shlex.quote(ow.namespace or "default")

    k8s_warning = None
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            hostname=cluster.control_ip,
            port=22,
            username=cluster.username,
            password=cluster.password,
            timeout=30,
        )

        def _run(cmd: str, timeout: int = 180):
            _, out, err = ssh.exec_command(cmd, timeout=timeout)
            out.channel.recv_exit_status()
            return out.read().decode(errors="replace").strip()

        # Remove env vars (suffix - removes the var)
        _run(
            f"kubectl set env deployment/{shlex.quote(dep_name)} "
            f"VECTOR_DB- PGVECTOR_DB_URL- "
            f"-n {ns_q} 2>&1"
        )
        _run(
            f"kubectl rollout status deployment/{shlex.quote(dep_name)} "
            f"-n {ns_q} --timeout=120s 2>&1",
            timeout=140,
        )
        ssh.close()

    except Exception as e:
        k8s_warning = str(e)[:300]
        logger.warning(f"[AppDeploy] disconnect K8s step failed id={openwebui_id}: {e}")

    ow.linked_vectordb_id = None
    db.commit()

    result = {"openwebui_id": openwebui_id, "linked_vectordb_id": None}
    if k8s_warning:
        result["k8s_warning"] = f"DB unlinked but K8s env remove failed: {k8s_warning}"
    return response_format.success_response(200, "VectorDB disconnected successfully", result)


# ─────────────────────────────────────────────────────────────────────────────
# Connect Private LLM to OpenWebUI
# ─────────────────────────────────────────────────────────────────────────────

def _load_k8s_apps_client(cluster: KubernetesCluster):
    """kubeconfig DB se load karo, AppsV1Api return karo. No SSH."""
    import re, yaml, tempfile, os
    from kubernetes import client as kc, config as kcfg

    if not cluster.kubeconfig:
        raise HTTPException(status_code=409, detail="K8s cluster kubeconfig not found in DB")

    kc_dict = yaml.safe_load(cluster.kubeconfig)
    if cluster.control_ip:
        for ce in kc_dict.get("clusters", []):
            srv = ce.get("cluster", {}).get("server", "")
            if srv:
                ce["cluster"]["server"] = re.sub(
                    r"https://[^:/]+", f"https://{cluster.control_ip}", srv
                )

    kc_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(kc_dict, f)
            kc_path = f.name
        kcfg.load_kube_config(config_file=kc_path)
    finally:
        if kc_path:
            try:
                os.unlink(kc_path)
            except OSError:
                pass

    return kc.AppsV1Api()


_DAAS_OW_API_KEY = "daas-openwebui-api-key"


def _ow_sync_connections(service_url: str, desired_urls: list[str]) -> str | None:
    """
    OpenWebUI connections sync via admin config API.
    GET  /api/v1/openai/config         → read current URLs
    POST /api/v1/openai/config/update  → write new URL list
    Returns None on success, error string on failure.
    """
    import httpx

    svc     = service_url.rstrip("/")
    headers = {"Authorization": f"Bearer {_DAAS_OW_API_KEY}",
               "Content-Type": "application/json"}

    try:
        # ── Step 1: Read current config ───────────────────────────────────────
        r = httpx.get(f"{svc}/api/v1/openai/config", headers=headers, timeout=10)
        logger.info(f"[OWSync] GET /api/v1/openai/config → {r.status_code} body={r.text[:400]}")

        if r.status_code in (401, 403):
            return f"Auth failed ({r.status_code}) — WEBUI_API_KEY not accepted by pod"
        if r.status_code != 200:
            return f"GET /api/v1/openai/config → {r.status_code}: {r.text[:200]}"
        if not r.text.strip():
            return "GET /api/v1/openai/config → empty body (pod still starting?)"

        try:
            cfg = r.json()
        except Exception:
            return f"GET /api/v1/openai/config → non-JSON: {r.text[:200]}"

        # ── Step 2: Write desired URLs ────────────────────────────────────────
        payload = {
            "OPENAI_API_BASE_URLS": desired_urls,
            "OPENAI_API_KEYS":      ["none"] * len(desired_urls),
        }
        # Preserve any per-connection config keys the pod already has
        if isinstance(cfg, dict) and "OPENAI_API_CONFIGS" in cfg:
            payload["OPENAI_API_CONFIGS"] = cfg["OPENAI_API_CONFIGS"]

        wr = httpx.post(f"{svc}/api/v1/openai/config/update",
                        headers=headers, json=payload, timeout=10)
        logger.info(f"[OWSync] POST /api/v1/openai/config/update → {wr.status_code} body={wr.text[:300]}")

        if wr.status_code < 400:
            return None
        return f"config/update → {wr.status_code}: {wr.text[:200]}"

    except Exception as exc:
        return str(exc)[:300]


def _auto_inject_api_key(ow: AppDeployment, db: Session) -> str | None:
    """
    WEBUI_API_KEY ko K8s deployment mein auto-inject karo if missing.
    Returns None on success, error string on failure.
    Triggers ONE rolling update (first-time only); future calls use API directly.
    """
    import re
    from kubernetes.client.models import V1EnvVar

    cluster = db.query(KubernetesCluster).filter(KubernetesCluster.id == ow.k8s_cluster_id).first()
    if not cluster or not cluster.kubeconfig:
        return "K8s cluster kubeconfig missing — add kubeconfig to this cluster record"

    rname    = re.sub(r"[^a-z0-9-]", "-", ow.name.lower())
    rname    = re.sub(r"-+", "-", rname).strip("-")[:52]
    dep_name = f"{rname}-openwebui"
    namespace = ow.namespace or "default"

    try:
        apps_v1    = _load_k8s_apps_client(cluster)
        deployment = apps_v1.read_namespaced_deployment(name=dep_name, namespace=namespace)
        for container in (deployment.spec.template.spec.containers or []):
            if container.name == "openwebui":
                existing_keys = {e.name for e in (container.env or [])}
                if "WEBUI_API_KEY" not in existing_keys:
                    container.env = list(container.env or []) + [
                        V1EnvVar(name="WEBUI_API_KEY", value=_DAAS_OW_API_KEY)
                    ]
                    apps_v1.patch_namespaced_deployment(
                        name=dep_name, namespace=namespace, body=deployment
                    )
                    logger.info(f"[AppDeploy] WEBUI_API_KEY auto-injected into {dep_name} (one-time rolling update)")
                else:
                    logger.info(f"[AppDeploy] WEBUI_API_KEY already set in {dep_name} — pod may still be starting")
                return None
        return f"container 'openwebui' not found in deployment '{dep_name}'"
    except Exception as e:
        return str(e)[:300]


async def connect_private_llm(openwebui_id: int, llm_ids: list[int], db: Session) -> dict:
    """
    Primary: OpenWebUI REST API (instant, no restart).
    Fallback: Temporal workflow — injects WEBUI_API_KEY + OPENAI_API_BASE_URLS via K8s,
              waits for pod rollout, model appears via env vars. After this one-time
              rolling update all future calls use the API directly (no restart).
    """
    import json as _json
    import re
    import uuid
    from models.llm_inference_v2_model import LLMInferenceJob

    if not llm_ids:
        raise HTTPException(status_code=400, detail="llm_ids list cannot be empty")

    ow = db.query(AppDeployment).filter(AppDeployment.id == openwebui_id).first()
    if not ow:
        raise HTTPException(status_code=404, detail=f"OpenWebUI deployment id={openwebui_id} not found")
    if ow.deployment_type != "openwebui":
        raise HTTPException(status_code=400, detail=f"id={openwebui_id} is not an OpenWebUI deployment")
    if ow.status not in ("deployed", "failed", "llm_connect_failed"):
        raise HTTPException(status_code=409, detail=f"OpenWebUI is in '{ow.status}' state — deploy it first")

    def _llm_url(llm) -> str:
        if llm.endpoint_url:
            url = llm.endpoint_url.rstrip("/")
        elif llm.head_ip:
            url = f"http://{llm.head_ip}:8000"
        else:
            raise HTTPException(status_code=409,
                                detail=f"LLM id={llm.id} has no endpoint_url or head_ip")
        return url if url.endswith("/v1") else f"{url}/v1"

    try:
        current_ids = _json.loads(ow.linked_llm_ids) if ow.linked_llm_ids else []
        if not isinstance(current_ids, list):
            current_ids = []
    except Exception:
        current_ids = []

    already = [i for i in llm_ids if i in current_ids]
    if already:
        raise HTTPException(status_code=409,
                            detail=f"LLM id(s) {already} are already connected")

    for lid in llm_ids:
        if not db.query(LLMInferenceJob).filter(LLMInferenceJob.id == lid).first():
            raise HTTPException(status_code=404, detail=f"Private LLM id={lid} not found")

    new_ids   = current_ids + llm_ids
    all_urls: list[str] = []
    for lid in new_ids:
        llm = db.query(LLMInferenceJob).filter(LLMInferenceJob.id == lid).first()
        if llm:
            all_urls.append(_llm_url(llm))

    # ── Path 1: OpenWebUI REST API (instant, preferred) ──────────────────────
    if ow.service_url:
        err = _ow_sync_connections(ow.service_url, all_urls)
        if not err:
            logger.info(f"[AppDeploy] LLM connected via OpenWebUI API: id={openwebui_id} new_ids={new_ids}")
            ow.linked_llm_ids = _json.dumps(new_ids)
            ow.status         = "deployed"
            ow.error_message  = None
            db.commit()
            return response_format.success_response(200, "Private LLM connected to OpenWebUI", {
                "openwebui_id": openwebui_id,
                "llm_ids":      llm_ids,
                "all_ids":      new_ids,
                "urls":         all_urls,
            })
        logger.warning(f"[AppDeploy] OpenWebUI API failed id={openwebui_id}: {err} — falling back to Temporal workflow")
    else:
        err = "service_url not set on this deployment"
        logger.warning(f"[AppDeploy] id={openwebui_id}: {err} — falling back to Temporal workflow")

    # ── Path 2: Temporal workflow fallback (K8s env vars + WEBUI_API_KEY) ────
    # Used when WEBUI_API_KEY is missing in pod or service_url unreachable.
    # Workflow injects OPENAI_API_BASE_URLS + WEBUI_API_KEY, waits for rollout.
    # After pod restart OpenWebUI reads env vars → model appears automatically.
    from service.temporalResource.workers.workers_connect_llm import TASK_QUEUE
    from service.temporalResource.workflows.workflows_connect_llm import ConnectLLMWorkflow
    from utils.temporal_client import TemporalClientManager

    cluster = db.query(KubernetesCluster).filter(KubernetesCluster.id == ow.k8s_cluster_id).first()
    if not cluster or not cluster.kubeconfig:
        raise HTTPException(status_code=409, detail=(
            f"OpenWebUI API unreachable ({err[:100]}) and K8s kubeconfig missing — cannot connect"
        ))

    rname    = re.sub(r"[^a-z0-9-]", "-", ow.name.lower())
    rname    = re.sub(r"-+", "-", rname).strip("-")[:52]
    dep_name = f"{rname}-openwebui"

    wf_payload = {
        "deploy_id":      openwebui_id,
        "llm_ids":        llm_ids,
        "new_ids":        new_ids,
        "base_urls":      all_urls,
        "k8s_cluster_id": ow.k8s_cluster_id,
        "dep_name":       dep_name,
        "namespace":      ow.namespace or "default",
        "current_ids":    current_ids,
    }

    workflow_id = f"connect-llm-{openwebui_id}-{uuid.uuid4().hex[:8]}"
    try:
        client = await TemporalClientManager.get_temporal_client()
        handle = await client.start_workflow(
            ConnectLLMWorkflow.run,
            wf_payload,
            id         = workflow_id,
            task_queue = TASK_QUEUE,
        )
        ow.linked_llm_ids = _json.dumps(new_ids)
        ow.status         = "connecting_llm"
        ow.workflow_id    = handle.id
        ow.error_message  = None
        db.commit()

        import json as _json2, time as _time
        try:
            existing_log = _json2.loads(ow.steps_log) if ow.steps_log else []
        except Exception:
            existing_log = []
        existing_log.append({
            "step": f"API fallback reason: {err}",
            "ts":   _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime()),
        })
        ow.steps_log = _json2.dumps(existing_log)
        db.commit()

        logger.info(f"[AppDeploy] ConnectLLM workflow started: {workflow_id}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start connect-llm workflow: {e}")

    return response_format.success_response(200, "Private LLM connect started — pod rolling update in progress", {
        "openwebui_id":       openwebui_id,
        "llm_ids":            llm_ids,
        "all_ids":            new_ids,
        "urls":               all_urls,
        "workflow_id":        handle.id,
        "api_failure_reason": err,
        "note": (
            "OpenWebUI API unreachable — one-time rolling update started. "
            "Model will appear after pod restarts (~60s). "
            "Future calls will be instant without restart."
        ),
    })


# ─────────────────────────────────────────────────────────────────────────────
# Disconnect Private LLM from OpenWebUI
# ─────────────────────────────────────────────────────────────────────────────

async def disconnect_private_llm(openwebui_id: int, llm_id: int | None, db: Session) -> dict:
    import re, json as _json, uuid
    from models.llm_inference_v2_model import LLMInferenceJob

    ow = db.query(AppDeployment).filter(AppDeployment.id == openwebui_id).first()
    if not ow:
        raise HTTPException(status_code=404, detail=f"OpenWebUI deployment id={openwebui_id} not found")

    if ow.status in ("connecting_llm", "disconnecting_llm"):
        raise HTTPException(
            status_code=409,
            detail=f"A workflow is already running (status='{ow.status}'). Wait for it to complete before disconnecting."
        )

    # Parse current ID list
    try:
        current_ids = _json.loads(ow.linked_llm_ids) if ow.linked_llm_ids else []
        if not isinstance(current_ids, list):
            current_ids = []
    except Exception:
        current_ids = []

    if llm_id is not None:
        if llm_id not in current_ids:
            raise HTTPException(status_code=404,
                                detail=f"LLM id={llm_id} is not connected to this OpenWebUI")
        remaining_ids = [i for i in current_ids if i != llm_id]
    else:
        remaining_ids = []   # disconnect all

    # Collect remaining URLs
    remaining_urls: list[str] = []
    for lid in remaining_ids:
        llm = db.query(LLMInferenceJob).filter(LLMInferenceJob.id == lid).first()
        if llm:
            url = llm.endpoint_url or (f"http://{llm.head_ip}:8000" if llm.head_ip else None)
            if url:
                if not url.endswith("/v1"):
                    url = f"{url}/v1"
                remaining_urls.append(url)

    # ── Path 1: OpenWebUI REST API (instant, preferred) ──────────────────────
    if ow.service_url:
        err = _ow_sync_connections(ow.service_url, remaining_urls)
        if not err:
            logger.info(f"[AppDeploy] LLM disconnected via OpenWebUI API: id={openwebui_id} remaining={remaining_ids}")
            ow.linked_llm_ids = _json.dumps(remaining_ids)
            ow.status         = "deployed"
            ow.error_message  = None
            db.commit()
            return response_format.success_response(200, "Private LLM disconnected from OpenWebUI successfully", {
                "openwebui_id":    openwebui_id,
                "disconnected_id": llm_id,
                "linked_llm_ids":  remaining_ids,
            })
        logger.warning(f"[AppDeploy] OpenWebUI disconnect API failed id={openwebui_id}: {err} — falling back to Temporal workflow")
    else:
        err = "service_url not set on this deployment"
        logger.warning(f"[AppDeploy] id={openwebui_id}: {err} — falling back to Temporal workflow")

    # ── Path 2: Temporal workflow fallback (K8s env vars update) ─────────────
    from service.temporalResource.workers.workers_connect_llm import TASK_QUEUE
    from service.temporalResource.workflows.workflows_connect_llm import DisconnectLLMWorkflow
    from utils.temporal_client import TemporalClientManager

    cluster = db.query(KubernetesCluster).filter(KubernetesCluster.id == ow.k8s_cluster_id).first()
    if not cluster or not cluster.kubeconfig:
        raise HTTPException(status_code=409, detail=(
            f"OpenWebUI API unreachable ({err[:100]}) and K8s kubeconfig missing — cannot disconnect"
        ))

    rname    = re.sub(r"[^a-z0-9-]", "-", ow.name.lower())
    rname    = re.sub(r"-+", "-", rname).strip("-")[:52]
    dep_name = f"{rname}-openwebui"

    wf_payload = {
        "deploy_id":       openwebui_id,
        "remaining_ids":   remaining_ids,
        "remaining_urls":  remaining_urls,
        "disconnected_id": llm_id,
        "k8s_cluster_id":  ow.k8s_cluster_id,
        "dep_name":        dep_name,
        "namespace":       ow.namespace or "default",
    }

    workflow_id = f"disconnect-llm-{openwebui_id}-{uuid.uuid4().hex[:8]}"
    try:
        client = await TemporalClientManager.get_temporal_client()
        handle = await client.start_workflow(
            DisconnectLLMWorkflow.run,
            wf_payload,
            id         = workflow_id,
            task_queue = TASK_QUEUE,
        )
        ow.linked_llm_ids = _json.dumps(remaining_ids)
        ow.status         = "disconnecting_llm"
        ow.workflow_id    = handle.id
        ow.error_message  = None
        db.commit()

        # API failure reason steps_log mein save karo
        import json as _json2, time as _time
        try:
            existing_log = _json2.loads(ow.steps_log) if ow.steps_log else []
        except Exception:
            existing_log = []
        existing_log.append({
            "step": f"API fallback reason: {err}",
            "ts":   _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime()),
        })
        ow.steps_log = _json2.dumps(existing_log)
        db.commit()

        logger.info(f"[AppDeploy] DisconnectLLM workflow started: {workflow_id}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start disconnect-llm workflow: {e}")

    return response_format.success_response(200, "Private LLM disconnect started — pod rolling update in progress", {
        "openwebui_id":    openwebui_id,
        "disconnected_id": llm_id,
        "linked_llm_ids":  remaining_ids,
        "workflow_id":     handle.id,
        "api_failure_reason": err,
        "note": (
            "OpenWebUI API unreachable — K8s env vars update started (rolling update ~60s). "
            "Model will disappear after pod restarts."
        ),
    })

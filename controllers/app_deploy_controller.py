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
    "pending":       {"step": 0, "label": "Queued",            "pct": 0},
    "connecting":    {"step": 1, "label": "SSH Connecting",     "pct": 10},
    "deploying":     {"step": 2, "label": "Applying Manifests", "pct": 40},
    "waiting_ready": {"step": 3, "label": "Waiting for Pods",   "pct": 70},
    "deployed":      {"step": 4, "label": "Deployed",           "pct": 100},
    "failed":        {"step": -1,"label": "Failed",            "pct": 0},
}


def _to_dict(d: AppDeployment) -> dict:
    import json as _json
    progress = _PROGRESS.get(d.status, {"step": 0, "label": d.status, "pct": 0})
    try:
        steps_log = _json.loads(d.steps_log) if d.steps_log else []
    except Exception:
        steps_log = []
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
            "items": [_to_dict(d) for d in items],
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
    return response_format.success_response(200, "App deployment details retrieved successfully", _to_dict(d))


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

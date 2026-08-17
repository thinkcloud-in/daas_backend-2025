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
    "openwebui":  {"open_web_ui", "container"},
    "vectordb":   {"vectordb",    "container"},
    "postgresql": {"postgresql",  "container"},
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
        from models.llm_inference_model import LLMInferenceJob
        for llm_id in llm_id_list:
            try:
                llm = db.query(LLMInferenceJob).filter(LLMInferenceJob.id == llm_id).first()
                if llm:
                    linked_llms.append({
                        "id":           llm.id,
                        "name":         llm.name,
                        "endpoint_url": llm.endpoint_url,
                        "head_ip":      llm.head_ip,
                        "status":       llm.status,
                    })
                else:
                    linked_llms.append({"id": llm_id})
            except Exception as _le:
                logger.warning(f"[AppDeploy] linked_llm id={llm_id} fetch error: {_le}")
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
        "postgresql_deploy_id": d.postgresql_deploy_id,
        "linked_vectordb_id": d.linked_vectordb_id,
        "linked_llm_ids":     llm_id_list,
        "linked_llms":        linked_llms,
        "keycloak_config":    _parse_keycloak_config(d.keycloak_config),
        "admin_email":        d.admin_email,
        "admin_password":     d.admin_password,
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

    name                 = body["name"]
    deployment_type      = body["deployment_type"]
    k8s_cluster_id       = body["k8s_cluster_id"]
    harbor_registry_id   = body.get("harbor_registry_id")
    version_id           = body["version_id"]
    namespace            = body.get("namespace", "default")
    postgresql_deploy_id = body.get("postgresql_deploy_id")
    admin_email          = (body.get("admin_email") or "admin@admin.com").strip()
    admin_password       = (body.get("admin_password") or "").strip() or None

    # ── Validate deployment_type ─────────────────────────────────────────────
    if deployment_type not in VALID_DEPLOY_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"deployment_type '{deployment_type}' invalid. Allowed: {sorted(VALID_DEPLOY_TYPES)}"
        )

    # OpenWebUI ke liye postgresql_deploy_id mandatory hai
    if deployment_type == "openwebui":
        if not postgresql_deploy_id:
            raise HTTPException(
                status_code=400,
                detail="openwebui deployment ke liye 'postgresql_deploy_id' mandatory hai — pehle postgresql deploy karo"
            )
        _pg = db.query(AppDeployment).filter(AppDeployment.id == postgresql_deploy_id).first()
        if not _pg:
            raise HTTPException(status_code=404, detail=f"PostgreSQL deployment id={postgresql_deploy_id} not found")
        if _pg.deployment_type != "postgresql":
            raise HTTPException(status_code=400, detail=f"id={postgresql_deploy_id} is not a postgresql deployment")
        if _pg.status != "deployed":
            raise HTTPException(status_code=409, detail=f"PostgreSQL deployment id={postgresql_deploy_id} is '{_pg.status}' — deployed hona chahiye")
        if not _pg.service_url:
            raise HTTPException(status_code=409, detail=f"PostgreSQL id={postgresql_deploy_id} service_url not set")

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
        name                 = name,
        deployment_type      = deployment_type,
        k8s_cluster_id       = k8s_cluster_id,
        harbor_registry_id   = harbor_registry_id,
        library_item_id      = version_id,
        namespace            = namespace,
        harbor_url           = harbor_url,
        image                = image,
        postgresql_deploy_id = postgresql_deploy_id,
        status               = "pending",
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
        "storage_class":        body.get("storage_class"),
        "postgresql_deploy_id": postgresql_deploy_id,
        "admin_email":          admin_email,
        "admin_password":       admin_password,
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

                pvc_name = f"{svc_name}-data"
                _run(
                    f"kubectl delete deployment {shlex.quote(svc_name)} "
                    f"-n {ns_q} --ignore-not-found=true 2>&1"
                )
                _run(
                    f"kubectl delete service {shlex.quote(svc_name)} "
                    f"-n {ns_q} --ignore-not-found=true 2>&1"
                )
                _run(
                    f"kubectl delete pvc {shlex.quote(pvc_name)} "
                    f"-n {ns_q} --ignore-not-found=true 2>&1"
                )

                # Namespace delete karo agar bilkul khaali ho (default kabhi delete nahi hoga)
                ns_actual = d.namespace or "default"
                if ns_actual != "default":
                    _pods_raw = _run(
                        f"kubectl get pods -n {ns_q} --no-headers 2>/dev/null"
                        f" | grep -v '^$' | grep -v 'No resources' | wc -l"
                    )
                    try:
                        _pod_count = int(_pods_raw.strip())
                    except ValueError:
                        _pod_count = 1  # safe default — delete mat karo
                    if _pod_count == 0:
                        _run(f"kubectl delete namespace {ns_q} --ignore-not-found=true 2>&1")
                        logger.info(f"[AppDeploy] Namespace {ns_actual} deleted (empty)")
                    else:
                        logger.info(f"[AppDeploy] Namespace {ns_actual} kept ({_pod_count} pod(s) remaining)")

                ssh.close()

                k8s_cleaned = True
                logger.info(f"[AppDeploy] K8s resources deleted: {svc_name}, PVC: {pvc_name}, ns={d.namespace}")

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
    from kubernetes.client.models import V1EnvVar

    ow = db.query(AppDeployment).filter(AppDeployment.id == openwebui_id).first()
    if not ow:
        raise HTTPException(status_code=404, detail=f"OpenWebUI deployment id={openwebui_id} not found")
    if ow.deployment_type != "openwebui":
        raise HTTPException(status_code=400, detail=f"id={openwebui_id} is not an OpenWebUI deployment")
    if ow.status not in ("deployed", "failed", "llm_connect_failed"):
        raise HTTPException(status_code=409, detail=f"OpenWebUI is in '{ow.status}' state — deploy it first")

    vdb = db.query(AppDeployment).filter(AppDeployment.id == vectordb_deploy_id).first()
    if not vdb:
        raise HTTPException(status_code=404, detail=f"VectorDB deployment id={vectordb_deploy_id} not found")
    if vdb.deployment_type != "vectordb":
        raise HTTPException(status_code=400, detail=f"id={vectordb_deploy_id} is not a VectorDB deployment")
    if vdb.status != "deployed":
        raise HTTPException(status_code=409, detail=f"VectorDB is in '{vdb.status}' state")
    if not vdb.external_ip:
        raise HTTPException(status_code=409, detail="VectorDB external_ip not set — deploy it first")

    cluster = db.query(KubernetesCluster).filter(KubernetesCluster.id == ow.k8s_cluster_id).first()
    if not cluster or not cluster.kubeconfig:
        raise HTTPException(status_code=409, detail="OpenWebUI cluster kubeconfig is missing")

    rname     = re.sub(r"[^a-z0-9-]", "-", ow.name.lower())
    rname     = re.sub(r"-+", "-", rname).strip("-")[:52]
    dep_name  = f"{rname}-openwebui"
    namespace = ow.namespace or "default"
    rname_vdb = re.sub(r"[^a-z0-9-]", "-", vdb.name.lower())
    rname_vdb = re.sub(r"-+", "-", rname_vdb).strip("-")[:52]
    vdb_ns    = vdb.namespace or "default"
    pgurl     = f"postgresql://postgres:postgres123@{rname_vdb}-vectordb.{vdb_ns}.svc.cluster.local:5432/vectordb"

    try:
        apps_v1    = _load_k8s_apps_client(cluster)
        deployment = apps_v1.read_namespaced_deployment(name=dep_name, namespace=namespace)

        for container in (deployment.spec.template.spec.containers or []):
            if container.name == "openwebui":
                clean_env = [e for e in (container.env or []) if e.name not in _VECTORDB_ENV_KEYS]
                clean_env.append(V1EnvVar(name="VECTOR_DB",       value="pgvector"))
                clean_env.append(V1EnvVar(name="PGVECTOR_DB_URL", value=pgurl))
                # DATABASE_URL restore karo agar purane bug se remove ho gaya tha
                if not any(e.name == "DATABASE_URL" for e in clean_env) and ow.postgresql_deploy_id:
                    _pg = db.query(AppDeployment).filter(AppDeployment.id == ow.postgresql_deploy_id).first()
                    if _pg:
                        _pg_rn   = re.sub(r"[^a-z0-9-]", "-", _pg.name.lower())
                        _pg_rn   = re.sub(r"-+", "-", _pg_rn).strip("-")[:52]
                        _pg_ns   = _pg.namespace or "default"
                        _ow_db   = f"openwebui_{ow.id}"
                        _dburl   = f"postgresql://postgres:postgres123@{_pg_rn}-postgresql.{_pg_ns}.svc.cluster.local:5432/{_ow_db}"
                        clean_env.append(V1EnvVar(name="DATABASE_URL", value=_dburl))
                        logger.info(f"[AppDeploy] DATABASE_URL auto-restored db={_ow_db} (postgresql_deploy_id={ow.postgresql_deploy_id})")
                clean_env = _inject_llm_env_vars(ow, clean_env, db)
                container.env = clean_env
                break

        apps_v1.replace_namespaced_deployment(name=dep_name, namespace=namespace, body=deployment)
        logger.info(f"[AppDeploy] VectorDB env vars set for {dep_name} (pgurl={pgurl})")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"K8s patch failed: {str(e)[:300]}")

    ow.linked_vectordb_id = vectordb_deploy_id
    db.commit()

    logger.info(f"[AppDeploy] OpenWebUI id={openwebui_id} linked to VectorDB id={vectordb_deploy_id}")
    return response_format.success_response(200, "VectorDB connected — pod rolling update in progress (~60s)", {
        "openwebui_id": openwebui_id,
        "vectordb_id":  vectordb_deploy_id,
        "pgvector_url": pgurl,
        "note":         "Only VECTOR_DB + PGVECTOR_DB_URL set. DATABASE_URL is managed via postgresql deployment.",
    })


# ─────────────────────────────────────────────────────────────────────────────
# Disconnect VectorDB from OpenWebUI
# ─────────────────────────────────────────────────────────────────────────────

def disconnect_vectordb(openwebui_id: int, db: Session) -> dict:
    import re
    from kubernetes.client.models import V1EnvVar

    ow = db.query(AppDeployment).filter(AppDeployment.id == openwebui_id).first()
    if not ow:
        raise HTTPException(status_code=404, detail=f"OpenWebUI deployment id={openwebui_id} not found")
    if ow.deployment_type != "openwebui":
        raise HTTPException(status_code=400, detail=f"id={openwebui_id} is not an OpenWebUI deployment")
    if not ow.linked_vectordb_id:
        raise HTTPException(status_code=404, detail="No VectorDB is connected to this OpenWebUI")

    cluster = db.query(KubernetesCluster).filter(KubernetesCluster.id == ow.k8s_cluster_id).first()
    if not cluster or not cluster.kubeconfig:
        raise HTTPException(status_code=409, detail="Cluster kubeconfig is missing")

    rname     = re.sub(r"[^a-z0-9-]", "-", ow.name.lower())
    rname     = re.sub(r"-+", "-", rname).strip("-")[:52]
    dep_name  = f"{rname}-openwebui"
    namespace = ow.namespace or "default"

    k8s_warning = None
    try:
        apps_v1    = _load_k8s_apps_client(cluster)
        deployment = apps_v1.read_namespaced_deployment(name=dep_name, namespace=namespace)

        for container in (deployment.spec.template.spec.containers or []):
            if container.name == "openwebui":
                clean_env = [e for e in (container.env or []) if e.name not in _VECTORDB_ENV_KEYS]
                # DATABASE_URL restore karo agar purane bug se remove ho gaya tha
                if not any(e.name == "DATABASE_URL" for e in clean_env) and ow.postgresql_deploy_id:
                    _pg = db.query(AppDeployment).filter(AppDeployment.id == ow.postgresql_deploy_id).first()
                    if _pg:
                        _pg_rn   = re.sub(r"[^a-z0-9-]", "-", _pg.name.lower())
                        _pg_rn   = re.sub(r"-+", "-", _pg_rn).strip("-")[:52]
                        _pg_ns   = _pg.namespace or "default"
                        _ow_db   = f"openwebui_{ow.id}"
                        _dburl   = f"postgresql://postgres:postgres123@{_pg_rn}-postgresql.{_pg_ns}.svc.cluster.local:5432/{_ow_db}"
                        clean_env.append(V1EnvVar(name="DATABASE_URL", value=_dburl))
                        logger.info(f"[AppDeploy] DATABASE_URL auto-restored on disconnect db={_ow_db}")
                # Pod restart hone par linked LLMs ko env vars me preserve karo
                clean_env = _inject_llm_env_vars(ow, clean_env, db)
                container.env = clean_env
                break

        apps_v1.replace_namespaced_deployment(name=dep_name, namespace=namespace, body=deployment)
        logger.info(f"[AppDeploy] VectorDB env vars removed for {dep_name}")
    except Exception as e:
        k8s_warning = str(e)[:300]
        logger.warning(f"[AppDeploy] disconnect VectorDB K8s step failed id={openwebui_id}: {e}")

    ow.linked_vectordb_id = None
    db.commit()

    result = {"openwebui_id": openwebui_id, "linked_vectordb_id": None}
    if k8s_warning:
        result["k8s_warning"] = f"DB unlinked but K8s env remove failed: {k8s_warning}"
    return response_format.success_response(200, "VectorDB disconnected — pod rolling update in progress (~60s)", result)


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


_DAAS_OW_API_KEY   = "daas-openwebui-api-key"
_VECTORDB_ENV_KEYS = {"VECTOR_DB", "PGVECTOR_DB_URL"}
_LLM_ENV_KEYS      = {"OPENAI_API_BASE_URL", "OPENAI_API_BASE_URLS",
                      "OPENAI_API_KEY", "OPENAI_API_KEYS", "WEBUI_API_KEY"}


def _ow_apply_keycloak_config(
    service_url: str, admin_email: str, admin_password: str,
    openid_url: str, client_id: str, client_secret: str,
    provider_name: str, oauth_scopes: str,
) -> str:
    """
    OpenWebUI DB mein Keycloak config + login form disable karo via admin API.
    K8s env vars ke saath-saath call karo taaki change turant dikhe (no pod restart needed).
    Returns "ok" on success, error string on failure.
    """
    import httpx as _hx

    svc = service_url.rstrip("/")
    try:
        # Admin JWT lo
        _sr = _hx.post(
            f"{svc}/api/v1/auths/signin",
            json={"email": admin_email, "password": admin_password},
            timeout=10,
        )
        if _sr.status_code != 200:
            return f"admin signin failed ({_sr.status_code})"
        _tok  = _sr.json().get("token", "")
        _hdrs = {"Authorization": f"Bearer {_tok}", "Content-Type": "application/json"}

        # Login form disable, OAuth signup OFF (sirf admin assign-roles se user banega)
        _ar = _hx.post(
            f"{svc}/api/v1/auths/config/update",
            headers=_hdrs,
            json={
                "enable_login_form":             False,
                "enable_signup":                 False,
                "enable_oauth_signup":           False,   # auto user creation band
                "oauth_provider_name":           provider_name,
                "openid_provider_url":           openid_url,
                "oauth_client_id":               client_id,
                "oauth_client_secret":           client_secret,
                "oauth_scopes":                  oauth_scopes,
                "oauth_merge_accounts_by_email": True,    # email se existing account link
            },
            timeout=10,
        )
        logger.info(f"[OWKeycloak] auths/config/update → {_ar.status_code} {_ar.text[:150]}")

        if _ar.status_code < 400:
            return "ok"
        return f"auths/config/update → {_ar.status_code}: {_ar.text[:100]}"

    except Exception as _e:
        return f"error: {str(_e)[:150]}"


def _ow_remove_keycloak_config(service_url: str, admin_email: str, admin_password: str) -> str:
    """
    OpenWebUI DB mein login form wapas enable karo (Keycloak disconnect ke baad).
    Returns "ok" on success, error string on failure.
    """
    import httpx as _hx

    svc = service_url.rstrip("/")
    try:
        _sr = _hx.post(
            f"{svc}/api/v1/auths/signin",
            json={"email": admin_email, "password": admin_password},
            timeout=10,
        )
        if _sr.status_code != 200:
            return f"admin signin failed ({_sr.status_code})"
        _tok  = _sr.json().get("token", "")
        _hdrs = {"Authorization": f"Bearer {_tok}", "Content-Type": "application/json"}

        _ar = _hx.post(
            f"{svc}/api/v1/auths/config/update",
            headers=_hdrs,
            json={
                "enable_login_form": True,
                "enable_signup":     False,
            },
            timeout=10,
        )
        logger.info(f"[OWKeycloak] disconnect config update → {_ar.status_code} {_ar.text[:150]}")
        return "ok" if _ar.status_code < 400 else f"{_ar.status_code}: {_ar.text[:100]}"
    except Exception as _e:
        return f"error: {str(_e)[:150]}"

def _ow_pg_set_config(ow_id: int, postgresql_deploy_id, patch: dict, db_session: "Session") -> str | None:
    """
    OpenWebUI PostgreSQL config table mein directly values patch karo.
    Returns None on success, error string on failure.
    Same approach as _try_pg_sync for LLM — proven to work immediately (no pod restart).
    """
    import json as _j, time as _t

    if not postgresql_deploy_id:
        return "postgresql_deploy_id not set on this deployment"

    try:
        import psycopg2
    except ImportError:
        return "psycopg2 not installed"

    pg = db_session.query(AppDeployment).filter(AppDeployment.id == postgresql_deploy_id).first()
    if not pg:
        return f"PostgreSQL deployment id={postgresql_deploy_id} not found"
    if not pg.external_ip or not pg.node_port:
        return "PostgreSQL has no external_ip/node_port"

    _dbname = f"openwebui_{ow_id}"
    try:
        conn = psycopg2.connect(
            host=pg.external_ip,
            port=int(pg.node_port),
            dbname=_dbname,
            user="postgres",
            password="postgres123",
            connect_timeout=10,
        )
        cur = conn.cursor()
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name='config'"
        )
        if not cur.fetchone():
            # Debug: list all tables in this DB so we know what actually exists
            cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
            )
            _tables = [r[0] for r in cur.fetchall()]
            conn.close()
            return f"'config' table not found in {_dbname}. Tables present: {_tables}"

        cur.execute("SELECT id, data FROM config ORDER BY id LIMIT 1")
        _row = cur.fetchone()
        if _row:
            try:
                _data = _j.loads(_row[1]) if _row[1] else {}
            except Exception:
                _data = {}
            _data.update(patch)
            cur.execute(
                "UPDATE config SET data = %s, updated_at = %s WHERE id = %s",
                (_j.dumps(_data), int(_t.time() * 1000), _row[0]),
            )
        else:
            _ts = int(_t.time() * 1000)
            cur.execute(
                "INSERT INTO config (data, version, created_at, updated_at) VALUES (%s, 1, %s, %s)",
                (_j.dumps(patch), _ts, _ts),
            )
        conn.commit()
        cur.close()
        conn.close()
        return None
    except Exception as e:
        return f"PG error: {str(e)[:200]}"


def _ow_pg_exec_set_config(ow: "AppDeployment", patch: dict, db: "Session") -> str | None:
    """
    PostgreSQL pod ke andar exec karke config table directly update karo.
    OW API ya network routing ki zaroorat nahi — K8s API se PG pod ke andar psql chalate hain.
    patch: flat dict e.g. {"enable_login_form": False, "enable_signup": False}
    Returns None on success, error string on failure.
    """
    import json as _j, yaml, tempfile, os as _os, re as _re
    from kubernetes import client as kc, config as kcfg
    from kubernetes.stream import stream as k8s_stream

    if not ow.postgresql_deploy_id:
        return "postgresql_deploy_id not set on this OpenWebUI deployment"

    pg = db.query(AppDeployment).filter(AppDeployment.id == ow.postgresql_deploy_id).first()
    if not pg:
        return f"PostgreSQL AppDeployment id={ow.postgresql_deploy_id} not found"

    cluster = db.query(KubernetesCluster).filter(KubernetesCluster.id == ow.k8s_cluster_id).first()
    if not cluster or not cluster.kubeconfig:
        return "K8s cluster kubeconfig not found"

    kc_dict = yaml.safe_load(cluster.kubeconfig)
    if cluster.control_ip:
        for ce in kc_dict.get("clusters", []):
            srv = ce.get("cluster", {}).get("server", "")
            if srv:
                ce["cluster"]["server"] = _re.sub(r"https://[^:/]+", f"https://{cluster.control_ip}", srv)
    kc_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(kc_dict, f)
            kc_path = f.name
        kcfg.load_kube_config(config_file=kc_path)
    finally:
        if kc_path:
            try: _os.unlink(kc_path)
            except OSError: pass

    try:
        core_v1 = kc.CoreV1Api()
        pg_ns   = pg.namespace or "postgresql"
        pods    = core_v1.list_namespaced_pod(pg_ns)
        pod     = next(
            (p for p in pods.items if (p.status.phase or "").lower() == "running"),
            None,
        )
        if pod is None:
            return f"No running pod in PostgreSQL namespace '{pg_ns}'"
        pod_name = pod.metadata.name
    except Exception as e:
        return f"PG pod list failed: {str(e)[:200]}"

    _dbname = f"openwebui_{ow.id}"
    _ts     = "EXTRACT(EPOCH FROM NOW())::bigint*1000"

    # OpenWebUI PersistentConfig stores keys with dot-prefix (e.g. "ui.enable_login_form")
    # AND sometimes bare keys — write both variants to cover all OW versions/builds
    _expanded: dict = {}
    for k, v in patch.items():
        _expanded[k] = v
        if "." not in k:
            _expanded[f"ui.{k}"] = v

    # config table schema: key (text PK) | value (json) | updated_at (bigint)
    # Each setting is a separate row — UPSERT per key
    _upserts = " ".join(
        f"INSERT INTO config (key, value, updated_at) "
        f"VALUES ($k{i}${k}$k{i}$, $v{i}${_j.dumps(v)}$v{i}$::json, {_ts}) "
        f"ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at;"
        for i, (k, v) in enumerate(_expanded.items())
    )

    # Diagnostic SELECT — log current config keys so we can verify key format
    _sel_sql = "SELECT key, value FROM config ORDER BY key;"

    # Try multiple psql locations — bitnami uses /opt/bitnami/postgresql/bin/psql
    _psql_bins = ["psql", "/opt/bitnami/postgresql/bin/psql"]
    for _psql in _psql_bins:
        try:
            # 1. Log current config table state
            _sel_out = k8s_stream(
                core_v1.connect_get_namespaced_pod_exec,
                pod_name, pg_ns,
                command=[_psql, "-U", "postgres", "-d", _dbname, "-c", _sel_sql],
                stderr=True, stdin=False, stdout=True, tty=False,
                _preload_content=True,
            )
            logger.info(f"[PGExecConfig] SELECT config keys db={_dbname}: {repr(_sel_out[:600])}")
            if "not found" in _sel_out.lower() or "no such file" in _sel_out.lower():
                continue  # try next psql path

            # 2. UPSERT
            _out = k8s_stream(
                core_v1.connect_get_namespaced_pod_exec,
                pod_name, pg_ns,
                command=[_psql, "-U", "postgres", "-d", _dbname, "-c", _upserts],
                stderr=True, stdin=False, stdout=True, tty=False,
                _preload_content=True,
            )
            logger.info(f"[PGExecConfig] UPSERT pod={pod_name} db={_dbname} keys={list(_expanded)} out={repr(_out[:300])}")
            _out_lower = _out.lower()
            if "not found" in _out_lower or "no such file" in _out_lower:
                continue  # try next psql path
            if "error" in _out_lower:
                return f"PG exec SQL error: {repr(_out[:250])}"
            # psql prints "INSERT 0 1" or "UPDATE 1" on success
            if "insert" in _out_lower or "update" in _out_lower:
                return None
            return f"PG exec unexpected output: {repr(_out[:200])}"
        except Exception as e:
            _msg = str(e)
            if "not found" in _msg.lower() or "no such file" in _msg.lower():
                continue
            return f"PG exec failed ({_psql}): {_msg[:200]}"
    return f"PG exec: psql not found in pod '{pod_name}' — tried {_psql_bins}"


_KEYCLOAK_ENV_KEYS = {
    "ENABLE_OAUTH_SIGNUP", "OAUTH_PROVIDER_NAME", "OPENID_PROVIDER_URL",
    "OAUTH_CLIENT_ID", "OAUTH_CLIENT_SECRET", "OAUTH_MERGE_ACCOUNTS_BY_EMAIL",
    "ENABLE_LOGIN_FORM", "OAUTH_SCOPES", "DEFAULT_USER_ROLE",
    "OAUTH_ROLES_CLAIM", "OAUTH_ADMIN_ROLES",
}


def _parse_keycloak_config(raw: str | None) -> dict | None:
    if not raw:
        return None
    try:
        import json as _j
        cfg = _j.loads(raw)
        cfg.pop("client_secret", None)   # secret response mein nahi bhejenge
        return cfg
    except Exception:
        return None


def _inject_llm_env_vars(ow: AppDeployment, env_list: list, db: Session) -> list:
    """
    Pod restart (Keycloak/VectorDB connect) ke time linked LLMs ko env vars me preserve karo.
    SQLite wipe hone par bhi LLM connections survive karti hain.
    """
    import json as _json
    from kubernetes.client.models import V1EnvVar

    try:
        llm_ids = _json.loads(ow.linked_llm_ids) if ow.linked_llm_ids else []
    except Exception:
        llm_ids = []
    if not llm_ids:
        return env_list

    from models.llm_inference_model import LLMInferenceJob
    llm_urls = []
    for lid in llm_ids:
        llm = db.query(LLMInferenceJob).filter(LLMInferenceJob.id == lid).first()
        if llm:
            url = llm.endpoint_url or (f"http://{llm.head_ip}:8000" if llm.head_ip else None)
            if url:
                if not url.endswith("/v1"):
                    url = f"{url}/v1"
                llm_urls.append(url)
    if not llm_urls:
        return env_list

    result = [e for e in env_list if e.name not in _LLM_ENV_KEYS]
    result.append(V1EnvVar(name="OPENAI_API_BASE_URL",  value=llm_urls[0]))
    result.append(V1EnvVar(name="OPENAI_API_BASE_URLS", value=";".join(llm_urls)))
    result.append(V1EnvVar(name="OPENAI_API_KEY",       value="none"))
    result.append(V1EnvVar(name="OPENAI_API_KEYS",      value=";".join(["none"] * len(llm_urls))))
    result.append(V1EnvVar(name="WEBUI_API_KEY",        value=_DAAS_OW_API_KEY))
    return result


def _k8s_patch_keycloak(ow, oauth_env_vars: dict | None, db: Session) -> str | None:
    """
    OpenWebUI K8s deployment mein Keycloak env vars set (connect) ya remove (disconnect) karo.
    oauth_env_vars=None means disconnect — sab KEYCLOAK env vars hata do.
    Returns None on success, error string on failure.
    """
    import re
    from kubernetes.client.models import V1EnvVar

    cluster = db.query(KubernetesCluster).filter(
        KubernetesCluster.id == ow.k8s_cluster_id
    ).first()

    if not cluster or not cluster.kubeconfig:
        return "K8s cluster kubeconfig missing"

    rname    = re.sub(r"[^a-z0-9-]", "-", ow.name.lower())
    rname    = re.sub(r"-+", "-", rname).strip("-")[:52]
    dep_name = f"{rname}-openwebui"
    namespace = ow.namespace or "default"

    try:
        apps_v1    = _load_k8s_apps_client(cluster)
        deployment = apps_v1.read_namespaced_deployment(name=dep_name, namespace=namespace)

        for container in (deployment.spec.template.spec.containers or []):
            if container.name == "openwebui":
                clean_env = [e for e in (container.env or []) if e.name not in _KEYCLOAK_ENV_KEYS]
                if oauth_env_vars:
                    for k, v in oauth_env_vars.items():
                        clean_env.append(V1EnvVar(name=k, value=str(v)))
                # Pod restart hone par linked LLMs ko env vars me preserve karo
                clean_env = _inject_llm_env_vars(ow, clean_env, db)
                container.env = clean_env
                break

        apps_v1.replace_namespaced_deployment(name=dep_name, namespace=namespace, body=deployment)
        logger.info(f"[AppDeploy] Keycloak env vars {'set' if oauth_env_vars else 'removed'} for {dep_name}")
        return None
    except Exception as e:
        return str(e)[:300]


def _ow_sync_connections(service_url: str, desired_urls: list[str],
                          admin_email: str = None, admin_password: str = None,
                          api_keys: list[str] = None) -> str | None:
    """
    OpenWebUI connections sync via admin config API.
    Admin JWT se try karta hai pehle (reliable), fallback WEBUI_API_KEY.
    GET  /openai/config         → read current URLs
    POST /openai/config/update  → write new URL list
    Returns None on success, error string on failure.
    """
    import httpx

    svc = service_url.rstrip("/")

    # ── Step 1: Auth token lo — admin JWT preferred, WEBUI_API_KEY fallback ──
    token = None
    if admin_email and admin_password:
        try:
            sr = httpx.post(
                f"{svc}/api/v1/auths/signin",
                json={"email": admin_email, "password": admin_password},
                timeout=10,
            )
            if sr.status_code == 200:
                token = sr.json().get("token")
                logger.info(f"[OWSync] Admin signin OK for {admin_email}")
            else:
                logger.warning(f"[OWSync] Admin signin failed ({sr.status_code}) — trying WEBUI_API_KEY")
        except Exception as _se:
            logger.warning(f"[OWSync] Admin signin error: {_se} — trying WEBUI_API_KEY")

    if not token:
        token = _DAAS_OW_API_KEY

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    try:
        # ── Step 2: Read current config ───────────────────────────────────────
        r = httpx.get(f"{svc}/openai/config", headers=headers, timeout=10)
        logger.info(f"[OWSync] GET /openai/config → {r.status_code} body={r.text[:400]}")

        if r.status_code in (401, 403):
            return f"Auth failed ({r.status_code}) — admin signin aur WEBUI_API_KEY dono reject"
        if r.status_code != 200:
            return f"GET /openai/config → {r.status_code}: {r.text[:200]}"
        if not r.text.strip():
            return "GET /openai/config → empty body (pod still starting?)"

        try:
            cfg = r.json()
        except Exception:
            return f"GET /openai/config → non-JSON: {r.text[:200]}"

        # ── Step 3: Write desired URLs ────────────────────────────────────────
        # OPENAI_API_CONFIGS: har URL ek key, value exactly working kubectl command jaisa
        _api_configs = {u: {"enable": True, "prefix_id": None} for u in desired_urls}
        payload = {
            "ENABLE_OPENAI_API":    True,
            "OPENAI_API_BASE_URLS": desired_urls,
            "OPENAI_API_KEYS":      api_keys if api_keys and len(api_keys) == len(desired_urls)
                                    else ["sk-EMPTY"] * len(desired_urls),
            "OPENAI_API_CONFIGS":   _api_configs,
        }

        wr = httpx.post(f"{svc}/openai/config/update",
                        headers=headers, json=payload, timeout=10)
        logger.info(f"[OWSync] POST /openai/config/update → {wr.status_code} body={wr.text[:300]}")

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


def _ow_direct_pg_sync(ow: "AppDeployment", urls: list, db_session: "Session") -> str | None:
    """
    OpenWebUI ke PostgreSQL DB ko directly update karo — no kubectl, no JWT, no pod exec.
    DB update ke baad Temporal pod restart karega jisse pod naye values DB se padhega.
    Returns None on success, error string on failure.
    """
    import json as _j, time as _t

    try:
        import psycopg2
    except ImportError:
        return "psycopg2 not installed on DaaS backend server"

    if not ow.postgresql_deploy_id:
        return "postgresql_deploy_id not set on this deployment"

    pg = db_session.query(AppDeployment).filter(AppDeployment.id == ow.postgresql_deploy_id).first()
    if not pg:
        return f"PostgreSQL AppDeployment id={ow.postgresql_deploy_id} not found"
    if not pg.external_ip or not pg.node_port:
        return f"PostgreSQL has no external_ip/node_port (external_ip={pg.external_ip} node_port={pg.node_port})"

    _dbname = f"openwebui_{ow.id}"

    try:
        conn = psycopg2.connect(
            host=pg.external_ip,
            port=int(pg.node_port),
            dbname=_dbname,
            user="postgres",
            password="postgres123",
            connect_timeout=10,
        )
        cur = conn.cursor()

        # Kaunse tables hain pehle check karo
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='public' ORDER BY table_name"
        )
        _tables = [r[0] for r in cur.fetchall()]
        logger.info(f"[OWPgSync] openwebui_{ow.id} tables: {_tables}")

        _new_vals = {
            "ENABLE_OPENAI_API":    True,
            "OPENAI_API_BASE_URLS": urls,
            "OPENAI_API_KEYS":      ["sk-EMPTY"] * len(urls),
            "OPENAI_API_CONFIGS":   {u: {"enable": True, "prefix_id": None} for u in urls},
        }

        _updated = False
        if "config" in _tables:
            cur.execute("SELECT id, data FROM config ORDER BY id LIMIT 1")
            _row = cur.fetchone()
            if _row:
                try:
                    _data = _j.loads(_row[1]) if _row[1] else {}
                except Exception:
                    _data = {}
                _data.update(_new_vals)
                cur.execute(
                    "UPDATE config SET data = %s, updated_at = %s WHERE id = %s",
                    (_j.dumps(_data), int(_t.time() * 1000), _row[0]),
                )
            else:
                _ts = int(_t.time() * 1000)
                cur.execute(
                    "INSERT INTO config (data, version, created_at, updated_at) VALUES (%s, 1, %s, %s)",
                    (_j.dumps(_new_vals), _ts, _ts),
                )
            _updated = True

        if not _updated:
            conn.close()
            return f"No 'config' table found in {_dbname}. Tables: {_tables}"

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"[OWPgSync] Config updated in {_dbname} — OPENAI_API_BASE_URLS={urls}")
        return None

    except Exception as _e:
        return f"PostgreSQL connect/update failed: {str(_e)[:200]}"


def _ow_pod_exec_sync(kubeconfig: str, dep_name: str, namespace: str,
                       urls: list, admin_email: str = "", admin_password: str = "") -> str | None:
    """
    OpenWebUI pod ke andar directly config update karo via kubectl exec.
    Same mechanism as manual kubectl exec command — 100% reliable.
    Returns None on success, error string on failure.
    """
    import json as _json, tempfile, os, subprocess

    _kf = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as _f:
            _f.write(kubeconfig)
            _kf = _f.name

        # Running pod name dhundo
        _gp = subprocess.run(
            ["kubectl", "--kubeconfig", _kf, "get", "pod", "-n", namespace,
             "--no-headers", "-o", "custom-columns=NAME:.metadata.name,PHASE:.status.phase"],
            capture_output=True, text=True, timeout=15,
        )
        if _gp.returncode != 0:
            return f"kubectl get pod failed: {_gp.stderr[:100]}"

        _pod_name = None
        for _line in _gp.stdout.strip().split("\n"):
            _parts = _line.split()
            if len(_parts) >= 2 and dep_name in _parts[0] and _parts[1] == "Running":
                _pod_name = _parts[0]
                break

        if not _pod_name:
            return f"No running pod with prefix '{dep_name}' in namespace '{namespace}'"

        _urls_json = _json.dumps(urls)
        _keys_json = _json.dumps(["sk-EMPTY"] * len(urls))

        # Exact same script as manual kubectl command —
        # jose = OpenWebUI ka JWT library (python-jose), PyJWT fallback bhi hai
        _script = f"""
import json, urllib.request, os, time, sys
BASE           = 'http://localhost:8080'
base_urls      = {_urls_json}
api_keys       = {_keys_json}
admin_email    = {repr(admin_email)}
admin_password = {repr(admin_password)}
token          = None

if admin_email and admin_password:
    try:
        req   = urllib.request.Request(BASE + '/api/v1/auths/signin',
                    data=json.dumps({{'email': admin_email, 'password': admin_password}}).encode(),
                    headers={{'Content-Type': 'application/json'}})
        token = json.loads(urllib.request.urlopen(req, timeout=10).read())['token']
        print('SIGNIN_OK')
    except Exception as e:
        print('SIGNIN_FAIL:', e)

if not token:
    try:
        import psycopg2
        from urllib.parse import urlparse as _up
        secret = os.environ.get('WEBUI_SECRET_KEY', 'daas-openwebui-secret')
        db_url = os.environ.get('DATABASE_URL', '')
        p      = _up(db_url)
        conn   = psycopg2.connect(host=p.hostname, port=p.port,
                     dbname=p.path.lstrip('/'), user=p.username, password=p.password)
        cur    = conn.cursor()
        cur.execute('SELECT id FROM "user" WHERE role = %s LIMIT 1', ('admin',))
        row    = cur.fetchone()
        conn.close()
        if not row:
            raise ValueError('no admin user in DB')
        _pl = {{'id': str(row[0]), 'exp': int(time.time()) + 3600,
                'iat': int(time.time()), 'jti': 'daas-sync'}}
        try:
            from jose import jwt as _jlib
        except ImportError:
            import jwt as _jlib
        token = _jlib.encode(_pl, secret, algorithm='HS256')
        if isinstance(token, bytes): token = token.decode()
        print('JWT_OK')
    except Exception as e:
        print('JWT_FAIL:', e)

if not token:
    print('NO_AUTH')
    sys.exit(1)

cfg = {{'ENABLE_OPENAI_API': True, 'OPENAI_API_BASE_URLS': base_urls,
        'OPENAI_API_KEYS': api_keys, 'OPENAI_API_CONFIGS': {{}}}}
req  = urllib.request.Request(BASE + '/openai/config/update',
           data=json.dumps(cfg).encode(),
           headers={{'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'}})
resp = json.loads(urllib.request.urlopen(req, timeout=15).read())
print('UPDATE_OK')
"""

        _ex = subprocess.run(
            ["kubectl", "--kubeconfig", _kf, "exec", "-n", namespace, _pod_name,
             "--", "python3", "-c", _script],
            capture_output=True, text=True, timeout=30,
        )
        _out = (_ex.stdout + _ex.stderr).strip()
        logger.info(f"[OWPodExec] rc={_ex.returncode} pod={_pod_name} out={_out[:300]}")

        if _ex.returncode != 0 or "UPDATE_OK" not in _out:
            return f"pod exec failed (rc={_ex.returncode}): {_out[:250]}"
        return None

    except FileNotFoundError:
        return "kubectl not found in PATH"
    except Exception as _e:
        return str(_e)[:200]
    finally:
        if _kf:
            try:
                os.unlink(_kf)
            except Exception:
                pass


def update_admin_credentials(openwebui_id: int, admin_email: str, admin_password: str, db: Session) -> dict:
    ow = db.query(AppDeployment).filter(AppDeployment.id == openwebui_id).first()
    if not ow:
        raise HTTPException(status_code=404, detail=f"Deployment id={openwebui_id} not found")
    if ow.deployment_type != "openwebui":
        raise HTTPException(status_code=400, detail="Only openwebui deployments have admin credentials")
    ow.admin_email    = admin_email.strip()
    ow.admin_password = admin_password.strip()
    db.commit()
    logger.info(f"[AppDeploy] Admin credentials updated for openwebui_id={openwebui_id} email={admin_email}")
    return response_format.success_response(200, "Admin credentials updated", {
        "openwebui_id": openwebui_id,
        "admin_email":  ow.admin_email,
    })


async def connect_private_llm(openwebui_id: int, llm_ids: list[int], db: Session) -> dict:
    import json as _json, re, uuid
    from models.llm_inference_model import LLMInferenceJob
    from service.temporalResource.workers.workers_connect_llm import TASK_QUEUE
    from service.temporalResource.workflows.workflows_connect_llm import ConnectLLMWorkflow
    from utils.temporal_client import TemporalClientManager

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
        url = (llm.endpoint_url or "").rstrip("/") or (f"http://{llm.head_ip}:8000" if llm.head_ip else None)
        if not url:
            raise HTTPException(status_code=409, detail=f"LLM id={llm.id} has no endpoint_url or head_ip")
        return url if url.endswith("/v1") else f"{url}/v1"

    try:
        current_ids = _json.loads(ow.linked_llm_ids) if ow.linked_llm_ids else []
        if not isinstance(current_ids, list):
            current_ids = []
    except Exception:
        current_ids = []

    to_add = [i for i in llm_ids if i not in current_ids]

    for lid in llm_ids:
        if not db.query(LLMInferenceJob).filter(LLMInferenceJob.id == lid).first():
            raise HTTPException(status_code=404, detail=f"Private LLM id={lid} not found")

    # Sabhi already connected hain → sirf re-sync karo (no 409, idempotent)
    if not to_add:
        all_urls = []
        for lid in current_ids:
            llm = db.query(LLMInferenceJob).filter(LLMInferenceJob.id == lid).first()
            if llm:
                all_urls.append(_llm_url(llm))

        # HTTP API path try karo
        sync_err = None
        method   = "resync_http"
        if ow.service_url:
            sync_err = _ow_sync_connections(ow.service_url, all_urls, ow.admin_email, ow.admin_password)

        # HTTP fail → direct PostgreSQL update fallback
        if sync_err is not None:
            _pg_err2 = _ow_direct_pg_sync(ow, all_urls, db)
            if _pg_err2 is None:
                sync_err = None
                method   = "resync_pg_direct"
            else:
                sync_err = _pg_err2

        msg = "LLM connections re-synced — model should appear in OpenWebUI now" if sync_err is None \
              else f"LLMs already connected; re-sync error: {sync_err}"
        return response_format.success_response(200, msg, {
            "openwebui_id": openwebui_id,
            "llm_ids":      llm_ids,
            "all_ids":      current_ids,
            "urls":         all_urls,
            "method":       method,
        })

    new_ids  = current_ids + to_add
    all_urls = []
    for lid in new_ids:
        llm = db.query(LLMInferenceJob).filter(LLMInferenceJob.id == lid).first()
        if llm:
            all_urls.append(_llm_url(llm))

    cluster = db.query(KubernetesCluster).filter(KubernetesCluster.id == ow.k8s_cluster_id).first()
    if not cluster or not cluster.kubeconfig:
        raise HTTPException(status_code=409, detail="K8s kubeconfig missing — cannot connect LLM")

    rname     = re.sub(r"[^a-z0-9-]", "-", ow.name.lower())
    rname     = re.sub(r"-+", "-", rname).strip("-")[:52]
    dep_name  = f"{rname}-openwebui"
    namespace = ow.namespace or "default"

    # ── Always start Temporal workflow (HTTP API + PG direct are tried INSIDE the activity) ──
    workflow_id = f"connect-llm-{openwebui_id}-{uuid.uuid4().hex[:8]}"
    try:
        client = await TemporalClientManager.get_temporal_client()
        handle = await client.start_workflow(
            ConnectLLMWorkflow.run,
            {
                "deploy_id":             openwebui_id,
                "llm_ids":               to_add,
                "new_ids":               new_ids,
                "base_urls":             all_urls,
                "k8s_cluster_id":        ow.k8s_cluster_id,
                "dep_name":              dep_name,
                "namespace":             namespace,
                "current_ids":           current_ids,
                "service_url":           ow.service_url or "",
                "admin_email":           ow.admin_email or "",
                "admin_password":        ow.admin_password or "",
                "postgresql_deploy_id":  ow.postgresql_deploy_id,
            },
            id         = workflow_id,
            task_queue = TASK_QUEUE,
        )
        ow.linked_llm_ids = _json.dumps(new_ids)
        ow.status         = "connecting_llm"
        ow.workflow_id    = handle.id
        ow.error_message  = None
        db.commit()
        logger.info(f"[AppDeploy] ConnectLLM workflow started: {workflow_id}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start connect-llm workflow: {e}")

    return response_format.success_response(200, "Private LLM connect workflow started", {
        "openwebui_id": openwebui_id,
        "llm_ids":      llm_ids,
        "all_ids":      new_ids,
        "urls":         all_urls,
        "workflow_id":  handle.id,
        "note":         "Temporal workflow started — check steps_log for progress. Model appears immediately if HTTP API succeeds, else after pod restart (~60s).",
    })


# ─────────────────────────────────────────────────────────────────────────────
# Sync LLM connections — bina pod restart ke OpenWebUI DB update karo
# ─────────────────────────────────────────────────────────────────────────────

async def sync_llm_connections(openwebui_id: int, db: Session) -> dict:
    """
    OpenWebUI ke DB me currently linked LLM URLs sync karo via REST API.
    No pod restart. Useful jab env var inject ho chuka hai lekin DB me
    purani URLs hain (PersistentConfig mismatch).
    """
    import json as _json
    from models.llm_inference_model import LLMInferenceJob

    ow = db.query(AppDeployment).filter(AppDeployment.id == openwebui_id).first()
    if not ow:
        raise HTTPException(status_code=404, detail=f"OpenWebUI deployment id={openwebui_id} not found")
    if ow.deployment_type != "openwebui":
        raise HTTPException(status_code=400, detail=f"id={openwebui_id} is not an OpenWebUI deployment")
    if not ow.service_url:
        raise HTTPException(status_code=409, detail="service_url not set — cannot call OpenWebUI API")

    # Build current URLs from linked LLMs
    try:
        llm_id_list = _json.loads(ow.linked_llm_ids) if ow.linked_llm_ids else []
        if not isinstance(llm_id_list, list):
            llm_id_list = []
    except Exception:
        llm_id_list = []

    def _llm_url(llm) -> str:
        url = (llm.endpoint_url or "").rstrip("/") or (f"http://{llm.head_ip}:8000" if llm.head_ip else None)
        if not url:
            return None
        return url if url.endswith("/v1") else f"{url}/v1"

    all_urls = []
    for lid in llm_id_list:
        llm = db.query(LLMInferenceJob).filter(LLMInferenceJob.id == lid).first()
        if llm:
            u = _llm_url(llm)
            if u:
                all_urls.append(u)

    err = _ow_sync_connections(ow.service_url, all_urls, ow.admin_email, ow.admin_password)
    if err:
        raise HTTPException(status_code=502, detail=f"OpenWebUI API sync failed: {err}")

    return response_format.success_response(200, "OpenWebUI connections synced via API — no pod restart needed", {
        "openwebui_id": openwebui_id,
        "llm_ids":      llm_id_list,
        "urls":         all_urls,
        "note":         "Model should appear immediately in OpenWebUI.",
    })


# ─────────────────────────────────────────────────────────────────────────────
# Disconnect Private LLM from OpenWebUI
# ─────────────────────────────────────────────────────────────────────────────

async def disconnect_private_llm(openwebui_id: int, llm_id: int | None, db: Session) -> dict:
    import re, json as _json, uuid
    from models.llm_inference_model import LLMInferenceJob
    from service.temporalResource.workers.workers_connect_llm import TASK_QUEUE
    from service.temporalResource.workflows.workflows_connect_llm import DisconnectLLMWorkflow
    from utils.temporal_client import TemporalClientManager

    ow = db.query(AppDeployment).filter(AppDeployment.id == openwebui_id).first()
    if not ow:
        raise HTTPException(status_code=404, detail=f"OpenWebUI deployment id={openwebui_id} not found")

    if ow.status in ("connecting_llm", "disconnecting_llm"):
        raise HTTPException(
            status_code=409,
            detail=f"A workflow is already running (status='{ow.status}'). Wait for it to complete before disconnecting."
        )

    try:
        current_ids = _json.loads(ow.linked_llm_ids) if ow.linked_llm_ids else []
        if not isinstance(current_ids, list):
            current_ids = []
    except Exception:
        current_ids = []

    if llm_id is not None:
        if llm_id not in current_ids:
            raise HTTPException(status_code=404, detail=f"LLM id={llm_id} is not connected to this OpenWebUI")
        remaining_ids = [i for i in current_ids if i != llm_id]
    else:
        remaining_ids = []

    remaining_urls: list[str] = []
    for lid in remaining_ids:
        llm = db.query(LLMInferenceJob).filter(LLMInferenceJob.id == lid).first()
        if llm:
            url = llm.endpoint_url or (f"http://{llm.head_ip}:8000" if llm.head_ip else None)
            if url:
                if not url.endswith("/v1"):
                    url = f"{url}/v1"
                remaining_urls.append(url)

    cluster = db.query(KubernetesCluster).filter(KubernetesCluster.id == ow.k8s_cluster_id).first()
    if not cluster or not cluster.kubeconfig:
        raise HTTPException(status_code=409, detail="K8s kubeconfig missing — cannot disconnect LLM")

    rname    = re.sub(r"[^a-z0-9-]", "-", ow.name.lower())
    rname    = re.sub(r"-+", "-", rname).strip("-")[:52]
    dep_name = f"{rname}-openwebui"

    # ── Always start Temporal workflow (HTTP API + PG direct are tried INSIDE the activity) ──
    workflow_id = f"disconnect-llm-{openwebui_id}-{uuid.uuid4().hex[:8]}"
    try:
        client = await TemporalClientManager.get_temporal_client()
        handle = await client.start_workflow(
            DisconnectLLMWorkflow.run,
            {
                "deploy_id":             openwebui_id,
                "remaining_ids":         remaining_ids,
                "remaining_urls":        remaining_urls,
                "disconnected_id":       llm_id,
                "k8s_cluster_id":        ow.k8s_cluster_id,
                "dep_name":              dep_name,
                "namespace":             ow.namespace or "default",
                "service_url":           ow.service_url or "",
                "admin_email":           ow.admin_email or "",
                "admin_password":        ow.admin_password or "",
                "postgresql_deploy_id":  ow.postgresql_deploy_id,
            },
            id         = workflow_id,
            task_queue = TASK_QUEUE,
        )
        ow.linked_llm_ids = _json.dumps(remaining_ids)
        ow.status         = "disconnecting_llm"
        ow.workflow_id    = handle.id
        ow.error_message  = None
        db.commit()
        logger.info(f"[AppDeploy] DisconnectLLM workflow started: {workflow_id}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start disconnect-llm workflow: {e}")

    return response_format.success_response(200, "Private LLM disconnect workflow started", {
        "openwebui_id":    openwebui_id,
        "disconnected_id": llm_id,
        "linked_llm_ids":  remaining_ids,
        "workflow_id":     handle.id,
        "note":            "Temporal workflow started — check steps_log for progress. Model removed immediately if HTTP API succeeds, else after pod restart (~60s).",
    })


# ─────────────────────────────────────────────────────────────────────────────
# Connect / Disconnect Keycloak SSO
# ─────────────────────────────────────────────────────────────────────────────

def _keycloak_admin_token(kc_url: str, admin_user: str, admin_pass: str) -> str:
    """Keycloak master realm se admin access token lo."""
    import httpx
    r = httpx.post(
        f"{kc_url}/realms/master/protocol/openid-connect/token",
        data={
            "grant_type": "password",
            "client_id":  "admin-cli",
            "username":   admin_user,
            "password":   admin_pass,
        },
        timeout=15,
        verify=False,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Keycloak admin login failed ({r.status_code}): {r.text[:200]}")
    return r.json()["access_token"]


def _keycloak_ensure_client(
    kc_url: str, token: str, realm: str, client_id: str, redirect_uris: list
) -> tuple:
    """
    Keycloak me OpenWebUI client dhundo ya banao.
    Returns (client_uuid, client_secret).
    """
    import httpx
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    r = httpx.get(
        f"{kc_url}/admin/realms/{realm}/clients",
        params={"clientId": client_id},
        headers=headers, timeout=15, verify=False,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Keycloak clients list failed ({r.status_code}): {r.text[:200]}")

    clients = r.json()
    if clients:
        client_uuid = clients[0]["id"]
        # Redirect URIs update karo
        updated = dict(clients[0])
        updated["redirectUris"] = redirect_uris
        updated["webOrigins"]   = ["*"]
        httpx.put(
            f"{kc_url}/admin/realms/{realm}/clients/{client_uuid}",
            headers=headers, json=updated, timeout=15, verify=False,
        )
        logger.info(f"[Keycloak] Client '{client_id}' found, redirect URIs updated")
    else:
        # Naya client banao
        r2 = httpx.post(
            f"{kc_url}/admin/realms/{realm}/clients",
            headers=headers,
            json={
                "clientId":            client_id,
                "enabled":             True,
                "protocol":            "openid-connect",
                "publicClient":        False,
                "standardFlowEnabled": True,
                "redirectUris":        redirect_uris,
                "webOrigins":          ["*"],
            },
            timeout=15, verify=False,
        )
        if r2.status_code not in (200, 201):
            raise RuntimeError(f"Keycloak client creation failed ({r2.status_code}): {r2.text[:200]}")
        r3 = httpx.get(
            f"{kc_url}/admin/realms/{realm}/clients",
            params={"clientId": client_id},
            headers=headers, timeout=15, verify=False,
        )
        client_uuid = r3.json()[0]["id"]
        logger.info(f"[Keycloak] Client '{client_id}' created (uuid={client_uuid})")

    # Secret get karo
    r4 = httpx.get(
        f"{kc_url}/admin/realms/{realm}/clients/{client_uuid}/client-secret",
        headers=headers, timeout=15, verify=False,
    )
    if r4.status_code != 200:
        raise RuntimeError(f"Keycloak secret fetch failed ({r4.status_code}): {r4.text[:200]}")

    secret = (r4.json() or {}).get("value") or ""
    if not secret:
        # Secret regenerate karo
        r5 = httpx.post(
            f"{kc_url}/admin/realms/{realm}/clients/{client_uuid}/client-secret",
            headers=headers, timeout=15, verify=False,
        )
        secret = (r5.json() or {}).get("value", "")

    return client_uuid, secret


def repair_database_url(openwebui_id: int, db: Session) -> dict:
    """
    OpenWebUI pod mein DATABASE_URL restore karo.
    Ye tab use karo jab pgvector bug se DATABASE_URL remove ho gaya ho aur login fail ho raha ho.
    Linked postgresql_deploy_id se URL rebuild karke K8s deployment patch karta hai.
    """
    import re
    from kubernetes.client.models import V1EnvVar

    ow = db.query(AppDeployment).filter(AppDeployment.id == openwebui_id).first()
    if not ow:
        raise HTTPException(status_code=404, detail=f"OpenWebUI deployment id={openwebui_id} not found")
    if ow.deployment_type != "openwebui":
        raise HTTPException(status_code=400, detail=f"id={openwebui_id} is not an OpenWebUI deployment")
    if not ow.postgresql_deploy_id:
        raise HTTPException(status_code=400, detail="No postgresql_deploy_id linked — cannot restore DATABASE_URL")

    pg = db.query(AppDeployment).filter(AppDeployment.id == ow.postgresql_deploy_id).first()
    if not pg:
        raise HTTPException(status_code=404, detail=f"PostgreSQL deployment id={ow.postgresql_deploy_id} not found")

    pg_rn   = re.sub(r"[^a-z0-9-]", "-", pg.name.lower())
    pg_rn   = re.sub(r"-+", "-", pg_rn).strip("-")[:52]
    pg_ns   = pg.namespace or "default"
    ow_db   = f"openwebui_{ow.id}"
    db_url  = f"postgresql://postgres:postgres123@{pg_rn}-postgresql.{pg_ns}.svc.cluster.local:5432/{ow_db}"

    cluster = db.query(KubernetesCluster).filter(KubernetesCluster.id == ow.k8s_cluster_id).first()
    if not cluster or not cluster.kubeconfig:
        raise HTTPException(status_code=409, detail="K8s cluster kubeconfig missing")

    rname     = re.sub(r"[^a-z0-9-]", "-", ow.name.lower())
    rname     = re.sub(r"-+", "-", rname).strip("-")[:52]
    dep_name  = f"{rname}-openwebui"
    namespace = ow.namespace or "default"

    try:
        apps_v1    = _load_k8s_apps_client(cluster)
        deployment = apps_v1.read_namespaced_deployment(name=dep_name, namespace=namespace)

        for container in (deployment.spec.template.spec.containers or []):
            if container.name == "openwebui":
                env_list = [e for e in (container.env or []) if e.name != "DATABASE_URL"]
                env_list.append(V1EnvVar(name="DATABASE_URL", value=db_url))
                container.env = env_list
                break

        apps_v1.replace_namespaced_deployment(name=dep_name, namespace=namespace, body=deployment)
        logger.info(f"[AppDeploy] DATABASE_URL repaired for openwebui id={openwebui_id}: {db_url}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"K8s patch failed: {str(e)[:300]}")

    return response_format.success_response(200, "DATABASE_URL restored — pod rolling update in progress (~60s)", {
        "openwebui_id": openwebui_id,
        "database_url": db_url,
        "note":         "Pod restart ho raha hai (~60s). Phir admin@admin.com + deployment record ka admin_password use karo.",
    })


def connect_keycloak(openwebui_id: int, body: dict, db: Session) -> dict:
    import os, json as _json

    ow = db.query(AppDeployment).filter(AppDeployment.id == openwebui_id).first()
    if not ow:
        raise HTTPException(status_code=404, detail=f"OpenWebUI deployment id={openwebui_id} not found")
    if ow.deployment_type != "openwebui":
        raise HTTPException(status_code=400, detail=f"id={openwebui_id} is not an OpenWebUI deployment")
    if ow.status not in ("deployed", "failed", "llm_connect_failed"):
        raise HTTPException(status_code=409, detail=f"OpenWebUI is in '{ow.status}' state — deploy it first")

    kc_url    = (os.getenv("KEYCLOAK_ROOT_URL") or "").strip().rstrip("/")
    kc_admin  = (os.getenv("KEYCLOAK_ADMIN")    or "admin").strip()
    kc_pass   = (os.getenv("KEYCLOAK_PASSWORD")  or "admin").strip()
    kc_realm  = (os.getenv("KEYCLOAK_RELAM") or os.getenv("KEYCLOAK_REALM") or "").strip()
    client_id = (body.get("client_id") or "openwebui").strip()

    if not kc_url:
        raise HTTPException(status_code=500, detail="KEYCLOAK_ROOT_URL not set in backend .env")
    if not kc_realm:
        raise HTTPException(status_code=500, detail="KEYCLOAK_RELAM not set in backend .env")

    provider_name = (body.get("provider_name") or "Keycloak").strip()
    oauth_scopes  = (body.get("oauth_scopes")  or "openid email profile").strip()

    base_url      = (ow.service_url or f"http://{ow.external_ip}").rstrip("/")
    redirect_uris = list(dict.fromkeys([f"{base_url}/*", f"http://{ow.external_ip}/*"]))

    # Keycloak Admin API: client create/find + secret fetch
    try:
        token = _keycloak_admin_token(kc_url, kc_admin, kc_pass)
        client_uuid, client_secret = _keycloak_ensure_client(
            kc_url, token, kc_realm, client_id, redirect_uris
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Keycloak Admin API error: {str(e)[:400]}")

    # OW pod ke liye public URL — multinode cluster se reachable hona chahiye.
    # KEYCLOAK_PUBLIC_URL set hai to wahi use karo, warna KEYCLOAK_ROOT_URL fallback.
    _kc_public = (os.getenv("KEYCLOAK_PUBLIC_URL") or kc_url).strip().rstrip("/")
    openid_provider_url = f"{_kc_public}/realms/{kc_realm}/.well-known/openid-configuration"

    oauth_env_vars = {
        "ENABLE_OAUTH_SIGNUP":           "false",
        "OAUTH_PROVIDER_NAME":           provider_name,
        "OPENID_PROVIDER_URL":           openid_provider_url,
        "OAUTH_CLIENT_ID":               client_id,
        "OAUTH_CLIENT_SECRET":           client_secret,
        "OAUTH_MERGE_ACCOUNTS_BY_EMAIL": "true",
        "ENABLE_LOGIN_FORM":             "false",
        "OAUTH_SCOPES":                  oauth_scopes,
        "DEFAULT_USER_ROLE":             "user",
        "OAUTH_ROLES_CLAIM":             "realm_access.roles",
        "OAUTH_ADMIN_ROLES":             "admin",
    }

    _kc_api_patch = {
        "enable_login_form":             False,
        "enable_signup":                 False,
        "enable_oauth_signup":           False,
        "oauth_provider_name":           provider_name,
        "openid_provider_url":           openid_provider_url,
        "oauth_client_id":               client_id,
        "oauth_client_secret":           client_secret,
        "oauth_scopes":                  oauth_scopes,
        "oauth_merge_accounts_by_email": True,
    }

    # ── Step 1: Direct HTTP API (backend → OW service_url) ───────────────────
    _form_disabled = False
    _exec_err_msg  = None
    if ow.service_url and ow.admin_email and ow.admin_password:
        _api_result = _ow_apply_keycloak_config(
            service_url    = ow.service_url,
            admin_email    = ow.admin_email,
            admin_password = ow.admin_password,
            openid_url     = openid_provider_url,
            client_id      = client_id,
            client_secret  = client_secret,
            provider_name  = provider_name,
            oauth_scopes   = oauth_scopes,
        )
        _form_disabled = (_api_result == "ok")
        logger.info(f"[Keycloak] HTTP API → {'ok' if _form_disabled else _api_result}")

    # ── Step 1.5: PostgreSQL pod exec — config table directly update karo ──────
    # OW API ya network routing ki zaroorat nahi — PG pod ke andar psql chalate hain
    if not _form_disabled:
        _exec_err_msg = _ow_pg_exec_set_config(ow, {
            "ui.enable_login_form":   False,
            "ui.enable_signup":       False,
            "ui.enable_oauth_signup": False,
            "enable_login_form":      False,
            "enable_signup":          False,
            "enable_oauth_signup":    False,
        }, db)
        _form_disabled = (_exec_err_msg is None)
        logger.info(f"[Keycloak] PG exec → {'ok' if _form_disabled else _exec_err_msg}")

    # ── Step 2: K8s env vars patch (OAuth credentials + pod restart) ─────────
    err = _k8s_patch_keycloak(ow, oauth_env_vars, db)
    if err:
        raise HTTPException(status_code=502, detail=f"K8s patch failed: {err}")

    ow.keycloak_config = _json.dumps({
        "keycloak_url":  kc_url,
        "realm":         kc_realm,
        "client_id":     client_id,
        "client_uuid":   client_uuid,
        "client_secret": client_secret,
        "provider_name": provider_name,
        "oauth_scopes":  oauth_scopes,
    })
    db.commit()

    logger.info(
        f"[AppDeploy] Keycloak connected ow={openwebui_id} realm={kc_realm} "
        f"client={client_id} login_form_disabled={_form_disabled}"
    )
    _resp_data = {
        "openwebui_id":        openwebui_id,
        "realm":               kc_realm,
        "client_id":           client_id,
        "redirect_uris":       redirect_uris,
        "status":              "connected",
        "login_form_disabled": _form_disabled,
    }
    if _exec_err_msg:
        _resp_data["exec_error"] = _exec_err_msg
    return response_format.success_response(200, "Keycloak SSO connected", _resp_data)


def disconnect_keycloak(openwebui_id: int, db: Session) -> dict:
    import os, threading as _thr

    ow = db.query(AppDeployment).filter(AppDeployment.id == openwebui_id).first()
    if not ow:
        raise HTTPException(status_code=404, detail=f"OpenWebUI deployment id={openwebui_id} not found")
    if ow.deployment_type != "openwebui":
        raise HTTPException(status_code=400, detail=f"id={openwebui_id} is not an OpenWebUI deployment")
    if not ow.keycloak_config:
        raise HTTPException(status_code=404, detail="No Keycloak SSO is connected to this OpenWebUI")

    # ── Step 1: Direct HTTP API — login form wapas enable karo ──────────────
    _form_restored = False
    if ow.service_url and ow.admin_email and ow.admin_password:
        _re_result = _ow_remove_keycloak_config(ow.service_url, ow.admin_email, ow.admin_password)
        _form_restored = (_re_result == "ok")
        logger.info(f"[Keycloak] HTTP API restore → {'ok' if _form_restored else _re_result}")

    # ── Step 1.5: PostgreSQL pod exec — config table restore karo ───────────────
    if not _form_restored:
        _exec_restore_err = _ow_pg_exec_set_config(ow, {
            "ui.enable_login_form": True,
            "ui.enable_signup":     False,
            "enable_login_form":    True,
            "enable_signup":        False,
        }, db)
        _form_restored = (_exec_restore_err is None)
        logger.info(f"[Keycloak] PG exec restore → {'ok' if _form_restored else _exec_restore_err}")

    # ── Step 3: Capture Keycloak emails before K8s patch ──────────────────────
    _kc_cfg   = _parse_keycloak_config(ow.keycloak_config)
    _kc_url   = ((_kc_cfg.get("keycloak_url", "") if _kc_cfg else
                  os.getenv("KEYCLOAK_ROOT_URL") or "")).rstrip("/")
    _kc_realm = (_kc_cfg.get("realm", "") if _kc_cfg else
                 (os.getenv("KEYCLOAK_RELAM") or os.getenv("KEYCLOAK_REALM") or ""))
    _kc_admin = os.getenv("KEYCLOAK_ADMIN") or "admin"
    _kc_pass  = os.getenv("KEYCLOAK_PASSWORD") or "admin"

    _kc_emails: set = set()
    if _kc_url and _kc_realm:
        try:
            import httpx as _hx0
            _kt = _keycloak_admin_token(_kc_url, _kc_admin, _kc_pass)
            _ku_r = _hx0.get(
                f"{_kc_url}/admin/realms/{_kc_realm}/users",
                params={"max": 1000},
                headers={"Authorization": f"Bearer {_kt}"},
                timeout=15, verify=False,
            )
            if _ku_r.status_code == 200:
                _kc_emails = {(u.get("email") or "").lower()
                              for u in _ku_r.json() if u.get("email")}
        except Exception as _ke:
            logger.warning(f"[Keycloak] email capture failed: {_ke}")

    # ── Step 4: K8s patch (pod restart) ──────────────────────────────────────
    err = _k8s_patch_keycloak(ow, None, db)
    if err:
        raise HTTPException(status_code=502, detail=f"K8s patch failed: {err}")

    ow.keycloak_config = None
    db.commit()

    # ── Step 5: Background thread — pod wapas aane ke baad users delete ───────
    _svc_snap    = (ow.service_url or "").rstrip("/")
    _adm_email   = ow.admin_email or ""
    _adm_pass    = ow.admin_password or ""
    _adm_lower   = _adm_email.lower()

    def _bg_delete_kc_users():
        import httpx as _hx, time as _time
        _ready = False
        for _ in range(36):
            try:
                if _hx.get(f"{_svc_snap}/health", timeout=5).status_code == 200:
                    _ready = True
                    break
            except Exception:
                pass
            _time.sleep(5)
        if not _ready:
            logger.warning("[BgDeleteKcUsers] Pod did not come up in 3min — skipping user deletion")
            return
        _time.sleep(5)
        try:
            _sr = _hx.post(f"{_svc_snap}/api/v1/auths/signin",
                           json={"email": _adm_email, "password": _adm_pass}, timeout=10)
            if _sr.status_code != 200:
                return
            _hdrs = {"Authorization": f"Bearer {_sr.json().get('token', '')}"}
            _ow_users = _ow_all_users(_svc_snap, _hdrs)
            _count = 0
            for _u in _ow_users:
                if not isinstance(_u, dict):
                    continue
                _email = (_u.get("email") or "").lower()
                _uid   = _u.get("id")
                if not _uid or _email == _adm_lower:
                    continue
                if _kc_emails and _email not in _kc_emails:
                    continue
                _dr = _hx.delete(f"{_svc_snap}/api/v1/users/{_uid}", headers=_hdrs, timeout=10)
                if _dr.status_code in (200, 204):
                    _count += 1
                    logger.info(f"[BgDeleteKcUsers] Deleted: {_email}")
            logger.info(f"[BgDeleteKcUsers] Done — {_count} user(s) deleted")
        except Exception as _be:
            logger.error(f"[BgDeleteKcUsers] Error: {_be}", exc_info=True)

    _thr.Thread(target=_bg_delete_kc_users, daemon=True, name="BgDeleteKcUsers").start()

    _exec_restore_note = _exec_restore_err if not _form_restored else None
    return response_format.success_response(200, "Keycloak SSO disconnected", {
        "openwebui_id":       openwebui_id,
        "status":             "disconnected",
        "form_restored":      _form_restored,
        "kc_emails_captured": len(_kc_emails),
        **({"exec_error": _exec_restore_note} if _exec_restore_note else {}),
        "note": (
            f"Login form restore hua. {len(_kc_emails)} Keycloak user(s) background mein delete ho rahe hain (~60s)."
            if _form_restored
            else "Login form pod restart ke baad wapas aayega (env var se)."
        ),
    })


# ─────────────────────────────────────────────────────────────────────────────
# API 1 — Keycloak Users List
# ─────────────────────────────────────────────────────────────────────────────

def get_keycloak_users(openwebui_id: int, page: int, page_size: int, db: Session, search: str = None) -> dict:
    import httpx, os

    ow = db.query(AppDeployment).filter(AppDeployment.id == openwebui_id).first()
    if not ow:
        raise HTTPException(status_code=404, detail=f"OpenWebUI deployment id={openwebui_id} not found")
    if ow.deployment_type != "openwebui":
        raise HTTPException(status_code=400, detail=f"id={openwebui_id} is not an OpenWebUI deployment")

    kc_cfg = _parse_keycloak_config(ow.keycloak_config)
    if kc_cfg:
        kc_url   = kc_cfg.get("keycloak_url", "").rstrip("/")
        kc_realm = kc_cfg.get("realm", "")
    else:
        kc_url   = (os.getenv("KEYCLOAK_ROOT_URL") or "").strip().rstrip("/")
        kc_realm = (os.getenv("KEYCLOAK_RELAM") or os.getenv("KEYCLOAK_REALM") or "").strip()

    kc_admin = (os.getenv("KEYCLOAK_ADMIN")   or "admin").strip()
    kc_pass  = (os.getenv("KEYCLOAK_PASSWORD") or "admin").strip()

    if not kc_url or not kc_realm:
        raise HTTPException(status_code=400, detail="Keycloak not configured — connect Keycloak first or set KEYCLOAK_ROOT_URL + KEYCLOAK_RELAM in .env")

    try:
        token   = _keycloak_admin_token(kc_url, kc_admin, kc_pass)
        headers = {"Authorization": f"Bearer {token}"}

        # Total count (search ke saath bhi)
        count_params = {"search": search} if search else {}
        count_r = httpx.get(
            f"{kc_url}/admin/realms/{kc_realm}/users/count",
            headers=headers, params=count_params, timeout=15, verify=False,
        )
        total = count_r.json() if count_r.status_code == 200 else 0

        # Paginated users — Keycloak first=offset, max=limit, search=query
        offset      = (page - 1) * page_size
        user_params = {"first": offset, "max": page_size}
        if search:
            user_params["search"] = search  # name, email, username sab mein search karta hai
        r = httpx.get(
            f"{kc_url}/admin/realms/{kc_realm}/users",
            headers=headers,
            params=user_params,
            timeout=15,
            verify=False,
        )
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Keycloak users fetch failed ({r.status_code}): {r.text[:200]}")

        users  = r.json()
        result = [
            {
                "id":         u.get("id"),
                "username":   u.get("username"),
                "email":      u.get("email"),
                "first_name": u.get("firstName"),
                "last_name":  u.get("lastName"),
                "enabled":    u.get("enabled", True),
            }
            for u in users
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Keycloak error: {str(e)[:200]}")

    total_pages = (total + page_size - 1) // page_size if page_size else 1
    return response_format.success_response(200, f"{len(result)} Keycloak user(s) found", {
        "openwebui_id": openwebui_id,
        "realm":        kc_realm,
        "users":        result,
        "pagination": {
            "page":        page,
            "page_size":   page_size,
            "total":       total,
            "total_pages": total_pages,
        },
    })


# ─────────────────────────────────────────────────────────────────────────────
# API 2 — OpenWebUI Users with Assigned Roles
# ─────────────────────────────────────────────────────────────────────────────

def _kc_role_users(kc_url: str, kc_realm: str, headers: dict, role_name: str) -> list:
    """Keycloak se ek role ke saare users fetch karo."""
    import httpx
    r = httpx.get(
        f"{kc_url}/admin/realms/{kc_realm}/roles/{role_name}/users",
        headers=headers, params={"max": 1000}, timeout=15, verify=False,
    )
    if r.status_code != 200:
        return []
    return [
        {
            "id":         u.get("id"),
            "username":   u.get("username"),
            "email":      u.get("email"),
            "first_name": u.get("firstName"),
            "last_name":  u.get("lastName"),
        }
        for u in r.json()
    ]


def _paginate_and_search(users: list, search: str | None, page: int, page_size: int) -> dict:
    """Search filter + pagination apply karo."""
    if search:
        q = search.lower()
        users = [
            u for u in users
            if q in (u.get("username") or "").lower()
            or q in (u.get("email")    or "").lower()
            or q in (u.get("first_name") or "").lower()
            or q in (u.get("last_name")  or "").lower()
        ]
    total       = len(users)
    total_pages = (total + page_size - 1) // page_size if page_size else 1
    offset      = (page - 1) * page_size
    return {
        "users": users[offset: offset + page_size],
        "pagination": {
            "page":        page,
            "page_size":   page_size,
            "total":       total,
            "total_pages": total_pages,
        },
    }


def _kc_setup(ow) -> tuple:
    """Keycloak credentials aur headers return karo."""
    import os
    kc_cfg = _parse_keycloak_config(ow.keycloak_config)
    if kc_cfg:
        kc_url   = kc_cfg.get("keycloak_url", "").rstrip("/")
        kc_realm = kc_cfg.get("realm", "")
    else:
        kc_url   = (os.getenv("KEYCLOAK_ROOT_URL") or "").strip().rstrip("/")
        kc_realm = (os.getenv("KEYCLOAK_RELAM") or os.getenv("KEYCLOAK_REALM") or "").strip()
    kc_admin = (os.getenv("KEYCLOAK_ADMIN")   or "admin").strip()
    kc_pass  = (os.getenv("KEYCLOAK_PASSWORD") or "admin").strip()
    if not kc_url or not kc_realm:
        raise HTTPException(status_code=400, detail="Keycloak not configured — connect Keycloak first")
    token = _keycloak_admin_token(kc_url, kc_admin, kc_pass)
    return kc_url, kc_realm, {"Authorization": f"Bearer {token}"}


def _kc_find_role_name(kc_url: str, kc_realm: str, headers: dict, target: str) -> str | None:
    """Realm roles mein case-insensitive role name dhundo."""
    import httpx
    r = httpx.get(f"{kc_url}/admin/realms/{kc_realm}/roles", headers=headers, timeout=15, verify=False)
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Keycloak roles fetch failed ({r.status_code})")
    for role in r.json():
        if role.get("name", "").lower() == target.lower():
            return role["name"]
    return None


def _ow_all_users(svc: str, ow_headers: dict) -> list:
    """
    OpenWebUI se saare users fetch karo.
    Response list ya dict-wrapped dono handle karta hai.
    """
    import httpx

    # skip/limit params se saare users ek baar mein lo
    _users_r = httpx.get(
        f"{svc}/api/v1/users/",
        params={"skip": 0, "limit": 10000},
        headers=ow_headers,
        timeout=15,
    )
    if _users_r.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"OpenWebUI users fetch failed ({_users_r.status_code}): {_users_r.text[:100]}"
        )

    _raw = _users_r.json()
    if isinstance(_raw, list):
        return _raw
    if isinstance(_raw, dict):
        return (
            _raw.get("users")
            or _raw.get("data")
            or _raw.get("items")
            or []
        )
    return []


def _ow_fetch_users_by_role(ow, role_filter: str, page: int, page_size: int, search: str | None) -> dict:
    """OpenWebUI se users fetch karo aur role ke hisab se filter karo."""
    import httpx

    svc = (ow.service_url or "").rstrip("/")
    if not svc:
        raise HTTPException(status_code=400, detail="OpenWebUI service_url set nahi hai — pehle deploy karo")
    if not ow.admin_email or not ow.admin_password:
        raise HTTPException(status_code=400, detail="admin_email/admin_password not set — deployment incomplete")

    # Admin JWT lo
    _signin_r = httpx.post(
        f"{svc}/api/v1/auths/signin",
        json={"email": ow.admin_email, "password": ow.admin_password},
        timeout=15,
    )
    if _signin_r.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"OpenWebUI admin signin failed ({_signin_r.status_code}): {_signin_r.text[:100]}"
        )

    _ow_token   = _signin_r.json().get("token", "")
    _ow_headers = {"Authorization": f"Bearer {_ow_token}"}

    all_users = _ow_all_users(svc, _ow_headers)

    # Role filter — admin user (ow.admin_email) ko list se bahar rakho
    admin_email_lower = (ow.admin_email or "").lower()
    filtered = [
        u for u in all_users
        if isinstance(u, dict)
        and u.get("role", "").lower() == role_filter
        and (u.get("email") or "").lower() != admin_email_lower
    ]

    # Search filter (name, email par)
    if search:
        _s = search.lower()
        filtered = [
            u for u in filtered
            if _s in (u.get("name") or "").lower()
            or _s in (u.get("email") or "").lower()
        ]

    total      = len(filtered)
    offset     = (page - 1) * page_size
    page_items = filtered[offset: offset + page_size]

    return {
        "users":       page_items,
        "total":       total,
        "page":        page,
        "page_size":   page_size,
        "total_pages": (total + page_size - 1) // page_size if total else 1,
    }


def get_ow_admin_users(openwebui_id: int, page: int, page_size: int, search: str | None, db: Session) -> dict:
    ow = db.query(AppDeployment).filter(AppDeployment.id == openwebui_id).first()
    if not ow:
        raise HTTPException(status_code=404, detail=f"OpenWebUI deployment id={openwebui_id} not found")
    if ow.deployment_type != "openwebui":
        raise HTTPException(status_code=400, detail=f"id={openwebui_id} is not an OpenWebUI deployment")
    try:
        data = _ow_fetch_users_by_role(ow, "admin", page, page_size, search)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OpenWebUI error: {str(e)[:200]}")
    return response_format.success_response(200, "Admin users fetched from OpenWebUI", {
        "openwebui_id": openwebui_id,
        "role":         "admin",
        **data,
    })


def get_ow_member_users(openwebui_id: int, page: int, page_size: int, search: str | None, db: Session) -> dict:
    ow = db.query(AppDeployment).filter(AppDeployment.id == openwebui_id).first()
    if not ow:
        raise HTTPException(status_code=404, detail=f"OpenWebUI deployment id={openwebui_id} not found")
    if ow.deployment_type != "openwebui":
        raise HTTPException(status_code=400, detail=f"id={openwebui_id} is not an OpenWebUI deployment")
    try:
        data = _ow_fetch_users_by_role(ow, "user", page, page_size, search)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OpenWebUI error: {str(e)[:200]}")
    return response_format.success_response(200, "Member users fetched from OpenWebUI", {
        "openwebui_id": openwebui_id,
        "role":         "user",
        **data,
    })


# ─────────────────────────────────────────────────────────────────────────────
# API 3 — OpenWebUI User Role Update
# ─────────────────────────────────────────────────────────────────────────────

def update_ow_user_role(openwebui_id: int, ow_user_id: str, role: str, db: Session) -> dict:
    import httpx

    if role not in ("admin", "user"):
        raise HTTPException(status_code=400, detail="role must be 'admin' or 'user'")

    ow = db.query(AppDeployment).filter(AppDeployment.id == openwebui_id).first()
    if not ow:
        raise HTTPException(status_code=404, detail=f"OpenWebUI deployment id={openwebui_id} not found")
    if ow.deployment_type != "openwebui":
        raise HTTPException(status_code=400, detail=f"id={openwebui_id} is not an OpenWebUI deployment")
    if not ow.service_url:
        raise HTTPException(status_code=409, detail="OpenWebUI not yet deployed — service_url missing")
    if not ow.admin_email or not ow.admin_password:
        raise HTTPException(status_code=409, detail="Admin credentials not set — redeploy OpenWebUI to auto-create admin user")

    svc = ow.service_url.rstrip("/")

    try:
        _sr   = httpx.post(f"{svc}/api/v1/auths/signin",
                           json={"email": ow.admin_email, "password": ow.admin_password}, timeout=10)
        if _sr.status_code != 200:
            raise HTTPException(status_code=502, detail=f"OW admin signin failed ({_sr.status_code})")
        token   = _sr.json().get("token", "")
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        r = httpx.post(
            f"{svc}/api/v1/users/{ow_user_id}/update",
            headers=headers,
            json={"role": role, "active": True},
            timeout=15,
        )
        if r.status_code == 404:
            raise HTTPException(status_code=404, detail=f"OpenWebUI user id='{ow_user_id}' not found")
        if r.status_code >= 400:
            raise HTTPException(status_code=502, detail=f"Role update failed ({r.status_code}): {r.text[:200]}")

        updated = r.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OpenWebUI error: {str(e)[:200]}")

    logger.info(f"[AppDeploy] OW user '{ow_user_id}' role → {role} (openwebui_id={openwebui_id})")
    return response_format.success_response(200, f"User role updated to '{role}'", {
        "openwebui_id": openwebui_id,
        "user_id":      ow_user_id,
        "role":         role,
        "user":         updated,
    })


# ─────────────────────────────────────────────────────────────────────────────
# Bulk Role Assign — Keycloak
# ─────────────────────────────────────────────────────────────────────────────

def assign_roles(openwebui_id: int, assignments: list, db: Session) -> dict:
    import httpx

    valid_roles = {"admin", "user"}
    for a in assignments:
        if a.role not in valid_roles:
            raise HTTPException(status_code=400, detail=f"Invalid role '{a.role}' for user '{a.user_id}' — must be 'admin' or 'user'")

    ow = db.query(AppDeployment).filter(AppDeployment.id == openwebui_id).first()
    if not ow:
        raise HTTPException(status_code=404, detail=f"OpenWebUI deployment id={openwebui_id} not found")
    if ow.deployment_type != "openwebui":
        raise HTTPException(status_code=400, detail=f"id={openwebui_id} is not an OpenWebUI deployment")

    try:
        kc_url, kc_realm, headers = _kc_setup(ow)

        # Hardcoded role name map: "admin" → "Admin", "user" → "user"
        _ROLE_NAME = {"admin": "Admin", "user": "user"}

        def _get_or_create_role(key: str) -> dict:
            kc_name = _ROLE_NAME[key]
            r = httpx.get(
                f"{kc_url}/admin/realms/{kc_realm}/roles/{kc_name}",
                headers=headers, timeout=10, verify=False,
            )
            if r.status_code == 200:
                return {"id": r.json()["id"], "name": r.json()["name"]}
            # 404 → role nahi hai, create karo
            cr = httpx.post(
                f"{kc_url}/admin/realms/{kc_realm}/roles",
                headers={**headers, "Content-Type": "application/json"},
                json={"name": kc_name},
                timeout=10, verify=False,
            )
            if cr.status_code not in (200, 201):
                raise HTTPException(status_code=502, detail=f"Keycloak role '{kc_name}' create failed ({cr.status_code})")
            # Create ke baad GET karo (id milta hai tabhi)
            r2 = httpx.get(
                f"{kc_url}/admin/realms/{kc_realm}/roles/{kc_name}",
                headers=headers, timeout=10, verify=False,
            )
            return {"id": r2.json()["id"], "name": r2.json()["name"]}

        role_objs = {key: _get_or_create_role(key) for key in ("admin", "user")}

        succeeded = []
        failed    = []

        # Keycloak user email cache — ek baar fetch, baar baar use
        _kc_user_cache: dict = {}

        def _kc_user_email(uid: str) -> str | None:
            if uid in _kc_user_cache:
                return _kc_user_cache[uid]
            try:
                _ur = httpx.get(f"{kc_url}/admin/realms/{kc_realm}/users/{uid}",
                                headers=headers, timeout=10, verify=False)
                if _ur.status_code == 200:
                    email = _ur.json().get("email") or _ur.json().get("username")
                    name  = _ur.json().get("firstName", "") + " " + _ur.json().get("lastName", "")
                    name  = name.strip() or _ur.json().get("username", email)
                    _kc_user_cache[uid] = (email, name)
                    return (email, name)
            except Exception:
                pass
            _kc_user_cache[uid] = None
            return None

        for a in assignments:
            user_id     = a.user_id
            target_role = a.role                        # "admin" | "user"
            other_role  = "user" if target_role == "admin" else "admin"

            try:
                role_url = f"{kc_url}/admin/realms/{kc_realm}/users/{user_id}/role-mappings/realm"

                # Pehle opposite role remove karo (agar hai to)
                import json as _json
                _del_headers = {**headers, "Content-Type": "application/json"}
                httpx.request(
                    "DELETE", role_url,
                    headers=_del_headers,
                    data=_json.dumps([role_objs[other_role]]),
                    timeout=10, verify=False,
                )

                # Naya role assign karo
                ar = httpx.post(
                    role_url,
                    headers={**headers, "Content-Type": "application/json"},
                    json=[role_objs[target_role]],
                    timeout=10, verify=False,
                )
                if ar.status_code in (200, 204):
                    succeeded.append({"user_id": user_id, "role": target_role})
                else:
                    failed.append({"user_id": user_id, "role": target_role, "error": ar.text[:100]})

            except Exception as _ue:
                failed.append({"user_id": user_id, "role": target_role, "error": str(_ue)[:100]})

        # ── OpenWebUI sync ───────────────────────────────────────────────────────
        ow_sync = []
        svc = (ow.service_url or "").rstrip("/")
        admin_email    = ow.admin_email
        admin_password = ow.admin_password

        if svc and admin_email and admin_password and succeeded:
            try:
                # Admin JWT lo
                _signin_r = httpx.post(
                    f"{svc}/api/v1/auths/signin",
                    json={"email": admin_email, "password": admin_password},
                    timeout=15,
                )
                if _signin_r.status_code == 200:
                    _ow_token = _signin_r.json().get("token", "")
                    _ow_headers = {"Authorization": f"Bearer {_ow_token}", "Content-Type": "application/json"}

                    # Sare OW users ek baar fetch karo (helper handles list/dict both)
                    _ow_users    = _ow_all_users(svc, _ow_headers)
                    _ow_by_email = {
                        (u.get("email") or "").lower(): u
                        for u in _ow_users
                        if isinstance(u, dict)
                    }

                    import secrets as _sec
                    for s in succeeded:
                        _uid      = s["user_id"]
                        _role     = s["role"]
                        _info     = _kc_user_email(_uid)
                        if not _info:
                            ow_sync.append({"user_id": _uid, "status": "skipped", "reason": "email not found in Keycloak"})
                            continue
                        _email, _name = _info
                        if not _email:
                            ow_sync.append({"user_id": _uid, "status": "skipped", "reason": "no email in Keycloak"})
                            continue

                        _email_lower = _email.lower()
                        _ow_user = _ow_by_email.get(_email_lower)

                        if _ow_user:
                            # User exist karta hai (active ya pending) — role + active set karo
                            _ow_uid = _ow_user.get("id")
                            _upd_r = httpx.post(
                                f"{svc}/api/v1/users/{_ow_uid}/update",
                                headers=_ow_headers,
                                json={"role": _role, "active": True},
                                timeout=10,
                            )
                            if _upd_r.status_code == 200:
                                ow_sync.append({"user_id": _uid, "email": _email,
                                                "status": "activated", "ow_role": _role})
                            else:
                                ow_sync.append({"user_id": _uid, "email": _email,
                                                "status": "update_failed",
                                                "error": _upd_r.text[:100]})
                        else:
                            # User exist nahi karta — admin API se create karo
                            # /api/v1/auths/add admin JWT se kaam karta hai,
                            # signup disabled hone pe bhi (ENABLE_LOGIN_FORM=false ke baad)
                            _rand_pass = _sec.token_urlsafe(16)
                            _add_r = httpx.post(
                                f"{svc}/api/v1/auths/add",
                                headers=_ow_headers,
                                json={"name": _name or _email, "email": _email,
                                      "password": _rand_pass, "role": _role},
                                timeout=15,
                            )
                            if _add_r.status_code in (200, 201):
                                _new_user = _add_r.json()
                                _new_id   = (_new_user.get("id")
                                             or (_new_user.get("user") or {}).get("id"))
                                # Role + active confirm karo
                                if _new_id:
                                    httpx.post(
                                        f"{svc}/api/v1/users/{_new_id}/update",
                                        headers=_ow_headers,
                                        json={"role": _role, "active": True},
                                        timeout=10,
                                    )
                                ow_sync.append({"user_id": _uid, "email": _email,
                                                "status": "created", "ow_role": _role})
                            elif _add_r.status_code in (400, 409):
                                # Email already exists (pending account) — GET se dhundo aur activate karo
                                _all_r = httpx.get(
                                    f"{svc}/api/v1/users/all",
                                    headers=_ow_headers, timeout=10,
                                )
                                _all_users = _all_r.json() if _all_r.status_code == 200 else []
                                _found = next(
                                    (u for u in _all_users
                                     if (u.get("email") or "").lower() == _email_lower),
                                    None,
                                )
                                if _found:
                                    httpx.post(
                                        f"{svc}/api/v1/users/{_found['id']}/update",
                                        headers=_ow_headers,
                                        json={"role": _role, "active": True},
                                        timeout=10,
                                    )
                                    ow_sync.append({"user_id": _uid, "email": _email,
                                                    "status": "activated", "ow_role": _role})
                                else:
                                    ow_sync.append({"user_id": _uid, "email": _email,
                                                    "status": "create_failed",
                                                    "error": _add_r.text[:100]})
                            else:
                                ow_sync.append({"user_id": _uid, "email": _email,
                                                "status": "create_failed",
                                                "error": _add_r.text[:100]})
                else:
                    ow_sync.append({"status": "skipped", "reason": f"OW admin signin failed ({_signin_r.status_code})"})
            except Exception as _owe:
                ow_sync.append({"status": "error", "reason": str(_owe)[:150]})
        elif succeeded and not (admin_email and admin_password):
            ow_sync.append({"status": "skipped", "reason": "admin_email/admin_password not set in deployment"})

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Keycloak error: {str(e)[:200]}")

    logger.info(f"[AppDeploy] Bulk role assign: {len(succeeded)} ok, {len(failed)} failed, {len(ow_sync)} ow_sync (openwebui_id={openwebui_id})")
    return response_format.success_response(200, f"{len(succeeded)} role(s) assigned, {len(failed)} failed", {
        "openwebui_id": openwebui_id,
        "succeeded":    succeeded,
        "failed":       failed,
        "ow_sync":      ow_sync,
    })

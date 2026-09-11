from typing import Any, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from controllers import kubernetes_controller
from db_configuration.config import get_db
from models.API_Response_model import APIResponse

kubernetes_router = APIRouter(prefix="/v1/kubernetes/clusters", tags=["kubernetes"])


class KubernetesClusterBody(BaseModel):
    name:        str
    control_ip:  str
    port:        Optional[int] = 6443
    username:    Optional[str] = None
    password:    Optional[str] = None
    auth_token:  Optional[str] = None   # Bearer token (optional)
    kubeconfig:  Optional[str] = None   # Full kubeconfig YAML string (optional)


class KubernetesTestBody(BaseModel):
    control_ip:  Optional[str] = None   # kubeconfig me IP ho to optional
    port:        Optional[int] = 6443
    username:    Optional[str] = None
    password:    Optional[str] = None
    auth_token:  Optional[str] = None
    kubeconfig:  Optional[str] = None


class KubernetesClusterUpdateBody(BaseModel):
    """Update ke liye — sab optional, sirf jo change karna ho wo do."""
    name:        Optional[str] = None
    control_ip:  Optional[str] = None
    port:        Optional[int] = None
    username:    Optional[str] = None
    password:    Optional[str] = None
    auth_token:  Optional[str] = None
    kubeconfig:  Optional[str] = None


# Ek cluster record ka common shape (list/get/add/update responses mein embed hota hai):
#   {
#     "id": int, "name": str, "control_ip": str, "port": int, "username": str|None,
#     "auth_token": "***"|None,      # kabhi raw value nahi, sirf masked indicator
#     "kubeconfig": "***"|None,      # kabhi raw value nahi, sirf masked indicator
#     "has_kubeconfig": bool,
#     "status": "connected"|"failed"|str, "last_tested": iso-datetime|None,
#     "created_at": iso-datetime, "updated_at": iso-datetime
#   }
# `password` field yahan kabhi nahi aata (response se hamesha excluded).


# NOTE: /test route pehle register karo warna /{cluster_id} usse match kar leta
@kubernetes_router.post("/test", response_model=APIResponse[Any])
async def test_cluster_pre_save(body: KubernetesTestBody):
    """
    DB me save kiye bina connection test karo.
    Frontend se pehle ye call karo, agar connected aaye toh add karo.

    Response 200 — `data` (status="connected"):
        {"status": "connected", ...cluster-info jo test se mila...}
    Response 400 (error_response, status="Failed") — `data`:
        {"status": "failed", "error": str}

    Errors: 400 agar control_ip na diya ho aur kubeconfig se bhi na nikal paaye.
    """
    return await kubernetes_controller.test_k8s_connection_direct(body.model_dump())


@kubernetes_router.post("", response_model=APIResponse[Any])
async def add_cluster(body: KubernetesClusterBody, db: Session = Depends(get_db)):
    """
    Pehle connection test karo, connected hua toh DB me save karo.

    Response 201 — `data`: cluster record (upar wala common shape) + `test_result`.
    Response 400 (error_response) agar connection test fail ho — `data`: {"error": str}.

    Errors: 409 agar isi `name` ka cluster already exist karta ho.
    """
    return await kubernetes_controller.add_k8s_cluster(body.model_dump(), db)


@kubernetes_router.post("/{cluster_id}/test", response_model=APIResponse[Any])
async def test_saved_cluster(cluster_id: int, db: Session = Depends(get_db)):
    """
    Already saved cluster ka connection re-test karo (status update hoga DB me).

    Response 200/400 — `data`: {..test-result.., "cluster_id": int}
    Errors: 404 agar cluster_id na mile.
    """
    return await kubernetes_controller.test_k8s_cluster(cluster_id, db)


@kubernetes_router.get("", response_model=APIResponse[Any])
def list_clusters(
    page:      int = Query(1,  ge=1),
    page_size: int = Query(10, ge=1, le=100),
    db:        Session = Depends(get_db),
):
    """
    Saare saved Kubernetes clusters list karo (paginated).

    Response 200 — `data`:
        {
          "total": int, "page": int, "page_size": int, "total_pages": int,
          "has_next": bool, "has_prev": bool,
          "clusters": [ <cluster record>, ... ]   # upar wala common shape
        }
    """
    return kubernetes_controller.list_k8s_clusters(db, page=page, page_size=page_size)


@kubernetes_router.put("/{cluster_id}", response_model=APIResponse[Any])
async def update_cluster(cluster_id: int, body: KubernetesClusterUpdateBody, db: Session = Depends(get_db)):
    """
    Cluster credentials update karo (auth_token, kubeconfig, password, etc.) + connection re-test.
    Sirf jo fields change karni hain wo do, baaki same rahengi. Agar
    `auth_token`/`kubeconfig` mein masked value `"***"` ya `""` bhej do, wo
    field untouched rehta hai (GET response se copy-paste karne par safe hai).

    Response 200 — `data`: updated cluster record + `test_result`.
    Response 400 (error_response) agar re-test fail ho jaaye (update phir bhi save ho chuka hota hai).

    Errors: 404 agar cluster_id na mile.
    """
    return await kubernetes_controller.update_k8s_cluster(cluster_id, body.model_dump(exclude_none=True), db)


@kubernetes_router.get("/{cluster_id}", response_model=APIResponse[Any])
async def get_cluster(cluster_id: int, db: Session = Depends(get_db)):
    """
    Single Kubernetes cluster detail + saare nodes (master/worker) + machine specs.

    Response 200 — `data`: cluster record (upar wala common shape) +
        {
          "cluster_summary": {...} | {"error": str},
          "nodes": {...} | {"error": str},
          "system_components": [...]
        }
    Live cluster query fail ho jaaye to bhi 200 hi aata hai, bas upar wale 3
    keys mein error string aa jaata hai (cluster record delete nahi hota).

    Errors: 404 agar cluster_id na mile.
    """
    return await kubernetes_controller.get_k8s_cluster(cluster_id, db)


@kubernetes_router.delete("/{cluster_id}", response_model=APIResponse[Any])
def delete_cluster(cluster_id: int, db: Session = Depends(get_db)):
    """
    Kubernetes cluster record delete karo (sirf DB record — actual cluster
    ko touch nahi karta).

    Response 200 — `data`: null (msg mein confirmation).
    Errors: 404 agar cluster_id na mile.
    """
    return kubernetes_controller.delete_k8s_cluster(cluster_id, db)


# ─────────────────────────────────────────────────────────────────────────────
# Harbor Deploy on K8s node
# ─────────────────────────────────────────────────────────────────────────────

class HarborDeployBody(BaseModel):
    library_item_id: int            # Library me upload hua Harbor zip ka ID
    name:            str            # Deployment ka naam
    namespace:       Optional[str] = "harbor"   # K8s namespace (default: harbor)
    http_port:       Optional[int] = 80          # Harbor HTTP port (default: 80)


# NOTE: static paths (deployments) pehle register karo /{cluster_id} se pehle nahi
#       pero ye /{cluster_id}/deployments hai isliye ok hai
@kubernetes_router.post("/{cluster_id}/deployments", response_model=APIResponse[Any])
async def deploy_harbor(
    cluster_id: int,
    body:       HarborDeployBody,
    db:         Session = Depends(get_db),
):
    """
    Library me uploaded Harbor zip ko K8s cluster pe deploy karo (Temporal
    workflow ke through, async).

    Flow: library item fetch (zip path) → K8s API se manifests apply →
    Harbor pods ready hone ka wait → harbor_url set.

    Async hai — deploy_id + status milega turant.
    GET /{cluster_id}/deployments/{deploy_id} se status + harbor_url poll karo.

    Response 201 — `data`:
        {
          "deploy_id": int, "cluster_id": int, "name": str, "node_ip": str|None,
          "namespace": str, "status": "deploying", "workflow_id": str,
          "harbor_url": null, "message": str
        }

    Errors: 404 agar cluster_id/library_item_id na mile, 400 agar cluster ka
    kubeconfig set na ho.
    """
    return await kubernetes_controller.deploy_harbor_to_k8s(cluster_id, body.model_dump(), db)


@kubernetes_router.get("/{cluster_id}/deployments", response_model=APIResponse[Any])
def list_deployments(cluster_id: int, db: Session = Depends(get_db)):
    """
    Cluster ke saare Harbor deployments list karo.

    Response 200 — `data`: {"deployments": [ <deployment record>, ... ]}
    Har `<deployment record>`: id, cluster_id, name, node_ip, namespace,
    status (deploying|running|failed), workflow_id, harbor_url, error_message,
    created_at, updated_at.
    """
    return kubernetes_controller.list_k8s_deployments(cluster_id, db)


@kubernetes_router.get("/{cluster_id}/deployments/{deploy_id}", response_model=APIResponse[Any])
def get_deployment(cluster_id: int, deploy_id: int, db: Session = Depends(get_db)):
    """
    Single deployment ka status + harbor_url fetch karo (poll karne ke liye).

    Response 200 — `data`: ek `<deployment record>` (dekho `list_deployments` docstring).
    Errors: 404 agar deploy_id (is cluster_id ke saath) na mile.
    """
    return kubernetes_controller.get_k8s_deployment(cluster_id, deploy_id, db)

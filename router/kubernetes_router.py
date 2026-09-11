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
    control_ip:  Optional[str] = None   # optional if the IP is embedded in the kubeconfig
    port:        Optional[int] = 6443
    username:    Optional[str] = None
    password:    Optional[str] = None
    auth_token:  Optional[str] = None
    kubeconfig:  Optional[str] = None


class KubernetesClusterUpdateBody(BaseModel):
    """For updates — everything optional, only send what you want to change."""
    name:        Optional[str] = None
    control_ip:  Optional[str] = None
    port:        Optional[int] = None
    username:    Optional[str] = None
    password:    Optional[str] = None
    auth_token:  Optional[str] = None
    kubeconfig:  Optional[str] = None


# The common shape of a cluster record (embedded in list/get/add/update responses):
#   {
#     "id": int, "name": str, "control_ip": str, "port": int, "username": str|None,
#     "auth_token": "***"|None,      # never the raw value, just a masked indicator
#     "kubeconfig": "***"|None,      # never the raw value, just a masked indicator
#     "has_kubeconfig": bool,
#     "status": "connected"|"failed"|str, "last_tested": iso-datetime|None,
#     "created_at": iso-datetime, "updated_at": iso-datetime
#   }
# The `password` field never appears here (always excluded from the response).


# NOTE: register /test before /{cluster_id}, or /{cluster_id} would match it first
@kubernetes_router.post("/test", response_model=APIResponse[Any])
async def test_cluster_pre_save(body: KubernetesTestBody):
    """
    Test the connection without saving to the DB.
    Call this from the frontend first — only add the cluster if it comes back connected.

    Response 200 — `data` (status="connected"):
        {"status": "connected", ...cluster-info from the test...}
    Response 400 (error_response, status="Failed") — `data`:
        {"status": "failed", "error": str}

    Errors: 400 if control_ip isn't given and can't be derived from the kubeconfig either.
    """
    return await kubernetes_controller.test_k8s_connection_direct(body.model_dump())


@kubernetes_router.post("", response_model=APIResponse[Any])
async def add_cluster(body: KubernetesClusterBody, db: Session = Depends(get_db)):
    """
    Test the connection first, and save to the DB only if it connects.

    Response 201 — `data`: the cluster record (common shape above) + `test_result`.
    Response 400 (error_response) if the connection test fails — `data`: {"error": str}.

    Errors: 409 if a cluster with this `name` already exists.
    """
    return await kubernetes_controller.add_k8s_cluster(body.model_dump(), db)


@kubernetes_router.post("/{cluster_id}/test", response_model=APIResponse[Any])
async def test_saved_cluster(cluster_id: int, db: Session = Depends(get_db)):
    """
    Re-test the connection for an already-saved cluster (updates its status in the DB).

    Response 200/400 — `data`: {..test-result.., "cluster_id": int}
    Errors: 404 if cluster_id is not found.
    """
    return await kubernetes_controller.test_k8s_cluster(cluster_id, db)


@kubernetes_router.get("", response_model=APIResponse[Any])
def list_clusters(
    page:      int = Query(1,  ge=1),
    page_size: int = Query(10, ge=1, le=100),
    db:        Session = Depends(get_db),
):
    """
    List all saved Kubernetes clusters (paginated).

    Response 200 — `data`:
        {
          "total": int, "page": int, "page_size": int, "total_pages": int,
          "has_next": bool, "has_prev": bool,
          "clusters": [ <cluster record>, ... ]   # common shape above
        }
    """
    return kubernetes_controller.list_k8s_clusters(db, page=page, page_size=page_size)


@kubernetes_router.put("/{cluster_id}", response_model=APIResponse[Any])
async def update_cluster(cluster_id: int, body: KubernetesClusterUpdateBody, db: Session = Depends(get_db)):
    """
    Update cluster credentials (auth_token, kubeconfig, password, etc.) + re-test the connection.
    Only send the fields you want to change, the rest stay the same. If you
    send back the masked value `"***"` or `""` for `auth_token`/`kubeconfig`,
    that field is left untouched (safe to copy-paste straight from a GET response).

    Response 200 — `data`: the updated cluster record + `test_result`.
    Response 400 (error_response) if the re-test fails (the update is still saved either way).

    Errors: 404 if cluster_id is not found.
    """
    return await kubernetes_controller.update_k8s_cluster(cluster_id, body.model_dump(exclude_none=True), db)


@kubernetes_router.get("/{cluster_id}", response_model=APIResponse[Any])
async def get_cluster(cluster_id: int, db: Session = Depends(get_db)):
    """
    A single Kubernetes cluster's detail + all its nodes (master/worker) + machine specs.

    Response 200 — `data`: the cluster record (common shape above) +
        {
          "cluster_summary": {...} | {"error": str},
          "nodes": {...} | {"error": str},
          "system_components": [...]
        }
    Even if the live cluster query fails, this still returns 200 — the error
    just shows up as a string in those 3 keys (the cluster record itself is not deleted).

    Errors: 404 if cluster_id is not found.
    """
    return await kubernetes_controller.get_k8s_cluster(cluster_id, db)


@kubernetes_router.delete("/{cluster_id}", response_model=APIResponse[Any])
def delete_cluster(cluster_id: int, db: Session = Depends(get_db)):
    """
    Delete a Kubernetes cluster record (DB record only — doesn't touch the
    actual cluster).

    Response 200 — `data`: null (confirmation is in msg).
    Errors: 404 if cluster_id is not found.
    """
    return kubernetes_controller.delete_k8s_cluster(cluster_id, db)


# ─────────────────────────────────────────────────────────────────────────────
# Harbor Deploy on K8s node
# ─────────────────────────────────────────────────────────────────────────────

class HarborDeployBody(BaseModel):
    library_item_id: int            # ID of the Harbor zip uploaded to the Library
    name:            str            # Deployment name
    namespace:       Optional[str] = "harbor"   # K8s namespace (default: harbor)
    http_port:       Optional[int] = 80          # Harbor HTTP port (default: 80)


# NOTE: register static paths (deployments) before /{cluster_id} — but this
#       is /{cluster_id}/deployments, so it's fine as-is
@kubernetes_router.post("/{cluster_id}/deployments", response_model=APIResponse[Any])
async def deploy_harbor(
    cluster_id: int,
    body:       HarborDeployBody,
    db:         Session = Depends(get_db),
):
    """
    Deploy a Harbor zip uploaded to the Library onto a K8s cluster (via a
    Temporal workflow, asynchronously).

    Flow: fetch the library item (zip path) → apply manifests via the K8s API →
    wait for Harbor pods to become ready → set harbor_url.

    This is async — you get back deploy_id + status right away.
    Poll GET /{cluster_id}/deployments/{deploy_id} for status + harbor_url.

    Response 201 — `data`:
        {
          "deploy_id": int, "cluster_id": int, "name": str, "node_ip": str|None,
          "namespace": str, "status": "deploying", "workflow_id": str,
          "harbor_url": null, "message": str
        }

    Errors: 404 if cluster_id/library_item_id is not found, 400 if the
    cluster's kubeconfig isn't set.
    """
    return await kubernetes_controller.deploy_harbor_to_k8s(cluster_id, body.model_dump(), db)


@kubernetes_router.get("/{cluster_id}/deployments", response_model=APIResponse[Any])
def list_deployments(cluster_id: int, db: Session = Depends(get_db)):
    """
    List all Harbor deployments for a cluster.

    Response 200 — `data`: {"deployments": [ <deployment record>, ... ]}
    Each `<deployment record>`: id, cluster_id, name, node_ip, namespace,
    status (deploying|running|failed), workflow_id, harbor_url, error_message,
    created_at, updated_at.
    """
    return kubernetes_controller.list_k8s_deployments(cluster_id, db)


@kubernetes_router.get("/{cluster_id}/deployments/{deploy_id}", response_model=APIResponse[Any])
def get_deployment(cluster_id: int, deploy_id: int, db: Session = Depends(get_db)):
    """
    Fetch a single deployment's status + harbor_url (for polling).

    Response 200 — `data`: one `<deployment record>` (see `list_deployments` docstring).
    Errors: 404 if deploy_id (under this cluster_id) is not found.
    """
    return kubernetes_controller.get_k8s_deployment(cluster_id, deploy_id, db)

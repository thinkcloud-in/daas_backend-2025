from typing import Any, Optional

from fastapi import APIRouter, Depends
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


# NOTE: /test route pehle register karo warna /{cluster_id} usse match kar leta
@kubernetes_router.post("/test", response_model=APIResponse[Any])
async def test_cluster_pre_save(body: KubernetesTestBody):
    """
    DB me save kiye bina connection test karo.
    Frontend se pehle ye call karo, agar connected aaye toh add karo.
    """
    return await kubernetes_controller.test_k8s_connection_direct(body.model_dump())


@kubernetes_router.post("", response_model=APIResponse[Any])
async def add_cluster(body: KubernetesClusterBody, db: Session = Depends(get_db)):
    """
    Pehle connection test karo, connected hua toh DB me save karo.
    """
    return await kubernetes_controller.add_k8s_cluster(body.model_dump(), db)


@kubernetes_router.post("/{cluster_id}/test", response_model=APIResponse[Any])
async def test_saved_cluster(cluster_id: int, db: Session = Depends(get_db)):
    """
    Already saved cluster ka connection re-test karo (status update hoga DB me).
    """
    return await kubernetes_controller.test_k8s_cluster(cluster_id, db)


@kubernetes_router.get("", response_model=APIResponse[Any])
def list_clusters(db: Session = Depends(get_db)):
    """
    Saare saved Kubernetes clusters list karo.
    """
    return kubernetes_controller.list_k8s_clusters(db)


@kubernetes_router.put("/{cluster_id}", response_model=APIResponse[Any])
async def update_cluster(cluster_id: int, body: KubernetesClusterUpdateBody, db: Session = Depends(get_db)):
    """
    Cluster credentials update karo (auth_token, kubeconfig, password, etc.) + connection re-test.
    Sirf jo fields change karni hain wo do, baaki same rahengi.
    """
    return await kubernetes_controller.update_k8s_cluster(cluster_id, body.model_dump(exclude_none=True), db)


@kubernetes_router.get("/{cluster_id}", response_model=APIResponse[Any])
async def get_cluster(cluster_id: int, db: Session = Depends(get_db)):
    """
    Single Kubernetes cluster detail + saare nodes (master/worker) + machine specs.
    """
    return await kubernetes_controller.get_k8s_cluster(cluster_id, db)


@kubernetes_router.delete("/{cluster_id}", response_model=APIResponse[Any])
def delete_cluster(cluster_id: int, db: Session = Depends(get_db)):
    """
    Kubernetes cluster record delete karo.
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
    Library me uploaded Harbor zip ko K8s cluster ke ek node pe deploy karo.

    Flow:
    1. Library item fetch (zip path)
    2. SSH to node_ip
    3. Zip transfer + extract
    4. Harbor setup (install.sh / helm / kubectl apply)
    5. Wait for Harbor ready
    6. harbor_url return karo

    Async hai — deploy_id + status milega turant.
    GET /{cluster_id}/deployments/{deploy_id} se status + harbor_url poll karo.
    """
    return await kubernetes_controller.deploy_harbor_to_k8s(cluster_id, body.model_dump(), db)


@kubernetes_router.get("/{cluster_id}/deployments", response_model=APIResponse[Any])
def list_deployments(cluster_id: int, db: Session = Depends(get_db)):
    """Cluster ke saare Harbor deployments list karo."""
    return kubernetes_controller.list_k8s_deployments(cluster_id, db)


@kubernetes_router.get("/{cluster_id}/deployments/{deploy_id}", response_model=APIResponse[Any])
def get_deployment(cluster_id: int, deploy_id: int, db: Session = Depends(get_db)):
    """Single deployment ka status + harbor_url fetch karo."""
    return kubernetes_controller.get_k8s_deployment(cluster_id, deploy_id, db)

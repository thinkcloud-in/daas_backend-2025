from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Any, Optional

from controllers import app_deploy_controller
from db_configuration.config import get_db
from models.API_Response_model import APIResponse

app_deploy_router = APIRouter(prefix="/v1/app-deploy", tags=["app-deploy"])


class AppDeployBody(BaseModel):
    name:                str
    deployment_type:     str            # "openwebui" | "vectordb"
    k8s_cluster_id:      int
    harbor_registry_id:  Optional[int] = None   # kubernetes_deployments.id (harbor instance)
    version_id:          int            # library.id — specific version to deploy
    namespace:           Optional[str] = "default"


@app_deploy_router.post("", response_model=APIResponse[Any])
async def create_app_deployment(body: AppDeployBody, db: Session = Depends(get_db)):
    """
    OpenWebUI ya VectorDB ko K8s cluster pe deploy karo.
    Ek baar mein ek hi deployment_type dena hai.

    Fields:
    - name              : deployment ka display naam
    - deployment_type   : "openwebui" | "vectordb"
    - k8s_cluster_id    : kubernetes_clusters.id (SSH credentials wala cluster)
    - harbor_registry_id: kubernetes_deployments.id jisme harbor deploy hai (harbor_url ke liye)
    - version_id        : library.id — wo version jo deploy karni hai (harbor_image set honi chahiye)
    - namespace         : K8s namespace (default: "default")

    Response mein milega:
    - service_url   : http://external_ip:nodeport (openwebui) ya postgresql://...
    - external_ip   : K8s node ka bahar se accessible IP
    - node_port     : K8s NodePort number
    - steps_log     : har step ka timestamp
    """
    return await app_deploy_controller.create_app_deployment(body.model_dump(), db)


@app_deploy_router.get("", response_model=APIResponse[Any])
def list_app_deployments(
    page:            int           = Query(1,    ge=1),
    page_size:       int           = Query(10,   ge=1, le=100),
    deployment_type: Optional[str] = Query(None, description="Filter: openwebui | vectordb"),
    db: Session = Depends(get_db),
):
    return app_deploy_controller.list_app_deployments(db, page, page_size, deployment_type)


@app_deploy_router.get("/{deploy_id}", response_model=APIResponse[Any])
def get_app_deployment(deploy_id: int, db: Session = Depends(get_db)):
    return app_deploy_controller.get_app_deployment(deploy_id, db)


@app_deploy_router.delete("/{deploy_id}", response_model=APIResponse[Any])
def delete_app_deployment(deploy_id: int, db: Session = Depends(get_db)):
    return app_deploy_controller.delete_app_deployment(deploy_id, db)


class ConnectVectorDBBody(BaseModel):
    vectordb_deploy_id: int


@app_deploy_router.post("/{openwebui_id}/connect-vectordb", response_model=APIResponse[Any])
def connect_vectordb(
    openwebui_id: int,
    body: ConnectVectorDBBody,
    db: Session = Depends(get_db),
):
    """
    OpenWebUI deployment ke saath VectorDB link karo.
    K8s pe VECTOR_DB + PGVECTOR_DB_URL env vars inject karta hai aur rollout wait karta hai.
    """
    return app_deploy_controller.connect_vectordb(openwebui_id, body.vectordb_deploy_id, db)


@app_deploy_router.delete("/{openwebui_id}/connect-vectordb", response_model=APIResponse[Any])
def disconnect_vectordb(openwebui_id: int, db: Session = Depends(get_db)):
    """
    OpenWebUI se VectorDB ka link hatao — K8s env vars remove + rollout.
    """
    return app_deploy_controller.disconnect_vectordb(openwebui_id, db)


class ConnectLLMBody(BaseModel):
    llm_ids: list[int]


@app_deploy_router.post("/{openwebui_id}/connect-llm", response_model=APIResponse[Any])
async def connect_private_llm(
    openwebui_id: int,
    body: ConnectLLMBody,
    db: Session = Depends(get_db),
):
    """
    OpenWebUI ke saath Private LLM(s) connect karo.
    Primary: OpenWebUI REST API (instant, no restart).
    Fallback: Temporal workflow — K8s env vars inject + rollout wait (one-time only).
    """
    return await app_deploy_controller.connect_private_llm(openwebui_id, body.llm_ids, db)


@app_deploy_router.delete("/{openwebui_id}/connect-llm", response_model=APIResponse[Any])
async def disconnect_private_llm(
    openwebui_id: int,
    llm_id: Optional[int] = Query(None, description="LLM id to disconnect. Omit to disconnect all."),
    db: Session = Depends(get_db),
):
    """
    OpenWebUI se Private LLM ka link hatao.
    Primary: OpenWebUI REST API (instant, no restart).
    Fallback: Temporal workflow — K8s env vars update + rollout (one-time only).
    """
    return await app_deploy_controller.disconnect_private_llm(openwebui_id, llm_id, db)

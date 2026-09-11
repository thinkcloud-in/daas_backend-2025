from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Any, Optional

from controllers import app_deploy_controller
from db_configuration.config import get_db
from models.API_Response_model import APIResponse

app_deploy_router = APIRouter(prefix="/v1/app-deploy", tags=["app-deploy"])


class AppDeployBody(BaseModel):
    name:                 str
    deployment_type:      str              # "openwebui" | "vectordb" | "postgresql"
    k8s_cluster_id:       int
    harbor_registry_id:   Optional[int] = None   # kubernetes_deployments.id (harbor instance)
    version_id:           int              # library.id — specific version to deploy
    namespace:            Optional[str] = "default"
    storage_class:        Optional[str] = None   # e.g. "ceph-rbd", "cephfs" — None = cluster default
    postgresql_deploy_id: Optional[int] = None   # mandatory for openwebui — app_deployments.id of postgresql
    # OpenWebUI admin credentials (optional — auto-generated if not provided)
    admin_email:          Optional[str] = None   # default: admin@admin.com
    admin_password:       Optional[str] = None   # default: auto-generated (returned by GET /{id})


@app_deploy_router.post("", response_model=APIResponse[Any])
async def create_app_deployment(body: AppDeployBody, db: Session = Depends(get_db)):
    """
    Deploy OpenWebUI / VectorDB / PostgreSQL onto a K8s cluster.

    Fields:
    - name                : display name of the deployment
    - deployment_type     : "openwebui" | "vectordb" | "postgresql"
    - k8s_cluster_id      : kubernetes_clusters.id (deploy target cluster)
    - harbor_registry_id  : kubernetes_deployments.id where Harbor is deployed (image source)
    - version_id          : library.id — the version that carries the harbor_image
    - namespace           : K8s namespace (default: "default")
    - storage_class       : PVC storage class (default: cluster default)
    - storage_size        : PVC size (default: "1Gi" for openwebui/vectordb, "5Gi" for postgresql)
    - postgresql_deploy_id: [MANDATORY for openwebui] app_deployments.id of a deployed postgresql

    You get back in the response:
    - service_url   : http://... (openwebui) | postgresql://...cluster.local/... (postgresql) | postgresql://external/vectordb
    - external_ip   : LB IP / node IP
    - node_port     : K8s NodePort (openwebui)
    - steps_log     : timestamp for each step

    Once OpenWebUI is deployed, automatically:
    - DATABASE_URL is set from the linked PostgreSQL
    - an admin@admin.com user is created (role=admin, active=true)
    - registration (signup) is disabled — no new user can self-register
    - WEBUI_API_KEY is pre-set: daas-openwebui-api-key
    """
    return await app_deploy_controller.create_app_deployment(body.model_dump(), db)


@app_deploy_router.get("", response_model=APIResponse[Any])
def list_app_deployments(
    page:            int           = Query(1,    ge=1),
    page_size:       int           = Query(10,   ge=1, le=100),
    deployment_type: Optional[str] = Query(None, description="Filter: openwebui | vectordb"),
    db: Session = Depends(get_db),
):
    """
    List app deployments (openwebui/vectordb/postgresql), optionally filtered by type.

    Response 200 — `data`:
        {
          "items": [ <deployment record>, ... ],
          "pagination": {"page": int, "page_size": int, "total": int, "total_pages": int, "has_next": bool, "has_prev": bool}
        }
    Each `<deployment record>` contains: id, name, deployment_type, k8s_cluster_id,
    namespace, status, service_url, external_ip, node_port, admin_email,
    linked postgresql/vectordb ids, `linked_llms` (connected private-LLM
    summaries), steps_log, timestamps.
    """
    return app_deploy_controller.list_app_deployments(db, page, page_size, deployment_type)


@app_deploy_router.get("/{deploy_id}", response_model=APIResponse[Any])
def get_app_deployment(deploy_id: int, db: Session = Depends(get_db)):
    """
    Get the full detail of one app deployment.

    Response 200 — `data`: a single `<deployment record>` (see `list_app_deployments` docstring).
    Errors: 404 if deploy_id is not found.
    """
    return app_deploy_controller.get_app_deployment(deploy_id, db)


@app_deploy_router.delete("/{deploy_id}", response_model=APIResponse[Any])
def delete_app_deployment(deploy_id: int, db: Session = Depends(get_db)):
    """
    Delete an app deployment — also attempts to clean up K8s resources
    (Deployment/Service/PVC) over SSH from the cluster, then deletes the DB
    record (K8s cleanup is best-effort — the DB record is still deleted even
    if it fails, with a warning).

    Response 200 — `data`: {"id": deploy_id, "k8s_cleaned": bool, "k8s_warning": str|None}
    Errors: 404 if deploy_id is not found.
    """
    return app_deploy_controller.delete_app_deployment(deploy_id, db)


class AdminCredentialsBody(BaseModel):
    admin_email:    str
    admin_password: str


@app_deploy_router.patch("/{openwebui_id}/admin-credentials", response_model=APIResponse[Any])
def update_admin_credentials(openwebui_id: int, body: AdminCredentialsBody, db: Session = Depends(get_db)):
    """
    Update the admin email/password of an OpenWebUI deployment.
    Fixes the case where it was deployed earlier and admin_password is NULL in the DB.
    connect-llm / sync-connections will use the admin JWT from this afterwards.
    """
    return app_deploy_controller.update_admin_credentials(openwebui_id, body.admin_email, body.admin_password, db)


class ConnectVectorDBBody(BaseModel):
    vectordb_deploy_id: int


@app_deploy_router.post("/{openwebui_id}/repair-db", response_model=APIResponse[Any])
def repair_database_url(openwebui_id: int, db: Session = Depends(get_db)):
    """
    Restore DATABASE_URL in the OpenWebUI pod.
    Use this when login fails because a pgvector bug removed DATABASE_URL.
    Rebuilds the URL from the linked postgresql_deploy_id and patches the K8s deployment (~60s pod restart).
    """
    return app_deploy_controller.repair_database_url(openwebui_id, db)


@app_deploy_router.post("/{openwebui_id}/connect-vectordb", response_model=APIResponse[Any])
def connect_vectordb(
    openwebui_id: int,
    body: ConnectVectorDBBody,
    db: Session = Depends(get_db),
):
    """
    Link a VectorDB to an OpenWebUI deployment.
    Injects VECTOR_DB + PGVECTOR_DB_URL env vars on K8s and waits for the rollout.
    """
    return app_deploy_controller.connect_vectordb(openwebui_id, body.vectordb_deploy_id, db)


@app_deploy_router.delete("/{openwebui_id}/connect-vectordb", response_model=APIResponse[Any])
def disconnect_vectordb(openwebui_id: int, db: Session = Depends(get_db)):
    """
    Remove the VectorDB link from OpenWebUI — removes the K8s env vars + rollout.
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
    Connect Private LLM(s) to OpenWebUI.
    Primary: OpenWebUI REST API (instant, no restart).
    Fallback: Temporal workflow — inject K8s env vars + wait for rollout (one-time only).
    """
    return await app_deploy_controller.connect_private_llm(openwebui_id, body.llm_ids, db)


@app_deploy_router.post("/{openwebui_id}/sync-connections", response_model=APIResponse[Any])
async def sync_llm_connections(openwebui_id: int, db: Session = Depends(get_db)):
    """
    Sync linked LLM URLs into the OpenWebUI DB via REST API — without a pod restart.
    Use this when models don't show up after connect-llm (DB still has stale URLs).
    """
    return await app_deploy_controller.sync_llm_connections(openwebui_id, db)


@app_deploy_router.delete("/{openwebui_id}/connect-llm", response_model=APIResponse[Any])
async def disconnect_private_llm(
    openwebui_id: int,
    llm_id: Optional[int] = Query(None, description="LLM id to disconnect. Omit to disconnect all."),
    db: Session = Depends(get_db),
):
    """
    Remove a Private LLM link from OpenWebUI.
    Primary: OpenWebUI REST API (instant, no restart).
    Fallback: Temporal workflow — update K8s env vars + rollout (one-time only).
    """
    return await app_deploy_controller.disconnect_private_llm(openwebui_id, llm_id, db)


class ConnectKeycloakBody(BaseModel):
    client_id:     Optional[str] = None              # Keycloak client ID (default: "openwebui")
    provider_name: Optional[str] = "Keycloak"        # SSO button label
    oauth_scopes:  Optional[str] = "openid email profile"


@app_deploy_router.post("/{openwebui_id}/connect-keycloak", response_model=APIResponse[Any])
def connect_keycloak(
    openwebui_id: int,
    body: ConnectKeycloakBody,
    db: Session = Depends(get_db),
):
    """
    Connect Keycloak SSO to OpenWebUI.
    Steps: create Keycloak client → disable OW login form via HTTP API → disable it directly via PG → K8s pod restart.
    The response's pg_error/api_error fields tell you the exact failure reason.
    """
    return app_deploy_controller.connect_keycloak(openwebui_id, body.model_dump(), db)


@app_deploy_router.delete("/{openwebui_id}/connect-keycloak", response_model=APIResponse[Any])
def disconnect_keycloak(openwebui_id: int, db: Session = Depends(get_db)):
    """
    Remove Keycloak SSO from OpenWebUI.
    Steps: restore OW login form via HTTP API → restore it directly via PG → K8s pod restart → background user delete.
    """
    return app_deploy_controller.disconnect_keycloak(openwebui_id, db)


@app_deploy_router.get("/{openwebui_id}/keycloak-users", response_model=APIResponse[Any])
def get_keycloak_users(
    openwebui_id: int,
    page:         int           = Query(1,    ge=1),
    page_size:    int           = Query(10,   ge=1, le=100),
    search:       Optional[str] = Query(None, description="Search by name, email, or username"),
    db:           Session       = Depends(get_db),
):
    """
    Paginated list of users in the Keycloak realm. Use `search` to filter by name/email/username.
    """
    return app_deploy_controller.get_keycloak_users(openwebui_id, page, page_size, db, search)


@app_deploy_router.get("/{openwebui_id}/ow-admin-users", response_model=APIResponse[Any])
def get_ow_admin_users(
    openwebui_id: int,
    page:         int           = Query(1,    ge=1),
    page_size:    int           = Query(10,   ge=1, le=100),
    search:       Optional[str] = Query(None, description="Search by name/email/username"),
    db:           Session       = Depends(get_db),
):
    """
    Users with the Admin role from Keycloak — paginated + search.
    """
    return app_deploy_controller.get_ow_admin_users(openwebui_id, page, page_size, search, db)


@app_deploy_router.get("/{openwebui_id}/ow-member-users", response_model=APIResponse[Any])
def get_ow_member_users(
    openwebui_id: int,
    page:         int           = Query(1,    ge=1),
    page_size:    int           = Query(10,   ge=1, le=100),
    search:       Optional[str] = Query(None, description="Search by name/email/username"),
    db:           Session       = Depends(get_db),
):
    """
    Users with the User role from Keycloak — paginated + search.
    """
    return app_deploy_controller.get_ow_member_users(openwebui_id, page, page_size, search, db)


class UpdateRoleBody(BaseModel):
    role: str   # "admin" | "user"


@app_deploy_router.post("/{openwebui_id}/ow-users/{ow_user_id}/role", response_model=APIResponse[Any])
def update_ow_user_role(
    openwebui_id: int,
    ow_user_id:   str,
    body:         UpdateRoleBody,
    db:           Session = Depends(get_db),
):
    """
    Update an OpenWebUI user's role — 'admin' or 'user'.
    ow_user_id: obtained from the GET /ow-users response (OpenWebUI's internal user ID).
    """
    return app_deploy_controller.update_ow_user_role(openwebui_id, ow_user_id, body.role, db)


class RoleAssignment(BaseModel):
    user_id: str   # Keycloak user ID
    role:    str   # "admin" | "user"


class BulkAssignRolesBody(BaseModel):
    assignments: list[RoleAssignment]


@app_deploy_router.post("/{openwebui_id}/assign-roles", response_model=APIResponse[Any])
def assign_roles(
    openwebui_id: int,
    body:         BulkAssignRolesBody,
    db:           Session = Depends(get_db),
):
    """
    Bulk-assign roles (admin/user) to Keycloak users.
    A single call can assign different or the same role to multiple users.
    The old role is removed first, then the new one is assigned.
    """
    return app_deploy_controller.assign_roles(openwebui_id, body.assignments, db)

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
    # OpenWebUI admin credentials (optional — agar nahi diya to auto-generate hoga)
    admin_email:          Optional[str] = None   # default: admin@admin.com
    admin_password:       Optional[str] = None   # default: auto-generated (GET /{id} se milega)


@app_deploy_router.post("", response_model=APIResponse[Any])
async def create_app_deployment(body: AppDeployBody, db: Session = Depends(get_db)):
    """
    OpenWebUI / VectorDB / PostgreSQL ko K8s cluster pe deploy karo.

    Fields:
    - name                : deployment ka display naam
    - deployment_type     : "openwebui" | "vectordb" | "postgresql"
    - k8s_cluster_id      : kubernetes_clusters.id (deploy target cluster)
    - harbor_registry_id  : kubernetes_deployments.id jisme harbor deploy hai (image source)
    - version_id          : library.id — harbor_image wala version
    - namespace           : K8s namespace (default: "default")
    - storage_class       : PVC storage class (default: cluster default)
    - storage_size        : PVC size (default: "1Gi" for openwebui/vectordb, "5Gi" for postgresql)
    - postgresql_deploy_id: [MANDATORY for openwebui] app_deployments.id of a deployed postgresql

    Response mein milega:
    - service_url   : http://... (openwebui) | postgresql://...cluster.local/... (postgresql) | postgresql://external/vectordb
    - external_ip   : LB IP / node IP
    - node_port     : K8s NodePort (openwebui)
    - steps_log     : har step ka timestamp

    OpenWebUI deploy hone ke baad automatically:
    - DATABASE_URL set hoti hai linked PostgreSQL se
    - admin@admin.com user create hota hai (role=admin, active=true)
    - Registration (signup) disable hoti hai — koi naya user khud se register nahi kar sakta
    - WEBUI_API_KEY pre-set: daas-openwebui-api-key
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
    App deployments list karo (openwebui/vectordb/postgresql), optionally
    type se filter karke.

    Response 200 — `data`:
        {
          "items": [ <deployment record>, ... ],
          "pagination": {"page": int, "page_size": int, "total": int, "total_pages": int, "has_next": bool, "has_prev": bool}
        }
    Har `<deployment record>` mein: id, name, deployment_type, k8s_cluster_id,
    namespace, status, service_url, external_ip, node_port, admin_email,
    linked postgresql/vectordb ids, `linked_llms` (connected private-LLM
    summaries), steps_log, timestamps.
    """
    return app_deploy_controller.list_app_deployments(db, page, page_size, deployment_type)


@app_deploy_router.get("/{deploy_id}", response_model=APIResponse[Any])
def get_app_deployment(deploy_id: int, db: Session = Depends(get_db)):
    """
    Ek app deployment ki poori detail lo.

    Response 200 — `data`: ek `<deployment record>` (dekho `list_app_deployments` docstring).
    Errors: 404 agar deploy_id na mile.
    """
    return app_deploy_controller.get_app_deployment(deploy_id, db)


@app_deploy_router.delete("/{deploy_id}", response_model=APIResponse[Any])
def delete_app_deployment(deploy_id: int, db: Session = Depends(get_db)):
    """
    App deployment delete karo — K8s resources (Deployment/Service/PVC) bhi
    SSH ke through cluster se cleanup karne ki koshish karta hai, phir DB
    record delete karta hai (K8s cleanup best-effort — fail ho to bhi DB
    record delete hota hai, warning ke saath).

    Response 200 — `data`: {"id": deploy_id, "k8s_cleaned": bool, "k8s_warning": str|None}
    Errors: 404 agar deploy_id na mile.
    """
    return app_deploy_controller.delete_app_deployment(deploy_id, db)


class AdminCredentialsBody(BaseModel):
    admin_email:    str
    admin_password: str


@app_deploy_router.patch("/{openwebui_id}/admin-credentials", response_model=APIResponse[Any])
def update_admin_credentials(openwebui_id: int, body: AdminCredentialsBody, db: Session = Depends(get_db)):
    """
    OpenWebUI deployment ke admin email/password update karo.
    Pehle deploy hua tha aur admin_password DB mein NULL hai — ye fix karo.
    Iske baad connect-llm / sync-connections admin JWT use karega.
    """
    return app_deploy_controller.update_admin_credentials(openwebui_id, body.admin_email, body.admin_password, db)


class ConnectVectorDBBody(BaseModel):
    vectordb_deploy_id: int


@app_deploy_router.post("/{openwebui_id}/repair-db", response_model=APIResponse[Any])
def repair_database_url(openwebui_id: int, db: Session = Depends(get_db)):
    """
    OpenWebUI pod mein DATABASE_URL restore karo.
    Use karo jab login fail ho aur pgvector bug se DATABASE_URL remove ho gaya ho.
    Linked postgresql_deploy_id se URL rebuild karke K8s deployment patch karta hai (~60s pod restart).
    """
    return app_deploy_controller.repair_database_url(openwebui_id, db)


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


@app_deploy_router.post("/{openwebui_id}/sync-connections", response_model=APIResponse[Any])
async def sync_llm_connections(openwebui_id: int, db: Session = Depends(get_db)):
    """
    OpenWebUI DB me linked LLM URLs sync karo via REST API — bina pod restart ke.
    Use karo jab connect-llm ke baad model na dikhe (DB me purani URLs ho).
    """
    return await app_deploy_controller.sync_llm_connections(openwebui_id, db)


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
    OpenWebUI ke saath Keycloak SSO connect karo.
    Steps: Keycloak client create → OW HTTP API disable form → PG direct disable form → K8s pod restart.
    Response mein pg_error/api_error se exact failure reason pata chalega.
    """
    return app_deploy_controller.connect_keycloak(openwebui_id, body.model_dump(), db)


@app_deploy_router.delete("/{openwebui_id}/connect-keycloak", response_model=APIResponse[Any])
def disconnect_keycloak(openwebui_id: int, db: Session = Depends(get_db)):
    """
    OpenWebUI se Keycloak SSO hatao.
    Steps: OW HTTP API restore form → PG direct restore → K8s pod restart → background user delete.
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
    Keycloak realm ke users ki paginated list. search param se name/email/username filter karo.
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
    Keycloak se Admin role waale users — paginated + search.
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
    Keycloak se User role waale users — paginated + search.
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
    OpenWebUI user ka role update karo — 'admin' ya 'user'.
    ow_user_id: GET /ow-users response mein milta hai (OpenWebUI ka internal user ID).
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
    Keycloak users ko bulk role assign karo (admin/user).
    Ek hi call mein multiple users ko alag alag ya same role assign ho sakta hai.
    Pehle purana role remove hota hai, phir naya assign hota hai.
    """
    return app_deploy_controller.assign_roles(openwebui_id, body.assignments, db)

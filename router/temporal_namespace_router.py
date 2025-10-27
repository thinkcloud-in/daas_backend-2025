from fastapi import APIRouter
from db_configuration.config import get_db
from service.temporalResource import namespaces_service
from controllers import temporal_namespace_controller

temporal_namespace_router = APIRouter(prefix="/v1/smtp", tags=["SMTP"])


# ----------------------- SMTP ROUTES -----------------------

@temporal_namespace_router.get("/get_namespaces", summary="Get all SMTP configurations")
def api_get_namespaces():
        return temporal_namespace_controller.api_get_namespaces()

@temporal_namespace_router.put("/update-retention", summary="Update retention policy for namespaces")
def api_update_namespace_retention(request: namespaces_service.UpdateRetentionRequest):
    return temporal_namespace_controller.api_update_namespace_retention(request)
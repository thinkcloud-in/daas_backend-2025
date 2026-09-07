from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from service.temporalResource import namespaces_service
from controllers import temporal_namespace_controller
from db_configuration.config import get_db

temporal_namespace_router = APIRouter(prefix="/v1/retention", tags=["Retention"])


# ----------------------- RETENTION ROUTES -----------------------

@temporal_namespace_router.get("/get_namespaces", summary="Get all Temporal namespaces and their current retention")
async def api_get_namespaces():
        return await temporal_namespace_controller.api_get_namespaces()

@temporal_namespace_router.put(
    "/update-retention",
    summary="Update retention policy — sets BOTH Temporal namespace retention and OpenSearch (backend-logs-*) daily cleanup in one call",
)
async def api_update_namespace_retention(
    request: namespaces_service.UpdateRetentionRequest,
    db: Session = Depends(get_db),
):
    return await temporal_namespace_controller.api_update_namespace_retention(request, db)

@temporal_namespace_router.get(
    "/settings",
    summary="Current retention configuration (retention_days, namespace, OpenSearch index pattern) — default is 30 days if never set",
)
async def api_get_retention_settings(db: Session = Depends(get_db)):
    return await temporal_namespace_controller.api_get_retention_settings(db)

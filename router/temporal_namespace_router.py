from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from service.temporalResource import namespaces_service
from controllers import temporal_namespace_controller
from db_configuration.config import get_db

temporal_namespace_router = APIRouter(prefix="/v1/retention", tags=["Retention"])


# ----------------------- RETENTION ROUTES -----------------------

@temporal_namespace_router.get("/get_namespaces", summary="Get all Temporal namespaces and their current retention")
async def api_get_namespaces():
    """
    List all Temporal namespaces, along with each one's current
    workflow-execution-retention (how many days a completed workflow's
    history stays queryable in Temporal).

    Response `data`: [ {"namespace": str, "retention_days": int, ...}, ... ]
    (a raw list — not wrapped in an APIResponse envelope, it's Temporal's own
    `/temporal/retention` endpoint's JSON as-is.)
    """
    return await temporal_namespace_controller.api_get_namespaces()

@temporal_namespace_router.put(
    "/update-retention",
    summary="Update retention policy — sets BOTH Temporal namespace retention and OpenSearch (backend-logs-*) daily cleanup in one call",
)
async def api_update_namespace_retention(
    request: namespaces_service.UpdateRetentionRequest,
    db: Session = Depends(get_db),
):
    """
    A single call updates two things:
    1. The Temporal default namespace's workflow-execution-retention (via a
       Temporal operator API/CLI wrapper).
    2. The ISM retention policy for OpenSearch's `backend-logs-*` indices
       (auto-deletes old log indices) — also recorded in the DB
       (`retention_settings` table) for audit/GET purposes.

    Request body: UpdateRetentionRequest = {"namespace": str, "retention_days": int, "email": str}
    (the `namespace` field is currently ignored — the app's default namespace
    is always the one updated.)

    Response `data`: the updated namespaces list (see `get_namespaces`).
    """
    return await temporal_namespace_controller.api_update_namespace_retention(request, db)

@temporal_namespace_router.get(
    "/settings",
    summary="Current retention configuration (retention_days, namespace, OpenSearch index pattern) — default is 30 days if never set",
)
async def api_get_retention_settings(db: Session = Depends(get_db)):
    """
    Show the retention setting from the DB if it's ever been updated,
    otherwise show the hardcoded default (30 days).

    Response `data`:
        {
          "retention_days": int, "temporal_namespace": str|None,
          "opensearch_index_pattern": str, "updated_by": str|None,
          "updated_at": iso-datetime|None, "is_default": bool
        }
    """
    return await temporal_namespace_controller.api_get_retention_settings(db)

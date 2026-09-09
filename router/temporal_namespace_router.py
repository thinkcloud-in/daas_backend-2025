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
    Saare Temporal namespaces list karo, har ek ki current
    workflow-execution-retention (kitne din tak completed workflow history
    Temporal mein queryable rehti hai) ke saath.

    Response `data`: [ {"namespace": str, "retention_days": int, ...}, ... ]
    (raw list — koi APIResponse envelope nahi wrap hota, Temporal ke apne
    `/temporal/retention` endpoint ka JSON as-is aata hai.)
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
    Ek hi call se do cheezein update hoti hain:
    1. Temporal ke default namespace ki workflow-execution-retention (Temporal
       operator API/CLI wrapper ke through).
    2. OpenSearch ke `backend-logs-*` indices ke liye ISM retention-policy
       (auto-delete purani log-indices) — DB (`retention_settings` table)
       mein bhi record hota hai audit/GET ke liye.

    Request body: UpdateRetentionRequest = {"namespace": str, "retention_days": int, "email": str}
    (`namespace` field abhi ignore hota hai — hamesha app ka default namespace update hota hai.)

    Response `data`: updated namespaces list (dekho `get_namespaces`).
    """
    return await temporal_namespace_controller.api_update_namespace_retention(request, db)

@temporal_namespace_router.get(
    "/settings",
    summary="Current retention configuration (retention_days, namespace, OpenSearch index pattern) — default is 30 days if never set",
)
async def api_get_retention_settings(db: Session = Depends(get_db)):
    """
    Retention setting kabhi update hui hai to DB se wo dikhao, warna
    hardcoded default (30 din) dikhao.

    Response `data`:
        {
          "retention_days": int, "temporal_namespace": str|None,
          "opensearch_index_pattern": str, "updated_by": str|None,
          "updated_at": iso-datetime|None, "is_default": bool
        }
    """
    return await temporal_namespace_controller.api_get_retention_settings(db)

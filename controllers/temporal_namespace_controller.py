"""
Temporal namespace + retention-policy controller.

Thin delegation layer — router/temporal_namespace_router.py ("/v1/retention")
seedha in functions ko call karta hai. Saara actual kaam
service/temporalResource/namespaces_service.py mein hota hai (Temporal
operator API/CLI wrapper + OpenSearch ISM policy).
"""
from sqlalchemy.orm import Session

from service.temporalResource import namespaces_service


async def api_get_namespaces():
    """
    Saare Temporal namespaces + unki current retention list karo.

    Used by: GET /v1/retention/get_namespaces
    Returns: namespaces_service.list_namespaces() ka result — raw list
    (koi APIResponse envelope nahi, `/temporal/retention` ka JSON as-is).
    """
    return await namespaces_service.list_namespaces()


async def api_update_namespace_retention(request, db: Session):
    """
    Temporal namespace retention + OpenSearch backend-logs retention dono
    ek saath update karo.

    Used by: PUT /v1/retention/update-retention
    Args:
        request: namespaces_service.UpdateRetentionRequest (namespace, retention_days, email).
        db: DB session — retention_settings table mein audit-record likhne ke liye.
    Returns: namespaces_service.update_namespace_retention() ka result —
    updated namespaces list.
    """
    return await namespaces_service.update_namespace_retention(request, db)


async def api_get_retention_settings(db: Session):
    """
    Current retention setting lo (DB se, ya default agar kabhi set na hui ho).

    Used by: GET /v1/retention/settings
    Returns: {"retention_days", "temporal_namespace", "opensearch_index_pattern",
    "updated_by", "updated_at", "is_default"}
    """
    return await namespaces_service.get_retention_settings(db)

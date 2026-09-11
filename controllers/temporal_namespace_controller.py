"""
Temporal namespace + retention-policy controller.

Thin delegation layer — router/temporal_namespace_router.py ("/v1/retention")
calls these functions directly. All the actual work happens in
service/temporalResource/namespaces_service.py (Temporal
operator API/CLI wrapper + OpenSearch ISM policy).
"""
from sqlalchemy.orm import Session

from service.temporalResource import namespaces_service


async def api_get_namespaces():
    """
    List all Temporal namespaces + their current retention.

    Used by: GET /v1/retention/get_namespaces
    Returns: the result of namespaces_service.list_namespaces() — a raw list
    (no APIResponse envelope, it's `/temporal/retention`'s JSON as-is).
    """
    return await namespaces_service.list_namespaces()


async def api_update_namespace_retention(request, db: Session):
    """
    Update both the Temporal namespace retention and the OpenSearch
    backend-logs retention in one go.

    Used by: PUT /v1/retention/update-retention
    Args:
        request: namespaces_service.UpdateRetentionRequest (namespace, retention_days, email).
        db: DB session — used to write an audit record to the retention_settings table.
    Returns: the result of namespaces_service.update_namespace_retention() —
    the updated namespaces list.
    """
    return await namespaces_service.update_namespace_retention(request, db)


async def api_get_retention_settings(db: Session):
    """
    Get the current retention setting (from the DB, or the default if it's
    never been set).

    Used by: GET /v1/retention/settings
    Returns: {"retention_days", "temporal_namespace", "opensearch_index_pattern",
    "updated_by", "updated_at", "is_default"}
    """
    return await namespaces_service.get_retention_settings(db)

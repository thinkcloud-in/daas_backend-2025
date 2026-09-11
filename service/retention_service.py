"""
retention_service — service layer for backend-log (OpenSearch) and Temporal-namespace retention.

Log deletion happens inside OpenSearch itself (native ISM — Index State Management policy),
not document-by-document — because the backend-logs-* indices are daily-rotated, an entire
old index is deleted at once. The retention setting (days + temporal_namespace) is stored in
a singleton DB row (RetentionSetting). Temporal namespace retention is set separately
(outside this file, via the Temporal namespace API) — this module only keeps the DB record.
Used by: the controllers' retention-settings endpoint (a separate concern from help_support
or guacamole retention — this is for the backend application logs).
"""
import os
from typing import Optional

import httpx
from fastapi import HTTPException
from sqlalchemy.orm import Session

from models.retention_model import RetentionSetting

# Same OpenSearch instance/creds as Help & Support log search — log-reader
# now also has the delete_by_query permission (on the backend-logs-* index pattern).
_OPENSEARCH_URL      = os.getenv("OPENSEARCH_URL", "https://172.16.0.101:30920").rstrip("/")
_OPENSEARCH_USER     = os.getenv("OPENSEARCH_USER", "log-reader")
_OPENSEARCH_PASSWORD = os.getenv("OPENSEARCH_PASSWORD", "")
_OPENSEARCH_INDEX    = os.getenv("OPENSEARCH_INDEX", "backend-logs-*")

_TIMEOUT = httpx.Timeout(connect=5.0, read=60.0, write=5.0, pool=5.0)

DEFAULT_RETENTION_DAYS = 30

# The backend-logs-* indices are already daily-rotated (backend-logs-YYYY.MM.dd),
# so OpenSearch's native ISM (Index State Management) fits directly here -- an
# entire old index is deleted at once, no document-by-document scan/delete is
# needed, and it runs entirely inside OpenSearch (ISM keeps doing its job even
# when our backend/Temporal is down).
_ISM_POLICY_ID = "backend-logs-retention"


def _ism_policy_body(retention_days: int) -> dict:
    """Build the OpenSearch ISM policy document: an index transitions from the "hot" state to the "delete" state `retention_days` days later."""
    delete_state = f"delete_after_{retention_days}d"
    return {
        "policy": {
            "description": f"Delete {_OPENSEARCH_INDEX} indices older than {retention_days} days",
            "default_state": "hot",
            "states": [
                {
                    "name": "hot",
                    "actions": [],
                    "transitions": [
                        {"state_name": delete_state, "conditions": {"min_index_age": f"{retention_days}d"}}
                    ],
                },
                {"name": delete_state, "actions": [{"delete": {}}], "transitions": []},
            ],
            "ism_template": {"index_patterns": [_OPENSEARCH_INDEX], "priority": 100},
        }
    }


def ensure_ism_retention_policy(retention_days: int) -> dict:
    """
    Create/update the OpenSearch ISM policy so that backend-logs-* indices
    delete themselves as soon as they are older than retention_days. Both new
    indices (via ism_template) and already-existing indices (via the add +
    change_policy calls) come under this policy.
    Returns: OpenSearch's PUT-policy response (JSON dict).
    Raises: HTTPException 502 if OpenSearch is unreachable or any step fails.
    """
    get_url = f"{_OPENSEARCH_URL}/_plugins/_ism/policies/{_ISM_POLICY_ID}"
    auth = (_OPENSEARCH_USER, _OPENSEARCH_PASSWORD)

    try:
        get_resp = httpx.get(get_url, auth=auth, timeout=_TIMEOUT, verify=False)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OpenSearch unreachable ({_OPENSEARCH_URL}): {e}")

    policy_body = _ism_policy_body(retention_days)

    if get_resp.status_code == 200:
        existing = get_resp.json()
        put_url = f"{get_url}?if_seq_no={existing['_seq_no']}&if_primary_term={existing['_primary_term']}"
    elif get_resp.status_code == 404:
        put_url = get_url
    else:
        raise HTTPException(
            status_code=502,
            detail=f"OpenSearch ISM policy lookup failed {get_resp.status_code}: {get_resp.text[:300]}",
        )

    put_resp = httpx.put(put_url, json=policy_body, auth=auth, timeout=_TIMEOUT, verify=False)
    if not put_resp.is_success:
        raise HTTPException(
            status_code=502,
            detail=f"OpenSearch ISM policy update failed {put_resp.status_code}: {put_resp.text[:300]}",
        )

    # Attach new (so far unmanaged) indices to the policy.
    add_url = f"{_OPENSEARCH_URL}/_plugins/_ism/add/{_OPENSEARCH_INDEX}"
    httpx.post(add_url, json={"policy_id": _ISM_POLICY_ID}, auth=auth, timeout=_TIMEOUT, verify=False)

    # Indices that are already managed by this policy also need to be
    # force-migrated to this new (updated retention_days) policy version --
    # otherwise they keep running with the old min_index_age.
    change_url = f"{_OPENSEARCH_URL}/_plugins/_ism/change_policy/{_OPENSEARCH_INDEX}"
    httpx.post(change_url, json={"policy_id": _ISM_POLICY_ID}, auth=auth, timeout=_TIMEOUT, verify=False)

    return put_resp.json()


def get_current_retention(db: Session) -> Optional[RetentionSetting]:
    """Fetch the current retention setting (singleton row). Returns None if it was never set."""
    return db.query(RetentionSetting).first()


def upsert_retention_setting(
    db: Session, retention_days: int, temporal_namespace: str, updated_by: Optional[str] = None
) -> RetentionSetting:
    """
    Singleton row — update the first row, or create it if none exists.
    Returns: RetentionSetting ORM object (created or updated).
    """
    row = db.query(RetentionSetting).first()
    if row:
        row.retention_days = retention_days
        row.temporal_namespace = temporal_namespace
        row.opensearch_index_pattern = _OPENSEARCH_INDEX
        row.updated_by = updated_by
    else:
        row = RetentionSetting(
            retention_days=retention_days,
            temporal_namespace=temporal_namespace,
            opensearch_index_pattern=_OPENSEARCH_INDEX,
            updated_by=updated_by,
        )
        db.add(row)
    db.commit()
    db.refresh(row)
    return row

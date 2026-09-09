import os
from typing import Optional

import httpx
from fastapi import HTTPException
from sqlalchemy.orm import Session

from models.retention_model import RetentionSetting

# Same OpenSearch instance/creds as Help & Support log search — log-reader
# ab delete_by_query permission bhi rakhta hai (backend-logs-* index pattern pe).
_OPENSEARCH_URL      = os.getenv("OPENSEARCH_URL", "https://172.16.0.101:30920").rstrip("/")
_OPENSEARCH_USER     = os.getenv("OPENSEARCH_USER", "log-reader")
_OPENSEARCH_PASSWORD = os.getenv("OPENSEARCH_PASSWORD", "")
_OPENSEARCH_INDEX    = os.getenv("OPENSEARCH_INDEX", "backend-logs-*")

_TIMEOUT = httpx.Timeout(connect=5.0, read=60.0, write=5.0, pool=5.0)

DEFAULT_RETENTION_DAYS = 30

# backend-logs-* indices already daily-rotated hain (backend-logs-YYYY.MM.dd),
# isliye OpenSearch ka native ISM (Index State Management) yahan seedha fit
# baithta hai -- poori purani index ek saath delete ho jaati hai, koi
# document-by-document scan/delete nahi karna padta, aur ye poori tarah
# OpenSearch ke andar hi chalta hai (hamara backend/Temporal down ho tab bhi
# ISM apna kaam karta rehta hai).
_ISM_POLICY_ID = "backend-logs-retention"


def _ism_policy_body(retention_days: int) -> dict:
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
    OpenSearch ISM policy create/update karo taaki backend-logs-* indices
    retention_days se purani hote hi khud delete ho jayein. Naye indices
    (ism_template ke through) aur already-existing indices (add + change_policy
    calls ke through) dono is policy ke ander aa jate hain.
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

    # Naye (abhi tak unmanaged) indices ko policy se jodo.
    add_url = f"{_OPENSEARCH_URL}/_plugins/_ism/add/{_OPENSEARCH_INDEX}"
    httpx.post(add_url, json={"policy_id": _ISM_POLICY_ID}, auth=auth, timeout=_TIMEOUT, verify=False)

    # Jo indices pehle se hi is policy se managed hain, unhe bhi is naye
    # (updated retention_days wale) policy version pe force-migrate karo --
    # warna wo purane min_index_age ke saath hi chalte rahenge.
    change_url = f"{_OPENSEARCH_URL}/_plugins/_ism/change_policy/{_OPENSEARCH_INDEX}"
    httpx.post(change_url, json={"policy_id": _ISM_POLICY_ID}, auth=auth, timeout=_TIMEOUT, verify=False)

    return put_resp.json()


def get_current_retention(db: Session) -> Optional[RetentionSetting]:
    return db.query(RetentionSetting).first()


def upsert_retention_setting(
    db: Session, retention_days: int, temporal_namespace: str, updated_by: Optional[str] = None
) -> RetentionSetting:
    """Singleton row — pehli hi row ko update karo, ya na ho to banao."""
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

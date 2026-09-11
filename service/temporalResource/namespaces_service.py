import asyncio
from datetime import datetime
import subprocess
from fastapi import HTTPException
from typing import Optional
from pydantic import BaseModel
import re
import subprocess
import json
from typing import Optional
import json
import os



class CreateNamespaceRequest(BaseModel):
    namespace: str
    retention_days: int
    description: Optional[str] = ""

class UpdateRetentionRequest(BaseModel):
    retention_days: int
    email: str

# The namespace is always the static "default" -- for both Temporal and OpenSearch.
# The frontend never sends a namespace.
DEFAULT_NAMESPACE = "default"

from sqlalchemy.orm import Session

from service.temporalResource.workflows import workflows_retentionPeriod
from utils.temporal_client import TemporalClientManager
from service import retention_service

def unique_id():
    unique_id = datetime.now()
    return f"{unique_id.hour }:{unique_id.minute}:{unique_id.second}"

    



def parse_ttl_to_seconds(ttl: str) -> int:

    if ttl.endswith("s") and ttl[:-1].isdigit():
        return int(ttl[:-1])
    
    match = re.match(r"(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?", ttl)
    if match:
        d, h, m, s = match.groups()
        d = int(d) if d else 0
        h = int(h) if h else 0
        m = int(m) if m else 0
        s = int(s) if s else 0
        return d * 86400 + h * 3600 + m * 60 + s

    return 0

def format_retention_as_days(ttl: str) -> str:
    
    seconds = parse_ttl_to_seconds(ttl)
    days = seconds // 86400
    return f"{days} day{'s' if days != 1 else ''}"

async def get_current_retention_days(namespace: str) -> Optional[int]:
   
    try:
        result = subprocess.run(
            [
                "temporal",
                "operator", "namespace", "describe",
                "--namespace", namespace,
                "--output", "json"
            ],
            capture_output=True,
            text=True,
            check=True
        )
        namespace_info = json.loads(result.stdout)

        ttl = namespace_info["config"].get("workflowExecutionRetentionTtl", "0s")
        return parse_ttl_to_seconds(ttl) // 86400  # return days
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        # FileNotFoundError: 'temporal' CLI is not installed / not on PATH on this host.
        # Non-critical -- this value is only used for a log message.
        return None





async def list_namespaces():
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    if client is None:
        return 



    
    handle = await client.start_workflow(
        workflows_retentionPeriod.GetNamespacesWorkflow.run,
        id=f"list-namespaces-workflow-{uniqueId}",
        task_queue="namespace-tasks"
    )
    namespaces = await handle.result()
    return namespaces

async def update_namespace_retention(request: UpdateRetentionRequest, db: Session):
    """
    A single call sets BOTH retentions:
      1. The Temporal namespace's own workflowExecutionRetentionTtl (existing)
      2. The ISM retention policy for OpenSearch (backend-logs-*) -- OpenSearch
         itself checks this policy periodically and deletes old indices, no
         Temporal schedule/worker is needed. It is also recorded in the DB
         (retention_settings), because OpenSearch has no direct way to query
         "what is currently set".
    """
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    if client is None:

        return

    current_retention = await get_current_retention_days(DEFAULT_NAMESPACE)
    action_message = f"Retention-Updation ({current_retention}d - {request.retention_days}d)"



    handle = await client.start_workflow(
        workflows_retentionPeriod.UpdateRetentionWorkflow.run,
        args=[DEFAULT_NAMESPACE, request.retention_days],
        id=f"update-retention-{DEFAULT_NAMESPACE}-{uniqueId}",
        task_queue="update-Retention-tasks",
        search_attributes={
            "Entity": [DEFAULT_NAMESPACE],
            "Action": [action_message],
            "UserName": [request.email]
        },
    )
    namespaces = await handle.result()

    # OpenSearch side — create/update the ISM policy, and record it in the DB
    retention_service.ensure_ism_retention_policy(request.retention_days)
    retention_service.upsert_retention_setting(
        db, request.retention_days, DEFAULT_NAMESPACE, updated_by=request.email,
    )

    return namespaces


async def get_retention_settings(db: Session) -> dict:
    """Current retention config — from the DB (defaults to 30 days if it was never set)."""
    row = retention_service.get_current_retention(db)
    if not row:
        return {
            "retention_days": retention_service.DEFAULT_RETENTION_DAYS,
            "temporal_namespace": None,
            "opensearch_index_pattern": retention_service._OPENSEARCH_INDEX,
            "updated_by": None,
            "updated_at": None,
            "is_default": True,
        }
    return {
        "retention_days": row.retention_days,
        "temporal_namespace": row.temporal_namespace,
        "opensearch_index_pattern": row.opensearch_index_pattern,
        "updated_by": row.updated_by,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        "is_default": False,
    }

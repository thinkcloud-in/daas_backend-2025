from typing import Any, Dict, List
from temporalio import activity
from temporalio.api.workflowservice.v1 import (
    UpdateNamespaceRequest,
    ListNamespacesRequest,
)
from temporalio.api.namespace.v1 import NamespaceConfig
from google.protobuf.duration_pb2 import Duration

from utils.temporal_client import TemporalClientManager

# These activities used to call a separate wrapper HTTP API
# (TEMPORAL_SERVER_ADDRESS) which itself ran a `temporal operator namespace
# update`-type command to change namespace retention. That service turned out
# to be down/unreachable. Temporal's own Python SDK client (already connected
# to TEMPORAL_SERVER) exposes these UpdateNamespace/ListNamespaces RPCs
# directly -- so no external wrapper dependency is needed any more, we use the
# already-connected client directly.


@activity.defn
async def list_namespaces_activity() -> Dict[str, Any]:
    client = await TemporalClientManager.get_temporal_client()

    response = await client.workflow_service.list_namespaces(
        ListNamespacesRequest()
    )

    namespaces: List[Dict[str, Any]] = []
    for ns in response.namespaces:
        retention_seconds = ns.config.workflow_execution_retention_ttl.ToSeconds()
        namespaces.append({
            "namespace": ns.namespace_info.name,
            "retentionDays": retention_seconds // 86400,
        })

    return {"namespaces": namespaces}


@activity.defn
async def update_retention_activity(namespace: str, retention_days: int) -> Dict[str, Any]:
    client = await TemporalClientManager.get_temporal_client()

    retention_duration = Duration(seconds=retention_days * 86400)

    await client.workflow_service.update_namespace(
        UpdateNamespaceRequest(
            namespace=namespace,
            config=NamespaceConfig(workflow_execution_retention_ttl=retention_duration),
        )
    )

    return {
        "newRetentionPeriod": retention_days,
        "message": f"Retention for namespace '{namespace}' updated to {retention_days} days.",
        "namespaceName": namespace,
    }

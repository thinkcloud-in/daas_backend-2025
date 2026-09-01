from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from service.temporalResource.activity.activities_connect_keycloak import (
        connect_keycloak_activity,
        disconnect_keycloak_activity,
    )

logger = workflow.logger
@workflow.defn(name="ConnectKeycloakWorkflow")
class ConnectKeycloakWorkflow:

    @workflow.run
    async def run(self, payload: dict) -> dict:
        workflow.logger.info(
            f"[ConnectKeycloak] Starting — openwebui_id={payload.get('openwebui_id')} "
            f"realm={payload.get('kc_realm')} client={payload.get('client_id')}"
        )
        result = await workflow.execute_activity(
            connect_keycloak_activity,
            payload,
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        workflow.logger.info(
            f"[ConnectKeycloak] Done — status={result.get('status')} "
            f"rollout_ok={result.get('rollout_ok')} pg_updated={result.get('pg_updated')}"
        )
        return result


@workflow.defn(name="DisconnectKeycloakWorkflow")
class DisconnectKeycloakWorkflow:

    @workflow.run
    async def run(self, payload: dict) -> dict:
        workflow.logger.info(
            f"[DisconnectKeycloak] Starting — openwebui_id={payload.get('openwebui_id')}"
        )
        result = await workflow.execute_activity(
            disconnect_keycloak_activity,
            payload,
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        workflow.logger.info(
            f"[DisconnectKeycloak] Done — status={result.get('status')} "
            f"rollout_ok={result.get('rollout_ok')} users_deleted={result.get('users_deleted')}"
        )
        return result

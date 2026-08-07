import logging
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from service.temporalResource.activity.activities_connect_llm import (
        connect_llm_activity,
        disconnect_llm_activity,
    )

logger = logging.getLogger(__name__)


@workflow.defn(name="ConnectLLMWorkflow")
class ConnectLLMWorkflow:

    @workflow.run
    async def run(self, payload: dict) -> dict:
        workflow.logger.info(
            f"[ConnectLLM] Starting — deploy_id={payload.get('deploy_id')} "
            f"dep_name={payload.get('dep_name')}"
        )
        result = await workflow.execute_activity(
            connect_llm_activity,
            payload,
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        workflow.logger.info(
            f"[ConnectLLM] Done — status={result.get('status')} rollout_ok={result.get('rollout_ok')}"
        )
        return result


@workflow.defn(name="DisconnectLLMWorkflow")
class DisconnectLLMWorkflow:

    @workflow.run
    async def run(self, payload: dict) -> dict:
        workflow.logger.info(
            f"[DisconnectLLM] Starting — deploy_id={payload.get('deploy_id')} "
            f"disconnected_id={payload.get('disconnected_id')} dep_name={payload.get('dep_name')}"
        )
        result = await workflow.execute_activity(
            disconnect_llm_activity,
            payload,
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        workflow.logger.info(
            f"[DisconnectLLM] Done — status={result.get('status')} rollout_ok={result.get('rollout_ok')}"
        )
        return result

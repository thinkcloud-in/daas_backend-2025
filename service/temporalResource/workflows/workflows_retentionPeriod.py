from typing import Any, Dict
from temporalio import workflow
from service.temporalResource.activity import activities_retentionPeriod
from temporalio.common import RetryPolicy
from datetime import timedelta

# NOTE: there is deliberately no try/except -> raise HTTPException/Exception here.
# Raising an HTTPException (or any custom re-wrapped Exception) from inside a
# Temporal workflow makes Temporal treat it as a "Workflow Task Failure"
# (not a business/activity failure) -- and the Temporal server retries
# Workflow Task Failures forever with no limit (no maximum_attempts cap), so
# the workflow never terminates in a "Failed" state and loops forever instead.
# Letting execute_activity()'s ActivityError propagate up as-is is the
# safe/correct approach -- Temporal itself converts it into a clean, single
# WorkflowExecutionFailed.


@workflow.defn(sandboxed=False)
class GetNamespacesWorkflow:
    @workflow.run
    async def run(self) -> Dict[str, Any]:
        # Fail fast/bounded (~30-45s max) when the TEMPORAL_SERVER_ADDRESS
        # wrapper API is unreachable -- previously, with 5 attempts x 60s
        # timeout, the request stayed "pending" for 4+ minutes.
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=10),
            maximum_attempts=3,
        )
        return await workflow.execute_activity(
            activities_retentionPeriod.list_namespaces_activity,
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(seconds=20),
        )


@workflow.defn(sandboxed=False)
class UpdateRetentionWorkflow:

    @workflow.run
    async def run(self, namespace: str, retention_days: int) -> Dict[str, Any]:
        # Fail fast/bounded (~30-45s max) when the TEMPORAL_SERVER_ADDRESS
        # wrapper API is unreachable -- previously, with 5 attempts x 60s
        # timeout, the request stayed "pending" for 4+ minutes.
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=10),
            maximum_attempts=3,
        )
        return await workflow.execute_activity(
            activities_retentionPeriod.update_retention_activity,
            retry_policy=retry_policy,
            args=[namespace, retention_days],
            start_to_close_timeout=timedelta(seconds=20),
        )
from typing import Any, Dict
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from service.temporalResource.activity import activities_guacamole_retention

# NOTE: there is deliberately no try/except -> raise HTTPException/Exception
# here. Raising an HTTPException (or any re-wrapped Exception) from inside a
# Temporal workflow makes Temporal treat it as a "Workflow Task Failure"
# (not a business failure) -- and it then retries forever with no limit, the
# workflow never terminates (this very bug once put the Temporal-retention
# feature into an infinite loop). Letting execute_activity()'s ActivityError
# propagate as-is is the safe approach -- Temporal itself turns it into a
# clean WorkflowExecutionFailed.


@workflow.defn(sandboxed=False)
class GuacamoleRetentionWorkflow:
    """Runs from a daily schedule (the schedule is set up in
    guacamole_retention_controller.py) — deletes both Guacamole
    connection-history and recordings data older than retention_days."""

    @workflow.run
    async def run(self, retention_days: int) -> Dict[str, Any]:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=5),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=60),
            maximum_attempts=3,
        )
        return await workflow.execute_activity(
            activities_guacamole_retention.guacamole_retention_cleanup_activity,
            retention_days,
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(seconds=300),
        )

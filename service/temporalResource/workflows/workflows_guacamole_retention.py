from typing import Any, Dict
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from service.temporalResource.activity import activities_guacamole_retention

# NOTE: jaan-boojh kar koi try/except -> raise HTTPException/Exception nahi
# hai yahan. Temporal workflow ke andar se HTTPException (ya koi bhi
# re-wrapped Exception) raise karne se Temporal ise "Workflow Task Failure"
# samajhta hai (business failure nahi) -- aur wo bina kisi limit ke hamesha
# retry hota rehta hai, workflow kabhi terminate nahi hota (isi bug ne
# pehle Temporal-retention feature ko infinite loop me daala tha).
# execute_activity() ka ActivityError ko as-is propagate hone dena hi safe
# tarika hai -- Temporal khud isse clean WorkflowExecutionFailed bana deta hai.


@workflow.defn(sandboxed=False)
class GuacamoleRetentionWorkflow:
    """Daily schedule se chalti hai (guacamole_retention_controller.py me
    schedule setup hoti hai) — Guacamole connection-history + recordings,
    dono retention_days se purana data delete karti hai."""

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

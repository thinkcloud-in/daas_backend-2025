from typing import Any, Dict
from temporalio import workflow
from service.temporalResource.activity import activities_retentionPeriod
from temporalio.common import RetryPolicy
from datetime import timedelta

# NOTE: yaha jaan-boojh kar koi try/except -> raise HTTPException/Exception nahi hai.
# Temporal workflow ke andar se HTTPException (ya koi bhi custom re-wrapped
# Exception) raise karne se Temporal ise ek "Workflow Task Failure" samajhta
# hai (business/activity failure nahi) -- aur Workflow Task Failures ko
# Temporal server bina kisi limit ke hamesha retry karta rehta hai (koi
# maximum_attempts cap nahi), jisse workflow kabhi "Failed" state me terminate
# nahi hota, hamesha ke liye loop karta rehta hai. execute_activity() ka
# ActivityError ko as-is upar propagate hone dena hi safe/correct tarika hai --
# Temporal ise khud ek clean, single WorkflowExecutionFailed me convert kar
# deta hai.


@workflow.defn(sandboxed=False)
class GetNamespacesWorkflow:
    @workflow.run
    async def run(self) -> Dict[str, Any]:
        # TEMPORAL_SERVER_ADDRESS wrapper API unreachable hone par turant/bounded
        # fail ho jaye (~30-45s max) -- pehle 5 attempts x 60s timeout se
        # request 4+ minute tak "pending" reh jaati thi.
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
        # TEMPORAL_SERVER_ADDRESS wrapper API unreachable hone par turant/bounded
        # fail ho jaye (~30-45s max) -- pehle 5 attempts x 60s timeout se
        # request 4+ minute tak "pending" reh jaati thi.
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
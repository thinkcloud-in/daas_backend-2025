from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from service.temporalResource.activity.activities_harbor_push import harbor_push_activity


@workflow.defn(name="HarborPushWorkflow")
class HarborPushWorkflow:
    @workflow.run
    async def run(self, params: dict) -> dict:
        return await workflow.execute_activity(
            harbor_push_activity,
            params,
            schedule_to_close_timeout=timedelta(hours=2),
            start_to_close_timeout=timedelta(hours=2),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )

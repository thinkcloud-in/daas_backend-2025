from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from service.temporalResource.activity.activities_llm_push import llm_push_activity


@workflow.defn(name="LLMPushWorkflow")
class LLMPushWorkflow:
    @workflow.run
    async def run(self, params: dict) -> dict:
        return await workflow.execute_activity(
            llm_push_activity,
            params,
            schedule_to_close_timeout=timedelta(hours=3),
            start_to_close_timeout=timedelta(hours=3),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )

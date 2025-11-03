from datetime import timedelta
from temporalio import workflow
from service.temporalResource.activity import activities_pollingStatus
from temporalio.common import RetryPolicy


@workflow.defn(sandboxed=False)
class StatusPollerWorkflow:
    @workflow.run
    async def run(self):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        result = await workflow.execute_activity(
            activities_pollingStatus.poll_and_update_machine_status_activity, 
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(seconds=300),
        )

        return result
 
    


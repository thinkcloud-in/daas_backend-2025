from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from service.temporalResource.activity import activities_harbor_image as acts

_NO_RETRY = RetryPolicy(maximum_attempts=1)


@workflow.defn(sandboxed=False)
class HarborImageWorkflow:
    @workflow.run
    async def run(self, payload: dict) -> dict:
        return await workflow.execute_activity(
            acts.sftp_and_push_harbor_image_activity,
            args=[payload],
            retry_policy=_NO_RETRY,
            start_to_close_timeout=timedelta(hours=2),
            heartbeat_timeout=timedelta(minutes=10),
        )

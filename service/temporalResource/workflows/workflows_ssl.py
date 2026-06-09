from temporalio import workflow
from datetime import timedelta

from temporalio.common import RetryPolicy
with workflow.unsafe.imports_passed_through():
    from service.temporalResource.activity import activities_ssl


@workflow.defn
class SSLUploadWorkflow:
    @workflow.run
    async def run(self, payload: dict) -> dict:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )

        return await workflow.execute_activity(
            activities_ssl.upload_certificate_activity,
            payload,
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(seconds=60)
        )

@workflow.defn
class SSLRenewWorkflow:
    @workflow.run
    async def run(self, payload: dict) -> dict:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        return await workflow.execute_activity(
            activities_ssl.renew_certificate_activity,
            payload,
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(seconds=60)
        )

@workflow.defn
class SSLDeleteWorkflow:
    @workflow.run
    async def run(self) -> dict:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        return await workflow.execute_activity(
            activities_ssl.delete_certificate_activity,
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(seconds=30)
        )

@workflow.defn
class SSLGetStatusWorkflow:
    @workflow.run
    async def run(self) -> dict:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        return await workflow.execute_activity(
            activities_ssl.get_certificate_status_activity,
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(seconds=30)
        )
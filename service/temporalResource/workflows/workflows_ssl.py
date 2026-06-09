from temporalio import workflow
from datetime import timedelta
from temporalio.common import RetryPolicy
from service.temporalResource.activity import activities_ssl


@workflow.defn(sandboxed=False)
class SSLUploadWorkflow:
    @workflow.run
    async def run(self, payload: dict) -> dict:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )

        try:
            return await workflow.execute_activity(
                activities_ssl.upload_certificate_activity,
                payload,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60)
            )
        except Exception as e:
            raise Exception(f"Error in SSLUploadWorkflow: {str(e)}")

@workflow.defn(sandboxed=False)
class SSLRenewWorkflow:
    @workflow.run
    async def run(self, payload: dict) -> dict:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )

        try:
            return await workflow.execute_activity(
                activities_ssl.renew_certificate_activity,
                payload,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60)
            )
        except Exception as e:
            raise Exception(f"Error in SSLRenewWorkflow: {str(e)}")

@workflow.defn(sandboxed=False)
class SSLDeleteWorkflow:
    @workflow.run
    async def run(self) -> dict:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            return await workflow.execute_activity(
                activities_ssl.delete_certificate_activity,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=30)
            )
        except Exception as e:
            raise Exception(f"Error in SSLDeleteWorkflow: {str(e)}")

@workflow.defn(sandboxed=False)
class SSLGetStatusWorkflow:
    @workflow.run
    async def run(self) -> dict:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )

        try:
            return await workflow.execute_activity(
                activities_ssl.get_certificate_status_activity,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=30)
            )
        except Exception as e:
            raise Exception(f"Error in SSLGetStatusWorkflow: {str(e)}")
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from service.temporalResource.activity.activities_app_deploy import app_deploy_activity

logger = workflow.logger
@workflow.defn(name="AppDeployWorkflow")
class AppDeployWorkflow:

    @workflow.run
    async def run(self, payload: dict) -> dict:
        workflow.logger.info(
            f"[AppDeploy] Starting deploy_id={payload.get('deploy_id')} "
            f"ns={payload.get('namespace')} node={payload.get('node_ip')}"
        )
        result = await workflow.execute_activity(
            app_deploy_activity,
            payload,
            start_to_close_timeout=timedelta(minutes=20),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        workflow.logger.info(f"[AppDeploy] Done: {result.get('status')}")
        return result

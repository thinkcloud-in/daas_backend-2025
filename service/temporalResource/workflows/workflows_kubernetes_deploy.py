from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from service.temporalResource.activity.activities_kubernetes_deploy import (
        k8s_harbor_deploy_activity,
    )

logger = workflow.logger
@workflow.defn(name="K8sHarborDeployWorkflow")
class K8sHarborDeployWorkflow:

    @workflow.run
    async def run(self, payload: dict) -> dict:
        workflow.logger.info(
            f"[K8sHarborDeploy] Starting for deploy_id={payload.get('deploy_id')} "
            f"node={payload.get('node_ip')}"
        )
        result = await workflow.execute_activity(
            k8s_harbor_deploy_activity,
            payload,
            start_to_close_timeout = timedelta(minutes=30),
            retry_policy = RetryPolicy(maximum_attempts=1),
        )
        workflow.logger.info(f"[K8sHarborDeploy] Done: {result.get('status')}")
        return result

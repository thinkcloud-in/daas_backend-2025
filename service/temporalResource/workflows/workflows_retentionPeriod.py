from typing import Any, Dict, List
from temporalio import workflow
from service.temporalResource.activity import activities_retentionPeriod
from temporalio.common import RetryPolicy
from datetime import timedelta
from fastapi import HTTPException

    

@workflow.defn(sandboxed=False)
class GetNamespacesWorkflow:
    @workflow.run
    async def run(self) -> Dict[str, Any]:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            print('Running workflow...')
            result = await workflow.execute_activity(
                activities_retentionPeriod.list_namespaces_activity,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")
        

@workflow.defn(sandboxed=False)
class UpdateRetentionWorkflow:
   
    @workflow.run
    async def run(self, namespace: str, retention_days: int) -> Dict[str, Any]:
        # userName = pool_data.get('email')
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            print('Running workflow...')
            result = await workflow.execute_activity(
                activities_retentionPeriod.update_retention_activity,
                retry_policy=retry_policy,
                args=[namespace, retention_days],
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            action_message = f"Retention for namespace {namespace} updated to {retention_days} days."

            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")
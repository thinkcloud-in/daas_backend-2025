from datetime import timedelta
from fastapi import HTTPException
from temporalio import workflow
from temporalio.common import RetryPolicy
from service.temporalResource.activity import activities_nodes

@workflow.defn(sandboxed=False)
class GetNodesProxmoxWorkflow:
    @workflow.run
    async def run(self) -> list[dict]:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            print('Running Retrieve Pool Data Workflow...')
            result = await workflow.execute_activity(
                activities_nodes.get_all_nodes_activity,
                # args=[pool_name],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")
        
    
        
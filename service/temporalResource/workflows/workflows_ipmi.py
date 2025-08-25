from datetime import timedelta
from fastapi import HTTPException
from temporalio import workflow
from temporalio.common import RetryPolicy
from service.temporalResource.activity import activities_ipmi
@workflow.defn(sandboxed=False)
class IpmiCreationWorkflow:
    @workflow.run
    async def run(self, ipmi_data: dict) -> dict:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            print('Running workflow...')
            result = await workflow.execute_activity(
                activities_ipmi.ipmi_activity,
                ipmi_data,  
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")
@workflow.defn(sandboxed=False)
class IpmiUpdateWorkflow:
    @workflow.run
    async def run(self, ipmi_id: int, ipmi_data: dict) -> dict:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            print('Running workflow...')
            result = await workflow.execute_activity(
                activities_ipmi.update_ipmi_activity,
                args=[ipmi_id, ipmi_data],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")
@workflow.defn(sandboxed=False)
class IpmiDeletionWorkflow:
    @workflow.run
    async def run(self, ipmi_id: int) -> dict:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            print('Running workflow...')
            result = await workflow.execute_activity(
                activities_ipmi.delete_ipmi_activity,
                ipmi_id,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")      

from fastapi import HTTPException
from temporalio import workflow
from temporalio.common import RetryPolicy
from datetime import timedelta
from service.temporalResource.activity import activities_schedule



@workflow.defn(sandboxed=False)
class get_report_data_workflow:
    @workflow.run
    async def run(self) -> dict:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            
            result = await workflow.execute_activity(
                activities_schedule.get_schedule_data_activity, 
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")
        

@workflow.defn(sandboxed=False)
class get_report_data_by_id_workflow:
    @workflow.run
    async def run(self, item_id: int):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            result = await workflow.execute_activity(
                activities_schedule.get_schedule_data_by_id_activity, 
                item_id,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")
        

@workflow.defn(sandboxed=False)
class get_report_along_report_workflow:
    @workflow.run
    async def run(self, report: str, limit: int, offset: int):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            result = await workflow.execute_activity(
                activities_schedule.get_schedule_along_report_activity, 
                args=[report, limit, offset],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")
        
@workflow.defn(sandboxed=False)
class update_schedule_data_id_workflow:
    @workflow.run
    async def run(self, item_id: int, item):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            
            result = await workflow.execute_activity(
                activities_schedule.update_schedule_data_id_activity, 
                args=[item_id,item],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            return result
        except Exception as e:
            
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")
        

@workflow.defn(sandboxed=False)
class delete_schedule_data_id_workflow:
    @workflow.run
    async def run(self, item_id: int):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:            
            result = await workflow.execute_activity(
                activities_schedule.delete_schedule_data_id_activity, 
                item_id,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )            
            return result
        except Exception as e:            
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")
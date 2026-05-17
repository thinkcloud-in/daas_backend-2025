from typing import Any
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
            raise Exception(f"Error in workflow: {str(e)}")
        

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
            raise Exception(f"Error in workflow: {str(e)}")
        

@workflow.defn(sandboxed=False)
class get_report_along_report_workflow:
    @workflow.run
    async def run(self, limit_or_args: Any, offset: int = None):
        if isinstance(limit_or_args, list):
            limit, offset = limit_or_args
        else:
            limit = limit_or_args
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            result = await workflow.execute_activity(
                activities_schedule.get_schedule_along_report_activity, 
                args=[limit, offset],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            return result
        except Exception as e:
            raise Exception(f"Error in workflow: {str(e)}")
        
@workflow.defn(sandboxed=False)
class update_schedule_data_id_workflow:
    @workflow.run
    async def run(self, id_or_args: Any, item: dict = None):
        if isinstance(id_or_args, list):
            item_id, item = id_or_args
        else:
            item_id = id_or_args
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
            raise Exception(f"Error in workflow: {str(e)}")
        

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
            
            # If database deletion was successful and we have a schedule_id, delete from Temporal
            if result and isinstance(result, dict) and result.get('schedule_id'):
                await workflow.execute_activity(
                    activities_schedule.delete_temporal_schedule,
                    result['schedule_id'],
                    retry_policy=retry_policy,
                    start_to_close_timeout=timedelta(seconds=60),
                )
                
            return result
        except Exception as e:
            raise Exception(f"Error in workflow: {str(e)}")
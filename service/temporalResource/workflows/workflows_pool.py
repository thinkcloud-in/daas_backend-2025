from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy
from service.temporalResource.activity import activities_pool
# Workflows should not raise FastAPI HTTPException; re-raise original exceptions

@workflow.defn(sandboxed=False)
class PoolCreationWorkflow:
    @workflow.run
    async def run(self, pool_data: dict) -> dict:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            
            result = await workflow.execute_activity(
                activities_pool.create_pool_activity,
                pool_data,  
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            return result
        except Exception:
            # Re-raise the original exception so Temporal preserves the activity failure
            raise
        

@workflow.defn(sandboxed=False)
class PoolUpdateWorkflow:
    @workflow.run
    async def run(self, pool_id: int, pool_data: dict) -> dict:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            
            result = await workflow.execute_activity(
                activities_pool.update_pool_activity,
                args=[pool_id,pool_data],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            return result
        except Exception as e:
            return {"msg": f"Pool update failed: {str(e)}"}  
        

@workflow.defn(sandboxed=False)
class PoolDeletionWorkflow:
    @workflow.run
    async def run(self, pool_id: int) -> dict:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            
            result = await workflow.execute_activity(
                activities_pool.delete_pool_activity,
                args=[pool_id],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            return result
        except Exception as e:
            
            raise RuntimeError(str(e))


@workflow.defn(sandboxed=False)
class RetrievePoolDataWorkflow:
    @workflow.run
    async def run(self, pool_name: str) -> dict:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            
            result = await workflow.execute_activity(
                activities_pool.retrieve_pool_data_activity,
                args=[pool_name],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            return result
        except Exception:
             raise Exception(500, detail="Error in workflow")
        
    
@workflow.defn(sandboxed=False)
class GetListofPoolNamesWorkflow:
    @workflow.run
    async def run(self) :
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            
            result = await workflow.execute_activity(
                activities_pool.list_all_pool_names_activity,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            return result
        except Exception:
             raise Exception(500, detail="Error in workflow")
        

@workflow.defn(sandboxed=False)
class get_all_pools_workflow:
    @workflow.run
    async def run(self):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            
            result = await workflow.execute_activity(
                activities_pool.get_all_pools_activity,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            return result
        except Exception:
             raise Exception(500, detail="Error in workflow")
        

@workflow.defn(sandboxed=False)
class get_pool_details_ID_workflow:
    @workflow.run
    async def run(self, pool_id: int):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            
            result = await workflow.execute_activity(
                activities_pool.get_pool_details_id_activity,
                args=[pool_id],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            return result
        except Exception as e:
            
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")

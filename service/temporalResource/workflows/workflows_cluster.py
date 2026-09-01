from temporalio import workflow
from temporalio.common import RetryPolicy
from datetime import timedelta
from service.temporalResource.activity import activities_cluster

# Logging is configured centrally in utils/logging_config.py (called from
# main.py at startup) — do not reconfigure it per-module.
logger = workflow.logger
@workflow.defn(sandboxed=False)
class CreateUserWorkflow:
    @workflow.run
    async def run(self, cluster_data: dict, root_username: str, root_password: str, creds: dict):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            logger.info("Running workflow to create user...")
            result = await workflow.execute_activity(
                activities_cluster.create_user_activity,
                args=[cluster_data, root_username, root_password, creds],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            logger.info("Workflow completed successfully.")
            return result
        except Exception as e:
            logger.error(f"Error occurred in workflow: {str(e)}", exc_info=True)
            raise e
        
@workflow.defn(sandboxed=False)
class AssignRoleToUserWorkflow:
    @workflow.run
    async def run(self, cluster_data: dict,role: str, path: str, root_username: str, root_password: str, creds: dict):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            logger.info("Running workflow to create user...")
            result = await workflow.execute_activity(
                activities_cluster.Assign_role_to_user_activity,
                args=[cluster_data, role,path,root_username, root_password, creds],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            logger.info("Workflow completed successfully.")
            return result
        except Exception as e:
            logger.error(f"Error occurred in workflow: {str(e)}", exc_info=True)
            raise e

@workflow.defn(sandboxed=False)
class CreateClusterWorkflow:
    @workflow.run
    async def run(self, cluster_data: dict):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        
        try:
            logger.info("Running workflow to create cluster...")
            result = await workflow.execute_activity(
                activities_cluster.create_cluster_activity,
                args=[cluster_data],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            logger.info("Workflow completed successfully.")
            return result
        
        except Exception as e:
            logger.error(f"Error occurred in workflow: {str(e)}", exc_info=True)
            raise e



@workflow.defn(sandboxed=False)
class DeleteClusterWorkflow:
    # Entry point for the workflow
    @workflow.run
    async def run(self, cluster_id: str):
        # Define retry policy
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        
        try:
            logger.info("Running workflow to create cluster...")

            # Execute the activity with retry policy
            result = await workflow.execute_activity(
                activities_cluster.delete_cluster_activity,
                cluster_id,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            logger.info("Workflow completed successfully.")
            return result
        
        except Exception as e:
            logger.error(f"Error occurred in workflow: {str(e)}", exc_info=True)
            raise e

@workflow.defn(sandboxed=False)
class UpdateClusterWorkflow:
    # Entry point for the workflow
    @workflow.run
    async def run(self, cluster_data: dict, cluster_id: str):
        # Define retry policy
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        
        try:
            logger.info("Running workflow to create cluster...")

            # Execute the activity with retry policy
            result = await workflow.execute_activity(
                activities_cluster.update_cluster_activity,
                args=[cluster_data,cluster_id],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            logger.info("Workflow completed successfully.")
            return result
        
        except Exception as e:
            logger.error(f"Error occurred in workflow: {str(e)}", exc_info=True)
            raise e







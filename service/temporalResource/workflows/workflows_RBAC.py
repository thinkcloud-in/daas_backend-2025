from datetime import datetime, timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy
from service.temporalResource.activity import activities_RBAC


@workflow.defn(sandboxed=False)
class GetClientWorkflow:
    # Entry point for the workflow
    @workflow.run
    async def run(self):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2), 
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30), 
            maximum_attempts=5,
        )
        try:
            # Execute the activity with retry policy
            print('Running workflow...')
            result = await workflow.execute_activity(
                activities_RBAC.get_client_activity,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise e
        

@workflow.defn(sandboxed=False)
class GetClientRolesWorkflow:
    @workflow.run
    async def run(self):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2), 
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30), 
            maximum_attempts=5,
        )
        try:
            # Execute the activity with retry policy
            print('Running workflow...')
            result = await workflow.execute_activity(
                activities_RBAC.get_client_roles_activity,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise e
        
@workflow.defn(sandboxed=False)
class CreatingRoleWorkflow:
    @workflow.run
    async def run(self,role_name: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2), 
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30), 
            maximum_attempts=5,
        )
        try:
            # Execute the activity with retry policy
            print('Running workflow...')
            result = await workflow.execute_activity(
                activities_RBAC.creating_role_activity,
                role_name,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise e


@workflow.defn(sandboxed=False)
class DeletingRoleWorkflow:
    @workflow.run
    async def run(self,role_name: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2), 
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30), 
            maximum_attempts=5,
        )
        try:
            # Execute the activity with retry policy
            print('Running workflow...')
            result = await workflow.execute_activity(
                activities_RBAC.deleting_role_activity,
                role_name,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise e
        

@workflow.defn(sandboxed=False)
class UpdateRoleComponentWorkflow:
    @workflow.run
    async def run(self,request: dict):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2), 
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30), 
            maximum_attempts=5,
        )
        try:
            # Execute the activity with retry policy
            print('Running workflow...')
            result = await workflow.execute_activity(
                activities_RBAC.updating_role_component_activity,
                request,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise e
        

@workflow.defn(sandboxed=False)
class GetRoleComponentWorkflow:
    @workflow.run
    async def run(self,role: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2), 
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30), 
            maximum_attempts=5,
        )
        try:
            # Execute the activity with retry policy
            print('Running workflow...')
            result = await workflow.execute_activity(
                activities_RBAC.getting_role_components_activity,
                role,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise e
        

@workflow.defn(sandboxed=False)
class AssignUserRoleworkflow:
    @workflow.run
    async def run(self,request :dict):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2), 
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30), 
            maximum_attempts=5,
        )
        try:
            # Execute the activity with retry policy
            print('Running workflow...')
            result = await workflow.execute_activity(
                activities_RBAC.assign_user_role_activity,
                request,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise e
        

@workflow.defn(sandboxed=False)
class GetUserPermissionsWorkflow:
    @workflow.run
    async def run(self,username: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2), 
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30), 
            maximum_attempts=5,
        )
        try:
            # Execute the activity with retry policy
            print('Running workflow...')
            result = await workflow.execute_activity(
                activities_RBAC.get_user_permissions_activity,
                username,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise e
        

@workflow.defn(sandboxed=False)
class DeleteRoleFromUserWorkflow:
    @workflow.run
    async def run(self,request: dict):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2), 
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30), 
            maximum_attempts=5,
        )
        try:
            # Execute the activity with retry policy
            print('Running workflow...')
            result = await workflow.execute_activity(
                activities_RBAC.delete_role_from_user_activity,
                request,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise e
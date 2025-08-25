from fastapi import HTTPException
from temporalio import workflow
from temporalio.common import RetryPolicy
from datetime import timedelta

from service.temporalResource.activity import activities_ldap

@workflow.defn(sandboxed=False)
class ad_ldap_configuration_workflow:
    @workflow.run
    async def run(self, ldap_data: dict) -> dict:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            print('Running ad_ldap_configuration_workflow...')
            result = await workflow.execute_activity(
                activities_ldap.ad_ldap_configuration_activity,
                ldap_data,  
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")
        
@workflow.defn(sandboxed=False)
class get_LDAPs_from_keycloak_workflow:
    @workflow.run
    async def run(self):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            print('Running get_lDAPS_from_keycloak_workflow...')
            result = await workflow.execute_activity(
                activities_ldap.get_LDAPs_from_keycloak_activity,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")
        
        
@workflow.defn(sandboxed=False)
class test_ldap_connection_workflow:
    @workflow.run
    async def run(self, ldap_data: dict):  
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            print('Running test_ldap_connection_workflow...')
            result = await workflow.execute_activity(
                activities_ldap.test_ldap_connection_activity,
                ldap_data,  
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")
        
@workflow.defn(sandboxed=False)
class test_ldap_authentication_workflow:
    @workflow.run
    async def run(self, ldap_data: dict):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            print('Running test_ldap_authentication_workflow...')
            result = await workflow.execute_activity(
                activities_ldap.test_ldap_authentication_activity,
                ldap_data,  
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")
        

@workflow.defn(sandboxed=False)
class delete_ldap_config_workflow:
    @workflow.run
    async def run(self, ldap_id: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            print('Running delete_ldap_config_workflow...')
            result = await workflow.execute_activity(
                activities_ldap.delete_ldap_config_activity,
                ldap_id,  
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")
        

@workflow.defn(sandboxed=False)
class get_LDAP_by_id_workflow:
    @workflow.run
    async def run(self, ldap_id: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            print('Running get_ldap_by_id_workflow...')
            result = await workflow.execute_activity(
                activities_ldap.get_LDAP_by_id_activity,
                ldap_id,  
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")
        

@workflow.defn(sandboxed=False)
class sync_user_from_keycloak_Byid_workflow:
    @workflow.run
    async def run(self, ldap_id: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            print('Running sync_user_from_keycloak_Byid_workflow...')
            result = await workflow.execute_activity(
                activities_ldap.sync_user_from_keycloak_Byid_activity,
                ldap_id,  
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")
        

@workflow.defn(sandboxed=False)
class sync_changed_users_from_keycloak_workflow:
    @workflow.run
    async def run(self, ldap_id:str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            print('Running sync_changed_users_from_keycloak_workflow...')
            result = await workflow.execute_activity(
                activities_ldap.sync_changed_users_from_keycloak_activity,
                ldap_id,  
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")
        

@workflow.defn(sandboxed=False)
class unlink_users_from_keycloak_workflow:
    @workflow.run
    async def run(self, ldap_id: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            print('Running unlink_users_from_keycloak_workflow...')
            result = await workflow.execute_activity(
                activities_ldap.unlink_users_from_keycloak_activity,
                ldap_id,  
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")
        

@workflow.defn(sandboxed=False)
class remove_imported_users_from_keycloak_workflow:
    @workflow.run
    async def run(self, ldap_id: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            print('Running remove_imported_users_from_keycloak_workflow...')
            result = await workflow.execute_activity(
                activities_ldap.remove_imported_users_from_keycloak_activity,
                ldap_id,  
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")
        

@workflow.defn(sandboxed=False)
class update_ldap_config_workflow:
    @workflow.run
    async def run(self, ldap_data: dict,ldap_id: str,):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            print('Running update_ldap_config_workflow...')
            result = await workflow.execute_activity(
                activities_ldap.update_ldap_config_activity,
                args=[ldap_data,ldap_id,] ,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            raise HTTPException(status_code=500, detail=f"Error in workflow: {str(e)}")
    
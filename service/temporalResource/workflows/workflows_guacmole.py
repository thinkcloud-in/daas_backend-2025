from datetime import datetime, timedelta
from typing import Dict, List
from temporalio import workflow
from temporalio.common import RetryPolicy
from models.models import CreateMachineBase
from service.temporalResource.activity import activities_guacmole
@workflow.defn(sandboxed=False)
class LoginWorkflow:
    # Entry point for the workflow
    @workflow.run
    async def run(self) -> str:
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
                activities_guacmole.login_with_guacamole_activity,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            auth_token = result.get("authToken")
            print('Workflow completed.')
            return auth_token
        except Exception as e:
            print(f"Error in workflow: {e}")
            return None
        

@workflow.defn(sandboxed=False)
class ListOfGuacoUsersWorkflow:
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
                activities_guacmole.list_of_guaco_users_activity,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            return None

@workflow.defn(sandboxed=False)
class GetUserlistFromKeycloakWorkflow:
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
                activities_guacmole.get_userlist_from_keycloak_activity,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            return None
    
@workflow.defn(sandboxed=False)
class ListOfMachinesWorkflow:
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
                activities_guacmole.list_of_machine_activity,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            return None
        
from dto.machineDto import MachineDto

@workflow.defn(sandboxed=False)
class CreateMachineWorkflow:
    @workflow.run
    async def run(self,machine_data:CreateMachineBase):
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
                activities_guacmole.creating_machine_activity,
                args=[machine_data],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            return None
    
@workflow.defn(sandboxed=False)
class GetSessionReportWorkflow:
    @workflow.run
    async def run(self,start_date_range: str, end_date_range: str):
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
                activities_guacmole.get_session_reports_activity,
                args=[start_date_range, end_date_range],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            return None
        

@workflow.defn(sandboxed=False)
class GetAllUsersVamanitWorkflow:
    @workflow.run
    async def run(self,session_reports:list):
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
                activities_guacmole.get_all_users_vamanit_activity,
                args=[session_reports],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            return None
        

@workflow.defn(sandboxed=False)
class GetPerticularUserSessionReportWorkflow:
    @workflow.run
    async def run(self,session_reports:list, username:str):
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
                activities_guacmole.get_perticular_user_session_report_activity,
                args=[session_reports, username],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            return None
        
@workflow.defn(sandboxed=False)
class GetDailyReportsWorkflow:
    @workflow.run
    async def run(self,session_reports:list):
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
                activities_guacmole.get_daily_reports_activity,
                args=[session_reports],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            return None
        

@workflow.defn(sandboxed=False)
class getPerticularUserDailyReportWorkflow:
    @workflow.run
    async def run(self,daily_reports : list, username:str):
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
                activities_guacmole.get_perticular_user_daily_report_activity,
                args=[daily_reports,username],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            return None
        

@workflow.defn(sandboxed=False)
class GetCompaniesWorkflow:
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
                activities_guacmole.get_companies_activity,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            return None

@workflow.defn(sandboxed=False)
class GetCompaniesByReportNameWorkflow:
    @workflow.run
    async def run(self,report_type : str):
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
                activities_guacmole.get_companies_by_report_type_activity,
                report_type,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            return None
        

@workflow.defn(sandboxed=False)
class DeleteReportWorkflow:
    @workflow.run
    async def run(self,report_type:str):
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
                activities_guacmole.delete_report_activity,
                report_type,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            return None
        

@workflow.defn(sandboxed=False)
class UpdateReportWorkflow:
    @workflow.run
    async def run(self,company_name: str, company_logo: bytes, report_type: str):
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
                activities_guacmole.update_report_activity,
                args = [company_name, company_logo, report_type],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            return None
        
@workflow.defn(sandboxed=False)
class InsertReportWorkflow:
    @workflow.run
    async def run(self,company_name: str, company_logo: bytes, report_type: str):
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
                activities_guacmole.insert_report_activity,
                args = [company_name, company_logo, report_type],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            return None
        

@workflow.defn(sandboxed=False)
class GenerateReportWorkflow:
    @workflow.run
    async def run(self,start_date: str, end_date: str, report_type: str):
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
                activities_guacmole.generate_report_activity,
                args = [start_date, end_date, report_type],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            return None
        


@workflow.defn(sandboxed=False)
class GenerateUserBasedReportWorkflow:
    @workflow.run
    async def run(self,start_date: str, end_date: str, report_type: str,username:str):
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
                activities_guacmole.generate_user_based_report_activity,
                args = [start_date, end_date, report_type, username],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            return None
        

@workflow.defn(sandboxed=False)
class GetUsersTotalDurationWithinTimerangeWorkflow:
    @workflow.run
    async def run(self,day_duration: List[Dict]):
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
                activities_guacmole.get_users_total_duration_within_timerange_activity,
                day_duration,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            return None
        

@workflow.defn(sandboxed=False)
class ConsolidateReportPerticularUserWorkflow:
    @workflow.run
    async def run(self,user_total_duration, user):
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
                activities_guacmole.consolidate_report_perticular_user_activity,
                args = [user_total_duration, user],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            return None
        

@workflow.defn(sandboxed=False)
class GetGuacamoleHistoryWorkflow:
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
                activities_guacmole.get_guacamole_history_activity,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            return None
        

@workflow.defn(sandboxed=False)
class GetGuacamoleActiveSessionsWorkflow:
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
                activities_guacmole.get_guacamole_ActiveSessions_activity,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            print('Workflow completed.')
            return result
        except Exception as e:
            print(f"Error in workflow: {e}")
            return None
 
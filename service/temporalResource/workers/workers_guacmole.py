import os
import asyncio
from temporalio.worker import Worker
from service.temporalResource.activity import activities_guacmole
from service.temporalResource.workflows import workflows_guacmole
from utils.temporal_client import TemporalClientManager

async def combined_guac_worker():
    client = await TemporalClientManager.get_temporal_client()
    
    worker = Worker(
        client,
        task_queue="guacamole-combined-task-queue",
        workflows=[
            workflows_guacmole.LoginWorkflow,
            workflows_guacmole.ListOfGuacoUsersWorkflow,
            workflows_guacmole.GetUserlistFromKeycloakWorkflow,
            workflows_guacmole.ListOfMachinesWorkflow,
            workflows_guacmole.CreateMachineWorkflow,
            workflows_guacmole.GetSessionReportWorkflow,
            workflows_guacmole.GetAllUsersVamanitWorkflow,
            workflows_guacmole.GetPerticularUserSessionReportWorkflow,
            workflows_guacmole.GetDailyReportsWorkflow,
            workflows_guacmole.getPerticularUserDailyReportWorkflow,
            workflows_guacmole.GetCompaniesWorkflow,
            workflows_guacmole.GetCompaniesByReportNameWorkflow,
            workflows_guacmole.DeleteReportWorkflow,
            workflows_guacmole.UpdateReportWorkflow,
            workflows_guacmole.InsertReportWorkflow,
            workflows_guacmole.GenerateReportWorkflow,
            workflows_guacmole.GenerateUserBasedReportWorkflow,
            workflows_guacmole.GetUsersTotalDurationWithinTimerangeWorkflow,
            workflows_guacmole.ConsolidateReportPerticularUserWorkflow,
            workflows_guacmole.GetGuacamoleHistoryWorkflow,
            workflows_guacmole.GetGuacamoleActiveSessionsWorkflow,
        ],
        activities=[
            activities_guacmole.login_with_guacamole_activity,
            activities_guacmole.list_of_guaco_users_activity,
            activities_guacmole.get_userlist_from_keycloak_activity,
            activities_guacmole.list_of_machine_activity,
            activities_guacmole.creating_machine_activity,
            activities_guacmole.get_session_reports_activity,
            activities_guacmole.get_all_users_vamanit_activity,
            activities_guacmole.get_perticular_user_session_report_activity,
            activities_guacmole.get_daily_reports_activity,
            activities_guacmole.get_perticular_user_daily_report_activity,
            activities_guacmole.get_companies_activity,
            activities_guacmole.get_companies_by_report_type_activity,
            activities_guacmole.delete_report_activity,
            activities_guacmole.update_report_activity,
            activities_guacmole.insert_report_activity,
            activities_guacmole.generate_report_activity,
            activities_guacmole.generate_user_based_report_activity,
            activities_guacmole.get_users_total_duration_within_timerange_activity,
            activities_guacmole.consolidate_report_perticular_user_activity,
            activities_guacmole.get_guacamole_history_activity,
            activities_guacmole.get_guacamole_ActiveSessions_activity,
        ],
    )
    
    # We also need to map the old task queues to this combined worker if we want to keep them separate,
    # OR we can just use the combined one. 
    # Actually, for 100% safety, I will keep the separate Workers but start them ONCE in main.py.
    
    # Let's define individual workers but run them in a way that we can start them once.
    pass

async def start_all_guac_workers():
    client = await TemporalClientManager.get_temporal_client()
    
    queues = [
        ("login-task-queue", [workflows_guacmole.LoginWorkflow], [activities_guacmole.login_with_guacamole_activity]),
        ("list-guaco-users-task", [workflows_guacmole.ListOfGuacoUsersWorkflow], [activities_guacmole.list_of_guaco_users_activity]),
        ("list-guaco-users-with-keycloak-task", [workflows_guacmole.GetUserlistFromKeycloakWorkflow], [activities_guacmole.get_userlist_from_keycloak_activity]),
        ("list-of-machine", [workflows_guacmole.ListOfMachinesWorkflow], [activities_guacmole.list_of_machine_activity]),
        ("create-connection", [workflows_guacmole.CreateMachineWorkflow], [activities_guacmole.creating_machine_activity]),
        ("get_session_reports", [workflows_guacmole.GetSessionReportWorkflow], [activities_guacmole.get_session_reports_activity]),
        ("get_all_users_vamanit", [workflows_guacmole.GetAllUsersVamanitWorkflow], [activities_guacmole.get_all_users_vamanit_activity]),
        ("get_perticular_user_session_report", [workflows_guacmole.GetPerticularUserSessionReportWorkflow], [activities_guacmole.get_perticular_user_session_report_activity]),
        ("get_daily_report", [workflows_guacmole.GetDailyReportsWorkflow], [activities_guacmole.get_daily_reports_activity]),
        ("get_perticular_user_daily_reports", [workflows_guacmole.getPerticularUserDailyReportWorkflow], [activities_guacmole.get_perticular_user_daily_report_activity]),
        ("get_companies_taskqueue", [workflows_guacmole.GetCompaniesWorkflow], [activities_guacmole.get_companies_activity]),
        ("get_companies_by_report_name_taskqueue", [workflows_guacmole.GetCompaniesByReportNameWorkflow], [activities_guacmole.get_companies_by_report_type_activity]),
        ("delete_report_taskqueue", [workflows_guacmole.DeleteReportWorkflow], [activities_guacmole.delete_report_activity]),
        ("update_report_taskqueue", [workflows_guacmole.UpdateReportWorkflow], [activities_guacmole.update_report_activity]),
        ("insert_report_taskqueue", [workflows_guacmole.InsertReportWorkflow], [activities_guacmole.insert_report_activity]),
        ("generate_report_taskqueue", [workflows_guacmole.GenerateReportWorkflow], [activities_guacmole.generate_report_activity]),
        ("generate_user_based_report_taskqueue", [workflows_guacmole.GenerateUserBasedReportWorkflow], [activities_guacmole.generate_user_based_report_activity]),
        ("get_users_total_duration_within_timerange_taskqueue", [workflows_guacmole.GetUsersTotalDurationWithinTimerangeWorkflow], [activities_guacmole.get_users_total_duration_within_timerange_activity]),
        ("consolidate_report_perticular_user_taskqueue", [workflows_guacmole.ConsolidateReportPerticularUserWorkflow], [activities_guacmole.consolidate_report_perticular_user_activity]),
        ("get_guacamole_history_taskqueue", [workflows_guacmole.GetGuacamoleHistoryWorkflow], [activities_guacmole.get_guacamole_history_activity]),
        ("guacamole_active_sessions_taskqueue", [workflows_guacmole.GetGuacamoleActiveSessionsWorkflow], [activities_guacmole.get_guacamole_ActiveSessions_activity]),
    ]
    
    workers = []
    for queue_name, wfs, acts in queues:
        workers.append(Worker(client, task_queue=queue_name, workflows=wfs, activities=acts))
    
    await asyncio.gather(*[w.run() for w in workers])
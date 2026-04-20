import os
from temporalio.worker import Worker
from temporalio.client import Client
from service.temporalResource.activity import activities_guacmole
from service.temporalResource.workflows import workflows_guacmole
from utils.temporal_client import TemporalClientManager
from dotenv import load_dotenv

load_dotenv()

    


async def login_with_guacamole_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="login-task-queue",  
        workflows=[workflows_guacmole.LoginWorkflow], 
        activities=[activities_guacmole.login_with_guacamole_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        
    

async def list_of_guaco_users_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="list-guaco-users-task",
        workflows=[workflows_guacmole.ListOfGuacoUsersWorkflow], 
        activities=[activities_guacmole.list_of_guaco_users_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        

async def list_of_guaco_user_with_Keyclock_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="list-guaco-users-with-keycloak-task",
        workflows=[workflows_guacmole.GetUserlistFromKeycloakWorkflow], 
        activities=[activities_guacmole.get_userlist_from_keycloak_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        

async def list_of_machines_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="list-of-machine",
        workflows=[workflows_guacmole.ListOfMachinesWorkflow], 
        activities=[activities_guacmole.list_of_machine_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        

async def creating_machines_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="create-connection",
        workflows=[workflows_guacmole.CreateMachineWorkflow], 
        activities=[activities_guacmole.creating_machine_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        

async def get_session_report_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="get_session_reports",
        workflows=[workflows_guacmole.GetSessionReportWorkflow], 
        activities=[activities_guacmole.get_session_reports_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        

async def get_all_reports_vamanit_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="get_all_users_vamanit",
        workflows=[workflows_guacmole.GetAllUsersVamanitWorkflow], 
        activities=[activities_guacmole.get_all_users_vamanit_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e

async def get_perticular_user_session_report_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="get_perticular_user_session_report",
        workflows=[workflows_guacmole.GetPerticularUserSessionReportWorkflow], 
        activities=[activities_guacmole.get_perticular_user_session_report_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        


async def get_daily_report_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="get_daily_report",
        workflows=[workflows_guacmole.GetDailyReportsWorkflow], 
        activities=[activities_guacmole.get_daily_reports_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        

async def get_perticular_user_daily_report_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="get_perticular_user_daily_reports",
        workflows=[workflows_guacmole.getPerticularUserDailyReportWorkflow], 
        activities=[activities_guacmole.get_perticular_user_daily_report_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        

async def get_companies_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="get_companies_taskqueue",
        workflows=[workflows_guacmole.GetCompaniesWorkflow], 
        activities=[activities_guacmole.get_companies_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        

async def get_companies_by_report_name_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="get_companies_by_report_name_taskqueue",
        workflows=[workflows_guacmole.GetCompaniesByReportNameWorkflow], 
        activities=[activities_guacmole.get_companies_by_report_type_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        

async def delete_report_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="delete_report_taskqueue",
        workflows=[workflows_guacmole.DeleteReportWorkflow], 
        activities=[activities_guacmole.delete_report_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        


async def update_report_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="update_report_taskqueue",
        workflows=[workflows_guacmole.UpdateReportWorkflow], 
        activities=[activities_guacmole.update_report_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        


async def insert_report_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="insert_report_taskqueue",
        workflows=[workflows_guacmole.InsertReportWorkflow], 
        activities=[activities_guacmole.insert_report_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        


async def generate_report_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="generate_report_taskqueue",
        workflows=[workflows_guacmole.GenerateReportWorkflow], 
        activities=[activities_guacmole.generate_report_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        


async def generate_user_based_report_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="generate_user_based_report_taskqueue",
        workflows=[workflows_guacmole.GenerateUserBasedReportWorkflow], 
        activities=[activities_guacmole.generate_user_based_report_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        


async def get_users_total_duration_within_timerange_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="get_users_total_duration_within_timerange_taskqueue",
        workflows=[workflows_guacmole.GetUsersTotalDurationWithinTimerangeWorkflow], 
        activities=[activities_guacmole.get_users_total_duration_within_timerange_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        


async def consolidate_report_perticular_user_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="consolidate_report_perticular_user_taskqueue",
        workflows=[workflows_guacmole.ConsolidateReportPerticularUserWorkflow], 
        activities=[activities_guacmole.consolidate_report_perticular_user_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        



async def get_guacamole_history_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="get_guacamole_history_taskqueue",
        workflows=[workflows_guacmole.GetGuacamoleHistoryWorkflow],
        activities=[activities_guacmole.get_guacamole_history_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
        


async def get_guacamole_active_sessions_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="guacamole_active_sessions_taskqueue",
        workflows=[workflows_guacmole.GetGuacamoleActiveSessionsWorkflow],
        activities=[activities_guacmole.get_guacamole_ActiveSessions_activity],
    )
    
    try:
        await worker.run()
    except Exception as e:
        raise e

async def run_all_guacamole_workers():
    """Starts all Guacamole-related workers concurrently in the same event loop."""
    import asyncio
    
    tasks = [
        login_with_guacamole_worker(),
        list_of_guaco_users_worker(),
        list_of_guaco_user_with_Keyclock_worker(),
        list_of_machines_worker(),
        creating_machines_worker(),
        get_session_report_worker(),
        get_all_reports_vamanit_worker(),
        get_perticular_user_session_report_worker(),
        get_daily_report_worker(),
        get_perticular_user_daily_report_worker(),
        get_companies_worker(),
        get_companies_by_report_name_worker(),
        delete_report_worker(),
        update_report_worker(),
        insert_report_worker(),
        generate_report_worker(),
        generate_user_based_report_worker(),
        get_users_total_duration_within_timerange_worker(),
        consolidate_report_perticular_user_worker(),
        get_guacamole_history_worker(),
        get_guacamole_active_sessions_worker()
    ]
    
    print("Starting all Guacamole workers...")
    await asyncio.gather(*tasks)
        

       
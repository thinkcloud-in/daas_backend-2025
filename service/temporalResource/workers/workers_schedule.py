import os
from temporalio.worker import Worker
from temporalio.client import Client
from service.temporalResource.activity import activities_schedule
from service.temporalResource.workflows import workflows_schedule


async def connectionWithTemporal():
    
    try:
        client = await Client.connect(os.getenv('TEMPORAL_SERVER'))  
        
        return client
    except Exception as e:
        raise e

async def get_report_data_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="GetReportData-task-queue",
        workflows=[workflows_schedule.get_report_data_workflow], 
        activities=[activities_schedule.get_schedule_data_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e


async def get_report_data_by_id_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="GetReportDataById-task-queue",
        workflows=[workflows_schedule.get_report_data_by_id_workflow], 
        activities=[activities_schedule.get_schedule_data_by_id_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e

async def get_schedule_data_along_report_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="GetScheduleDataAlongReport-task-queue",
        workflows=[workflows_schedule.get_report_along_report_workflow], 
        activities=[activities_schedule.get_schedule_along_report_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
\

async def update_schedule_data_id_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="UpdateScheduleDataById-task-queue",
        workflows=[workflows_schedule.update_schedule_data_id_workflow], 
        activities=[activities_schedule.update_schedule_data_id_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e


async def delete_schedule_data_id_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="DeleteScheduleDataById-task-queue",
        workflows=[workflows_schedule.delete_schedule_data_id_workflow], 
        activities=[activities_schedule.delete_schedule_data_id_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e
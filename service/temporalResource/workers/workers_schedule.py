import os
from temporalio.worker import Worker
from temporalio.client import Client

from service.temporalResource.activity import activities_schedule
from service.temporalResource.workflows import workflows_schedule


async def connectionWithTemporal():
    print('Connecting to Temporal server...')
    try:
        client = await Client.connect(os.getenv('TEMPORAL_SERVER'))  
        print('Connected to Temporal server.')
        return client
    except Exception as e:
        print(f"Connection Refused to Temporal server: {e}")
        return None
async def get_report_data_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="GetReportData-task-queue",
        workflows=[workflows_schedule.get_report_data_workflow], 
        activities=[activities_schedule.get_schedule_data_activity],
    )
    print('Worker starting...')
    try:
        await worker.run()
        print("Temporal worker started. Waiting for tasks...")
    except Exception as e:
        print(f"Error in worker: {e}")


async def get_report_data_by_id_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="GetReportDataById-task-queue",
        workflows=[workflows_schedule.get_report_data_by_id_workflow], 
        activities=[activities_schedule.get_schedule_data_by_id_activity],
    )
    print('Worker starting...')
    try:
        await worker.run()
        print("Temporal worker started. Waiting for tasks...")
    except Exception as e:
        print(f"Error in worker: {e}")

async def get_schedule_data_along_report_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="GetScheduleDataAlongReport-task-queue",
        workflows=[workflows_schedule.get_report_along_report_workflow], 
        activities=[activities_schedule.get_schedule_along_report_activity],
    )
    print('Worker starting...')
    try:
        await worker.run()
        print("Temporal worker started. Waiting for tasks...")
    except Exception as e:
        print(f"Error in worker: {e}")


async def update_schedule_data_id_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="UpdateScheduleDataById-task-queue",
        workflows=[workflows_schedule.update_schedule_data_id_workflow], 
        activities=[activities_schedule.update_schedule_data_id_activity],
    )
    print('Worker starting...')
    try:
        await worker.run()
        print("Temporal worker started. Waiting for tasks...")
    except Exception as e:
        print(f"Error in worker: {e}")


async def delete_schedule_data_id_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="DeleteScheduleDataById-task-queue",
        workflows=[workflows_schedule.delete_schedule_data_id_workflow], 
        activities=[activities_schedule.delete_schedule_data_id_activity],
    )
    print('Worker starting...')
    try:
        await worker.run()
        print("Temporal worker started. Waiting for tasks...")
    except Exception as e:
        print(f"Error in worker: {e}")
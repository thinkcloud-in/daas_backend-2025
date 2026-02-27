import os
import asyncio
from temporalio.worker import Worker
from service.temporalResource.activity import activities_schedule
from service.temporalResource.workflows import workflows_schedule
from utils.temporal_client import TemporalClientManager

async def start_all_schedule_workers():
    client = await TemporalClientManager.get_temporal_client()
    
    queues = [
        ("GetReportData-task-queue", [workflows_schedule.get_report_data_workflow], [activities_schedule.get_schedule_data_activity]),
        ("GetReportDataById-task-queue", [workflows_schedule.get_report_data_by_id_workflow], [activities_schedule.get_schedule_data_by_id_activity]),
        ("GetScheduleDataAlongReport-task-queue", [workflows_schedule.get_report_along_report_workflow], [activities_schedule.get_schedule_along_report_activity]),
        ("UpdateScheduleDataById-task-queue", [workflows_schedule.update_schedule_data_id_workflow], [activities_schedule.update_schedule_data_id_activity]),
        ("DeleteScheduleDataById-task-queue", [workflows_schedule.delete_schedule_data_id_workflow], [activities_schedule.delete_schedule_data_id_activity]),
    ]
    
    workers = []
    for queue_name, wfs, acts in queues:
        workers.append(Worker(client, task_queue=queue_name, workflows=wfs, activities=acts))
    
    await asyncio.gather(*[w.run() for w in workers])
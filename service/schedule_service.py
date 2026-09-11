"""
schedule_service — service layer for report scheduling (Guacamole session reports emailed periodically).

As soon as a new schedule is created, a Temporal Schedule (recurring cron) is set up in a
background thread (`temporalService.temporal_schedules` — the asyncio event loop runs in a
separate thread so the sync `post_data()` caller is not blocked). The remaining CRUD
reads/updates/deletes all go through Temporal workflows.
Used by: controllers/schedule_controller.py.
"""
import threading
from fastapi import HTTPException
from models.schedule_model import Schdeule
from datetime import datetime
from service.temporalResource.workflows import workflows_schedule
from .temporalService import temporal_schedules
import asyncio
from models.SMTP_models import SMTP
import asyncio
from temporalio.client import (
    Client,
    ScheduleActionExecutionStartWorkflow,
)
from utils.temporal_client import TemporalClientManager
from dotenv import load_dotenv
import os

load_dotenv()
GUACAMOLE_REPORT_URL = os.getenv('GUACAMOLE_REPORT_URL')

HORIZON_REPORT_URL = os.getenv('HORIZON_REPORT_URL')

def run_temporal_schedule(*args):
    """
    Entry point for the background thread: creates its own new asyncio event loop and runs
    `temporal_schedules(*args)` (which sets up the Temporal recurring Schedule for report emailing).
    Raises: HTTPException 500 if scheduling fails (inside the thread — it does not propagate to the
            caller, only the thread crashes; `post_data` starts this fire-and-forget in a daemon thread).
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(temporal_schedules(*args))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in Temporal scheduling: {str(e)}")
    finally:
        loop.close()
def schedul_id(username: str, schedule_type: str, report_type: str):
    """Build a human-readable unique schedule ID: "{user}-{schedule-type-initial}-{report-code}-{timestamp}"."""
    date_time = datetime.now().replace(microsecond=0)
    dateTime = date_time.strftime("%Y%m%d%H-%M%S")
    
    user = username.split('@')[0]
    
    schedule = schedule_type[0]
    
    report = report_type[0] + report_type[-7]
    
    res = user + '-' + schedule + '-' + report + '-' + dateTime
    return res
def post_data(item, db):
    """
    Create a new report-schedule in the DB and kick off its Temporal recurring-schedule setup
    in a daemon background thread (non-blocking — the HTTP response does not wait for the
    schedule to be created).
    Params: item — {.userEmail,.receiverEmail (comma-separated),.reportName,.time,.schedule_date,.schedule_type}.
    Returns: Schdeule ORM object (the DB record; Temporal schedule creation may still be pending in the background).
    Raises: HTTPException 500 if the SMTP config is missing or a DB error occurs (after rollback).
    """
    try:
        scheduleId = schedul_id(
            username=item.userEmail, 
            schedule_type=item.schedule_type, 
            report_type=item.reportName
        )
        smtp_response = db.query(SMTP).first()
        if smtp_response is None:
            raise Exception("Failed to get smtp data")
        smtp_config = {
            "email": smtp_response.email,
            "userName": smtp_response.userName,
            "password": smtp_response.password,
            "serverIP": smtp_response.serverIP,
            "serverPort": smtp_response.serverPort,
            "connOption": smtp_response.connOption
        }
        
        db_item = Schdeule(
        userEmail=item.userEmail,
        receiverEmail=item.receiverEmail,
        reportName=item.reportName,
        time=item.time,
        schedule_date=item.schedule_date,
        schedule_type=item.schedule_type,
        schedule_id=scheduleId,
        )
        receiver_email = [email.strip() for email in db_item.receiverEmail.split(',')]
        PDF_API_URL = f"{GUACAMOLE_REPORT_URL}/v1/guacamole/generate_report"

        db.add(db_item)
        db.commit()
        db.refresh(db_item)
        threading.Thread(
            target=run_temporal_schedule, 
            args=(scheduleId,
                db_item.time,
                db_item.schedule_type,
                smtp_config,
                receiver_email,
                PDF_API_URL,
                db_item.reportName),
            daemon=True
        ).start()

        return db_item
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error while creating item: {str(e)}")

def unique_id():
    """Build an `HH:MM:SS` string from the current time, for use as a workflow-id suffix."""
    unique_id = datetime.now()
    return f"{unique_id.hour }:{unique_id.minute}:{unique_id.second}"


async def get_data():
    """Start `get_report_data_workflow` — lists all schedules. Returns the workflow result."""
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    
    handle = await client.start_workflow(
        workflows_schedule.get_report_data_workflow.run,
        id=f"Retrieving-schedule-data-{uniqueId}",
        task_queue="GetReportData-task-queue",
    )
    result = await handle.result()
    return result


async def get_data_id(item_id:int):
    """Start `get_report_data_by_id_workflow` — the detail of a single schedule. Returns the workflow result."""
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    
    handle = await client.start_workflow(
        workflows_schedule.get_report_data_by_id_workflow.run,
        item_id,
        id=f"Retrieving-schedule-data-{uniqueId}",
        task_queue="GetReportDataById-task-queue",
    )
    result = await handle.result()
    return result


async def get_data_report(limit: int, offset: int, db):
    """Start `get_report_along_report_workflow` — paginated schedules + their report data. Returns the workflow result."""
    uniqueID = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    
    handle = await client.start_workflow(
        workflows_schedule.get_report_along_report_workflow.run,
        args=[limit, offset],
        id=f"Retrieving-schedule-data-{uniqueID}",
        task_queue="GetScheduleDataAlongReport-task-queue",
    )
    result = await handle.result()
    return result

async def update_data_id(item_id: int, item, db):
    """
    Start `update_schedule_data_id_workflow` — updates one schedule record.
    Returns: the workflow result's "data" field.
    Raises: HTTPException with the workflow's own status_code/msg if the result code != 200; 500 on workflow-start failure.
    """
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()

    try:
        handle = await client.start_workflow(
            workflows_schedule.update_schedule_data_id_workflow.run,
            args=[item_id, item],
            id=f"Updating-schedule-data-ID-{item_id}-{uniqueId}",
            task_queue="UpdateScheduleDataById-task-queue",
        )
        result = await handle.result()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if result['code'] != 200:
        raise HTTPException(status_code=result["code"], detail=result['msg'])

    return result['data']


async def delete_data_id(item_id: int, db):
    """Start `delete_schedule_data_id_workflow` — deletes one schedule. Returns the workflow result."""
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()

    handle = await client.start_workflow(
        workflows_schedule.delete_schedule_data_id_workflow.run,
        item_id,
        id=f"Deleting-schedule-data-ID-{item_id}-{uniqueId}",
        task_queue="DeleteScheduleDataById-task-queue",
    )
    result = await handle.result()
    return result


async def get_temporal_status(schedule_id: str):
    """
    Resolve the latest execution status of a Temporal Schedule (schedule → its recent action →
    the workflow that action started → mapping its describe() to a status code).
    Returns: "PENDING" (no execution has happened yet), "RUNNING"/"COMPLETED"/"FAILED"/
             "CANCELED"/"TERMINATED"/"TIMED_OUT" (mapped from workflow status codes 1-6),
             "Workflow not found...", or an "Error : ..."/exception string on failure (never raises).
    """
    async def get_schedule_execution_details(schedule_id):
        client = await TemporalClientManager.get_temporal_client()
        schedule_handle = client.get_schedule_handle(schedule_id)
        try:
            description = await schedule_handle.describe()
            if not description.info.recent_actions:
                return "PENDING"
            latest_action = description.info.recent_actions[-1]
            if isinstance(latest_action.action, ScheduleActionExecutionStartWorkflow):
                workflow_id = latest_action.action.workflow_id
                run_id = latest_action.action.first_execution_run_id
                if workflow_id and run_id:
                    workflow_handle = client.get_workflow_handle(workflow_id)
                    status = await workflow_handle.describe()
                    if status.status == 1:
                        return "RUNNING"
                    elif status.status == 2:
                        return "COMPLETED"
                    elif status.status == 3:
                        return "FAILED"
                    elif status.status == 4:
                        return "CANCELED"
                    elif status.status == 5:
                        return "TERMINATED"
                    elif status.status == 6:
                        return "TIMED_OUT"
                else:
                    return "Workflow not found..."
        except Exception as e:
            return f"Error : {e}"

    try:
        return await get_schedule_execution_details(schedule_id)
    except Exception as e:
        return str(e)


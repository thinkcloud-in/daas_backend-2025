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

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(temporal_schedules(*args))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in Temporal scheduling: {str(e)}")
    finally:
        loop.close()
def schedul_id(username: str, schedule_type: str, report_type: str):
    date_time = datetime.now().replace(microsecond=0)
    dateTime = date_time.strftime("%Y%m%d%H-%M%S")
    
    user = username.split('@')[0]
    
    schedule = schedule_type[0]
    
    report = report_type[0] + report_type[-7]
    
    res = user + '-' + schedule + '-' + report + '-' + dateTime
    return res
def post_data(item, db):
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
    unique_id = datetime.now()
    return f"{unique_id.hour }:{unique_id.minute}:{unique_id.second}"

    
async def get_data():
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
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()

    handle = await client.start_workflow(
        workflows_schedule.delete_schedule_data_id_workflow.run,
        item_id,
        id=f"Deleting-schedule-data-ID-{item_id}-{uniqueId}",
        task_queue="DeleteScheduleDataById-task-queue",
    )
    result = await handle.result()
    
    # After deleting from DB, also delete the actual Temporal Schedule
    if result and isinstance(result, dict) and result.get('schedule_id'):
        from .temporalService import delete_temporal_schedule
        await delete_temporal_schedule(result['schedule_id'])
        
    return result


async def get_temporal_status(schedule_id: str):
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


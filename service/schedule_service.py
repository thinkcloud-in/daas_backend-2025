import threading
from fastapi import HTTPException
from models.schedule_model import Schdeule
from datetime import datetime

from service.temporalResource.workers import workers_schedule
from service.temporalResource.workflows import workflows_schedule
from .temporalService import temporal_schedules
import asyncio
from models.SMTP_models import SMTP
import asyncio
from temporalio.client import (
    Client,
    ScheduleActionExecutionStartWorkflow,
)
from dotenv import load_dotenv
import os

load_dotenv()
GUACAMOLE_REPORT_URL = os.getenv('GUACAMOLE_REPORT_URL')
print(GUACAMOLE_REPORT_URL)
HORIZON_REPORT_URL = os.getenv('HORIZON_REPORT_URL')
print(HORIZON_REPORT_URL)
def run_temporal_schedule(*args):

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(temporal_schedules(*args))
    except Exception as e:
        print(f"Temporal schedule error: {e}")
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
            "password": smtp_response.password,
            "serverIP": smtp_response.serverIP,
            "serverPort": smtp_response.serverPort,
            "connOption": smtp_response.connOption
        }
        print(scheduleId,"--------")
        db_item = Schdeule(
        userEmail=item.userEmail,
        receiverEmail=item.receiverEmail,
        report = item.report,
        reportName=item.reportName,
        time=item.time,
        schedule_date=item.schedule_date,
        schedule_type=item.schedule_type,
        schedule_id=scheduleId,
        )
        receiver_email = [email.strip() for email in db_item.receiverEmail.split(',')]
        if (db_item.report == 'Vamanit'):

            PDF_API_URL = f"{GUACAMOLE_REPORT_URL}/v1/guacamole/generate_report"

        elif (db_item.report == 'Horizon'):
            PDF_API_URL = HORIZON_REPORT_URL
        else:
            raise ValueError("Unsupported report type")

        db.add(db_item)
        db.commit()
        db.refresh(db_item)
        # asyncio.run(temporal_schedules(scheduleId))
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

async def connectionWithClient():
    try:
        client = await Client.connect(os.getenv('TEMPORAL_SERVER'))
        print('Connected to Temporal server.')
        return client
    except Exception as e:
        print(f"Failed to connect to Temporal server: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to connect to Temporal server: {e}")  
    
async def get_data():
    uniqueId = unique_id()
    client =await connectionWithClient()
    print("Retrieving pool data with temporal server...")
    try:
        print("worker about to start...")
        asyncio.create_task(workers_schedule.get_report_data_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_schedule.get_report_data_workflow.run,
        id=f"Retrieving-schedule-data-{uniqueId}",
        task_queue="GetReportData-task-queue",
    )
    result =  await handle.result()
    return result


async def get_data_id(item_id:int):
    uniqueId = unique_id()
    client = await connectionWithClient()
    print("Retrieving schedule data by id with temporal server...")
    try:
        print("worker about to start...")
        asyncio.create_task(workers_schedule.get_report_data_by_id_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_schedule.get_report_data_by_id_workflow.run,
        item_id,
        id=f"Retrieving-schedule-data-{uniqueId}",
        task_queue="GetReportDataById-task-queue",
        
    )
    result =  await handle.result()
    return result


async def get_data_report(report:str, limit: int, offset: int,db):
    uniqueID = unique_id()
    client = await connectionWithClient()
    print("Retrieving pool data with temporal server...")
    try:
        print("worker about to start...")
        asyncio.create_task(workers_schedule.get_schedule_data_along_report_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_schedule.get_report_along_report_workflow.run,
        args=[report, limit, offset],
        id=f"Retrieving-schedule-data-{report}-{uniqueID}",
        task_queue="GetScheduleDataAlongReport-task-queue",
    )
    result =  await handle.result()
    return result

async def update_data_id(item_id:int, item,db):
    uniqueId = unique_id()
    client = await connectionWithClient()
    print("Updating pool data with temporal server...")
    try:
        print("worker about to start...")
        asyncio.create_task(workers_schedule.update_schedule_data_id_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_schedule.update_schedule_data_id_workflow.run,
        args=[item_id,item],
        id=f"Updating-schedule-data-ID-{item_id}-{uniqueId}",
        task_queue="UpdateScheduleDataById-task-queue",
    )
    result =  await handle.result()
    return result

async def delete_data_id(item_id:int,db):
    uniqueId = unique_id()
    client = await connectionWithClient()
    print("Deleting pool data with temporal server...")
    try:
        print("worker about to start...")
        asyncio.create_task(workers_schedule.delete_schedule_data_id_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_schedule.delete_schedule_data_id_workflow.run,
        item_id,
        id=f"Deleting-schedule-data-ID-{item_id}-{uniqueId}",
        task_queue="DeleteScheduleDataById-task-queue",
    )
    result =  await handle.result()
    return result 


async def get_temporal_status(schedule_id : str):

    async def get_schedule_execution_details(schedule_id):
        client = await Client.connect(os.getenv("TEMPORAL_SERVER"))
        
        schedule_handle = client.get_schedule_handle(schedule_id)
        
        try:
            description = await schedule_handle.describe()
            latest_action = description.info.recent_actions[-1] #latest
            if isinstance(latest_action.action, ScheduleActionExecutionStartWorkflow):
                workflow_id = latest_action.action.workflow_id
                run_id = latest_action.action.first_execution_run_id
                # return workflow_id, run_id, client
                # workflow_id, run_id, client = await get_schedule_execution_details("workflow-schedule-id")
                if workflow_id and run_id:
                    # print(f"Workflow ID: {workflow_id}")
                    # print(f"Run ID: {run_id}")

                    workflow_handle = client.get_workflow_handle(workflow_id)
                    status = await workflow_handle.describe() #status
                    # print(f"Status: {status.status}")
                    if status.status == 1:
                        return("RUNNING")
                    elif status.status == 2:
                        return("COMPLETED")
                    elif status.status == 3:
                        return("FAILED") 
                    elif status.status == 4:
                        return("CANCELED") 
                    elif status.status == 5:
                        return("TERMINATED") 
                    elif status.status == 6:
                        return("TIMED_OUT")  
                else:
                    print("Workflow not found...")
                    return("Workflow not found...")
        except Exception as e:
            # print(f"{e}")
            # print(len(description.info.recent_actions))
            return (f"Error : {e}")
    try:
        return await get_schedule_execution_details(schedule_id)
    except Exception as e:
        print(e)
        return str(e)


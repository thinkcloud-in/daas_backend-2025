import asyncio
from http.client import HTTPException

from sqlalchemy import desc
from models.models import Machine
from service.gucamoleService import connectionWithClient
from temporalio.client import WorkflowExecutionStatus
from service.temporalResource.workflows import workflows_pollingStatus
from service.temporalResource.workers import worker_pollingStatus
from db_configuration.config import get_db
from sqlalchemy.orm import Session
import logging
from service import controllers
from temporalio.api.enums.v1 import EventType

logger = logging.getLogger(__name__)



def update_workflow_status(db, machine_id: int, wfid: str, status: str, error: str,vm_status: str = None):
    # logger.info(f"[update_workflow_status] Called with: machine_id={machine_id}, wfid={wfid}, status={status}, error={error}")

    machine = db.query(Machine).filter(Machine.id == machine_id).first()
    if not machine:
        logger.error(f"[update_workflow_status] Machine {machine_id} not found")
        raise Exception(f"Machine {machine_id} not found")

    workflow_status = machine.workflow_status or {}
    workflow_status[wfid] = {"status": status, "error": error}
    machine.workflow_status = workflow_status

    statuses = [v["status"] for v in workflow_status.values()]
    errors = [v["error"] for v in workflow_status.values() if v.get("error")]

    # logger.info(f"[update_workflow_status] statuses={statuses}, errors={errors}")

    if all(s == "COMPLETED" for s in statuses):
        machine.status = "COMPLETED"
        if vm_status:  # Use VM status if given
            machine.error_message = vm_status
    else:
        non_completed = next((s for s in statuses if s != "COMPLETED"), "RUNNING")
        machine.status = non_completed
        machine.error_message = "; ".join(errors) if errors else None
    db.commit()
    db.refresh(machine)
    new_db = next(get_db())
    try:
        fresh_machine = new_db.query(Machine).filter(Machine.id == machine_id).first()
        print("Fresh status after commit:", fresh_machine.status)
    finally:
        new_db.close()
    return machine.workflow_status, machine.status, machine.error_message

async def get_workflow_failure_message_simple(workflow_id):
    client = await connectionWithClient()
    handle = client.get_workflow_handle(workflow_id)
    desc = await handle.describe()
    status = WorkflowExecutionStatus(desc.status).name if isinstance(desc.status, int) else str(desc.status)

    def parse_failure(failure, depth=0, max_depth=10):
        if not failure or depth > max_depth:
            return ""
        message = getattr(failure, "message", "")
        cause = getattr(failure, "cause", None)
        cause_msg = parse_failure(cause, depth + 1, max_depth) if cause else ""
        return f"{message}: {cause_msg}" if cause_msg else message

    if status == "FAILED":
        history = await handle.fetch_history()
        for event in reversed(history.events):
            if event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
                failure = event.workflow_execution_failed_event_attributes.failure
                return {
                    "failure_message": parse_failure(failure),
                    "status": status
                }
    return {"failure_message": None, "status": status}


# Global variable to track if worker is already running
_worker_task = None
_status_poller_workflow_id = None

async def is_status_poller_cron_running():

    try:
        client = await connectionWithClient()
        async for workflow in client.list_workflows("WorkflowId STARTS_WITH 'status-poller-'"):
            if workflow.status == WorkflowExecutionStatus.RUNNING:
                # logger.info(f"Found running status poller workflow: {workflow.id}")
                return True, workflow.id
        return False, None
    except Exception as e:
        logger.error(f"Error checking status poller workflow: {e}")
        return False, None

async def start_status_poller_workflow():
    global _worker_task, _status_poller_workflow_id
    
    # Check if workflow is already running
    is_running, existing_id = await is_status_poller_cron_running()
    if is_running:
        # logger.info(f"Status poller workflow already running with ID: {existing_id}")
        _status_poller_workflow_id = existing_id
        return {
            "message": "Status poller workflow already running",
            "workflow_id": existing_id,
            "status": "already_running"
        }
    
    uniqueId = controllers.unique_id()
    client = await connectionWithClient()
    # workflowid = f"status-poller-{uniqueId}"
    
    try:
        # Start the worker only if it's not already running
        if _worker_task is None or _worker_task.done():
            print("Starting status poller worker...")
            # logger.info("Starting status poller worker...")
            _worker_task = asyncio.create_task(worker_pollingStatus.status_poller_worker())
            # Give the worker some time to initialize
            await asyncio.sleep(3)
        else:
            print("Status poller worker is already running")
            logger.info("Status poller worker is already running")
            
    except Exception as e:
        logger.error(f"Error starting worker: {e}")
        # FIX: Create HTTPException with proper arguments
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=f"Error while starting worker task: {str(e)}")
    
    try:
        # Start the cron workflow
        handle = await client.start_workflow(
            workflows_pollingStatus.StatusPollerWorkflow.run,
            id="status-poller",
            task_queue="status-poller-task-queue",
            cron_schedule="* * * * *",  # every minute - CONTINUOUS
        )

        _status_poller_workflow_id = handle.id
        # print(f"Status poller cron workflow started with ID: {_status_poller_workflow_id}")
        # logger.info(f"Status poller cron workflow started with ID: {_status_poller_workflow_id}")

        # Don't wait for result since it's a cron workflow that runs indefinitely
        return {
            "message": "Status poller cron workflow started successfully",
            "workflow_id": _status_poller_workflow_id,
            "status": "started",
            "will_run": "every minute continuously"
        }
        
    except Exception as e:
        logger.error(f"Error starting status poller workflow: {e}")
        # FIX: Create HTTPException with proper arguments
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=f"Error starting status poller workflow: {str(e)}")

async def ensure_status_poller_running():
    try:
        result = await start_status_poller_workflow()
        logger.info(f"Status poller ensured: {result}")
        return result
    except Exception as e:
        logger.error(f"Error ensuring status poller: {e}")
        # Don't fail machine creation if status poller fails
        return {"message": f"Error: {str(e)}", "status": "error"}





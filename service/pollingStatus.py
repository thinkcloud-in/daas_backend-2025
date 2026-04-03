import asyncio
from models.models import Machine
from service.gucamoleService import connectionWithClient
from temporalio.client import WorkflowExecutionStatus
from service.temporalResource.workflows import workflows_pollingStatus
from service.temporalResource.workers import worker_pollingStatus
from db_configuration.config import get_db
import logging
from service import controllers
from temporalio.api.enums.v1 import EventType

logger = logging.getLogger(__name__)



def update_workflow_status(db, machine_id: int, wfid: str, status: str, error: str,vm_status: str = None):

    machine = db.query(Machine).filter(Machine.id == machine_id).first()
    if not machine:
        logger.error(f"[update_workflow_status] Machine {machine_id} not found")
        raise Exception(f"Machine {machine_id} not found")

    workflow_status = machine.workflow_status or {}
    workflow_status[wfid] = {"status": status, "error": error}
    machine.workflow_status = workflow_status

    statuses = [v["status"] for v in workflow_status.values()]
    errors = [v["error"] for v in workflow_status.values() if v.get("error")]


    if all(s == "COMPLETED" for s in statuses):
        machine.status = "COMPLETED"
        if vm_status:
            machine.error_message = vm_status
    else:
        non_completed = next((s for s in statuses if s != "COMPLETED"), "RUNNING")
        machine.status = non_completed
        machine.error_message = "; ".join(errors) if errors else "power-off"
    db.commit()
    db.refresh(machine)
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
    return {"failure_message": "power-off", "status": status}


_worker_task = None
_status_poller_workflow_id = None

async def is_status_poller_cron_running():

    try:
        client = await connectionWithClient()
        async for workflow in client.list_workflows("WorkflowId STARTS_WITH 'status-poller-'"):
            if workflow.status == WorkflowExecutionStatus.RUNNING:
                return True, workflow.id
        return False, None
    except Exception as e:
        logger.error(f"Error checking status poller workflow: {e}")
        return False, None

async def start_status_poller_workflow():
    global _worker_task, _status_poller_workflow_id
    
    is_running, existing_id = await is_status_poller_cron_running()
    if is_running:
        _status_poller_workflow_id = existing_id
        return {
            "message": "Status poller workflow already running",
            "workflow_id": existing_id,
            "status": "already_running"
        }
    
    uniqueId = controllers.unique_id()
    client = await connectionWithClient()
    
    try:
        if _worker_task is None or _worker_task.done():
      
            _worker_task = asyncio.create_task(worker_pollingStatus.status_poller_worker())
            await asyncio.sleep(3)
        else:
           
            logger.info("Status poller worker is already running")
            
    except Exception as e:
        logger.error(f"Error starting worker: {e}")
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=f"Error while starting worker task: {str(e)}")
    
    try:
        handle = await client.start_workflow(
            workflows_pollingStatus.StatusPollerWorkflow.run,
            id="status-poller",
            task_queue="status-poller-task-queue",
            cron_schedule="* * * * *",
        )

        _status_poller_workflow_id = handle.id
        return {
            "message": "Status poller cron workflow started successfully",
            "workflow_id": _status_poller_workflow_id,
            "status": "started",
            "will_run": "every minute continuously"
        }
        
    except Exception as e:
        logger.error(f"Error starting status poller workflow: {e}")
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=f"Error starting status poller workflow: {str(e)}")

async def ensure_status_poller_running():
    try:
        result = await start_status_poller_workflow()
        logger.info(f"Status poller ensured: {result}")
        return result
    except Exception as e:
        logger.error(f"Error ensuring status poller: {e}")
        return {"message": f"Error: {str(e)}", "status": "error"}





from models.models import Machine
from temporalio.client import WorkflowExecutionStatus
from service.temporalResource.workflows import workflows_pollingStatus
from utils.temporal_client import TemporalClientManager
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
        if errors:
            machine.error_message = "; ".join(errors)
        elif vm_status:
            machine.error_message = vm_status
        elif machine.error_message not in ["power-off", "power-on", "shutdown", "reboot"]:
            machine.error_message = None
    db.commit()
    db.refresh(machine)
    return machine.workflow_status, machine.status, machine.error_message

async def get_workflow_failure_message_simple(workflow_id):
    client = await TemporalClientManager.get_temporal_client()
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


# _status_poller_workflow_id now managed in main.py indirectly or on demand
_status_poller_workflow_id = None

async def is_status_poller_cron_running():
    # Routine/expected-path checks stay at DEBUG — this runs on every machine
    # creation, not just once, and would otherwise flood app.log with noise
    # that isn't business-logic-relevant. Only genuine failures are logged.
    try:
        logger.debug("[Poller] Checking if status-poller is already running...")
        client = await TemporalClientManager.get_temporal_client()
        handle = client.get_workflow_handle("status-poller")
        try:
            desc = await handle.describe()
            if desc.status == WorkflowExecutionStatus.RUNNING:
                logger.debug(f"[Poller] Found running workflow: {handle.id}")
                return True, handle.id
        except Exception:
            # Workflow doesn't exist or is not running
            pass
        return False, None
    except Exception as e:
        logger.error(f"[Poller] Error checking status poller workflow: {e}", exc_info=True)
        return False, None

async def start_status_poller_workflow(force=False):
    global _status_poller_workflow_id

    is_running, existing_id = await is_status_poller_cron_running()
    if is_running and not force:
        _status_poller_workflow_id = existing_id
        return {
            "message": "Status poller workflow already running",
            "workflow_id": existing_id,
            "status": "already_running"
        }

    client = await TemporalClientManager.get_temporal_client()
    if is_running and force:
        try:
            logger.debug(f"[Poller] Terminating existing workflow {existing_id} for force restart...")
            handle = client.get_workflow_handle(existing_id)
            await handle.terminate(reason="Force restart requested")
            logger.debug("[Poller] Termination successful.")
        except Exception as e:
            logger.warning(f"[Poller] Termination failed (might already be closed): {e}")

    uniqueId = controllers.unique_id()
    

    
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

async def ensure_status_poller_running(force=False):
    try:
        result = await start_status_poller_workflow(force=force)
        logger.info(f"Status poller ensured (force={force}): {result}")
        return result
    except Exception as e:
        logger.error(f"Error ensuring status poller: {e}")
        return {"message": f"Error: {str(e)}", "status": "error"}





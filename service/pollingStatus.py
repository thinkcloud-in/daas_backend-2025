"""
pollingStatus — Machine workflow-status aggregation + Temporal "status-poller" cron management.

A machine can have multiple sub-workflows (create/clone-related) — this module's core
job is to aggregate their combined status/error into `Machine.status`/`Machine.error_message`
(`update_workflow_status`). The remaining functions manage an always-running Temporal cron
workflow (`StatusPollerWorkflow`, "* * * * *" — every minute) that periodically refreshes
machine statuses; this cron follows a singleton pattern (the workflow id is always
"status-poller", and it first checks whether one is already running to prevent a duplicate
start).
"""
from models.models import Machine
from temporalio.client import WorkflowExecutionStatus
from service.temporalResource.workflows import workflows_pollingStatus
from utils.temporal_client import TemporalClientManager
import logging
from service import controllers
from temporalio.api.enums.v1 import EventType

logger = logging.getLogger(__name__)



def update_workflow_status(db, machine_id: int, wfid: str, status: str, error: str,vm_status: str = None):
    """
    Record a new status/error for one workflow-id in a machine's `workflow_status` JSON dict,
    then combine the statuses of all workflow-ids to set the machine's overall `.status`/`.error_message`:
    if all are "COMPLETED" the machine is "COMPLETED" too (vm_status, if given, goes into error_message),
    otherwise the first non-completed status is used and errors are joined with "; " into error_message.

    Returns: (workflow_status dict, machine.status, machine.error_message) tuple.
    Raises: Exception if machine_id is not found in the DB.
    """
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
    """
    Describe a Temporal workflow's current status, and if it is FAILED, extract the actual
    failure message from its event history (recursively unwrapping the nested `cause` chain).

    Returns: {"failure_message": str, "status": str} if FAILED, otherwise
             {"failure_message": "power-off", "status": <status>} for non-failed states
             (this is a default placeholder, not an actual power-off event).
    """
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
    """
    Check whether the Temporal cron workflow named "status-poller" is already in the RUNNING state.
    Returns: (True, workflow_id) if running, otherwise (False, None) (exceptions also return (False, None)).
    """
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
    """
    Start the "status-poller" Temporal cron workflow (`StatusPollerWorkflow`, "* * * * *"),
    unless one is already running. If `force=True` is given, the existing running workflow is
    terminated and restarted fresh.

    Returns: {"message","workflow_id","status": "already_running"|"started", ["will_run"]}.
    Raises: HTTPException 500 if an error occurs while starting the workflow.
    """
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
    """
    A safe wrapper around `start_status_poller_workflow()` — called from app startup; it
    swallows exceptions and returns an error dict (so it never crashes startup).
    Returns: the result of `start_status_poller_workflow()`, or {"message": "Error: ...", "status": "error"}.
    """
    try:
        result = await start_status_poller_workflow(force=force)
        logger.info(f"Status poller ensured (force={force}): {result}")
        return result
    except Exception as e:
        logger.error(f"Error ensuring status poller: {e}")
        return {"message": f"Error: {str(e)}", "status": "error"}





from temporalio.worker import Worker
from utils.temporal_client import TemporalClientManager
from service.temporalResource.workflows import workflows_pollingStatus
from service.temporalResource.activity import activities_pollingStatus
import logging

logger = logging.getLogger(__name__)

async def status_poller_worker():

    try:
        client = await TemporalClientManager.get_temporal_client()
        
        worker = Worker(
            client,
            task_queue="status-poller-task-queue",
            workflows=[workflows_pollingStatus.StatusPollerWorkflow],
            activities=[
                activities_pollingStatus.poll_and_update_machine_status_activity,
                # activities_pollingStatus.get_proxmox_vm_status_activity,
                # activities_pollingStatus.get_hyperv_vm_status_activity,
            ],
            max_concurrent_workflow_tasks=10,
            max_concurrent_activities=5,
        )
        
        logger.info('Status poller worker starting...')
        await worker.run()
        
    except Exception as e:
        logger.error(f"Error in Temporal status poller worker: {e}")
        
        raise
    finally:
        logger.info('Status poller worker stopped...')
        








import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.worker import Worker

from service.temporalResource.activity import activities_lxc_restore
from service.temporalResource.workflows.workflows_lxc_restore import LXCRestoreWorkflow
from utils.temporal_client import TemporalClientManager

logger = logging.getLogger(__name__)

TASK_QUEUE = "lxc-restore-task-queue"


async def lxc_restore_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[LXCRestoreWorkflow],
        activities=[
            activities_lxc_restore.upload_lxc_to_proxmox_activity,
            activities_lxc_restore.restore_lxc_container_activity,
            activities_lxc_restore.setup_lxc_container_activity,
            activities_lxc_restore.oras_login_to_harbor_activity,
            activities_lxc_restore.update_lxc_job_status_activity,
        ],
        activity_executor=ThreadPoolExecutor(max_workers=5),
    )
    logger.info(f"LXC restore worker started on queue: {TASK_QUEUE}")
    await worker.run()


async def run_all_lxc_workers():
    await asyncio.gather(lxc_restore_worker())

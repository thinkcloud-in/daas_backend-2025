import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.worker import Worker

from service.temporalResource.activity import activities_guacamole_retention
from service.temporalResource.workflows.workflows_guacamole_retention import GuacamoleRetentionWorkflow
from utils.temporal_client import TemporalClientManager

logger = logging.getLogger(__name__)

TASK_QUEUE = "guacamole-retention-tasks"


async def guacamole_retention_worker():
    client = await TemporalClientManager.get_temporal_client()
    if client is None:
        return None

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[GuacamoleRetentionWorkflow],
        activities=[activities_guacamole_retention.guacamole_retention_cleanup_activity],
        activity_executor=ThreadPoolExecutor(max_workers=5),
    )
    logger.info(f"Guacamole retention worker started on queue: {TASK_QUEUE}")
    await worker.run()


async def run_all_guacamole_retention_workers():
    await asyncio.gather(guacamole_retention_worker())

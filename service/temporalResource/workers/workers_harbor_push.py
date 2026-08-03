import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.worker import Worker

from service.temporalResource.activity.activities_harbor_push import harbor_push_activity
from service.temporalResource.workflows.workflows_harbor_push import HarborPushWorkflow
from utils.temporal_client import TemporalClientManager

logger = logging.getLogger(__name__)

TASK_QUEUE = "harbor-push-queue"


async def harbor_push_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[HarborPushWorkflow],
        activities=[harbor_push_activity],
        activity_executor=ThreadPoolExecutor(max_workers=5),
        max_concurrent_activities=5,
    )
    logger.info(f"[HarborPushWorker] polling '{TASK_QUEUE}'")
    await worker.run()


async def run_harbor_push_worker():
    await asyncio.gather(harbor_push_worker())

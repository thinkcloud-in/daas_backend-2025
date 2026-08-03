import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.worker import Worker

from service.temporalResource.activity.activities_llm_push import llm_push_activity
from service.temporalResource.workflows.workflows_llm_push import LLMPushWorkflow
from utils.temporal_client import TemporalClientManager

logger = logging.getLogger(__name__)

TASK_QUEUE = "llm-push-queue"


async def llm_push_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[LLMPushWorkflow],
        activities=[llm_push_activity],
        activity_executor=ThreadPoolExecutor(max_workers=3),
        max_concurrent_activities=3,
    )
    logger.info(f"[LLMPushWorker] polling '{TASK_QUEUE}'")
    await worker.run()


async def run_llm_push_worker():
    await asyncio.gather(llm_push_worker())

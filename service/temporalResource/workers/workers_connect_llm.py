import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.worker import Worker

from service.temporalResource.activity.activities_connect_llm import (
    connect_llm_activity,
    disconnect_llm_activity,
)
from service.temporalResource.workflows.workflows_connect_llm import (
    ConnectLLMWorkflow,
    DisconnectLLMWorkflow,
)
from utils.temporal_client import TemporalClientManager

logger = logging.getLogger(__name__)
TASK_QUEUE = "connect-llm-queue"


async def connect_llm_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue                = TASK_QUEUE,
        workflows                 = [ConnectLLMWorkflow, DisconnectLLMWorkflow],
        activities                = [connect_llm_activity, disconnect_llm_activity],
        activity_executor         = ThreadPoolExecutor(max_workers=3),
        max_concurrent_activities = 3,
    )
    logger.info(f"[ConnectLLMWorker] Listening on '{TASK_QUEUE}'")
    await worker.run()


async def run_connect_llm_worker():
    await asyncio.gather(connect_llm_worker())

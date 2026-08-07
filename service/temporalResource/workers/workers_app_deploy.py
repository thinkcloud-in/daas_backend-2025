import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.worker import Worker

from service.temporalResource.activity.activities_app_deploy import app_deploy_activity
from service.temporalResource.workflows.workflows_app_deploy import AppDeployWorkflow
from utils.temporal_client import TemporalClientManager

logger = logging.getLogger(__name__)
TASK_QUEUE = "app-deploy-queue"


async def app_deploy_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue                = TASK_QUEUE,
        workflows                 = [AppDeployWorkflow],
        activities                = [app_deploy_activity],
        activity_executor         = ThreadPoolExecutor(max_workers=3),
        max_concurrent_activities = 3,
    )
    logger.info(f"[AppDeployWorker] Listening on '{TASK_QUEUE}'")
    await worker.run()


async def run_app_deploy_worker():
    await asyncio.gather(app_deploy_worker())

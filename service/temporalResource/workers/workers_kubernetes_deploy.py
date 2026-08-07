import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.worker import Worker

from service.temporalResource.activity.activities_kubernetes_deploy import (
    k8s_harbor_deploy_activity,
)
from service.temporalResource.workflows.workflows_kubernetes_deploy import (
    K8sHarborDeployWorkflow,
)
from utils.temporal_client import TemporalClientManager

logger = logging.getLogger(__name__)
TASK_QUEUE = "k8s-harbor-deploy-queue"


async def k8s_deploy_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue                = TASK_QUEUE,
        workflows                 = [K8sHarborDeployWorkflow],
        activities                = [k8s_harbor_deploy_activity],
        activity_executor         = ThreadPoolExecutor(max_workers=3),
        max_concurrent_activities = 3,
    )
    logger.info(f"[K8sDeployWorker] Listening on '{TASK_QUEUE}'")
    await worker.run()


async def run_k8s_deploy_worker():
    await asyncio.gather(k8s_deploy_worker())

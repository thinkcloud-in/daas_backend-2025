import logging

from temporalio.client import Client
from temporalio.worker import Worker

from service.temporalResource.activity.activities_kubernetes import test_k8s_connection_activity
from service.temporalResource.workflows.workflows_kubernetes import KubernetesTestWorkflow

logger = logging.getLogger(__name__)
TASK_QUEUE = "kubernetes-queue"


async def run_kubernetes_worker():
    client = await Client.connect("localhost:7233")
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[KubernetesTestWorkflow],
        activities=[test_k8s_connection_activity],
    )
    logger.info(f"[Worker] Kubernetes worker started on '{TASK_QUEUE}'")
    await worker.run()

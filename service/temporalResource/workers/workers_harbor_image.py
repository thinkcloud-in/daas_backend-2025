import logging
from temporalio.client import Client
from temporalio.worker import Worker

from service.temporalResource.activity.activities_harbor_image import sftp_and_push_harbor_image_activity
from service.temporalResource.workflows.workflows_harbor_image import HarborImageWorkflow

logger = logging.getLogger(__name__)
TASK_QUEUE = "harbor-image-queue"


async def run_harbor_image_worker():
    client = await Client.connect("localhost:7233")
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[HarborImageWorkflow],
        activities=[sftp_and_push_harbor_image_activity],
    )
    logger.info(f"[Worker] HarborImage worker started on '{TASK_QUEUE}'")
    await worker.run()

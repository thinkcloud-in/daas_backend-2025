import asyncio
import logging

from temporalio.worker import Worker

from service.temporalResource.activity import activities_library
from service.temporalResource.workflows.workflows_library import LibraryUploadWorkflow
from utils.temporal_client import TemporalClientManager

logger = logging.getLogger(__name__)

TASK_QUEUE = "library-upload-task-queue"


async def library_upload_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[LibraryUploadWorkflow],
        activities=[
            activities_library.transfer_file_activity,
            activities_library.finalize_library_record_activity,
            activities_library.mark_upload_failed_activity,
        ],
    )
    logger.info(f"Library upload worker started on queue: {TASK_QUEUE}")
    await worker.run()


async def run_all_library_workers():
    await asyncio.gather(library_upload_worker())

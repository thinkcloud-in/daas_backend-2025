import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.worker import Worker

from service.temporalResource.activity import activities_library
from service.temporalResource.workflows.workflows_library import (
    LibraryUploadWorkflow,
    LibraryDeleteWorkflow,
    LibraryUpdateWorkflow,
)
from utils.temporal_client import TemporalClientManager

logger = logging.getLogger(__name__)

TASK_QUEUE = "library-upload-task-queue"


async def library_upload_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[LibraryUploadWorkflow, LibraryDeleteWorkflow, LibraryUpdateWorkflow],
        activities=[
            activities_library.transfer_file_activity,      # Library-Track-Upload-Progress
            activities_library.mark_upload_failed_activity,
            activities_library.delete_library_file_activity,
            activities_library.update_library_metadata_activity,
        ],
        activity_executor=ThreadPoolExecutor(max_workers=10),
    )
    logger.info(f"Library worker started on queue: {TASK_QUEUE}")
    await worker.run()


async def run_all_library_workers():
    await asyncio.gather(library_upload_worker())

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from service.temporalResource.activity import activities_library

logger = workflow.logger
_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
    maximum_attempts=5,
)
_RETRY_TRANSFER = RetryPolicy(
    initial_interval=timedelta(seconds=10),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=120),
    maximum_attempts=5,
)


@workflow.defn(sandboxed=False)
class LibraryUploadWorkflow:
    """
    HTTP handler seedha pod mein stream karta hai (no temp file).
    Ye workflow sirf Temporal UI mein progress dikhata hai — DB poll karke heartbeat bhejta hai.
    Jab DB status=ready ho jaaye, workflow complete.
    """

    @workflow.run
    async def run(self, payload: dict) -> dict:
        item_id    = payload["item_id"]
        total_size = payload.get("total_size", 0)

        result = await workflow.execute_activity(
            activities_library.transfer_file_activity,
            args=[{"item_id": item_id, "total_size": total_size}],
            retry_policy=_RETRY,
            start_to_close_timeout=timedelta(hours=6),
            heartbeat_timeout=timedelta(seconds=10),
        )

        logger.info(f"[Library] item={item_id} upload workflow complete — status={result['status']}")
        return result


@workflow.defn(sandboxed=False)
class LibraryDeleteWorkflow:
    """Deletes a library item's file from PV storage and removes the DB record."""

    @workflow.run
    async def run(self, payload: dict) -> dict:
        item_id = payload["item_id"]

        result = await workflow.execute_activity(
            activities_library.delete_library_file_activity,
            args=[{"item_id": item_id}],
            retry_policy=_RETRY,
            start_to_close_timeout=timedelta(minutes=5),
        )

        logger.info(f"[Library] item={item_id} delete workflow complete")
        return result


@workflow.defn(sandboxed=False)
class LibraryUpdateWorkflow:
    """Updates name and/or version metadata of a library item."""

    @workflow.run
    async def run(self, payload: dict) -> dict:
        result = await workflow.execute_activity(
            activities_library.update_library_metadata_activity,
            args=[payload],
            retry_policy=_RETRY,
            start_to_close_timeout=timedelta(minutes=2),
        )

        logger.info(f"[Library] item={payload.get('item_id')} update workflow complete")
        return result

import logging
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from service.temporalResource.activity import activities_library

logger = logging.getLogger(__name__)

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
    Transfers an uploaded file from the temp staging area to the PV mount.
    Temporal UI shows live progress via activity heartbeats:
      {"progress_pct": 42.5, "bytes_done": 403_000_000, "bytes_total": 1_047_000_000}
    """

    @workflow.run
    async def run(self, payload: dict) -> dict:
        item_id   = payload["item_id"]
        temp_path = payload["temp_path"]
        dest_path = payload["dest_path"]

        # Phase 1: Copy temp → PV (heartbeats every 5 MB)
        try:
            transfer_result = await workflow.execute_activity(
                activities_library.transfer_file_activity,
                args=[payload],
                retry_policy=_RETRY_TRANSFER,
                start_to_close_timeout=timedelta(hours=6),
                heartbeat_timeout=timedelta(minutes=2),
            )
        except Exception as exc:
            logger.error(f"[Library] item={item_id} transfer failed: {exc}")
            await workflow.execute_activity(
                activities_library.mark_upload_failed_activity,
                args=[{"item_id": item_id, "temp_path": temp_path, "dest_path": dest_path}],
                retry_policy=_RETRY,
                start_to_close_timeout=timedelta(minutes=2),
            )
            raise

        # Phase 2: Finalize DB record → status=ready
        await workflow.execute_activity(
            activities_library.finalize_library_record_activity,
            args=[{
                "item_id":     item_id,
                "file_path":   transfer_result["file_path"],
                "file_size":   transfer_result["file_size"],
                "workflow_id": workflow.info().workflow_id,
            }],
            retry_policy=_RETRY,
            start_to_close_timeout=timedelta(minutes=2),
        )

        logger.info(f"[Library] item={item_id} upload workflow complete")
        return {
            "item_id":   item_id,
            "file_path": transfer_result["file_path"],
            "file_size": transfer_result["file_size"],
            "status":    "ready",
        }


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

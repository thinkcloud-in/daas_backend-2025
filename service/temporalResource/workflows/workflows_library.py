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
    maximum_attempts=3,
)
_NO_RETRY = RetryPolicy(maximum_attempts=1)


@workflow.defn(sandboxed=False)
class LibraryUploadWorkflow:
    """
    Transfers an uploaded file from the temp staging area to the PV mount.

    Temporal UI shows live progress via activity heartbeats:
      {"progress_pct": 42.5, "bytes_done": 444596224, "bytes_total": 1073741824, "stage": "transferring"}
    """

    @workflow.run
    async def run(self, payload: dict) -> dict:
        item_id   = payload["item_id"]
        temp_path = payload["temp_path"]
        dest_path = payload["dest_path"]

        # ── Phase 1: Copy temp → PV (heartbeats every 5 MB) ────────────────────
        try:
            transfer_result = await workflow.execute_activity(
                activities_library.transfer_file_activity,
                args=[payload],
                retry_policy=_NO_RETRY,
                start_to_close_timeout=timedelta(hours=6),   # ISOs can be multi-GB
                heartbeat_timeout=timedelta(minutes=2),       # must heartbeat within 2 min per chunk
            )
        except Exception as exc:
            logger.error(f"[LibraryUpload] item={item_id} transfer failed: {exc}")
            # Best-effort cleanup
            await workflow.execute_activity(
                activities_library.mark_upload_failed_activity,
                args=[{"item_id": item_id, "temp_path": temp_path, "dest_path": dest_path}],
                retry_policy=_RETRY,
                start_to_close_timeout=timedelta(minutes=2),
            )
            raise

        # ── Phase 2: Finalize DB record ─────────────────────────────────────────
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

        logger.info(f"[LibraryUpload] item={item_id} workflow complete")
        return {
            "item_id":   item_id,
            "file_path": transfer_result["file_path"],
            "file_size": transfer_result["file_size"],
            "status":    "ready",
        }

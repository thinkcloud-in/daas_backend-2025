from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from service.temporalResource.activity import activities_lxc_restore as acts

_NO_RETRY  = RetryPolicy(maximum_attempts=1)
_STD_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
    maximum_attempts=3,
)


async def _fail(job_id: int, flag: str):
    """Update job status to a failure flag — best-effort, no retry."""
    await workflow.execute_activity(
        acts.update_lxc_job_status_activity,
        args=[{"job_id": job_id, "status": flag}],
        retry_policy=_NO_RETRY,
        start_to_close_timeout=timedelta(minutes=1),
    )


@workflow.defn(sandboxed=False)
class LXCRestoreWorkflow:
    """
    Phase 1 — SFTP LXC backup from K8s PV → Proxmox /var/lib/vz/dump/
    Phase 2 — pct restore (template) → pct clone → pct start
    Phase 3 — Enable SSH, fix Harbor configs, wait healthy, restart harbor-jobservice
    Phase 4 — Fix EXT_ENDPOINT, restart harbor-core, ORAS login
    Phase 5 — Mark job deployed_successfully in DB
    """

    @workflow.run
    async def run(self, payload: dict) -> dict:
        job_id = payload["job_id"]

        # ── Phase 1: Upload to Proxmox ────────────────────────────────────────
        try:
            upload_result = await workflow.execute_activity(
                acts.upload_lxc_to_proxmox_activity,
                args=[payload],
                retry_policy=_NO_RETRY,
                start_to_close_timeout=timedelta(hours=4),
                heartbeat_timeout=timedelta(minutes=5),
            )
        except Exception:
            await _fail(job_id, "UPLOAD_FAILED")
            raise

        # ── Phase 2: pct restore + clone + start ──────────────────────────────
        restore_payload = {
            **payload,
            "dump_path":        upload_result["dump_path"],
            "proxmox_ip":       upload_result["proxmox_ip"],
            "proxmox_ssh_user": upload_result["ssh_user"],
            "proxmox_ssh_pass": upload_result["ssh_pass"],
        }
        try:
            restore_result = await workflow.execute_activity(
                acts.restore_lxc_container_activity,
                args=[restore_payload],
                retry_policy=_NO_RETRY,
                start_to_close_timeout=timedelta(hours=2),
            )
        except Exception:
            await _fail(job_id, "RESTORE_FAILED")
            raise

        # ── Phase 3: SSH setup + Harbor configs + wait healthy ────────────────
        setup_payload = {
            "proxmox_ip":         upload_result["proxmox_ip"],
            "proxmox_ssh_user":   upload_result["ssh_user"],
            "proxmox_ssh_pass":   upload_result["ssh_pass"],
            "vmid":               restore_result["vmid"],
            "container_ip":       payload["ip_address"],
            "container_ssh_user": payload.get("ssh_user", "root"),
            "container_ssh_pass": payload.get("ssh_pass", ""),
        }
        try:
            await workflow.execute_activity(
                acts.setup_lxc_container_activity,
                args=[setup_payload],
                retry_policy=_NO_RETRY,
                start_to_close_timeout=timedelta(minutes=45),
                heartbeat_timeout=timedelta(minutes=15),
            )
        except Exception:
            await _fail(job_id, "SETUP_FAILED")
            raise

        # ── Phase 4: Fix EXT_ENDPOINT + ORAS login ───────────────────────────
        oras_payload = {
            "container_ip":       payload["ip_address"],
            "container_ssh_user": payload.get("ssh_user", "root"),
            "container_ssh_pass": payload.get("ssh_pass", ""),
        }
        try:
            await workflow.execute_activity(
                acts.oras_login_to_harbor_activity,
                args=[oras_payload],
                retry_policy=_STD_RETRY,
                start_to_close_timeout=timedelta(minutes=5),
            )
        except Exception:
            await _fail(job_id, "ORAS_FAILED")
            raise

        # ── Phase 5: Mark deployed successfully ──────────────────────────────
        await workflow.execute_activity(
            acts.update_lxc_job_status_activity,
            args=[{"job_id": job_id, "status": "deployed_successfully", "container_state": "running"}],
            retry_policy=_STD_RETRY,
            start_to_close_timeout=timedelta(minutes=2),
        )

        return {
            "job_id":     job_id,
            "vmid":       restore_result["vmid"],
            "node":       restore_result["node"],
            "ip_address": payload["ip_address"],
            "status":     "deployed_successfully",
        }

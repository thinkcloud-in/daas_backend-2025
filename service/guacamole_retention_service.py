"""
guacamole_retention_service — service layer for Guacamole connection-history/recording retention.

An external "guacamole-retention-tool" pod serves two REST endpoints
(/guacamole/logs/delete, /guacamole/recordings/delete); this module calls them and
maintains a daily Temporal Schedule (`ensure_retention_schedule`) that automatically
runs `GuacamoleRetentionWorkflow`. The retention-days setting is stored in a singleton
DB row (GuacamoleRetentionSetting).
Used by: controllers/guacamole_controller.py.
"""
import os
import logging
from typing import Optional

import httpx
from fastapi import HTTPException
from sqlalchemy.orm import Session

from temporalio.client import (
    Client as TemporalClient, Schedule, ScheduleActionStartWorkflow, ScheduleSpec,
    ScheduleCalendarSpec, ScheduleRange, ScheduleState,
)

from models.guacamole_retention_model import GuacamoleRetentionSetting

logger = logging.getLogger(__name__)

_RETENTION_SCHEDULE_ID = "guacamole-retention-cleanup-schedule"
_RETENTION_TASK_QUEUE  = "guacamole-retention-tasks"

DEFAULT_RETENTION_DAYS = 30


def _clean_env_url(value: Optional[str]) -> str:
    """Take a URL from an env var and normalize it by stripping any `#` comment suffix and trailing slash."""
    if not value:
        return ""
    return value.split("#", 1)[0].strip().rstrip("/")


# A single pod (guacamole-retention-tool) serves both retention APIs --
# /guacamole/logs/delete (connection history; DB creds come automatically
# from the pod's env) and /guacamole/recordings/delete (recordings PV cleanup).
_RETENTION_API_BASE_URL = _clean_env_url(os.getenv("GUACAMOLE_RETENTION_API_BASE_URL"))
# Optional override -- if not given, the external API uses its own default (/recordings).
_RECORDINGS_PATH = os.getenv("GUACAMOLE_RECORDINGS_PATH")


def call_history_retention_api(retention_days: int) -> dict:
    """
    Connection-history (logs) retention -- DB creds come from the pod's env, only retention_days is sent.
    Returns: {"skipped": True, "reason": "..."} if the base URL is not configured, otherwise
             the external API's JSON response as-is.
    Raises: HTTPException 502 if the API call fails.
    """
    if not _RETENTION_API_BASE_URL:
        logger.warning("[GuacamoleRetention] GUACAMOLE_RETENTION_API_BASE_URL not set — skipping logs delete")
        return {"skipped": True, "reason": "GUACAMOLE_RETENTION_API_BASE_URL not configured"}

    try:
        r = httpx.post(
            f"{_RETENTION_API_BASE_URL}/guacamole/logs/delete",
            json={"retention_days": retention_days},
            timeout=30.0,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Logs retention API call failed: {e}")


def call_recordings_retention_api(retention_days: int) -> dict:
    """
    Recordings-PV retention -- recordings_path is optional; if not given, the API's default (/recordings) is used.
    Returns: {"skipped": True, "reason": "..."} if the base URL is not configured, otherwise
             the external API's JSON response as-is.
    Raises: HTTPException 502 if the API call fails.
    """
    if not _RETENTION_API_BASE_URL:
        logger.warning("[GuacamoleRetention] GUACAMOLE_RETENTION_API_BASE_URL not set — skipping recordings delete")
        return {"skipped": True, "reason": "GUACAMOLE_RETENTION_API_BASE_URL not configured"}

    payload = {"retention_days": retention_days}
    if _RECORDINGS_PATH:
        payload["recordings_path"] = _RECORDINGS_PATH

    try:
        r = httpx.post(
            f"{_RETENTION_API_BASE_URL}/guacamole/recordings/delete",
            json=payload,
            timeout=30.0,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Recordings retention API call failed: {e}")


def get_current_retention(db: Session) -> Optional[GuacamoleRetentionSetting]:
    """Fetch the current retention setting (singleton row). Returns None if it was never set."""
    return db.query(GuacamoleRetentionSetting).first()


def upsert_retention_setting(db: Session, retention_days: int, updated_by: Optional[str] = None) -> GuacamoleRetentionSetting:
    """
    Singleton row -- update the first row, or create it if none exists.
    Returns: GuacamoleRetentionSetting ORM object (created or updated).
    """
    row = db.query(GuacamoleRetentionSetting).first()
    if row:
        row.retention_days = retention_days
        row.updated_by = updated_by
    else:
        row = GuacamoleRetentionSetting(retention_days=retention_days, updated_by=updated_by)
        db.add(row)
    db.commit()
    db.refresh(row)
    return row


async def ensure_retention_schedule(client: TemporalClient, retention_days: int):
    """
    A daily (02:30 UTC) Temporal Schedule that runs GuacamoleRetentionWorkflow
    with the new retention_days -- that workflow internally hits both external
    retention APIs. As soon as retention is updated, the old schedule (if any)
    is removed and a fresh one is created -- delete+recreate instead of an
    in-place update, because retention changes are rare and this is simpler/safer
    (the same pattern was used in the Temporal namespace retention feature).
    """
    from service.temporalResource.workflows.workflows_guacamole_retention import GuacamoleRetentionWorkflow

    try:
        handle = client.get_schedule_handle(_RETENTION_SCHEDULE_ID)
        await handle.delete()
    except Exception:
        pass  # the schedule did not exist yet -- that's fine

    await client.create_schedule(
        _RETENTION_SCHEDULE_ID,
        Schedule(
            action=ScheduleActionStartWorkflow(
                GuacamoleRetentionWorkflow.run,
                retention_days,
                id=f"{_RETENTION_SCHEDULE_ID}-workflow",
                task_queue=_RETENTION_TASK_QUEUE,
            ),
            spec=ScheduleSpec(calendars=[ScheduleCalendarSpec(
                hour=[ScheduleRange(start=2)],
                minute=[ScheduleRange(start=30)],
            )]),
            state=ScheduleState(note=f"Daily Guacamole retention cleanup — retention={retention_days} days"),
        ),
    )

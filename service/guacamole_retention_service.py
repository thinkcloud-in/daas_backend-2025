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
    if not value:
        return ""
    return value.split("#", 1)[0].strip().rstrip("/")


# Ek hi pod (guacamole-retention-tool) dono retention APIs serve karta hai --
# /guacamole/logs/delete (connection history, DB creds pod ke env se
# automatic) aur /guacamole/recordings/delete (recordings PV cleanup).
_RETENTION_API_BASE_URL = _clean_env_url(os.getenv("GUACAMOLE_RETENTION_API_BASE_URL"))
# Optional override -- na diya to external API apna default (/recordings) use karta hai.
_RECORDINGS_PATH = os.getenv("GUACAMOLE_RECORDINGS_PATH")


def call_history_retention_api(retention_days: int) -> dict:
    """Connection-history (logs) retention -- DB creds pod ke env se aate hain, sirf retention_days bhejna hai."""
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
    """Recordings-PV retention -- recordings_path optional hai, na diya to API ka default (/recordings) chalega."""
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
    return db.query(GuacamoleRetentionSetting).first()


def upsert_retention_setting(db: Session, retention_days: int, updated_by: Optional[str] = None) -> GuacamoleRetentionSetting:
    """Singleton row -- pehli hi row ko update karo, ya na ho to banao."""
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
    Daily (02:30 UTC) Temporal Schedule jo GuacamoleRetentionWorkflow chalati
    hai naye retention_days ke saath -- wahi workflow andar se dono external
    retention APIs ko hit karta hai. Retention update hote hi purani
    schedule (agar ho) hata ke fresh schedule bana di jaati hai -- in-place
    update ki jagah delete+recreate isliye kyunki retention change rare hai
    aur ye zyada simple/safe hai (Temporal namespace retention feature me
    bhi yahi pattern use hua tha).
    """
    from service.temporalResource.workflows.workflows_guacamole_retention import GuacamoleRetentionWorkflow

    try:
        handle = client.get_schedule_handle(_RETENTION_SCHEDULE_ID)
        await handle.delete()
    except Exception:
        pass  # schedule pehle se nahi thi -- theek hai

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

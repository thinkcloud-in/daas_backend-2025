from typing import Optional

from pydantic import BaseModel
from sqlalchemy.orm import Session

from service import guacamole_retention_service
from utils.temporal_client import TemporalClientManager


class UpdateGuacamoleRetentionRequest(BaseModel):
    retention_days: int
    email: str


async def update_guacamole_retention(request: UpdateGuacamoleRetentionRequest, db: Session) -> dict:
    """
    Persist the setting to the DB, and (re)create the daily Temporal Schedule
    — that schedule hits both external retention APIs (history + recordings)
    at a fixed time (02:30 UTC) with the new retention_days.
    """
    row = guacamole_retention_service.upsert_retention_setting(
        db, request.retention_days, updated_by=request.email,
    )

    schedule_status = "not_scheduled"
    client = await TemporalClientManager.get_temporal_client()
    if client is not None:
        await guacamole_retention_service.ensure_retention_schedule(client, request.retention_days)
        schedule_status = "daily @ 02:30 UTC"

    return {
        "retention_days": row.retention_days,
        "updated_by": row.updated_by,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        "schedule": schedule_status,
    }


async def get_guacamole_retention_settings(db: Session) -> dict:
    row = guacamole_retention_service.get_current_retention(db)
    if not row:
        return {
            "retention_days": guacamole_retention_service.DEFAULT_RETENTION_DAYS,
            "updated_by": None,
            "updated_at": None,
            "is_default": True,
        }
    return {
        "retention_days": row.retention_days,
        "updated_by": row.updated_by,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        "is_default": False,
    }

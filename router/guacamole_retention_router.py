from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from controllers import guacamole_retention_controller
from db_configuration.config import get_db

guacamole_retention_router = APIRouter(prefix="/v1/retention/guacamole", tags=["Guacamole Retention"])


@guacamole_retention_router.put(
    "/update-retention",
    summary="Set Guacamole connection-history retention — runs an immediate cleanup pass and persists the setting",
)
async def api_update_guacamole_retention(
    request: guacamole_retention_controller.UpdateGuacamoleRetentionRequest,
    db: Session = Depends(get_db),
):
    return await guacamole_retention_controller.update_guacamole_retention(request, db)


@guacamole_retention_router.get(
    "/settings",
    summary="Current Guacamole retention configuration — default is 30 days if never set",
)
async def api_get_guacamole_retention_settings(db: Session = Depends(get_db)):
    return await guacamole_retention_controller.get_guacamole_retention_settings(db)

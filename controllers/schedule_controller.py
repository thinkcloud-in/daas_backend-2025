"""
Report-schedule controller — router/schedule_router.py ("/v1/schedule") is
par delegate karta hai. Actual DB/Temporal logic service/schedule_service.py
mein hai.
"""
from fastapi import Depends, APIRouter
from sqlalchemy.orm import Session
from models.schedule_model import Schedule_report
from db_configuration.config import get_db
import service.schedule_service as service
from utils import response_format
from datetime import date, datetime, time

router = APIRouter( prefix="/v1/schedule", tags=["schedule"])


def sqlalchemy_to_dict(obj):
    """SQLAlchemy model instance ko dict mein convert karo (date/time columns → str)."""
    if not obj:
        return {}

    result = {}
    for column in obj.__table__.columns:
        value = getattr(obj, column.name)

        # Date/Time ko string me convert karo
        if isinstance(value, (datetime, date, time)):
            value = str(value)

        result[column.name] = value

    return result

def create_item(item, db):
    """
    Naya recurring report-schedule create karo.

    Used by: POST /v1/schedule/add_schedule
    Returns: success_response ke `data` mein saved record (id included).
    """
    try:
        db_item = service.post_data(item, db)
        data = sqlalchemy_to_dict(db_item)
        return response_format.success_response(201, "Schedule created successfully", data)
    except Exception as e:
        return response_format.error_response(500, "Internal Server Error", str(e))

# Read all items
async def get_items(db: Session = Depends(get_db)):
    """
    Saare schedules list karo.

    Used by: GET /v1/schedule/get_schedules
    Returns: service.get_data() ka result (already APIResponse-wrapped).
    """
    return await service.get_data(db)


async def get_item_id(item_id: int ,db: Session = Depends(get_db)):
    """
    Ek schedule ki detail lo, id se.

    Used by: GET /v1/schedule/get_schedule/{item_id}
    Returns: service.get_data_id() ka result (already APIResponse-wrapped).
    """
    return await service.get_data_id(item_id  ,db)

async def get_item_report(limit: int, offset: int, db: Session = Depends(get_db)):
    """
    Schedule-runs ki history/report nikalo.

    Used by: GET /v1/schedule/get_schedules_report
    Returns: success_response ke `data` mein [ {..run summary..}, ... ].
    """
    data = await service.get_data_report(limit, offset, db)
    return response_format.success_response(200, "Successfully retrieved schedule report", data)

# Update an item
async def update_item(item_id: int, item: Schedule_report, db: Session = Depends(get_db)):
    """
    Existing schedule update karo.

    Used by: PUT /v1/schedule/update_schedule/{item_id}
    Returns: success_response ke `data` mein updated <Schedule_report>.
    """
    data = await service.update_data_id(item_id, item, db)
    return response_format.success_response(200, "Successfully updated schedule", data)

# Delete an item
async def delete_item(item_id, db):
    """
    Schedule delete karo.

    Used by: DELETE /v1/schedule/delete_schedule/{item_id}
    Returns: success_response ke `data` mein deleted item summary/id.
    """
    data = await service.delete_data_id(item_id, db)
    return response_format.success_response(200, "Successfully deleted schedule", data)

async def get_status(schedule_id: str):
    """
    Ek schedule ke Temporal-schedule ki current status lo.

    Used by: GET /v1/schedule/get_schedule_status/{schedule_id}
    Returns: success_response ke `data` mein {"schedule_id", "status", ...}.
    """
    data = await service.get_temporal_status(schedule_id)
    return response_format.success_response(200, "Successfully retrieved schedule status", data)

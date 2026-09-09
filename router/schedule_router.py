from fastapi import APIRouter, Depends, Query
from typing import List,Any, Union
from sqlalchemy.orm import Session
from models.API_Response_model import APIResponse
from models.schedule_model import Schedule_report
from db_configuration.config import get_db
from controllers import schedule_controller

# ---------------------- ROUTER CONFIG ----------------------
schedule_router = APIRouter(prefix="/v1/schedule", tags=["Schedule"])


# ---------------------- CREATE ----------------------
@schedule_router.post("/add_schedule")
async def create_item(
    item: Schedule_report,
    db: Session = Depends(get_db)
):
    """
    Naya recurring report-schedule banao (kis report ko, kab, kisko email
    karna hai).

    Request body: Schedule_report.

    Response 201 — `data`: saved schedule record (id included).
    Errors: 500 (error_response) agar save fail ho.
    """
    return schedule_controller.create_item(item, db)


# ---------------------- READ ALL ----------------------
@schedule_router.get("/get_schedules", response_model=APIResponse[List[Schedule_report]])
async def get_items(db: Session = Depends(get_db)):
    """
    Saare schedules list karo.

    Response 200 — `data`: [ <Schedule_report>, ... ]
    """
    return await schedule_controller.get_items(db)


# ---------------------- READ BY ID ----------------------
@schedule_router.get("/get_schedule/{item_id}", response_model=APIResponse[Schedule_report])
async def get_item_id(
    item_id: int,
    db: Session = Depends(get_db)
):
    """
    Ek schedule ki detail lo, id se.

    Response 200 — `data`: <Schedule_report>
    """
    return await schedule_controller.get_item_id(item_id, db)

# ---------------------- READ BY REPORT ----------------------
@schedule_router.get("/get_schedules_report")
async def get_item_report(
    limit: int = Query(10, ge=1, description="Number of records to fetch"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    db: Session = Depends(get_db)
):
    """
    Schedule-runs ki report/history nikalo (kab-kab run hua, kya status raha).

    Response 200 — `data`: [ {..run summary.., "status": str, "ran_at": ...}, ... ]
    (limit/offset se manually paginated, `total`/`pagination` object nahi milta.)
    """
    return await schedule_controller.get_item_report(limit, offset, db)

# ---------------------- UPDATE ----------------------
@schedule_router.put("/update_schedule/{item_id}", response_model=APIResponse[Union[Schedule_report, Any]])
async def update_item(
    item_id: int,
    item: Schedule_report,
    db: Session = Depends(get_db)
):
    """
    Existing schedule update karo (frequency, recipients, report-type, etc.).

    Request body: Schedule_report (poori object bhejo, overwrite hoti hai).

    Response 200 — `data`: updated <Schedule_report>.
    """
    return await schedule_controller.update_item(item_id, item, db)

# ---------------------- DELETE ----------------------
@schedule_router.delete("/delete_schedule/{item_id}")
async def delete_item(
    item_id: int,
    db: Session = Depends(get_db)
):
    """
    Schedule delete karo.

    Response 200 — `data`: deleted item ka id/summary.
    """
    return await schedule_controller.delete_item(item_id, db)

# ---------------------- STATUS ----------------------
@schedule_router.get("/get_schedule_status/{schedule_id}")
async def get_status(
    schedule_id: str
):
    """
    Ek schedule ke Temporal workflow (schedule) ki current status lo.

    Response 200 — `data`: {"schedule_id": str, "status": str, ...}
    """
    return await schedule_controller.get_status(schedule_id)

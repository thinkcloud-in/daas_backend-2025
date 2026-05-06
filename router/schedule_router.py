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
    return schedule_controller.create_item(item, db)


# ---------------------- READ ALL ----------------------
@schedule_router.get("/get_schedules", response_model=APIResponse[List[Schedule_report]])
async def get_items(db: Session = Depends(get_db)):
    return await schedule_controller.get_items(db)


# ---------------------- READ BY ID ----------------------
@schedule_router.get("/get_schedule/{item_id}", response_model=APIResponse[Schedule_report])
async def get_item_id(
    item_id: int,
    db: Session = Depends(get_db)
):
   return await schedule_controller.get_item_id(item_id, db)

# ---------------------- READ BY REPORT ----------------------
@schedule_router.get("/get_schedules_report")
async def get_item_report(
    limit: int = Query(10, ge=1, description="Number of records to fetch"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    db: Session = Depends(get_db)
):
    return await schedule_controller.get_item_report(limit, offset, db)

# ---------------------- UPDATE ----------------------
@schedule_router.put("/update_schedule/{item_id}", response_model=APIResponse[Union[Schedule_report, Any]])
async def update_item(
    item_id: int,
    item: Schedule_report,
    db: Session = Depends(get_db)
):
    return await schedule_controller.update_item(item_id, item, db)

# ---------------------- DELETE ----------------------
@schedule_router.delete("/delete_schedule/{item_id}")
async def delete_item(
    item_id: int,
    db: Session = Depends(get_db)
):
    return await schedule_controller.delete_item(item_id, db)

# ---------------------- STATUS ----------------------
@schedule_router.get("/get_schedule_status/{schedule_id}")
async def get_status(
    schedule_id: str
):
    return await schedule_controller.get_status(schedule_id)

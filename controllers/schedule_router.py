from fastapi import Depends, APIRouter, Query
from sqlalchemy.orm import Session
from models.schedule_model import Schedule_report
from db_configuration.config import get_db
import service.schedule_service as service
router = APIRouter()


# Create an item
@router.post("/add_schedule/")
def create_item(item: Schedule_report, db: Session = Depends(get_db)):
    try:
        return service.post_data(item, db)
    except Exception as e:
        print("Error creating")

# Read all items
@router.get("/get_schedules/")
async def get_items(db: Session = Depends(get_db)):
    return await service.get_data(db)

# Read an item by ID
@router.get("/get_schedule_id/{item_id}" )
async def get_item_id(item_id: int ,db: Session = Depends(get_db)):
    return await service.get_data_id(item_id  ,db)



@router.get("/get_schedules_report/{report}")
async def get_item_report(report: str, limit: int , offset: int , db: Session = Depends(get_db)):
    return await service.get_data_report(report, limit, offset, db)


# Update an item
@router.put("/update_schedule/{item_id}" )
async def update_item(item_id: int, item: Schedule_report, db: Session = Depends(get_db)):
    return await service.update_data_id(item_id, item, db)

# Delete an item
@router.delete("/delete_schedule/{item_id}")
async def delete_item(item_id: int, db: Session = Depends(get_db)):
    return await service.delete_data_id(item_id, db)

@router.get("/get_schedule_status/{schedule_id}")
async def get_status(schedule_id: str):
    return  await service.get_temporal_status(schedule_id)



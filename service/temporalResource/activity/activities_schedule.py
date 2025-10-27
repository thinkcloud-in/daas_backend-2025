from fastapi import HTTPException
from sqlalchemy import select, func
from temporalio import activity
from sqlalchemy.orm import Session

from db_configuration.config import get_db
from models.schedule_model import Schdeule
from fastapi.encoders import jsonable_encoder


@activity.defn()
async def get_schedule_data_activity():
    db:Session = next(get_db())
    try:
        items = db.query(Schdeule).all()
        items_json = jsonable_encoder(items)
        return {"msg":"getting schedule reports data successfully", "items": items_json}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error while fetching items: {str(e)}")
    

@activity.defn()
async def get_schedule_data_by_id_activity(item_id: int):
    db: Session = next(get_db())
    try:
        item = db.query(Schdeule).filter(Schdeule.id == item_id).first()
        if item is None:
            raise HTTPException(status_code=404, detail="Schedule report not found")
        item_json = jsonable_encoder(item)
        return {"msg":"getting schedule report data successfully", "item": item_json}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error while fetching item: {str(e)}")


@activity.defn()
async def get_schedule_along_report_activity(report:str, limit: int , offset: int):
    db: Session = next(get_db())
    try:
        query = db.query(Schdeule).filter(Schdeule.report == report)
        total_count = query.count()
        items = query.offset(offset).limit(limit).all()
        if items is None:
            raise HTTPException(status_code=404, detail="Schedule report not found")
        items_json = jsonable_encoder(items)
        return {
            "msg": "Getting schedule reports data successfully",
            "items": items_json,
            "total": total_count,
            "limit": limit,
            "offset": offset
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error while fetching items: {str(e)}")
   
 
# @activity.defn()
# async def get_schedule_along_report_activity(report:str, limit: int , offset: int):
#     db: Session = next(get_db())
#     try:
#         query = db.query(Schdeule).filter(Schdeule.report == report).order_by(Schdeule.schedule_date.desc())
#         total_count = db.execute(
#             select(func.count()).select_from(Schdeule).where(Schdeule.report == report)
#         ).scalar()
#         items = query.offset(offset).limit(limit).execution_options(yield_per=50).all()
#         if items is None:
#             raise HTTPException(status_code=404, detail="Schedule report not found")
#         items_json = jsonable_encoder(items)
#         return {
#             "msg": "Getting schedule reports data successfully",
#             "items": items_json,
#             "total": total_count,
#             "limit": limit,
#             "offset": offset
#         }
#     except Exception as e:
#         db.rollback()
#         raise HTTPException(status_code=500, detail=f"Error while fetching items: {str(e)}")
 
 


@activity.defn()
async def update_schedule_data_id_activity(item_id: int, item_data: dict) -> dict:
    db: Session = next(get_db())  
    try:
        db_schedule = db.query(Schdeule).filter(Schdeule.id == item_id).first()
        if db_schedule is None:
            return {"code":509,"msg": "Schedule report not found", "data": None}

        for field, value in item_data.items():
            setattr(db_schedule, field, value)
        db.commit()
        db.refresh(db_schedule)

        db_schedule_json = jsonable_encoder(db_schedule)

        return {"code":200, "msg": "Schedule report updated successfully", "data": db_schedule_json}

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error while updating schedule report: {str(e)}")
    finally:
        db.close()

@activity.defn()
async def delete_schedule_data_id_activity(item_id: int) -> dict:
    db: Session = next(get_db())  
    try:
        db_schedule = db.query(Schdeule).filter(Schdeule.id == item_id).first()
        if db_schedule is None:
            raise HTTPException(status_code=404, detail="Schedule report not found")
        db.delete(db_schedule)
        db.commit()
        db_schedule_json = jsonable_encoder(db_schedule)
        return db_schedule_json
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error while deleting schedule report: {str(e)}")
    finally:
        db.close()
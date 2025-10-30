from fastapi import Depends,APIRouter
import service.smtp_service as smtp_service
from models.SMTP_models import SMTP_Config
from sqlalchemy.orm import Session
from db_configuration.config import get_db
from utils import response_format

smtpRouter = APIRouter(prefix="/v1/smtp", tags=["smtp"])

def smtp_create(item:SMTP_Config, dp:Session = Depends(get_db)):
    try:
        return smtp_service.smtp_post(item, dp)
    except Exception as e:
        return {"error": str(e)}

def sqlalchemy_obj_to_dict(obj):
    return {c.key: getattr(obj, c.key) for c in obj.__table__.columns}

def smtp_get(db):
    try:
        data_objs = smtp_service.smtp_get(db)
        data = [sqlalchemy_obj_to_dict(obj) for obj in data_objs]
        return response_format.success_response(200, "SMTP Servers retrieved successfully.", data)
    except Exception as e:
        return response_format.error_response(500, "Failed to retrieve SMTP Servers", str(e))

def smtp_update(item : SMTP_Config, db:Session = Depends(get_db)):
    try:
        data = smtp_service.smtp_update_data(item,db)
        return response_format.success_response(200, "SMTP Server updated successfully", sqlalchemy_obj_to_dict(data))
    except Exception as e:
        return response_format.error_response(500, "Failed to update SMTP Server", str(e))

def smtp_update_status(data:dict,db:Session = Depends(get_db)):
    try:
        smtp_status = data.get("smtpStatus")
        response = smtp_service.smtp_status_update(smtp_status, db)
        return {"smtpStatus":response.smtpStatus}
    except Exception as e:
        return {"error": str(e)}

def smtp_test_mail(data, db):
    try:
        data = smtp_service.smtp_test_mail(data, db)
        return data
    except Exception as e:
        raise Exception(f"Failed to send test email: {str(e)}")
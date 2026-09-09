"""
SMTP configuration controller — router/smtp_router.py ("/v1/smtp") is par
delegate karta hai. Actual DB/email logic service/smtp_service.py mein hai.
"""
from fastapi import Depends,APIRouter
import service.smtp_service as smtp_service
from models.SMTP_models import SMTP_Config
from sqlalchemy.orm import Session
from db_configuration.config import get_db
from utils import response_format

smtpRouter = APIRouter(prefix="/v1/smtp", tags=["smtp"])

def smtp_create(item:SMTP_Config, dp:Session = Depends(get_db)):
    """
    Nayi SMTP configuration create karo.

    Used by: POST /v1/smtp/smtp-post
    Returns: saved record (smtp_service.smtp_post() ka result) ya
    `{"error": str}` agar exception aaye (⚠ yeh path koi APIResponse
    envelope use nahi karta — is endpoint ki ek known quirk hai).
    """
    try:
        return smtp_service.smtp_post(item, dp)
    except Exception as e:
        return {"error": str(e)}

def sqlalchemy_obj_to_dict(obj):
    """SQLAlchemy model instance ko plain dict mein convert karo (saare columns)."""
    return {c.key: getattr(obj, c.key) for c in obj.__table__.columns}

def smtp_get(db):
    """
    Saari SMTP configurations list karo.

    Used by: GET /v1/smtp/smtp-get
    Returns: success_response ke `data` mein [ {..SMTP_Config columns...}, ... ]
    — `password` field explicitly response se pop kiya jaata hai (kabhi
    leak nahi hota, dusre password-leak issues (Cluster/Pool) se alag yeh
    endpoint already safe hai).
    """
    try:
        data_objs = smtp_service.smtp_get(db)
        data = []
        for obj in data_objs:
            obj_dict = sqlalchemy_obj_to_dict(obj)
            obj_dict.pop('password', None)
            data.append(obj_dict)
        return response_format.success_response(200, "SMTP Servers retrieved successfully.", data)
    except Exception as e:
        return response_format.error_response(500, "Failed to retrieve SMTP Servers", str(e))

def smtp_update(item : SMTP_Config, db:Session = Depends(get_db)):
    """
    Existing SMTP configuration update karo (item.id se match).

    Used by: PUT /v1/smtp/smtp-update
    Returns: success_response ke `data` mein updated record (saare columns,
    yahan `password` pop nahi hota — is endpoint mein us fix ki zaroorat
    pad sakti hai).
    """
    try:
        data = smtp_service.smtp_update_data(item,db)
        return response_format.success_response(200, "SMTP Server updated successfully", sqlalchemy_obj_to_dict(data))
    except Exception as e:
        return response_format.error_response(500, "Failed to update SMTP Server", str(e))

def smtp_update_status(data:dict,db:Session = Depends(get_db)):
    """
    SMTP config enable/disable karo (ek hi ek time pe "active" hoti hai).

    Used by: PATCH /v1/smtp/smtp-update-status
    Args: data = {"id": int, "smtpStatus": bool}
    Returns: {"smtpStatus": bool} ya {"error": str} — koi APIResponse
    envelope nahi (router isko khud wrap karta hai).
    """
    try:
        smtp_status = data.get("smtpStatus")
        response = smtp_service.smtp_status_update(smtp_status, db)
        return {"smtpStatus":response.smtpStatus}
    except Exception as e:
        return {"error": str(e)}

def smtp_test_mail(data, db):
    """
    Diye gaye SMTP settings se test email bhejo (DB mein save kiye bina).

    Used by: POST /v1/smtp/smtp-test-mail
    Returns: smtp_service.smtp_test_mail() ka result (input echo).
    Raises: Exception agar SMTP server se connect/send fail ho jaaye.
    """
    try:
        data = smtp_service.smtp_test_mail(data, db)
        return data
    except Exception as e:
        raise Exception(f"Failed to send test email: {str(e)}")

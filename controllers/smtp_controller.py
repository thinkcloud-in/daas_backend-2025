"""
SMTP configuration controller — router/smtp_router.py ("/v1/smtp")
delegates to this. The actual DB/email logic is in service/smtp_service.py.
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
    Create a new SMTP configuration.

    Used by: POST /v1/smtp/smtp-post
    Returns: the saved record (the result of smtp_service.smtp_post()) or
    `{"error": str}` if an exception occurs (⚠ this path doesn't use an
    APIResponse envelope — a known quirk of this endpoint).
    """
    try:
        return smtp_service.smtp_post(item, dp)
    except Exception as e:
        return {"error": str(e)}

def sqlalchemy_obj_to_dict(obj):
    """Convert a SQLAlchemy model instance into a plain dict (all columns)."""
    return {c.key: getattr(obj, c.key) for c in obj.__table__.columns}

def smtp_get(db):
    """
    List all SMTP configurations.

    Used by: GET /v1/smtp/smtp-get
    Returns: success_response's `data` has [ {..SMTP_Config columns...}, ... ]
    — the `password` field is explicitly popped from the response (never
    leaked; unlike the other password-leak issues found elsewhere
    (Cluster/Pool), this endpoint is already safe).
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
    Update an existing SMTP configuration (matched by item.id).

    Used by: PUT /v1/smtp/smtp-update
    Returns: success_response's `data` has the updated record (all columns —
    `password` is NOT popped here, this endpoint may need the same fix).
    """
    try:
        data = smtp_service.smtp_update_data(item,db)
        return response_format.success_response(200, "SMTP Server updated successfully", sqlalchemy_obj_to_dict(data))
    except Exception as e:
        return response_format.error_response(500, "Failed to update SMTP Server", str(e))

def smtp_update_status(data:dict,db:Session = Depends(get_db)):
    """
    Enable/disable an SMTP config (only one is "active" at a time).

    Used by: PATCH /v1/smtp/smtp-update-status
    Args: data = {"id": int, "smtpStatus": bool}
    Returns: {"smtpStatus": bool} or {"error": str} — no APIResponse
    envelope here (the router wraps it itself).
    """
    try:
        smtp_status = data.get("smtpStatus")
        response = smtp_service.smtp_status_update(smtp_status, db)
        return {"smtpStatus":response.smtpStatus}
    except Exception as e:
        return {"error": str(e)}

def smtp_test_mail(data, db):
    """
    Send a test email using the given SMTP settings (without saving to the DB).

    Used by: POST /v1/smtp/smtp-test-mail
    Returns: the result of smtp_service.smtp_test_mail() (echoes the input).
    Raises: Exception if connecting to/sending via the SMTP server fails.
    """
    try:
        data = smtp_service.smtp_test_mail(data, db)
        return data
    except Exception as e:
        raise Exception(f"Failed to send test email: {str(e)}")

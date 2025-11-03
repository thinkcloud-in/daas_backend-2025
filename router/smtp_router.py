from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from db_configuration.config import get_db
from models.API_Response_model import APIResponse
from models.SMTP_models import SMTP_Config
from controllers import smtp_controller
from utils import response_format

smtp_router = APIRouter(prefix="/v1/smtp", tags=["SMTP"])


# ----------------------- SMTP ROUTES -----------------------

@smtp_router.post("/smtp-post", summary="Create SMTP Configuration")
def smtp_create(item: SMTP_Config, db: Session = Depends(get_db)):
        return smtp_controller.smtp_create(item, db)

@smtp_router.get("/smtp-get", summary="Get all SMTP configurations")
def smtp_get(db: Session = Depends(get_db)):
    return smtp_controller.smtp_get(db)


@smtp_router.put("/smtp-update", summary="Update SMTP configuration")
def smtp_update(item: SMTP_Config, db: Session = Depends(get_db)):
        return smtp_controller.smtp_update(item, db)

@smtp_router.patch("/smtp-update-status", summary="Enable or disable SMTP configuration", response_model=APIResponse)
def smtp_update_status(data: dict, db: Session = Depends(get_db)):
    res = smtp_controller.smtp_update_status(data, db)
    return response_format.success_response(200, "SMTP configuration status updated successfully.", res)

@smtp_router.post("/smtp-test-mail", summary="Send test mail via SMTP configuration", response_model=APIResponse)
def smtp_test_mail(data: SMTP_Config, db: Session = Depends(get_db)):
    res = smtp_controller.smtp_test_mail(data, db)
    return response_format.success_response(200, "Test mail sent successfully.", res)

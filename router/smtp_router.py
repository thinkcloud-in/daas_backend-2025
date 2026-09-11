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
    """
    Create a new SMTP configuration (email-sending settings — host, port,
    username/password, from-address).

    Request body: SMTP_Config.

    Response 200 — `data`: the saved SMTP_Config record (with generated id).
    Note: this endpoint returns HTTP 200 even on failure, with
    `{"error": str}` in the body — it isn't validated by response_model, so
    the frontend needs to check for an `error` key.
    """
    return smtp_controller.smtp_create(item, db)

@smtp_router.get("/smtp-get", summary="Get all SMTP configurations")
def smtp_get(db: Session = Depends(get_db)):
    """
    List all saved SMTP configurations.

    Response 200 — `data`: [ {..SMTP_Config fields.., "id": int}, ... ]
    """
    return smtp_controller.smtp_get(db)


@smtp_router.put("/smtp-update", summary="Update SMTP configuration")
def smtp_update(item: SMTP_Config, db: Session = Depends(get_db)):
    """
    Update an existing SMTP configuration (matched by item.id).

    Request body: SMTP_Config (id required, the rest of the fields overwrite it).

    Response 200 — `data`: the updated record (dict of columns).
    """
    return smtp_controller.smtp_update(item, db)

@smtp_router.patch("/smtp-update-status", summary="Enable or disable SMTP configuration", response_model=APIResponse)
def smtp_update_status(data: dict, db: Session = Depends(get_db)):
    """
    Enable/disable an SMTP configuration (only one active configuration is
    used for sending emails).

    Request body: {"id": int, "smtpStatus": bool}  (these exact keys are
    consumed inside the service).

    Response 200 — `data`: {"smtpStatus": bool}
    """
    res = smtp_controller.smtp_update_status(data, db)
    return response_format.success_response(200, "SMTP configuration status updated successfully.", res)

@smtp_router.post("/smtp-test-mail", summary="Send test mail via SMTP configuration", response_model=APIResponse)
def smtp_test_mail(data: SMTP_Config, db: Session = Depends(get_db)):
    """
    Send a test email using the given SMTP settings (without saving to the
    DB) — for a "Test Connection" style button.

    Request body: SMTP_Config (the full settings to test).

    Response 200 — `data`: the input SMTP_Config echoed back (for confirmation).
    Errors: if the SMTP server can't be connected to, an exception is raised
    (500) — the frontend should handle this case too.
    """
    res = smtp_controller.smtp_test_mail(data, db)
    return response_format.success_response(200, "Test mail sent successfully.", res)

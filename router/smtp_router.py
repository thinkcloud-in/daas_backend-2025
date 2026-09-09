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
    Nayi SMTP configuration create karo (email-sending settings — host, port,
    username/password, from-address).

    Request body: SMTP_Config.

    Response 200 — `data`: saved SMTP_Config record (with generated id).
    Note: yeh endpoint failure pe bhi HTTP 200 deta hai, body mein
    `{"error": str}` — response_model se validate nahi hota, isliye
    frontend ko `error` key check karni chahiye.
    """
    return smtp_controller.smtp_create(item, db)

@smtp_router.get("/smtp-get", summary="Get all SMTP configurations")
def smtp_get(db: Session = Depends(get_db)):
    """
    Saari saved SMTP configurations list karo.

    Response 200 — `data`: [ {..SMTP_Config fields.., "id": int}, ... ]
    """
    return smtp_controller.smtp_get(db)


@smtp_router.put("/smtp-update", summary="Update SMTP configuration")
def smtp_update(item: SMTP_Config, db: Session = Depends(get_db)):
    """
    Existing SMTP configuration update karo (item.id se match hoti hai).

    Request body: SMTP_Config (id required, baaki fields overwrite karti hain).

    Response 200 — `data`: updated record (dict of columns).
    """
    return smtp_controller.smtp_update(item, db)

@smtp_router.patch("/smtp-update-status", summary="Enable or disable SMTP configuration", response_model=APIResponse)
def smtp_update_status(data: dict, db: Session = Depends(get_db)):
    """
    SMTP configuration ko enable/disable karo (ek hi active configuration
    kaam mein aati hai emails bhejne ke liye).

    Request body: {"id": int, "smtpStatus": bool}  (exact keys service ke
    andar consume hoti hain).

    Response 200 — `data`: {"smtpStatus": bool}
    """
    res = smtp_controller.smtp_update_status(data, db)
    return response_format.success_response(200, "SMTP configuration status updated successfully.", res)

@smtp_router.post("/smtp-test-mail", summary="Send test mail via SMTP configuration", response_model=APIResponse)
def smtp_test_mail(data: SMTP_Config, db: Session = Depends(get_db)):
    """
    Diye gaye SMTP settings se ek test email bhejo (bina DB mein save kiye) —
    "Test Connection" jaisa button ke liye.

    Request body: SMTP_Config (poori settings jo test karni hain).

    Response 200 — `data`: input SMTP_Config wapas (echo — confirmation ke liye).
    Errors: agar SMTP server se connect na ho paaye to exception raise hoti
    hai (500) — frontend ko yeh case bhi handle karna chahiye.
    """
    res = smtp_controller.smtp_test_mail(data, db)
    return response_format.success_response(200, "Test mail sent successfully.", res)

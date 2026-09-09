from typing import Any, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from controllers import help_support_controller
from models.API_Response_model import APIResponse

help_support_router = APIRouter(prefix="/v1/help-support", tags=["help-support"])


class ScrollIdBody(BaseModel):
    scroll_id: str


@help_support_router.get("/logs", response_model=APIResponse[Any])
def get_backend_logs(
    start_date: str = Query(..., description="Start date, format YYYY-MM-DD"),
    end_date:   str = Query(..., description="End date, format YYYY-MM-DD"),
    start_time: str = Query("00:00:00", description="Start time, format HH:MM:SS (default: start of day)"),
    end_time:   str = Query("23:59:59", description="End time, format HH:MM:SS (default: end of day)"),
    page:       int = Query(1, ge=1, description="Page number"),
    page_size:  int = Query(100, ge=1, le=1000, description="Log entries per page"),
    services:   Optional[str] = Query(None, description="Comma-separated service names to filter by, e.g. backend-code,keycloak,guacamole"),
):
    """
    Backend logs OpenSearch se search karo, diye gaye date+time range aur
    (optional) service names ke liye — page/page_size se poore matching range
    ke through paginate kiya ja sakta hai, chahe wahan hazaro logs kyu na hon
    (Help & Support ke liye — troubleshooting/support ke waqt use hoga).

    Response 200 — `data`:
        {
          "total": int, "page": int, "page_size": int, "total_pages": int,
          "has_next": bool, "has_prev": bool,
          "logs": [ {..structlog JSON fields: event, level, logger, timestamp, ...}, ... ]
        }

    Errors: 400 agar date/time format invalid ho, ya requested page
    OpenSearch ki 10,000-result window se aage chala jaaye (bade range ke
    liye scroll endpoints — neeche — use karo).
    """
    return help_support_controller.get_backend_logs(start_date, end_date, start_time, end_time, page, page_size, services)


@help_support_router.post("/logs/scroll/start", response_model=APIResponse[Any])
def start_log_scroll(
    start_date: str = Query(..., description="Start date, format YYYY-MM-DD"),
    end_date:   str = Query(..., description="End date, format YYYY-MM-DD"),
    start_time: str = Query("00:00:00", description="Start time, format HH:MM:SS (default: start of day)"),
    end_time:   str = Query("23:59:59", description="End time, format HH:MM:SS (default: end of day)"),
    services:   Optional[str] = Query(None, description="Comma-separated service names to filter by"),
    batch_size: int = Query(1000, ge=1, le=10000, description="Docs per scroll batch"),
):
    """
    Bade date-range exports ke liye scroll context kholta hai (jaha total
    results 10,000 se aage jaa sakte hain — jahan page/page_size wali normal
    pagination kaam nahi karti). Pehla batch turant milta hai; agla batch
    /logs/scroll/next se lo, aur khatam hone par /logs/scroll close karo.

    Response 200 — `data`:
        {"scroll_id": str, "total": int, "count": int, "has_more": bool, "logs": [...]}
    """
    return help_support_controller.start_log_scroll(start_date, end_date, start_time, end_time, services, batch_size)


@help_support_router.post("/logs/scroll/next", response_model=APIResponse[Any])
def continue_log_scroll(body: ScrollIdBody):
    """
    Pichhle scroll response (start ya isi endpoint) se mile scroll_id se
    agla batch lo. Response me `has_more: false` aaye to loop rok do aur
    /logs/scroll se cleanup karo.

    Request body: ScrollIdBody = {"scroll_id": str}  — hamesha LATEST
    response ka scroll_id use karo (har batch pe naya mil sakta hai).

    Response 200 — `data`: {"scroll_id": str, "total": int, "count": int, "has_more": bool, "logs": [...]}
    Errors: 400 agar scroll_id missing/expired ho.
    """
    return help_support_controller.continue_log_scroll(body.scroll_id)


@help_support_router.delete("/logs/scroll", response_model=APIResponse[Any])
def close_log_scroll(body: ScrollIdBody):
    """
    Scroll context ko explicitly release karo jab scrolling khatam ho jaye.
    Best-effort hai — na bhi karo to OpenSearch scroll TTL khatam hone par
    khud cleanup kar deta hai.

    Response 200 — `data`: {"scroll_id": str}
    """
    return help_support_controller.close_log_scroll(body.scroll_id)


@help_support_router.get("/logs/download")
def download_backend_logs(
    start_date: str = Query(..., description="Start date, format YYYY-MM-DD"),
    end_date:   str = Query(..., description="End date, format YYYY-MM-DD"),
    start_time: str = Query("00:00:00", description="Start time, format HH:MM:SS (default: start of day)"),
    end_time:   str = Query("23:59:59", description="End time, format HH:MM:SS (default: end of day)"),
    services:   Optional[str] = Query(None, description="Comma-separated service names to filter by"),
    batch_size: int = Query(1000, ge=1, le=10000, description="Docs per scroll batch fetched internally"),
):
    """
    Poore matching date range ke logs ko seedha ek .log file ke roop me
    stream karta hai — browser me click karte hi native download trigger
    hota hai (Content-Disposition: attachment). Andar hi scroll lifecycle
    (start/next/close) manage hoti hai, client ko scroll_id se kuch lena-
    dena nahi. Koi server-side storage/temp-file nahi banti.
    Frontend me seedha <a href="...">/window.location se hit karo, fetch+Blob
    ki zaroorat nahi.

    Response: 200, `Content-Type: text/plain` (ya similar), raw log lines
    ka streaming body — koi JSON/APIResponse envelope nahi (isliye is route
    pe `response_model` bhi declare nahi hai).
    """
    return help_support_controller.download_backend_logs(start_date, end_date, start_time, end_time, services, batch_size)

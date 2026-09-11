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
    Search backend logs in OpenSearch for the given date+time range and
    (optional) service names — page/page_size lets you paginate through the
    whole matching range even if there are thousands of logs (used by
    Help & Support for troubleshooting/support work).

    Response 200 — `data`:
        {
          "total": int, "page": int, "page_size": int, "total_pages": int,
          "has_next": bool, "has_prev": bool,
          "logs": [ {..structlog JSON fields: event, level, logger, timestamp, ...}, ... ]
        }

    Errors: 400 if the date/time format is invalid, or the requested page
    goes beyond OpenSearch's 10,000-result window (use the scroll endpoints
    below for larger ranges).
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
    Opens a scroll context for large date-range exports (where total results
    can go beyond 10,000 — where normal page/page_size pagination doesn't
    work). The first batch is returned immediately; fetch the next batch
    from /logs/scroll/next, and close /logs/scroll when done.

    Response 200 — `data`:
        {"scroll_id": str, "total": int, "count": int, "has_more": bool, "logs": [...]}
    """
    return help_support_controller.start_log_scroll(start_date, end_date, start_time, end_time, services, batch_size)


@help_support_router.post("/logs/scroll/next", response_model=APIResponse[Any])
def continue_log_scroll(body: ScrollIdBody):
    """
    Fetch the next batch using the scroll_id from the previous scroll
    response (start or this same endpoint). Stop looping once the response
    has `has_more: false`, and clean up via /logs/scroll.

    Request body: ScrollIdBody = {"scroll_id": str}  — always use the LATEST
    response's scroll_id (a new one can be returned on every batch).

    Response 200 — `data`: {"scroll_id": str, "total": int, "count": int, "has_more": bool, "logs": [...]}
    Errors: 400 if scroll_id is missing/expired.
    """
    return help_support_controller.continue_log_scroll(body.scroll_id)


@help_support_router.delete("/logs/scroll", response_model=APIResponse[Any])
def close_log_scroll(body: ScrollIdBody):
    """
    Explicitly release the scroll context once scrolling is done.
    Best-effort — even if you skip this, OpenSearch cleans it up itself once
    the scroll TTL expires.

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
    Streams the logs for the whole matching date range directly as a .log
    file — clicking it in the browser triggers a native download
    (Content-Disposition: attachment). The scroll lifecycle (start/next/close)
    is managed internally; the client never needs to touch scroll_id. No
    server-side storage/temp-file is created.
    Hit it directly from the frontend with <a href="...">/window.location —
    no need for fetch+Blob.

    Response: 200, `Content-Type: text/plain` (or similar), a streaming body
    of raw log lines — no JSON/APIResponse envelope (which is why this route
    doesn't declare a `response_model` either).
    """
    return help_support_controller.download_backend_logs(start_date, end_date, start_time, end_time, services, batch_size)

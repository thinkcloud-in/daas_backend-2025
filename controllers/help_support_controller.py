"""
Backend-logs (Help & Support) controller — router/help_support_router.py
("/v1/help-support") delegates to this. The actual OpenSearch queries live
in service/help_support_service.py.
"""
from datetime import datetime
from typing import Optional

import zipstream
from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from service import help_support_service
from utils import response_format


def _validate_range(start_date: str, end_date: str, start_time: str, end_time: str):
    """Validate start_date/end_date (YYYY-MM-DD) + start_time/end_time (HH:MM:SS) — raises HTTPException(400) if invalid or start > end."""
    if not start_date or not end_date:
        raise HTTPException(status_code=400, detail="start_date and end_date are required (format: YYYY-MM-DD)")
    try:
        start_dt = datetime.strptime(f"{start_date} {start_time}", "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(f"{end_date} {end_time}", "%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid date/time format — use date=YYYY-MM-DD and time=HH:MM:SS ({e})",
        )
    if start_dt > end_dt:
        raise HTTPException(status_code=400, detail="start_date/start_time must not be after end_date/end_time")


def _parse_services(services: Optional[str]):
    """Split a comma-separated services string into a list (None-safe)."""
    return [s.strip() for s in services.split(",") if s.strip()] if services else None


def get_backend_logs(
    start_date: str, end_date: str,
    start_time: str, end_time: str,
    page: int, page_size: int,
    services: Optional[str] = None,
):
    """
    Search backend logs in OpenSearch, with a date/time range + optional
    service filter (offset pagination).

    Used by: GET /v1/help-support/logs
    Returns: success_response's `data` has {"total", "page", "page_size",
    "total_pages", "has_next", "has_prev", "logs": [...]}
    Raises: 400 invalid range, 400 if the page window goes beyond
    OpenSearch's 10k-result limit.
    """
    _validate_range(start_date, end_date, start_time, end_time)
    result = help_support_service.search_backend_logs(
        start_date, end_date, start_time, end_time, page, page_size, _parse_services(services)
    )
    return response_format.success_response(200, "Backend logs fetched successfully", result)


def start_log_scroll(
    start_date: str, end_date: str,
    start_time: str, end_time: str,
    services: Optional[str] = None,
    batch_size: int = 1000,
):
    """
    Open an OpenSearch scroll context for large date-range exports (to
    bypass the 10k result-window limit).

    Used by: POST /v1/help-support/logs/scroll/start
    Returns: success_response's `data` has {"scroll_id", "total", "count", "has_more", "logs"}
    """
    _validate_range(start_date, end_date, start_time, end_time)
    result = help_support_service.start_log_scroll(
        start_date, end_date, start_time, end_time, _parse_services(services), batch_size
    )
    return response_format.success_response(200, "Scroll started", result)


def continue_log_scroll(scroll_id: str):
    """
    Fetch the next batch of a scroll.

    Used by: POST /v1/help-support/logs/scroll/next
    Returns: success_response's `data` has {"scroll_id", "total", "count", "has_more", "logs"}
    (a new scroll_id may come back — always use the latest one).
    """
    result = help_support_service.continue_log_scroll(scroll_id)
    return response_format.success_response(200, "Next batch fetched", result)


def close_log_scroll(scroll_id: str):
    """
    Release a scroll context (best-effort — silently ignored on failure).

    Used by: DELETE /v1/help-support/logs/scroll
    Returns: success_response's `data` has {"scroll_id": str}
    """
    help_support_service.close_log_scroll(scroll_id)
    return response_format.success_response(200, "Scroll closed", {"scroll_id": scroll_id})


def download_backend_logs(
    start_date: str, end_date: str,
    start_time: str, end_time: str,
    services: Optional[str] = None,
    batch_size: int = 1000,
):
    """
    Streams the logs for the whole matching date range as a .zip file
    straight to the browser download — both layers are genuinely streaming:
    the OpenSearch scroll-batches (no buffering) and, on top of that,
    zipstream-ng's ZIP encoding (each chunk goes to the client as soon as
    it's ready, the full ZIP is never built in memory/disk).
    The first batch is fetched HERE deliberately (outside the generator) —
    if OpenSearch is unreachable or the auth/date is wrong, the error is
    raised right here with a proper HTTP status, before streaming even
    starts (once streaming has started, the HTTP status can no longer be changed).

    Used by: GET /v1/help-support/logs/download
    Returns: StreamingResponse — `application/zip`, a single `.log` file
    inside the zip (no JSON envelope).
    Raises: 400 invalid range, 500/upstream errors if fetching the first
    batch fails.
    """
    _validate_range(start_date, end_date, start_time, end_time)
    service_list = _parse_services(services)

    first_batch = help_support_service.start_log_scroll(
        start_date, end_date, start_time, end_time, service_list, batch_size
    )

    log_filename = f"backend-logs_{start_date}_to_{end_date}.log"
    zip_filename = f"backend-logs_{start_date}_to_{end_date}.zip"

    zs = zipstream.ZipStream(compress_type=zipstream.ZIP_DEFLATED)
    zs.add(
        (line.encode("utf-8") for line in help_support_service.stream_log_batches(first_batch)),
        arcname=log_filename,
    )

    return StreamingResponse(
        zs,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{zip_filename}"'},
    )

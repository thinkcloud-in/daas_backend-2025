"""
Backend-logs (Help & Support) controller — router/help_support_router.py
("/v1/help-support") is par delegate karta hai. Actual OpenSearch queries
service/help_support_service.py mein hain.
"""
from datetime import datetime
from typing import Optional

import zipstream
from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from service import help_support_service
from utils import response_format


def _validate_range(start_date: str, end_date: str, start_time: str, end_time: str):
    """start_date/end_date (YYYY-MM-DD) + start_time/end_time (HH:MM:SS) validate karo — raises HTTPException(400) agar invalid ya start > end."""
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
    """Comma-separated services string ko list mein todo (None-safe)."""
    return [s.strip() for s in services.split(",") if s.strip()] if services else None


def get_backend_logs(
    start_date: str, end_date: str,
    start_time: str, end_time: str,
    page: int, page_size: int,
    services: Optional[str] = None,
):
    """
    OpenSearch se backend logs search karo, date/time range + optional
    service-filter ke saath (offset pagination).

    Used by: GET /v1/help-support/logs
    Returns: success_response ke `data` mein {"total", "page", "page_size",
    "total_pages", "has_next", "has_prev", "logs": [...]}
    Raises: 400 invalid range, 400 agar page window OpenSearch ki 10k-result
    limit se aage jaaye.
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
    Bade date-range exports ke liye OpenSearch scroll-context kholo (10k
    result-window ki limit bypass karne ke liye).

    Used by: POST /v1/help-support/logs/scroll/start
    Returns: success_response ke `data` mein {"scroll_id", "total", "count", "has_more", "logs"}
    """
    _validate_range(start_date, end_date, start_time, end_time)
    result = help_support_service.start_log_scroll(
        start_date, end_date, start_time, end_time, _parse_services(services), batch_size
    )
    return response_format.success_response(200, "Scroll started", result)


def continue_log_scroll(scroll_id: str):
    """
    Scroll ka agla batch fetch karo.

    Used by: POST /v1/help-support/logs/scroll/next
    Returns: success_response ke `data` mein {"scroll_id", "total", "count", "has_more", "logs"}
    (naya scroll_id mil sakta hai — hamesha latest use karo).
    """
    result = help_support_service.continue_log_scroll(scroll_id)
    return response_format.success_response(200, "Next batch fetched", result)


def close_log_scroll(scroll_id: str):
    """
    Scroll context release karo (best-effort — fail ho to bhi silently ignore).

    Used by: DELETE /v1/help-support/logs/scroll
    Returns: success_response ke `data` mein {"scroll_id": str}
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
    Poore matching date range ke logs ko ek .zip file ke roop me directly
    browser download karwata hai — dono layers genuinely streaming hain:
    OpenSearch se scroll-batches (koi buffering nahi) aur us par zipstream-ng
    ka ZIP encoding (jo bhi chunk ready ho turant client ko chala jaata hai,
    poori ZIP kabhi memory/disk pe nahi banti).
    Pehla batch YAHIN (generator ke bahar) fetch karte hain jaanbujhke — agar
    OpenSearch unreachable ho ya auth/date galat ho, to error yahi turant
    proper HTTP status ke saath raise ho jata hai, streaming shuru hone se
    pehle hi (streaming shuru hone ke baad HTTP status badalna possible nahi
    hota).

    Used by: GET /v1/help-support/logs/download
    Returns: StreamingResponse — `application/zip`, ek `.log` file zip ke
    andar (koi JSON envelope nahi).
    Raises: 400 invalid range, 500/upstream errors agar pehla batch fetch fail ho.
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

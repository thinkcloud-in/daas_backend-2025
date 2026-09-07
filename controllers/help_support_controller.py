from datetime import datetime
from typing import Optional

import zipstream
from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from service import help_support_service
from utils import response_format


def _validate_range(start_date: str, end_date: str, start_time: str, end_time: str):
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
    return [s.strip() for s in services.split(",") if s.strip()] if services else None


def get_backend_logs(
    start_date: str, end_date: str,
    start_time: str, end_time: str,
    page: int, page_size: int,
    services: Optional[str] = None,
):
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
    _validate_range(start_date, end_date, start_time, end_time)
    result = help_support_service.start_log_scroll(
        start_date, end_date, start_time, end_time, _parse_services(services), batch_size
    )
    return response_format.success_response(200, "Scroll started", result)


def continue_log_scroll(scroll_id: str):
    result = help_support_service.continue_log_scroll(scroll_id)
    return response_format.success_response(200, "Next batch fetched", result)


def close_log_scroll(scroll_id: str):
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

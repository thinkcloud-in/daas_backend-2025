"""
help_support_service — service layer for backend-logs search/export, based on OpenSearch.

Two modes: (1) offset pagination (`search_backend_logs`) for the UI list-view, (2) the scroll API
(`start_log_scroll`/`continue_log_scroll`/`close_log_scroll`/`stream_log_batches`) for large date-range
exports where the total results can go beyond OpenSearch's 10,000 from+size limit.
Used by: controllers/help_support_controller.py.
"""
import os
from typing import Generator, List, Optional

import httpx
import structlog
from fastapi import HTTPException

logger = structlog.get_logger("help_support_service")

_OPENSEARCH_URL      = os.getenv("OPENSEARCH_URL", "https://172.16.0.101:30920").rstrip("/")
_OPENSEARCH_USER     = os.getenv("OPENSEARCH_USER", "log-reader")
_OPENSEARCH_PASSWORD = os.getenv("OPENSEARCH_PASSWORD", "")
_OPENSEARCH_INDEX    = os.getenv("OPENSEARCH_INDEX", "backend-logs-*")

_TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=5.0, pool=5.0)


# OpenSearch's default index.max_result_window — going beyond this (from + size)
# would need a dedicated cursor mechanism (search_after) for deep pagination;
# offset pagination errors out past this limit.
_MAX_RESULT_WINDOW = 10000

# How long the scroll context stays alive after each request — whatever it is,
# every /scroll/next call renews it, so the scroll does not expire as long as
# the client keeps polling.
_SCROLL_TTL = "2m"


def _build_bool_must(start_date, end_date, start_time, end_time, services):
    """Build the "must" clause list of an OpenSearch bool query: timestamp range + optional service filter."""
    must = [
        {"range": {"@timestamp": {"gte": f"{start_date}T{start_time}Z", "lte": f"{end_date}T{end_time}Z"}}}
    ]
    if services:
        must.append({"terms": {"service.keyword": services}})
    return must


def _post(url: str, payload: dict) -> dict:
    """POST to OpenSearch (basic auth). Returns the parsed JSON body. Raises HTTPException 502 on unreachable/401/error status."""
    try:
        r = httpx.post(url, json=payload, auth=(_OPENSEARCH_USER, _OPENSEARCH_PASSWORD), timeout=_TIMEOUT, verify=False)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OpenSearch unreachable ({_OPENSEARCH_URL}): {e}")
    if r.status_code == 401:
        raise HTTPException(status_code=502, detail="OpenSearch credentials invalid (401)")
    if not r.is_success:
        raise HTTPException(status_code=502, detail=f"OpenSearch error {r.status_code}: {r.text[:300]}")
    return r.json()


def _extract_batch(body: dict) -> dict:
    """Extract one batch from an OpenSearch scroll response. Returns {"scroll_id","total","count","has_more","logs"}."""
    hits = body.get("hits", {})
    total_raw = hits.get("total", 0)
    total = total_raw.get("value", 0) if isinstance(total_raw, dict) else total_raw
    raw_hits = hits.get("hits", [])
    logs = [h.get("_source", {}) for h in raw_hits]
    return {
        "scroll_id": body.get("_scroll_id"),
        "total": total,
        "count": len(logs),
        "has_more": len(raw_hits) > 0,
        "logs": logs,
    }


def search_backend_logs(
    start_date: str, end_date: str,
    start_time: str = "00:00:00", end_time: str = "23:59:59",
    page: int = 1, page_size: int = 100,
    services: Optional[List[str]] = None,
) -> dict:
    """
    Search logs in the backend-logs-* index in OpenSearch by @timestamp range.
    start_date/end_date come in "YYYY-MM-DD" and start_time/end_time in
    "HH:MM:SS" format — together they form the exact datetime range (default is
    the whole day: 00:00:00 to 23:59:59).
    If services is given, only logs with those service.keyword values are
    returned (e.g. ["backend-code", "keycloak", "guacamole"]) — if not given,
    logs from all services are returned.
    page/page_size use OpenSearch's `from`+`size` offset pagination — even if a
    10-days-ago range has thousands of logs, they can all be retrieved
    page-by-page, not limited to just the first 100.
    Returns: {"total", "page", "page_size", "total_pages", "has_next",
              "has_prev", "logs"}
    """
    from_ = (page - 1) * page_size
    if from_ + page_size > _MAX_RESULT_WINDOW:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Requested page goes beyond {_MAX_RESULT_WINDOW} results "
                f"(OpenSearch's offset-pagination limit) — narrow the date/time "
                f"range or service filter instead of paging this deep."
            ),
        )

    url = f"{_OPENSEARCH_URL}/{_OPENSEARCH_INDEX}/_search"
    query = {
        "from": from_,
        "size": page_size,
        "sort": [{"@timestamp": {"order": "desc"}}],
        "query": {"bool": {"must": _build_bool_must(start_date, end_date, start_time, end_time, services)}},
    }

    body = _post(url, query)
    hits = body.get("hits", {})
    total_raw = hits.get("total", 0)
    total = total_raw.get("value", 0) if isinstance(total_raw, dict) else total_raw
    logs = [h.get("_source", {}) for h in hits.get("hits", [])]

    total_pages = (total + page_size - 1) // page_size if page_size else 1

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "has_next": page < total_pages,
        "has_prev": page > 1,
        "logs": logs,
    }


def start_log_scroll(
    start_date: str, end_date: str,
    start_time: str = "00:00:00", end_time: str = "23:59:59",
    services: Optional[List[str]] = None,
    batch_size: int = 1000,
) -> dict:
    """
    Opens a scroll context for large date-range exports (where the total
    results can go beyond OpenSearch's from+size limit — 10,000).
    Returns only the first batch; get the next batch via continue_log_scroll(),
    and call close_log_scroll() once the scroll is done.
    No `sort` here, deliberately — sorting in the scroll API hurts performance;
    OpenSearch uses its own internal (_doc) order.
    Returns: {"scroll_id", "total", "count", "has_more", "logs"}
    """
    url = f"{_OPENSEARCH_URL}/{_OPENSEARCH_INDEX}/_search?scroll={_SCROLL_TTL}"
    query = {
        "size": batch_size,
        "query": {"bool": {"must": _build_bool_must(start_date, end_date, start_time, end_time, services)}},
    }
    body = _post(url, query)
    return _extract_batch(body)


def continue_log_scroll(scroll_id: str) -> dict:
    """
    Fetch the next batch using the scroll_id from the previous batch's
    response (from start_log_scroll or this function). The scroll_id can be
    new each time — always use the LATEST one going forward, not the old one.
    Returns: {"scroll_id", "total", "count", "has_more", "logs"}
    """
    if not scroll_id:
        raise HTTPException(status_code=400, detail="scroll_id is required")
    url = f"{_OPENSEARCH_URL}/_search/scroll"
    body = _post(url, {"scroll": _SCROLL_TTL, "scroll_id": scroll_id})
    return _extract_batch(body)


def close_log_scroll(scroll_id: str) -> None:
    """
    Explicitly release the scroll context (to free server-side resources)
    once scrolling is done (or the UI closes). Best-effort — OpenSearch also
    cleans up on its own when the scroll TTL expires, so even if this call
    fails we do not fail the request.
    """
    if not scroll_id:
        raise HTTPException(status_code=400, detail="scroll_id is required")
    url = f"{_OPENSEARCH_URL}/_search/scroll"
    try:
        httpx.request(
            "DELETE", url,
            json={"scroll_id": [scroll_id]},
            auth=(_OPENSEARCH_USER, _OPENSEARCH_PASSWORD),
            timeout=_TIMEOUT,
            verify=False,
        )
    except Exception:
        pass


_LOG_LINE_HEADLINE_KEYS = {"timestamp", "@timestamp", "level", "service", "logger", "event"}


def _format_log_line(entry: dict) -> str:
    """
    Convert an OpenSearch log document into a readable .log line.
    The headline (timestamp/level/service/logger/event) shows first, and ALL
    the remaining fields (response_body, status_code, duration_ms, request_id,
    kubernetes metadata, temporal_activity, etc.) follow as key=value pairs —
    nothing is dropped.
    """
    import json as _json

    ts          = entry.get("timestamp") or entry.get("@timestamp") or ""
    level       = (entry.get("level") or "info").upper()
    service     = entry.get("service") or ""
    logger_name = entry.get("logger") or ""
    event       = entry.get("event") or ""

    extra_parts = []
    for k, v in entry.items():
        if k in _LOG_LINE_HEADLINE_KEYS:
            continue
        if isinstance(v, (dict, list)):
            v = _json.dumps(v, ensure_ascii=False)
        extra_parts.append(f"{k}={v}")

    extra_str = ("  " + " ".join(extra_parts)) if extra_parts else ""
    return f"{ts} [{level}] {service} {logger_name} - {event}{extra_str}\n"


def stream_log_batches(first_batch: dict) -> Generator[str, None, None]:
    """
    A generator that continues the scroll from an already-fetched first batch
    (from start_log_scroll, which the controller calls separately so that
    connection/auth errors become proper HTTP errors immediately, BEFORE
    streaming starts) and yields each log entry as a formatted line. The full
    result is never accumulated in memory — the next fetch happens as soon as
    one batch is processed.
    Used with a StreamingResponse (the download endpoint) — no server-side
    file/storage is created.
    """
    batch = first_batch
    scroll_id = batch.get("scroll_id")
    try:
        while True:
            for entry in batch.get("logs", []):
                yield _format_log_line(entry)
            if not batch.get("has_more"):
                break
            try:
                batch = continue_log_scroll(scroll_id)
            except HTTPException as e:
                # Streaming has already started — we can no longer change the
                # HTTP status, so we write the error into the file itself and
                # stop the stream, so the client knows something was left incomplete.
                logger.error("log_download_stream_failed", detail=str(e.detail))
                yield f"\n--- ERROR: log export incomplete: {e.detail} ---\n"
                break
            scroll_id = batch.get("scroll_id") or scroll_id
    finally:
        if scroll_id:
            close_log_scroll(scroll_id)

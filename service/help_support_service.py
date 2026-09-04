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


# OpenSearch ka default index.max_result_window — isse zyada (from + size)
# deep pagination ke liye ek dedicated cursor mechanism (search_after) chahiye
# hota, offset pagination is limit ke aage error deta hai.
_MAX_RESULT_WINDOW = 10000

# Scroll context kitni der zinda rahega har request ke baad — jitna bhi ho,
# har /scroll/next call isko renew kar deta hai, jab tak client polling
# continue rakhe scroll expire nahi hota.
_SCROLL_TTL = "2m"


def _build_bool_must(start_date, end_date, start_time, end_time, services):
    must = [
        {"range": {"@timestamp": {"gte": f"{start_date}T{start_time}Z", "lte": f"{end_date}T{end_time}Z"}}}
    ]
    if services:
        must.append({"terms": {"service.keyword": services}})
    return must


def _post(url: str, payload: dict) -> dict:
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
    OpenSearch me backend-logs-* index se @timestamp range ke hisaab se logs
    search karo. start_date/end_date "YYYY-MM-DD" aur start_time/end_time
    "HH:MM:SS" format me aate hain — dono milke exact datetime range banate
    hain (default poora din: 00:00:00 se 23:59:59 tak).
    services diya jaye to sirf unhi service.keyword values ke logs milenge
    (jaise ["backend-code", "keycloak", "guacamole"]) — na diya jaye to sabhi
    services ke logs aayenge.
    page/page_size se OpenSearch ka `from`+`size` offset pagination use hota
    hai — 10-din pehle ke range me hazaro logs ho to bhi page-by-page saare
    mil jaate hain, sirf pehle 100 tak simit nahi rehta.
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
    Scroll context kholta hai bade date-range exports ke liye (jaha total
    results OpenSearch ki from+size limit — 10,000 — se aage ja sakte hain).
    Sirf pehla batch deta hai; agla batch continue_log_scroll() se lo,
    scroll khatam hone par close_log_scroll() call karna zaroori hai.
    No `sort` yahan jaanbujhke — scroll API me sort lagana performance ko
    kharab karta hai, OpenSearch khud hi internal (_doc) order use karta hai.
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
    Pichhle batch (start_log_scroll ya isi function ke) response se mile
    scroll_id se agla batch fetch karo. Scroll_id har baar naya mil sakta
    hai — hamesha LATEST wala hi aage use karo, purana nahi.
    Returns: {"scroll_id", "total", "count", "has_more", "logs"}
    """
    if not scroll_id:
        raise HTTPException(status_code=400, detail="scroll_id is required")
    url = f"{_OPENSEARCH_URL}/_search/scroll"
    body = _post(url, {"scroll": _SCROLL_TTL, "scroll_id": scroll_id})
    return _extract_batch(body)


def close_log_scroll(scroll_id: str) -> None:
    """
    Scroll context ko explicitly release karo (server-side resources free
    karne ke liye) jab scrolling khatam ho jaye (ya UI band ho jaye). Best-
    effort hai — scroll TTL khatam hone par OpenSearch khud bhi cleanup kar
    deta hai, isliye ye call fail ho to bhi request ko fail nahi karte.
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


def _format_log_line(entry: dict) -> str:
    """Ek OpenSearch log document ko ek readable .log line me convert karo."""
    ts      = entry.get("timestamp") or entry.get("@timestamp") or ""
    level   = (entry.get("level") or "info").upper()
    service = entry.get("service") or ""
    logger_name = entry.get("logger") or ""
    event   = entry.get("event") or ""
    return f"{ts} [{level}] {service} {logger_name} - {event}\n"


def stream_log_batches(first_batch: dict) -> Generator[str, None, None]:
    """
    Generator jo ek already-fetched pehle batch (start_log_scroll se, jo
    controller me alag se call hota hai taaki connection/auth errors turant
    proper HTTP error ban ke jayein, streaming shuru hone se PEHLE) se aage
    scroll continue karta hai aur har log entry ko ek formatted line ke roop
    me yield karta hai. Poora result kabhi memory me ikattha nahi hota — ek
    batch process hote hi agla fetch hota hai.
    StreamingResponse ke saath use hota hai (download endpoint) — koi bhi
    server-side file/storage nahi banti.
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
                # Streaming already shuru ho chuki hai — ab HTTP status badal
                # nahi sakte, isliye error ko file ke andar hi likh ke stream
                # rok dete hain, taaki client ko pata chale kuch adhoora reh gaya.
                logger.error("log_download_stream_failed", detail=str(e.detail))
                yield f"\n--- ERROR: log export incomplete: {e.detail} ---\n"
                break
            scroll_id = batch.get("scroll_id") or scroll_id
    finally:
        if scroll_id:
            close_log_scroll(scroll_id)

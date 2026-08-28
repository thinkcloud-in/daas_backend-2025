import datetime
import uuid
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
import structlog
from starlette.responses import Response
from service.request_Logs_Service import save_request_log
from db_configuration.config import SessionLocal

logger = structlog.get_logger("request_logger")

class RequestLoggerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        tracked_methods = ["POST", "PUT", "PATCH", "DELETE"]

        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex[:12]
        structlog.contextvars.bind_contextvars(
            request_id=request_id,
            method=request.method,
            path=request.url.path,
        )

        start_time = datetime.datetime.now()
        status = "SUCCESS"
        response_data = None

        logger.info("request_started")

        try:
            try:
                response = await call_next(request)
                if hasattr(response, "body_iterator"):
                    body = [chunk async for chunk in response.body_iterator]
                    response.body_iterator = iter(body)
                    try:
                        response_data = b"".join(body).decode("utf-8")
                    except UnicodeDecodeError:
                        response_data = "<binary data>"

                    response = Response(
                        content=response_data,
                        status_code=response.status_code,
                        headers=dict(response.headers),
                        media_type=response.media_type
                    )

            except Exception as e:
                status = "FAILED"
                response_data = str(e)
                duration = (datetime.datetime.now() - start_time).total_seconds()
                logger.exception("request_failed", duration_ms=round(duration * 1000, 2))
                raise e

            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()

            user = request.headers.get("x-user", "Anonymous")

            logger.info(
                "request_finished",
                status_code=response.status_code,
                duration_ms=round(duration * 1000, 2),
                user=user,
            )

            log_entry = {
                "timestamp": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "user": user,
                "method": request.method,
                "url": request.url.path,
                "status": status,
                "duration": f"{duration:.2f}s",
                "response": response_data[:500] if response_data else None
            }

            if request.method in tracked_methods:
                try:
                    db = SessionLocal()
                    save_request_log(db, log_entry)
                except Exception as e:
                    logger.error("request_log_save_failed", error=str(e))
                finally:
                    if 'db' in locals():
                        db.close()

            response.headers["X-Request-ID"] = request_id
            return response
        finally:
            structlog.contextvars.clear_contextvars()

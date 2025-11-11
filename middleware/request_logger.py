import datetime
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
import json
import traceback
import logging
from starlette.responses import Response
from service.request_Logs_Service import save_request_log
from db_configuration.config import SessionLocal

logger = logging.getLogger("request_logger")
logger.setLevel(logging.INFO)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logger.addHandler(console)

class RequestLoggerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        tracked_methods = ["POST", "PUT", "PATCH", "DELETE"]

        start_time = datetime.datetime.now()
        status = "SUCCESS"
        response_data = None

        try:
            response = await call_next(request)
            if hasattr(response, "body_iterator"):
                body = [chunk async for chunk in response.body_iterator]
                response.body_iterator = iter(body)
                # response_data = b"".join(body).decode("utf-8")
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
            traceback.print_exc()
            raise e

        end_time = datetime.datetime.now()
        duration = (end_time - start_time).total_seconds()

        user = request.headers.get("x-user", "Anonymous")

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
                logger.error(f"Failed to save request log to database: {str(e)}")
            finally:
                if 'db' in locals():
                    db.close()
        return response

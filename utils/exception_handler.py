from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from sqlalchemy.exc import SQLAlchemyError
from utils.response_format import error_response
from temporalio.client import WorkflowFailureError
import traceback
import logging

logger = logging.getLogger(__name__)

class ClusterAlreadyExistsException(Exception):
    pass

def exception_handlers(app: FastAPI):

    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger.error(f"[Unhandled Exception] {request.url}: {exc}", exc_info=True)
        traceback.print_exc()
        response = error_response(500, "Internal Server Error", str(exc))
        return JSONResponse(status_code=500, content=response.dict())

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        logger.error(f"[Validation Error] {request.url}: {exc.errors()}", exc_info=True)
        response = error_response(422, "Validation Error", exc.errors())
        return JSONResponse(status_code=422, content=response.dict())

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException):
        logger.error(f"[HTTP Exception] {request.url}: {exc.detail}", exc_info=True)
        if isinstance(exc.detail, dict):
            msg  = exc.detail.get("message", "Error")
            data = {k: v for k, v in exc.detail.items() if k != "message"} or None
        else:
            msg  = exc.detail
            data = None
        response = error_response(exc.status_code, msg, data)
        return JSONResponse(status_code=exc.status_code, content=response.dict())

    @app.exception_handler(SQLAlchemyError)
    async def db_exception_handler(request: Request, exc: SQLAlchemyError):
        logger.error(f"[Database Error] {request.url}: {str(exc)}", exc_info=True)
        response = error_response(500, "Database Error", str(exc))
        return JSONResponse(status_code=500, content=response.dict())

    @app.exception_handler(WorkflowFailureError)
    async def workflow_failure_exception_handler(request: Request, exc: WorkflowFailureError):
        msg = "Workflow execution failed"
        cause = getattr(exc, "__cause__", None)
        while cause:
            if getattr(cause, "cause") and getattr(cause.cause, "message"):
                msg = cause.cause.message
                break

            msg = str(cause)
            cause = getattr(cause, "__cause__", None)
        logger.error(f"[WorkflowFailureError] {request.url}: {msg}", exc_info=True)
        response = error_response(400, "Workflow Error", msg)
        return JSONResponse(status_code=400, content=response.dict())

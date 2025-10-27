from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from sqlalchemy.exc import SQLAlchemyError
from utils.response_format import error_response
import traceback
import logging

logger = logging.getLogger(__name__)

def exception_handlers(app: FastAPI):

    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger.error(f"[Unhandled Exception] {request.url}: {exc}", exc_info=True)
        traceback.print_exc()
        response = error_response(500, "Internal Server Error", str(exc))
        return JSONResponse(status_code=500, content=response.dict())

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        response = error_response(422, "Validation Error", exc.errors())
        return JSONResponse(status_code=422, content=response.dict())

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException):
        response = error_response(exc.status_code, exc.detail)
        return JSONResponse(status_code=exc.status_code, content=response.dict())

    @app.exception_handler(SQLAlchemyError)
    async def db_exception_handler(request: Request, exc: SQLAlchemyError):
        response = error_response(500, "Database Error", str(exc))
        return JSONResponse(status_code=500, content=response.dict())

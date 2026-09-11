from models.API_Response_model import APIResponse, APIResponseWithLimit
from typing import Any, Optional
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

def success_response(code: int, msg: str, data: Optional[Any] = None) -> APIResponse:
    if data is None:
        data = None
    return APIResponse(status='OK', code=code, msg=msg, data=data)

def error_response(code: int, msg: str, data: Optional[Any] = None) -> JSONResponse:
    # The HTTP status line must match `code`, not just the JSON body -- an
    # error shipped as HTTP 200 is silently treated as success by axios/
    # redux-toolkit thunks on the frontend (.unwrap() only rejects on a
    # non-2xx status).
    body = APIResponse(status='Failed', code=code, msg=msg, data=data)
    return JSONResponse(status_code=code, content=jsonable_encoder(body))


def paginated_success_response(
    code: int,
    msg: str,
    data: Optional[Any],
    total: Optional[int],
    limit: int,
    offset: int,
    status: str = "OK"
) -> APIResponseWithLimit:
    return APIResponseWithLimit(
        status=status,
        code=code,
        msg=msg,
        data=data,
        total=total,
        limit=limit,
        offset=offset
    )

def paginated_Error_response(
    code: int,
    msg: str,
    data: Optional[Any],
    total: Optional[int],
    limit: int,
    offset: int,
    status: str = "Failed"
) -> JSONResponse:
    body = APIResponseWithLimit(
        status=status,
        code=code,
        msg=msg,
        data=data,
        total=total,
        limit=limit,
        offset=offset
    )
    return JSONResponse(status_code=code, content=jsonable_encoder(body))

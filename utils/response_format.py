from models.API_Response_model import APIResponse, APIResponseWithLimit
from typing import Any, Optional

def success_response(code: int, msg: str, data: Optional[Any] = None) -> APIResponse:
    if data is None:
        data = None
    return APIResponse(status='OK', code=code, msg=msg, data=data)

def error_response(code: int, msg: str, data: Optional[Any] = None) -> APIResponse:
    if data is None:
        data = None
    return APIResponse(status='Failed', code=code, msg=msg, data=data)


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

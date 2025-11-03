from typing import Optional, Generic, TypeVar
from pydantic.generics import GenericModel

T = TypeVar('T')
class APIResponse(GenericModel, Generic[T]):
    status: str
    code: int
    msg: str
    data: Optional[T] = None


class APIResponseWithLimit(GenericModel, Generic[T]):
    status: str
    code: int
    msg: str
    data: Optional[T] = None
    total: int
    limit: int
    offset: int
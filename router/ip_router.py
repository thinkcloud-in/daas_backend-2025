from fastapi import APIRouter, Query, Depends
from typing import Any, List, Optional
from controllers import ip_controller
from models.API_Response_model import APIResponse
from models.IPs_model import IPSRequest
from sqlalchemy.orm import Session
from db_configuration.config import get_db

ip_router = APIRouter(prefix="/v1/ips", tags=["ips"])

@ip_router.post("/add_ips")
def create_ip_pool(ips_data: IPSRequest, db: Session = Depends(get_db)):
    """
    Create a new IP pool (defines an IP range/list under a name, which is
    later assigned to VDI pools/deployments).

    Request body: IPSRequest (pool_name + IP list/range details).

    Response 201 — `data`: the saved pool record (dict of columns, id included).
    Errors: 409 if `pool_name` already exists.
    """
    return ip_controller.create_ips_routes(ips_data, db)

@ip_router.get("/ips/{ips_id}")
def get_ip_pool(ips_id: int):
    """
    Get one IP pool's detail (by id).

    Response 200 — `data`: the pool record. Response 404 (error_response) if not found.
    """
    return ip_controller.read_ips(ips_id)

@ip_router.get("/get_all_ips")
def get_all_ip_pools(page: int = 1, page_size: int = 10, db: Session = Depends(get_db)):
    """
    List all IP pools (paginated).

    Response 200 — `data`:
        {"items": [ <pool record>, ... ], "pagination": {page, page_size, total, total_pages, has_next, has_prev}}
    """
    return ip_controller.read_all_ips(page, page_size, db)

@ip_router.get("/ip_pool_names", response_model=APIResponse[List[str]])
def get_pool_names(db: Session = Depends(get_db)):
    """
    Get just the list of pool names (to populate a dropdown/select — not the
    full record).

    Response 200 — `data`: ["pool-name-1", "pool-name-2", ...]
    """
    return ip_controller.read_pool_names(db)

@ip_router.get("/available_ips/{pool_id}", response_model=APIResponse[List[str]])
def get_available_ips(pool_id: int, count: int = Query(1, ge=1), db: Session = Depends(get_db)):
    """
    Pull `count` free (unused) IPs from the given pool — doesn't reserve
    them, just for a preview/availability check.

    Response 200 — `data`: ["10.0.0.5", "10.0.0.6", ...]  (length <= count)
    Errors: 404 (error_response) if the pool doesn't have that many free IPs.
    """
    return ip_controller.get_available_ips_route(pool_id, count, db)

@ip_router.delete("/delete_pool_by_name/{pool_name}", response_model=APIResponse[Optional[Any]])
def delete_pool(pool_name: str, db: Session = Depends(get_db)):
    """
    Delete an IP pool (and all its IP entries), by name.

    Response 200 — `data`: a summary/id of the deleted pool.
    """
    return ip_controller.delete_pool_by_name(pool_name, db)

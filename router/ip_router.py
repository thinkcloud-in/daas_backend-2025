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
    Naya IP pool banao (ek naam ke neeh IP-range/list define hoti hai jo
    baad mein VDI pools/deployments ko assign hoti hai).

    Request body: IPSRequest (pool_name + IP list/range details).

    Response 201 — `data`: saved pool record (dict of columns, id included).
    Errors: 409 agar `pool_name` already exist karta ho.
    """
    return ip_controller.create_ips_routes(ips_data, db)

@ip_router.get("/ips/{ips_id}")
def get_ip_pool(ips_id: int):
    """
    Ek IP pool ki detail lo (id se).

    Response 200 — `data`: pool record. Response 404 (error_response) agar na mile.
    """
    return ip_controller.read_ips(ips_id)

@ip_router.get("/get_all_ips")
def get_all_ip_pools(page: int = 1, page_size: int = 10, db: Session = Depends(get_db)):
    """
    Saare IP pools list karo (paginated).

    Response 200 — `data`:
        {"items": [ <pool record>, ... ], "pagination": {page, page_size, total, total_pages, has_next, has_prev}}
    """
    return ip_controller.read_all_ips(page, page_size, db)

@ip_router.get("/ip_pool_names", response_model=APIResponse[List[str]])
def get_pool_names(db: Session = Depends(get_db)):
    """
    Sirf pool names ki list lo (dropdown/select populate karne ke liye —
    poora record nahi).

    Response 200 — `data`: ["pool-name-1", "pool-name-2", ...]
    """
    return ip_controller.read_pool_names(db)

@ip_router.get("/available_ips/{pool_id}", response_model=APIResponse[List[str]])
def get_available_ips(pool_id: int, count: int = Query(1, ge=1), db: Session = Depends(get_db)):
    """
    Diye gaye pool se `count` free (unused) IPs nikalo — reserve nahi karta,
    sirf preview/availability check ke liye.

    Response 200 — `data`: ["10.0.0.5", "10.0.0.6", ...]  (length <= count)
    Errors: 404 (error_response) agar pool mein itni free IPs na ho.
    """
    return ip_controller.get_available_ips_route(pool_id, count, db)

@ip_router.delete("/delete_pool_by_name/{pool_name}", response_model=APIResponse[Optional[Any]])
def delete_pool(pool_name: str, db: Session = Depends(get_db)):
    """
    IP pool (aur uski saari IP entries) delete karo, naam se.

    Response 200 — `data`: deleted pool ka summary/id.
    """
    return ip_controller.delete_pool_by_name(pool_name, db)

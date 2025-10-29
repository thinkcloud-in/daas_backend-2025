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
    return ip_controller.create_ips_routes(ips_data, db)

@ip_router.get("/ips/{ips_id}")
def get_ip_pool(ips_id: int):
    return ip_controller.read_ips(ips_id)

@ip_router.get("/get_all_ips")
def get_all_ip_pools(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    return ip_controller.read_all_ips(skip, limit, db)

@ip_router.get("/ip_pool_names", response_model=APIResponse[List[str]])
def get_pool_names(db: Session = Depends(get_db)):
    return ip_controller.read_pool_names(db)

@ip_router.get("/available_ips/{pool_id}", response_model=APIResponse[List[str]])
def get_available_ips(pool_id: int, count: int = Query(1, ge=1), db: Session = Depends(get_db)):
    return ip_controller.get_available_ips_route(pool_id, count, db)

@ip_router.delete("/delete_pool_by_name/{pool_name}", response_model=APIResponse[Optional[Any]])
def delete_pool(pool_name: str, db: Session = Depends(get_db)):
    return ip_controller.delete_pool_by_name(pool_name, db)

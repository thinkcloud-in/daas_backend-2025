from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session
from controllers import ipmi_controller
from models.API_Response_model import APIResponse
from models.IPMI_models import IPMIDeviceRequest
from db_configuration.config import get_db

ipmi_router = APIRouter(prefix="/v1/ipmi", tags=["ipmi"])

@ipmi_router.post("/add_ipmi_server")
async def add_ipmi_server(ipmi_data: IPMIDeviceRequest, db: Session = Depends(get_db)):
    return await ipmi_controller.create_ipmi_server_route(ipmi_data, db)

@ipmi_router.get("/get_all_ipmi_servers")
async def get_all_ipmi_servers_route(page: int = 1, page_size: int = 10, db: Session = Depends(get_db)):
    return await ipmi_controller.get_all_ipmi_servers_route(db, page, page_size)

@ipmi_router.get("/get_ipmi_server/{ipmi_id}", response_model=APIResponse[IPMIDeviceRequest])
async def get_ipmi_server(ipmi_id: int, db: Session = Depends(get_db)):
    return await ipmi_controller.get_ipmi_server_route(ipmi_id, db)

@ipmi_router.put("/update_ipmi_server/{ipmi_id}", response_model=APIResponse[IPMIDeviceRequest])
async def update_ipmi_server(ipmi_id: int, ipmi_data: IPMIDeviceRequest):
    return await ipmi_controller.update_ipmi_server_route(ipmi_id, ipmi_data)

@ipmi_router.delete("/delete_ipmi_server/{ipmi_id}", response_model=dict)
async def delete_ipmi_server(ipmi_id: int, request: Request, db: Session = Depends(get_db)):
    return await ipmi_controller.delete_ipmi_server_route(ipmi_id, request, db)

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session
from controllers import ipmi_controller
from models.API_Response_model import APIResponse
from models.IPMI_models import IPMIDeviceRequest
from db_configuration.config import get_db

ipmi_router = APIRouter(prefix="/v1/ipmi", tags=["ipmi"])

@ipmi_router.post("/add_ipmi_server")
async def add_ipmi_server(ipmi_data: IPMIDeviceRequest, db: Session = Depends(get_db)):
    """
    Add a new IPMI/BMC server record (connection details for out-of-band
    server management — power on/off, remote console).

    Request body: IPMIDeviceRequest (host/IP, username, password, etc.).

    Response 200 — `data`: the saved IPMI server record (id included).
    Errors: 500 (error_response) if the save fails.
    """
    return await ipmi_controller.create_ipmi_server_route(ipmi_data, db)

@ipmi_router.get("/get_all_ipmi_servers")
async def get_all_ipmi_servers_route(page: int = 1, page_size: int = 10, db: Session = Depends(get_db)):
    """
    List all saved IPMI servers (paginated).

    Response 200 — `data`:
        {"items": [ <IPMI server record>, ... ], "pagination": {page, page_size, total, total_pages, has_next, has_prev}}
    """
    return await ipmi_controller.get_all_ipmi_servers_route(db, page, page_size)

@ipmi_router.get("/get_ipmi_server/{ipmi_id}", response_model=APIResponse[IPMIDeviceRequest])
async def get_ipmi_server(ipmi_id: int, db: Session = Depends(get_db)):
    """
    Get one IPMI server's detail.

    Response 200 — `data`: the IPMI server record.
    """
    return await ipmi_controller.get_ipmi_server_route(ipmi_id, db)

@ipmi_router.put("/update_ipmi_server/{ipmi_id}", response_model=APIResponse[IPMIDeviceRequest])
async def update_ipmi_server(ipmi_id: int, ipmi_data: IPMIDeviceRequest):
    """
    Update an IPMI server's connection details.

    Request body: IPMIDeviceRequest.
    Response: `status`/`code`/`msg` come straight from the service's result
    (`data['status']`, `data['msg']`, `data['data']`) — msg/status vary
    dynamically depending on the backend, they're not fixed.
    """
    return await ipmi_controller.update_ipmi_server_route(ipmi_id, ipmi_data)

@ipmi_router.delete("/delete_ipmi_server/{ipmi_id}", response_model=dict)
async def delete_ipmi_server(ipmi_id: int, request: Request, db: Session = Depends(get_db)):
    """
    Delete an IPMI server record.

    Request body: JSON, must not be empty (e.g. `{"email": "..."}` for the
    audit log) — an empty body returns 400.

    Response 200 — `data`: the deletion result (`data['data']` from the service).
    Errors: 400 if the body is empty, 500 if the delete fails.
    """
    return await ipmi_controller.delete_ipmi_server_route(ipmi_id, request, db)

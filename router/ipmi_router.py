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
    Naya IPMI/BMC server record add karo (out-of-band server management —
    power on/off, remote console — ke liye connection details).

    Request body: IPMIDeviceRequest (host/IP, username, password, etc.).

    Response 200 — `data`: saved IPMI server record (id included).
    Errors: 500 (error_response) agar save fail ho.
    """
    return await ipmi_controller.create_ipmi_server_route(ipmi_data, db)

@ipmi_router.get("/get_all_ipmi_servers")
async def get_all_ipmi_servers_route(page: int = 1, page_size: int = 10, db: Session = Depends(get_db)):
    """
    Saare saved IPMI servers list karo (paginated).

    Response 200 — `data`:
        {"items": [ <IPMI server record>, ... ], "pagination": {page, page_size, total, total_pages, has_next, has_prev}}
    """
    return await ipmi_controller.get_all_ipmi_servers_route(db, page, page_size)

@ipmi_router.get("/get_ipmi_server/{ipmi_id}", response_model=APIResponse[IPMIDeviceRequest])
async def get_ipmi_server(ipmi_id: int, db: Session = Depends(get_db)):
    """
    Ek IPMI server ki detail lo.

    Response 200 — `data`: IPMI server record.
    """
    return await ipmi_controller.get_ipmi_server_route(ipmi_id, db)

@ipmi_router.put("/update_ipmi_server/{ipmi_id}", response_model=APIResponse[IPMIDeviceRequest])
async def update_ipmi_server(ipmi_id: int, ipmi_data: IPMIDeviceRequest):
    """
    IPMI server ki connection details update karo.

    Request body: IPMIDeviceRequest.
    Response: `status`/`code`/`msg` seedha service ke result se aate hain
    (`data['status']`, `data['msg']`, `data['data']`) — msg/status backend
    ke hisaab se dynamic hote hain, fixed nahi.
    """
    return await ipmi_controller.update_ipmi_server_route(ipmi_id, ipmi_data)

@ipmi_router.delete("/delete_ipmi_server/{ipmi_id}", response_model=dict)
async def delete_ipmi_server(ipmi_id: int, request: Request, db: Session = Depends(get_db)):
    """
    IPMI server record delete karo.

    Request body: JSON, khaali nahi hona chahiye (e.g. `{"email": "..."}`
    audit-log ke liye) — empty body pe 400 aata hai.

    Response 200 — `data`: deletion result (service se `data['data']`).
    Errors: 400 agar body empty ho, 500 delete fail hone par.
    """
    return await ipmi_controller.delete_ipmi_server_route(ipmi_id, request, db)

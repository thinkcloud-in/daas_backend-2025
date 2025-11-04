from fastapi import APIRouter
import controllers.hyper_v_controller as controller
from models.API_Response_model import APIResponse
from models.hyper_v_model import CloneVMRequest
from typing import Any

hyper_v_router = APIRouter(prefix="/v1/hyper_v", tags=["Hyper-V"])

# ----------------------- Hyper-V ROUTES -----------------------

@hyper_v_router.get("/get_vms")
async def get_vms():
        return await controller.get_vms()

@hyper_v_router.post("/clone_vm_for_single_node", response_model=APIResponse[Any])
async def clone_vm_for_single_node(request: CloneVMRequest):
        return await controller.clone_vm_for_single_node(request)

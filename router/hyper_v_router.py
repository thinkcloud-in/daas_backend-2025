from models.hyper_v_model import HandlePoolRebuildActionRequest
from models.hyper_v_model import HandleRebuildActionRequest
from fastapi import APIRouter, Depends
import controllers.hyper_v_controller as controller
from models.API_Response_model import APIResponse
from models.hyper_v_model import CloneVMRequest, HandleActionRequest, HandleDeleteDiskRequest
from typing import Any
from db_configuration.config import get_db
from sqlalchemy.orm import Session

hyper_v_router = APIRouter(prefix="/v1/hyper_v", tags=["Hyper-V"])

# ----------------------- Hyper-V ROUTES -----------------------

@hyper_v_router.get("/get_vms")
async def get_vms():
        return await controller.get_vms()

@hyper_v_router.post("/clone_vm_for_single_node", response_model=APIResponse[Any]) 
async def clone_vm_for_single_node(request: CloneVMRequest, db: Session = Depends(get_db)):
        return await controller.clone_vm_for_single_node(request, db)

@hyper_v_router.get("/get_vm_info/{vm_id}", response_model=APIResponse[Any])
async def get_vm_info(vm_id: str):
        return await controller.get_vm_info(vm_id)

@hyper_v_router.get("/get_switches", response_model=APIResponse[Any])
async def get_switches():
        return await controller.get_switches()

@hyper_v_router.delete("/delete_vm/{vm_id}", response_model=APIResponse[Any])
async def delete_vm(vm_id: str):
        return await controller.delete_vm(vm_id)

@hyper_v_router.get("/get_status/{vm_id}", response_model=APIResponse[Any])
async def get_status(vm_id: str):
        return await controller.get_status(vm_id)

@hyper_v_router.post("/handle_action", response_model=APIResponse[Any])
async def handle_action(request: HandleActionRequest):
        return await controller.handle_action(request)

@hyper_v_router.post("/vm_rebuild", response_model=APIResponse[Any])
async def rebuild_vm(request: HandleRebuildActionRequest):
        return await controller.rebuild_vm_endpoint(request)

@hyper_v_router.post("/pool_rebuild", response_model=APIResponse[Any])
async def pool_rebuild(request: HandlePoolRebuildActionRequest, db: Session = Depends(get_db)):
        return await controller.pool_rebuild(request, db)

@hyper_v_router.delete("/delete_disk", response_model=APIResponse[Any])
async def delete_disk(request: HandleDeleteDiskRequest):
        return await controller.delete_disk(request)

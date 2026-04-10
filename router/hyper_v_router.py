from typing import Optional, Union
from models.hyper_v_model import HandlePoolRebuildActionRequest
from models.hyper_v_model import HandleRebuildActionRequest
from fastapi import APIRouter, Depends
from controllers import hyper_v_controller
from models.API_Response_model import APIResponse
from models.hyper_v_model import CloneVMRequest, HandleActionRequest, HandleDeleteDiskRequest, VerifyStandaloneHyperVRequest
from typing import Any
from db_configuration.config import get_db
from sqlalchemy.orm import Session

hyper_v_router = APIRouter(prefix="/v1/hyper_v", tags=["Hyper-V"])

# ----------------------- Hyper-V ROUTES -----------------------

@hyper_v_router.get("/get_vms")
async def get_vms(cluster_id:int,db: Session = Depends(get_db)):
        return await hyper_v_controller.get_vms(cluster_id,db)

@hyper_v_router.get("/ping_agent", response_model=APIResponse[Any])
async def ping_agent(
    cluster_id: Optional[int] = None, 
    db: Session = Depends(get_db),
    ip: Optional[str] = None, 
    port: Optional[Union[int, str]] = None
):
    return await hyper_v_controller.ping_agent(cluster_id, db, ip, port)

# @hyper_v_router.post("/clone_vm_for_single_node", response_model=APIResponse[Any]) 
# async def clone_vm_for_single_node(request: CloneVMRequest, db: Session = Depends(get_db)):
#         return await controller.clone_vm_for_single_node(request, db)

@hyper_v_router.get("/get_vm_info/{vm_id}", response_model=APIResponse[Any])
async def get_vm_info(vm_id: str, db: Session = Depends(get_db)):
        return await hyper_v_controller.get_vm_info(vm_id, db)

@hyper_v_router.get("/get_switches", response_model=APIResponse[Any])
async def get_switches(cluster_id:int,db: Session = Depends(get_db)):
        return await hyper_v_controller.get_switches(cluster_id,db)

@hyper_v_router.delete("/delete_vm/{vm_id}", response_model=APIResponse[Any])
async def delete_vm(vm_id: str, db: Session = Depends(get_db)):
        return await hyper_v_controller.delete_vm(vm_id, db)

@hyper_v_router.get("/get_status/{vm_id}", response_model=APIResponse[Any])
async def get_status(vm_id: str, db: Session = Depends(get_db)):
        return await hyper_v_controller.get_status(vm_id, db)

# @hyper_v_router.post("/handle_action", response_model=APIResponse[Any])
# async def handle_action(request: HandleActionRequest, db: Session = Depends(get_db)):
#         return await controller.handle_action(request, db)

@hyper_v_router.post("/vm_rebuild", response_model=APIResponse[Any])
async def rebuild_vm(request: HandleRebuildActionRequest, db: Session = Depends(get_db)):
        return await hyper_v_controller.rebuild_vm_endpoint(request, db)

@hyper_v_router.post("/pool_rebuild", response_model=APIResponse[Any])
async def pool_rebuild(request: HandlePoolRebuildActionRequest, db: Session = Depends(get_db)):
        return await hyper_v_controller.pool_rebuild(request, db)

# @hyper_v_router.delete("/delete_disk", response_model=APIResponse[Any])
# async def delete_disk(request: HandleDeleteDiskRequest, db: Session = Depends(get_db)):
#         return await controller.delete_disk(request, db)

@hyper_v_router.post("/verify_standalone_hyper_v", response_model=APIResponse[Any])
async def verify_standalone_hyper_v(request: VerifyStandaloneHyperVRequest, db: Session = Depends(get_db), cluster_id: Optional[int] = None ):
        return await hyper_v_controller.verify_standalone_hyper_v(request,db,cluster_id)

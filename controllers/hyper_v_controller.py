import service.hyper_v_service as service
from utils.response_format import success_response
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, Union


async def get_vms(cluster_id: int, db: Session):
    result = await service.get_vms(cluster_id, db)
    return success_response(200, "Successfully retrieved Hyper-V data", result)

async def ping_agent(cluster_id: Optional[int], db: Session, ip: str, port: Union[int, str]):
    result = await service.ping_agent(cluster_id, db, ip, port)
    return success_response(200, "Successfully pinged Hyper-V agent", result)


async def clone_vm_hyper_v_controller(request, db):
    result = await service.clone_vm_hyper_v_service(request)
    return success_response(200, "Successfully cloned Hyper-V VM for single node", result)

async def get_vm_info(vm_id, db):
    result = await service.get_vm_info(vm_id, db)
    return success_response(200, "Successfully retrieved Hyper-V VM info", result)

async def get_switches(cluster_id: int, db: Session):
    result = await service.get_switches(cluster_id, db)
    return success_response(200, "Successfully retrieved Hyper-V switches", result)

async def delete_vm(vm_id, db):
    result = await service.delete_hyperv_vm(vm_id, db)
    return success_response(200, "Successfully deleted Hyper-V VM", result)

async def get_status(vm_id, db):
    result = await service.get_status(vm_id, db)
    return success_response(200, "Successfully retrieved Hyper-V VM status", result)

async def handle_action(request, db):
    result = await service.handle_action(request, db)
    return success_response(200, "Successfully performed action on Hyper-V VM", result)

async def delete_disk(request, db):
    result = await service.delete_disk(request, db)
    return success_response(200, "Successfully deleted disk from Hyper-V VM", result)

async def rebuild_vm_endpoint(request, db):
    result = await service.vm_rebuild(request, db)
    return success_response(200, "VM rebuild initiated.", result)

async def pool_rebuild(request, db):
    result = await service.pool_rebuild(request, db)
    return success_response(200, "Pool rebuild initiated.", result)

async def verify_hyper_v(request, db: Session, cluster_id: Optional[int] = None):
    result = await service.verify_hyper_v(request, db, cluster_id)
    return success_response(200, "Hyper-V verified.", result)


async def get_node_status_from_cluster(cluster_id: Optional[int]):
    result = await service.fetch_hyper_v_cluster_nodes(cluster_id)
    return success_response(200, "Successfully retrieved cluster nodes", result)

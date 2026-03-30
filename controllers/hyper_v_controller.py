import service.hyper_v_service as service
from utils.response_format import success_response
from sqlalchemy.ext.asyncio import AsyncSession


async def get_vms():
    result = await service.get_vms()
    return success_response(200, "Successfully retrieved Hyper-V data", result)

async def clone_vm_for_single_node(request, db):
    result = await service.clone_vm_for_single_node(request, db)
    return success_response(200, "Successfully cloned Hyper-V VM for single node", result)

async def get_vm_info(vm_id):
    result = await service.get_vm_info(vm_id)
    return success_response(200, "Successfully retrieved Hyper-V VM info", result)

async def get_switches():
    result = await service.get_switches()
    return success_response(200, "Successfully retrieved Hyper-V switches", result)

async def delete_vm(vm_id):
    result = await service.delete_hyperv_vm(vm_id)
    return success_response(200, "Successfully deleted Hyper-V VM", result)

async def get_status(vm_id):
    result = await service.get_status(vm_id)
    return success_response(200, "Successfully retrieved Hyper-V VM status", result)

async def handle_action(request):
    result = await service.handle_action(request)
    return success_response(200, "Successfully performed action on Hyper-V VM", result)

async def delete_disk(request):
    result = await service.delete_disk(request)
    return success_response(200, "Successfully deleted disk from Hyper-V VM", result)

async def rebuild_vm_endpoint(request):
    result = await service.vm_rebuild(request)
    return success_response(200, "VM rebuild initiated.", result)

async def pool_rebuild(request, db):
    result = await service.pool_rebuild(request, db)
    return success_response(200, "Pool rebuild initiated.", result)

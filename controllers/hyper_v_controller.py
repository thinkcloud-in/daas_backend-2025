import service.hyper_v_service as service
from utils.response_format import success_response

async def get_vms():
    result = await service.get_vms()
    return success_response(200, "Successfully retrieved Hyper-V data", result)

async def clone_vm_for_single_node(request):
    result = await service.clone_vm_for_single_node(request)
    return success_response(200, "Successfully cloned Hyper-V VM for single node", result)

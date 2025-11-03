import service.hyper_v_service as service
from utils.response_format import success_response, error_response

async def get_vms_from_hyperv():
    result = await service.get_vms_from_hyperv()
    return success_response(200, "Successfully retrieved Hyper-V data", result)

async def clone_vms():
    result = await service.clone_vms()
    return success_response(200, "Successfully cloned Hyper-V VMs", result)

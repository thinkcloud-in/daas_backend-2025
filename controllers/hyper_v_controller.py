"""
Hyper-V controller — router/hyper_v_router.py ("/v1/hyper_v") is par
delegate karta hai. Actual logic (remote Hyper-V HTTP agent se baat karna)
service/hyper_v_service.py mein hai. Har function ka `result` seedha agent
ke JSON response se aata hai — is layer mein transform nahi hota.
"""
import service.hyper_v_service as service
from utils.response_format import success_response
from sqlalchemy.orm import Session
from typing import Optional, Union


async def get_vms(cluster_id: int, db: Session):
    """
    Cluster ke saare Hyper-V VMs list karo.

    Used by: GET /v1/hyper_v/get_vms
    """
    result = await service.get_vms(cluster_id, db)
    return success_response(200, "Successfully retrieved Hyper-V data", result)

async def ping_agent(cluster_id: Optional[int], db: Session, ip: str, port: Union[int, str]):
    """
    Hyper-V agent reachable hai ya nahi check karo.

    Used by: GET /v1/hyper_v/ping_agent
    Args: `cluster_id` (DB se IP/port resolve) YA `ip`+`port` seedha.
    """
    result = await service.ping_agent(cluster_id, db, ip, port)
    return success_response(200, "Successfully pinged Hyper-V agent", result)


async def clone_vm_hyper_v_controller(request, db):
    """
    Single-node VM clone karo (currently router mein commented-out — unused).
    """
    result = await service.clone_vm_hyper_v_service(request)
    return success_response(200, "Successfully cloned Hyper-V VM for single node", result)

async def full_clone_vm_hyper_v_controller(request, db):
    """
    VM ka full (independent-disk) clone banao.

    Used by: POST /v1/hyper_v/full_clone_vm_hyper_v
    Args: request = FullCloneVMRequest (naming, network, domain-join fields).
    """
    result = await service.clone_vm_hyper_v_service(request)
    return success_response(200, "Successfully initiated full clone for Hyper-V VM", result)

async def get_vm_info(vm_id, db):
    """
    Ek VM ki detailed info lo.

    Used by: GET /v1/hyper_v/get_vm_info/{vm_id}
    """
    result = await service.get_vm_info(vm_id, db)
    return success_response(200, "Successfully retrieved Hyper-V VM info", result)

async def get_switches(cluster_id: int, db: Session):
    """
    Cluster ke virtual switches list karo.

    Used by: GET /v1/hyper_v/get_switches
    """
    result = await service.get_switches(cluster_id, db)
    return success_response(200, "Successfully retrieved Hyper-V switches", result)

async def delete_vm(request, db):
    """
    VM delete karo (cluster-role + AD computer-account cleanup samet).

    Used by: POST /v1/hyper_v/delete_vm
    Args: request = DeleteVMRequest.
    """
    result = await service.delete_hyperv_vm(request, db)
    return success_response(200, "Successfully deleted Hyper-V VM", result)

async def get_status(vm_id, db):
    """
    VM ka current power-state lo.

    Used by: GET /v1/hyper_v/get_status/{vm_id}
    """
    result = await service.get_status(vm_id, db)
    return success_response(200, "Successfully retrieved Hyper-V VM status", result)

async def handle_action(request, db):
    """
    Generic VM power-action handler (currently router mein commented-out — unused).
    """
    result = await service.handle_action(request, db)
    return success_response(200, "Successfully performed action on Hyper-V VM", result)

async def delete_disk(request, db):
    """
    VM se ek disk delete karo (currently router mein commented-out — unused).
    """
    result = await service.delete_disk(request, db)
    return success_response(200, "Successfully deleted disk from Hyper-V VM", result)

async def rebuild_vm_endpoint(request, db):
    """
    Ek VM ko uske pool-template se rebuild karo (delete + fresh clone).

    Used by: POST /v1/hyper_v/vm_rebuild
    Args: request = HandleRebuildActionRequest (vm_id, pool_id).
    """
    result = await service.vm_rebuild(request, db)
    return success_response(200, "VM rebuild initiated.", result)

async def pool_rebuild(request, db):
    """
    Poore pool ki saari VMs rebuild karo.

    Used by: POST /v1/hyper_v/pool_rebuild
    Args: request = HandlePoolRebuildActionRequest (pool_id, vhdPath).
    """
    result = await service.pool_rebuild(request, db)
    return success_response(200, "Pool rebuild initiated.", result)

async def verify_hyper_v(request, db: Session, cluster_id: Optional[int] = None):
    """
    Naya Hyper-V host/cluster add karne se pehle connectivity/credentials
    verify karo (DB me kuch save nahi hota).

    Used by: POST /v1/hyper_v/verify_hyper_v
    Args: request = VerifyHyperVRequest.
    """
    result = await service.verify_hyper_v(request, db, cluster_id)
    return success_response(200, "Hyper-V verified.", result)


async def get_node_status_from_cluster(request:dict):
    """
    Hyper-V failover-cluster ke saare nodes ka status lo.

    Used by: GET /v1/hyper_v/get_node_status_from_cluster
    Args: request = {"ip", "agent_port", "cluster_id"}
    """
    result = await service.fetch_hyper_v_cluster_nodes(request)
    return success_response(200, "Successfully retrieved cluster nodes", result)

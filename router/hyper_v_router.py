from typing import Optional, Union
from models.hyper_v_model import HandlePoolRebuildActionRequest
from models.hyper_v_model import HandleRebuildActionRequest
from fastapi import APIRouter, Depends
from controllers import hyper_v_controller
from models.API_Response_model import APIResponse
from models.hyper_v_model import VerifyHyperVRequest, DeleteVMRequest, FullCloneVMRequest
from typing import Any
from db_configuration.config import get_db
from sqlalchemy.orm import Session

hyper_v_router = APIRouter(prefix="/v1/hyper_v", tags=["Hyper-V"])

# ----------------------- Hyper-V ROUTES -----------------------
# NOTE: on every endpoint here, `data` comes straight from the remote Hyper-V
# agent's (a PowerShell HTTP agent running on the target Windows host/cluster)
# JSON response — it's not transformed in this codebase, so the exact fields
# depend on the agent-side implementation. Request models (below) already
# carry Field-level descriptions, which show up as-is in Swagger UI.

@hyper_v_router.post("/full_clone_vm_hyper_v", response_model=APIResponse[Any])
async def full_clone_vm_hyper_v(request: FullCloneVMRequest, db: Session = Depends(get_db)):
    """
    Create a Hyper-V "full clone" (a new independent VHD, not a linked-clone
    off the base disk) — naming, networking (ip/gateway/dns), and domain-join
    are all specified in a single call.

    Request body: FullCloneVMRequest.
    Response 200 — `data`: the agent's clone-result (new VM's id/status).
    """
    return await hyper_v_controller.full_clone_vm_hyper_v_controller(request, db)

@hyper_v_router.get("/get_vms")
async def get_vms(cluster_id:int,db: Session = Depends(get_db)):
    """
    List all Hyper-V VMs in a cluster (live query against the agent).

    Response 200 — `data`: [ {..VM info: id, name, state, memory, ...}, ... ]
    """
    return await hyper_v_controller.get_vms(cluster_id,db)

@hyper_v_router.get("/ping_agent", response_model=APIResponse[Any])
async def ping_agent(
    cluster_id: Optional[int] = None,
    db: Session = Depends(get_db),
    ip: Optional[str] = None,
    port: Optional[Union[int, str]] = None
):
    """
    Check whether the Hyper-V agent (the HTTP agent running on the host) is
    reachable — pass `cluster_id` (looks up IP/port from the DB) OR pass
    `ip`+`port` directly.

    Response 200 — `data`: {"reachable": bool, ...agent info...}
    """
    return await hyper_v_controller.ping_agent(cluster_id, db, ip, port)

# @hyper_v_router.post("/clone_vm_hyper_v", response_model=APIResponse[Any])
# async def clone_vm_hyper_v(request: CloneVMRequest, db: Session = Depends(get_db)):
#         return await hyper_v_controller.clone_vm_hyper_v_controller(request, db)

@hyper_v_router.get("/get_vm_info/{vm_id}", response_model=APIResponse[Any])
async def get_vm_info(vm_id: str, db: Session = Depends(get_db)):
    """
    Get detailed info for a VM (memory, disk, network adapters, state, etc.).

    Response 200 — `data`: the agent's VM-info object.
    """
    return await hyper_v_controller.get_vm_info(vm_id, db)

@hyper_v_router.get("/get_switches", response_model=APIResponse[Any])
async def get_switches(cluster_id:int,db: Session = Depends(get_db)):
    """
    List a cluster's Hyper-V virtual switches (options for the `switch`
    field when creating/cloning a VM).

    Response 200 — `data`: [ {"name": str, "type": str}, ... ]
    """
    return await hyper_v_controller.get_switches(cluster_id,db)

@hyper_v_router.post("/delete_vm", response_model=APIResponse[Any])
async def delete_vm(request: DeleteVMRequest, db: Session = Depends(get_db)):
    """
    Delete a Hyper-V VM (cluster-aware — if `is_cluster=True` it also removes
    the cluster role; if domain-joined, `domain`/`domain_password` are used
    to clean up the AD computer account too).

    Request body: DeleteVMRequest.
    Response 200 — `data`: the agent's delete-result.
    """
    return await hyper_v_controller.delete_vm(request, db)

@hyper_v_router.get("/get_status/{vm_id}", response_model=APIResponse[Any])
async def get_status(vm_id: str, db: Session = Depends(get_db)):
    """
    Get a VM's current power state (Running/Off/Saved/etc.).

    Response 200 — `data`: {"state": str, ...}
    """
    return await hyper_v_controller.get_status(vm_id, db)

# @hyper_v_router.post("/handle_action", response_model=APIResponse[Any])
# async def handle_action(request: HandleActionRequest, db: Session = Depends(get_db)):
#         return await controller.handle_action(request, db)

@hyper_v_router.post("/vm_rebuild", response_model=APIResponse[Any])
async def rebuild_vm(request: HandleRebuildActionRequest, db: Session = Depends(get_db)):
    """
    Rebuild a VM from its pool template (deletes the existing VM and creates
    a fresh clone in the same name/pool-slot) — the VDI "reset machine" flow.

    Request body: HandleRebuildActionRequest (vm_id, pool_id).
    Response 200 — `data`: {"status": "VM rebuild initiated.", ...}
    """
    return await hyper_v_controller.rebuild_vm_endpoint(request, db)

@hyper_v_router.post("/pool_rebuild", response_model=APIResponse[Any])
async def pool_rebuild(request: HandlePoolRebuildActionRequest, db: Session = Depends(get_db)):
    """
    Rebuild every VM in a pool from a new base VHD (bulk version of
    `vm_rebuild`).

    Request body: HandlePoolRebuildActionRequest (pool_id, vhdPath).
    Response 200 — `data`: {"status": "Pool rebuild initiated.", ...}
    """
    return await hyper_v_controller.pool_rebuild(request, db)

# @hyper_v_router.delete("/delete_disk", response_model=APIResponse[Any])
# async def delete_disk(request: HandleDeleteDiskRequest, db: Session = Depends(get_db)):
#         return await controller.delete_disk(request, db)

@hyper_v_router.post("/verify_hyper_v", response_model=APIResponse[Any])
async def verify_hyper_v(request: VerifyHyperVRequest, db: Session = Depends(get_db), cluster_id: Optional[int] = None ):
    """
    Verify connectivity + credentials for a Hyper-V host/cluster before
    adding it (nothing is saved to the DB — this is just a test).

    Request body: VerifyHyperVRequest (ip, username, password, agent_port, type).
    Response 200 — `data`: {"verified": bool, ...agent info...}
    """
    return await hyper_v_controller.verify_hyper_v(request,db,cluster_id)


@hyper_v_router.get("/get_node_status_from_cluster", response_model=APIResponse[Any])
async def get_node_status_from_cluster(
    ip: Optional[str] = None,
    agent_port: Optional[int] = None,
    cluster_id: Optional[int] = None
):
    """
    Get the status of every node in a Hyper-V failover cluster (via
    `cluster_id` looked up from the DB, or directly via `ip`+`agent_port`).

    Response 200 — `data`: [ {"node": str, "state": str, ...}, ... ]
    """
    request_data = {"ip": ip, "agent_port": agent_port, "cluster_id": cluster_id}
    return await hyper_v_controller.get_node_status_from_cluster(request_data)

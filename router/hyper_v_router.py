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
# NOTE: in saare endpoints ka `data` seedha remote Hyper-V agent (PowerShell
# HTTP agent jo target Windows host/cluster pe chalta hai) ke JSON response
# se aata hai — is codebase mein transform nahi hota, isliye exact fields
# agent-side implementation par depend karte hain. Request models (neeche)
# already Field-level descriptions ke saath hain, Swagger UI mein wahi dikhengi.

@hyper_v_router.post("/full_clone_vm_hyper_v", response_model=APIResponse[Any])
async def full_clone_vm_hyper_v(request: FullCloneVMRequest, db: Session = Depends(get_db)):
    """
    Ek Hyper-V VM ka "full clone" banao (naya independent VHD, base disk se
    linked-clone nahi) — naming, network (ip/gateway/dns), domain-join sab
    ek call mein specify hote hain.

    Request body: FullCloneVMRequest.
    Response 200 — `data`: agent ka clone-result (naye VM ki id/status).
    """
    return await hyper_v_controller.full_clone_vm_hyper_v_controller(request, db)

@hyper_v_router.get("/get_vms")
async def get_vms(cluster_id:int,db: Session = Depends(get_db)):
    """
    Cluster ke saare Hyper-V VMs list karo (agent se live query).

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
    Hyper-V agent (host pe chal raha HTTP agent) reachable hai ya nahi check
    karo — `cluster_id` do (DB se IP/port nikalega) YA `ip`+`port` seedha do.

    Response 200 — `data`: {"reachable": bool, ...agent info...}
    """
    return await hyper_v_controller.ping_agent(cluster_id, db, ip, port)

# @hyper_v_router.post("/clone_vm_hyper_v", response_model=APIResponse[Any])
# async def clone_vm_hyper_v(request: CloneVMRequest, db: Session = Depends(get_db)):
#         return await hyper_v_controller.clone_vm_hyper_v_controller(request, db)

@hyper_v_router.get("/get_vm_info/{vm_id}", response_model=APIResponse[Any])
async def get_vm_info(vm_id: str, db: Session = Depends(get_db)):
    """
    Ek VM ki detailed info lo (memory, disk, network adapters, state, etc.).

    Response 200 — `data`: agent ka VM-info object.
    """
    return await hyper_v_controller.get_vm_info(vm_id, db)

@hyper_v_router.get("/get_switches", response_model=APIResponse[Any])
async def get_switches(cluster_id:int,db: Session = Depends(get_db)):
    """
    Cluster ke Hyper-V virtual switches list karo (VM create/clone karte
    waqt `switch` field ke liye options).

    Response 200 — `data`: [ {"name": str, "type": str}, ... ]
    """
    return await hyper_v_controller.get_switches(cluster_id,db)

@hyper_v_router.post("/delete_vm", response_model=APIResponse[Any])
async def delete_vm(request: DeleteVMRequest, db: Session = Depends(get_db)):
    """
    Hyper-V VM delete karo (cluster-aware — `is_cluster=True` ho to cluster
    role bhi remove karta hai, domain-joined ho to `domain`/`domain_password`
    se AD-computer-account bhi cleanup hoti hai).

    Request body: DeleteVMRequest.
    Response 200 — `data`: agent ka delete-result.
    """
    return await hyper_v_controller.delete_vm(request, db)

@hyper_v_router.get("/get_status/{vm_id}", response_model=APIResponse[Any])
async def get_status(vm_id: str, db: Session = Depends(get_db)):
    """
    VM ka current power-state lo (Running/Off/Saved/etc.).

    Response 200 — `data`: {"state": str, ...}
    """
    return await hyper_v_controller.get_status(vm_id, db)

# @hyper_v_router.post("/handle_action", response_model=APIResponse[Any])
# async def handle_action(request: HandleActionRequest, db: Session = Depends(get_db)):
#         return await controller.handle_action(request, db)

@hyper_v_router.post("/vm_rebuild", response_model=APIResponse[Any])
async def rebuild_vm(request: HandleRebuildActionRequest, db: Session = Depends(get_db)):
    """
    Ek VM ko uske pool-template se rebuild karo (existing VM delete karke
    same naam/pool-slot pe fresh clone banata hai) — VDI pool "reset machine"
    jaisi flow.

    Request body: HandleRebuildActionRequest (vm_id, pool_id).
    Response 200 — `data`: {"status": "VM rebuild initiated.", ...}
    """
    return await hyper_v_controller.rebuild_vm_endpoint(request, db)

@hyper_v_router.post("/pool_rebuild", response_model=APIResponse[Any])
async def pool_rebuild(request: HandlePoolRebuildActionRequest, db: Session = Depends(get_db)):
    """
    Poore pool ki saari VMs ko naye base VHD se rebuild karo (bulk version
    of `vm_rebuild`).

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
    Naya Hyper-V host/cluster add karne se pehle uski connectivity +
    credentials verify karo (DB me kuch save nahi hota — sirf test).

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
    Hyper-V failover-cluster ke saare nodes ka status lo (`cluster_id` DB se
    ya seedha `ip`+`agent_port` se).

    Response 200 — `data`: [ {"node": str, "state": str, ...}, ... ]
    """
    request_data = {"ip": ip, "agent_port": agent_port, "cluster_id": cluster_id}
    return await hyper_v_controller.get_node_status_from_cluster(request_data)

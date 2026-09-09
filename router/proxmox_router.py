from fastapi import APIRouter, Depends, Request, Query
from typing import Any
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session
from controllers import proxmox_controller
from models.API_Response_model import APIResponse
from models.proxmox_model import VMPowerRequest, MigrateRequest
from controllers.proxmox_controller import CloneRequest, NodeGpusRequest
from db_configuration.config import get_db
from utils import response_format

proxmox_router = APIRouter(prefix="/v1/proxmox", tags=["proxmox"])


@proxmox_router.post("/generate-name", response_model=APIResponse[Any])
async def generate_name(db: Session = Depends(get_db)):
    """
    Naye VM/pool ke liye ek unique naam auto-generate karo (naming-pattern +
    existing names ke against collision-check).

    Response 200 — `data`: {"name": str}
    """
    return await proxmox_controller.generate_name(db)

# @proxmox_router.get("/get_all_cluster_vms", response_model=APIResponse[Any])
# async def get_all_cluster_vms(db: Session = Depends(get_db)):
#     return await proxmox_controller.get_nodes(db)

@proxmox_router.get("/get_templates")
async def get_templates(cluster_id: str = None, db: Session = Depends(get_db)):
    """
    Cluster ke nodes pe available VM templates list karo (clone karte waqt
    "base template" select karne ke liye).

    Response `data`: [ {"node": str, "templates": [ {"vmid": int, "name": str}, ... ]}, ... ]
    """
    return await proxmox_controller.get_templates_for_nodes(cluster_id, db)

@proxmox_router.post("/clone-vms", response_model=APIResponse[Any])
async def clone_vms(payload: CloneRequest, db: Session = Depends(get_db)):
    """
    Ek template se bulk VMs clone karo (ek ya zyada nodes pe distribute
    karke, `count` jitni copies, `name_template` pattern se naam banate hue).

    Request body: CloneRequest (node[], template_vm_id, name_template, count, cluster_id).
    Response 200 — `data`: {"cloned": [ {"vmid": int, "name": str, "node": str}, ... ]}
    """
    return await proxmox_controller.clone_vms_endpoint(payload, db)

@proxmox_router.put("/update-nodes", response_model=APIResponse[Any])
async def update_nodes(db: Session = Depends(get_db)):
    """
    Saare clusters ke nodes ki list DB mein refresh karo (Proxmox API se
    live fetch karke sync).

    Response 200 — `data`: refresh-result (updated node counts/summary).
    """
    res = await proxmox_controller.update_nodes_route(db)
    return response_format.success_response(200, "Updated nodes successfully.", res)

@proxmox_router.get("/get-cluster-nodes")
async def get_cluster_nodes(cluster_id: str, db: Session = Depends(get_db)):
    """
    Ek cluster ke saare nodes list karo (naam, IP, status).

    Response `data`: [ {"node": str, "status": str, ...}, ... ]
    """
    return await proxmox_controller.get_cluster_nodes_endpoint(cluster_id, db)

@proxmox_router.post("/get_node_gpus", response_model=APIResponse[Any])
async def get_node_gpus(payload: NodeGpusRequest, db: Session = Depends(get_db)):
    """
    Diye gaye nodes pe available GPU devices list karo (GPU-passthrough VM
    banate waqt select karne ke liye).

    Request body: NodeGpusRequest (cluster_id, nodes[]).
    Response `data`: {"<node>": [ {"id": str, "name": str}, ... ], ...}
    """
    return await proxmox_controller.get_node_gpus_endpoint(payload, db)

@proxmox_router.get("/get_influxdb_env_defaults", response_model=APIResponse[Any])
async def get_influxdb_env_defaults():
    """
    Naya InfluxDB metric-server add karte waqt pre-fill karne ke liye
    default values (env se) lo — server/port/org/bucket defaults.

    Response 200 — `data`: {"server": str, "port": int, "organization": str, "bucket": str, ...}
    """
    res = proxmox_controller.get_influxdb_env_defaults_endpoint()
    return response_format.success_response(200, "Fetched InfluxDB default configuration.", res)

@proxmox_router.get("/get_influxdb_metric_server", response_model=APIResponse[Any])
async def get_influxdb_metric_server(cluster_id: str, monitoring: bool = Query(False), db: Session = Depends(get_db)):
    """
    Cluster ka configured InfluxDB metric-server (monitoring integration) lo.

    Response 200 — `data`: MetricServerBase record | null (agar configure na hua ho).
    """
    res = await proxmox_controller.get_influxdb_metric_server_endpoint(cluster_id, monitoring, db)
    return response_format.success_response(200, "Fetched InfluxDB metric server successfully.", res)

@proxmox_router.get("/edit/get_influxdb_metric_server", response_model=APIResponse[Any])
async def get_influxdb_metric_server_endpoints(cluster_id: str,db: Session = Depends(get_db)):
    """
    Metric-server ka edit-form pre-fill karne ke liye poori detail (token
    bhi maskless — sirf edit-flow ke liye) lo.

    Response 200 — `data`: MetricServerBase record.
    """
    res = await proxmox_controller.get_influxdb_metric_server_endpoints(cluster_id, db)
    return response_format.success_response(200, "Fetched InfluxDB metric server endpoints successfully.", res)

@proxmox_router.post("/add_influxdb_metric_server", response_model=APIResponse[Any])
async def add_influxdb_metric_server(cluster_id: str, request: Request, db: Session = Depends(get_db)):
    """
    Cluster ke liye InfluxDB metric-server (monitoring) configure karo.

    Request body (JSON, raw Request se parse): MetricServerBase-shape fields
    (type, server, port, proto, organization, bucket, token, ...).

    Response 200 — `data`: saved MetricServerBase record.
    """
    res = await proxmox_controller.add_influxdb_metric_server_endpoint(cluster_id, request, db)
    return response_format.success_response(200, "InfluxDB metric server added successfully.", res)

@proxmox_router.delete("/delete_influxdb_metric_server", response_model=APIResponse[Any])
async def delete_influxdb_metric_server(cluster_id: str, db: Session = Depends(get_db)):
    """
    Cluster ka InfluxDB metric-server integration remove karo.

    Response 200 — `data`: deletion summary.
    """
    res = await proxmox_controller.delete_influxdb_metric_server_endpoint(cluster_id, db)
    return response_format.success_response(200, "InfluxDB metric server deleted successfully.", res)

@proxmox_router.post("/migrate_bucket_all_data", response_model=APIResponse[Any])
async def migrate_bucket_all_data(req: MigrateRequest, db: Session = Depends(get_db)):
    """
    Purane InfluxDB bucket se naye (is cluster ke configured) bucket mein
    saara historical metrics data migrate karo — Temporal workflow se async.

    Request body: MigrateRequest (src_url, src_token, src_org, src_bucket, cluster_id).
    Response 200 — `data`: {"workflow_id": str, "status": "started", ...}
    """
    res = await proxmox_controller.migrate_bucket_all_data_route(req, db)
    return response_format.success_response(200, "Migration workflow started successfully.", jsonable_encoder(res))

@proxmox_router.post("/get_metric_server", response_model=APIResponse[Any])
async def get_metric_server_endpoint(cluster_id: str):
    """
    Cluster ka metric-server config lo (query-param nahi, body/path se
    `cluster_id` leta hai — `get_influxdb_metric_server` ka simpler variant).

    Response 200 — `data`: metric-server record | null.
    """
    res = await proxmox_controller.get_metric_server_endpoint(cluster_id)
    return response_format.success_response(200, "Fetched InfluxDB metric server successfully.", jsonable_encoder(res))

@proxmox_router.post("/start_vm", response_model=APIResponse[Any])
async def start_vm(data: VMPowerRequest, vmid: str, pool_id: str = None, db: Session = Depends(get_db)):
    """
    VM start karo (power-on).

    Request body: VMPowerRequest (email, audit ke liye). Query: `vmid`
    (required), `pool_id` (optional — VDI-pool machine ho to).

    Response 200 — `data`: Proxmox task-result.
    """
    res = await proxmox_controller.start_vm_endpoint(data, vmid, pool_id, db)
    return response_format.success_response(200, "VM started successfully.", jsonable_encoder(res))

@proxmox_router.post("/stop_vm", response_model=APIResponse[Any])
async def stop_vm(data: VMPowerRequest, vmid: str, pool_id: str = None, db: Session = Depends(get_db)):
    """
    VM stop karo (force power-off, graceful shutdown nahi).

    Response 200 — `data`: Proxmox task-result.
    """
    res = await proxmox_controller.stop_vm_endpoint(data, vmid, pool_id, db)
    return response_format.success_response(200, "VM stopped successfully.", jsonable_encoder(res))

@proxmox_router.post("/reboot_vm", response_model=APIResponse[Any])
async def reboot_vm(data: VMPowerRequest, vmid: str, pool_id: str = None, db: Session = Depends(get_db)):
    """
    VM reboot karo.

    Response 200 — `data`: Proxmox task-result.
    """
    res = await proxmox_controller.reboot_vm_endpoint(data, vmid, pool_id, db)
    # if res.get("status") == "failed":
    #     return response_format.error_response(500, "Failed to reboot VM.", res)
    return response_format.success_response(200, "VM rebooted successfully.", jsonable_encoder(res))

@proxmox_router.post("/shutdown_vm", response_model=APIResponse[Any])
async def shutdown_vm(data: VMPowerRequest, vmid: str, pool_id: str = None, db: Session = Depends(get_db)):
    """
    VM graceful shutdown karo (guest OS ko ACPI signal — `stop_vm` se alag,
    jo hard power-off karta hai).

    Response 200 — `data`: Proxmox task-result.
    """
    res = await proxmox_controller.shutdown_vm_endpoint(data, vmid, pool_id, db)
    return response_format.success_response(200, "VM shut down successfully.", jsonable_encoder(res))

@proxmox_router.post("/vm_rebuild", response_model=APIResponse[Any])
async def rebuild_vm(data: VMPowerRequest, vmid: str, pool_id: str = None, db: Session = Depends(get_db)):
    """
    VM ko uske template se rebuild karo (delete + fresh clone, same
    vmid/pool-slot pe) — VDI pool "reset machine" flow (Proxmox variant of
    Hyper-V's `/hyper_v/vm_rebuild`).

    Response 200 — `data`: rebuild-result.
    """
    res = await proxmox_controller.rebuild_vm_endpoint(data,vmid, pool_id, db)
    return response_format.success_response(200, "VM rebuilt successfully.", jsonable_encoder(res))

@proxmox_router.get("/proxmox_vm_info/{vm_id}", response_model=APIResponse[Any])
async def proxmox_vm_detail(vm_id: str, db: Session = Depends(get_db)):
    """
    Ek VM ki live detail lo (Proxmox API se — status, config, resource usage).

    Response 200 — `data`: {"vmid": str, "status": str, "config": {...}, ...}
    """
    res = await proxmox_controller.proxmox_vm_details(vm_id, db)
    return response_format.success_response(200, "VM details fetched successfully.", jsonable_encoder(res))

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
    Auto-generate a unique name for a new VM/pool (naming-pattern +
    collision-check against existing names).

    Response 200 — `data`: {"name": str}
    """
    return await proxmox_controller.generate_name(db)

# @proxmox_router.get("/get_all_cluster_vms", response_model=APIResponse[Any])
# async def get_all_cluster_vms(db: Session = Depends(get_db)):
#     return await proxmox_controller.get_nodes(db)

@proxmox_router.get("/get_templates")
async def get_templates(cluster_id: str = None, db: Session = Depends(get_db)):
    """
    List the VM templates available on a cluster's nodes (for selecting a
    "base template" when cloning).

    Response `data`: [ {"node": str, "templates": [ {"vmid": int, "name": str}, ... ]}, ... ]
    """
    return await proxmox_controller.get_templates_for_nodes(cluster_id, db)

@proxmox_router.post("/clone-vms", response_model=APIResponse[Any])
async def clone_vms(payload: CloneRequest, db: Session = Depends(get_db)):
    """
    Bulk-clone VMs from a template (distributed across one or more nodes,
    `count` copies, named using the `name_template` pattern).

    Request body: CloneRequest (node[], template_vm_id, name_template, count, cluster_id).
    Response 200 — `data`: {"cloned": [ {"vmid": int, "name": str, "node": str}, ... ]}
    """
    return await proxmox_controller.clone_vms_endpoint(payload, db)

@proxmox_router.put("/update-nodes", response_model=APIResponse[Any])
async def update_nodes(db: Session = Depends(get_db)):
    """
    Refresh the node list for all clusters in the DB (live fetch + sync from
    the Proxmox API).

    Response 200 — `data`: the refresh result (updated node counts/summary).
    """
    res = await proxmox_controller.update_nodes_route(db)
    return response_format.success_response(200, "Updated nodes successfully.", res)

@proxmox_router.get("/get-cluster-nodes")
async def get_cluster_nodes(cluster_id: str, db: Session = Depends(get_db)):
    """
    List all nodes of a cluster (name, IP, status).

    Response `data`: [ {"node": str, "status": str, ...}, ... ]
    """
    return await proxmox_controller.get_cluster_nodes_endpoint(cluster_id, db)

@proxmox_router.post("/get_node_gpus", response_model=APIResponse[Any])
async def get_node_gpus(payload: NodeGpusRequest, db: Session = Depends(get_db)):
    """
    List available GPU devices on the given nodes (to select from when
    creating a GPU-passthrough VM).

    Request body: NodeGpusRequest (cluster_id, nodes[]).
    Response `data`: {"<node>": [ {"id": str, "name": str}, ... ], ...}
    """
    return await proxmox_controller.get_node_gpus_endpoint(payload, db)

@proxmox_router.get("/get_influxdb_env_defaults", response_model=APIResponse[Any])
async def get_influxdb_env_defaults():
    """
    Get default values (from env) to pre-fill the form when adding a new
    InfluxDB metric server — server/port/org/bucket defaults.

    Response 200 — `data`: {"server": str, "port": int, "organization": str, "bucket": str, ...}
    """
    res = proxmox_controller.get_influxdb_env_defaults_endpoint()
    return response_format.success_response(200, "Fetched InfluxDB default configuration.", res)

@proxmox_router.get("/get_influxdb_metric_server", response_model=APIResponse[Any])
async def get_influxdb_metric_server(cluster_id: str, monitoring: bool = Query(False), db: Session = Depends(get_db)):
    """
    Get a cluster's configured InfluxDB metric server (monitoring integration).

    Response 200 — `data`: MetricServerBase record | null (if not configured).
    """
    res = await proxmox_controller.get_influxdb_metric_server_endpoint(cluster_id, monitoring, db)
    return response_format.success_response(200, "Fetched InfluxDB metric server successfully.", res)

@proxmox_router.get("/edit/get_influxdb_metric_server", response_model=APIResponse[Any])
async def get_influxdb_metric_server_endpoints(cluster_id: str,db: Session = Depends(get_db)):
    """
    Get the full detail (token unmasked — for the edit flow only) to
    pre-fill the metric server's edit form.

    Response 200 — `data`: MetricServerBase record.
    """
    res = await proxmox_controller.get_influxdb_metric_server_endpoints(cluster_id, db)
    return response_format.success_response(200, "Fetched InfluxDB metric server endpoints successfully.", res)

@proxmox_router.post("/add_influxdb_metric_server", response_model=APIResponse[Any])
async def add_influxdb_metric_server(cluster_id: str, request: Request, db: Session = Depends(get_db)):
    """
    Configure an InfluxDB metric server (monitoring) for a cluster.

    Request body (JSON, parsed from the raw Request): MetricServerBase-shape
    fields (type, server, port, proto, organization, bucket, token, ...).

    Response 200 — `data`: the saved MetricServerBase record.
    """
    res = await proxmox_controller.add_influxdb_metric_server_endpoint(cluster_id, request, db)
    return response_format.success_response(200, "InfluxDB metric server added successfully.", res)

@proxmox_router.delete("/delete_influxdb_metric_server", response_model=APIResponse[Any])
async def delete_influxdb_metric_server(cluster_id: str, db: Session = Depends(get_db)):
    """
    Remove a cluster's InfluxDB metric server integration.

    Response 200 — `data`: deletion summary.
    """
    res = await proxmox_controller.delete_influxdb_metric_server_endpoint(cluster_id, db)
    return response_format.success_response(200, "InfluxDB metric server deleted successfully.", res)

@proxmox_router.post("/migrate_bucket_all_data", response_model=APIResponse[Any])
async def migrate_bucket_all_data(req: MigrateRequest, db: Session = Depends(get_db)):
    """
    Migrate all historical metrics data from an old InfluxDB bucket into the
    new one (this cluster's configured bucket) — async via a Temporal workflow.

    Request body: MigrateRequest (src_url, src_token, src_org, src_bucket, cluster_id).
    Response 200 — `data`: {"workflow_id": str, "status": "started", ...}
    """
    res = await proxmox_controller.migrate_bucket_all_data_route(req, db)
    return response_format.success_response(200, "Migration workflow started successfully.", jsonable_encoder(res))

@proxmox_router.post("/get_metric_server", response_model=APIResponse[Any])
async def get_metric_server_endpoint(cluster_id: str):
    """
    Get a cluster's metric-server config (takes `cluster_id` from body/path,
    not a query param — a simpler variant of `get_influxdb_metric_server`).

    Response 200 — `data`: the metric-server record | null.
    """
    res = await proxmox_controller.get_metric_server_endpoint(cluster_id)
    return response_format.success_response(200, "Fetched InfluxDB metric server successfully.", jsonable_encoder(res))

@proxmox_router.post("/start_vm", response_model=APIResponse[Any])
async def start_vm(data: VMPowerRequest, vmid: str, pool_id: str = None, db: Session = Depends(get_db)):
    """
    Start a VM (power-on).

    Request body: VMPowerRequest (email, for audit). Query: `vmid`
    (required), `pool_id` (optional — for a VDI-pool machine).

    Response 200 — `data`: the Proxmox task result.
    """
    res = await proxmox_controller.start_vm_endpoint(data, vmid, pool_id, db)
    return response_format.success_response(200, "VM started successfully.", jsonable_encoder(res))

@proxmox_router.post("/stop_vm", response_model=APIResponse[Any])
async def stop_vm(data: VMPowerRequest, vmid: str, pool_id: str = None, db: Session = Depends(get_db)):
    """
    Stop a VM (force power-off, not a graceful shutdown).

    Response 200 — `data`: the Proxmox task result.
    """
    res = await proxmox_controller.stop_vm_endpoint(data, vmid, pool_id, db)
    return response_format.success_response(200, "VM stopped successfully.", jsonable_encoder(res))

@proxmox_router.post("/reboot_vm", response_model=APIResponse[Any])
async def reboot_vm(data: VMPowerRequest, vmid: str, pool_id: str = None, db: Session = Depends(get_db)):
    """
    Reboot a VM.

    Response 200 — `data`: the Proxmox task result.
    """
    res = await proxmox_controller.reboot_vm_endpoint(data, vmid, pool_id, db)
    # if res.get("status") == "failed":
    #     return response_format.error_response(500, "Failed to reboot VM.", res)
    return response_format.success_response(200, "VM rebooted successfully.", jsonable_encoder(res))

@proxmox_router.post("/shutdown_vm", response_model=APIResponse[Any])
async def shutdown_vm(data: VMPowerRequest, vmid: str, pool_id: str = None, db: Session = Depends(get_db)):
    """
    Gracefully shut down a VM (ACPI signal to the guest OS — different from
    `stop_vm`, which does a hard power-off).

    Response 200 — `data`: the Proxmox task result.
    """
    res = await proxmox_controller.shutdown_vm_endpoint(data, vmid, pool_id, db)
    return response_format.success_response(200, "VM shut down successfully.", jsonable_encoder(res))

@proxmox_router.post("/vm_rebuild", response_model=APIResponse[Any])
async def rebuild_vm(data: VMPowerRequest, vmid: str, pool_id: str = None, db: Session = Depends(get_db)):
    """
    Rebuild a VM from its template (delete + fresh clone, same
    vmid/pool-slot) — the VDI pool "reset machine" flow (the Proxmox
    counterpart of Hyper-V's `/hyper_v/vm_rebuild`).

    Response 200 — `data`: the rebuild result.
    """
    res = await proxmox_controller.rebuild_vm_endpoint(data,vmid, pool_id, db)
    return response_format.success_response(200, "VM rebuilt successfully.", jsonable_encoder(res))

@proxmox_router.get("/proxmox_vm_info/{vm_id}", response_model=APIResponse[Any])
async def proxmox_vm_detail(vm_id: str, db: Session = Depends(get_db)):
    """
    Get a VM's live detail (from the Proxmox API — status, config, resource usage).

    Response 200 — `data`: {"vmid": str, "status": str, "config": {...}, ...}
    """
    res = await proxmox_controller.proxmox_vm_details(vm_id, db)
    return response_format.success_response(200, "VM details fetched successfully.", jsonable_encoder(res))

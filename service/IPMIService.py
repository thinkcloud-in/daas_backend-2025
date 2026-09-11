"""
IPMIService — service layer for IPMI device CRUD.

Create/Update/Delete manage IPMI server records by starting Temporal workflows
(workflows_ipmi.Ipmi{Creation,Update,Deletion}Workflow); List/Get are served directly from the DB.
Used by: controllers/ipmi_controller.py (which is called from router/ipmi_router.py).
"""
import asyncio
from datetime import datetime
import logging
from typing import Optional
from sqlalchemy.orm import Session
from models.IPMI_models import IPMIDeviceRequest, IPMIDevice
from fastapi import HTTPException, status
from service.temporalResource.workflows import workflows_ipmi
from utils.temporal_client import TemporalClientManager
from utils import response_format

def unique_id():
    """Build an `HH:MM:SS` string from the current time, for use as a workflow-id suffix (for uniqueness)."""
    unique_id = datetime.now()
    logger.info(f"Generated unique ID - {unique_id}")
    return f"{unique_id.hour }:{unique_id.minute}:{unique_id.second}"
 
 
# Logging is configured centrally in utils/logging_config.py (called from
# main.py at startup) — do not reconfigure it per-module.
logger = logging.getLogger("IPMIService")

async def create_ipmi_server(db: Session, ipmi_data: IPMIDeviceRequest):
    """
    Start `IpmiCreationWorkflow` for IPMI server creation (async, fire-and-forget —
    does not wait for the result, only returns the workflow_id).

    Params: ipmi_data (dict-like) — {"ipmi_server_ip","email",...}.
    Returns: {"workflow_id": <temporal workflow id>}.
    """
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    ipmi_ip = ipmi_data.get('ipmi_server_ip', "UnknownIP")
    userName = ipmi_data.get('email', "UnknownUser")


    handle = await client.start_workflow(
        workflows_ipmi.IpmiCreationWorkflow.run,
        ipmi_data,
        id=f'ipmi-creation-{uniqueId}',
        task_queue="Createipmi-task-queue",
        search_attributes={
            "Entity": [ipmi_ip],
            "Action": ["IPMI-Creation"],
            "UserName": [userName]
        },
    )

    return {"workflow_id": handle.id}


async def update_ipmi_server( ipmi_id: int, ipmi_data: IPMIDeviceRequest):
    """
    Start `IpmiUpdateWorkflow` for an IPMI server update and await its result (synchronous-style call).

    Params: ipmi_id, ipmi_data (dict-like updated fields).
    Returns: the workflow's result (the updated IPMI record/dict, depending on the workflow implementation).
    """
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    ipmi_ip = ipmi_data.get('ipmi_server_ip', "UnknownIP")
    userName = ipmi_data.get('email')
 

    handle = await client.start_workflow(
        workflows_ipmi.IpmiUpdateWorkflow.run,
        args=[ipmi_id, ipmi_data],
        id=f'ipmi-update-{uniqueId}',
        task_queue="Updateipmi-task-queue",
        search_attributes={
            "Entity": [ipmi_ip],
            "Action": ["IPMI-Update"],
            "UserName": [userName]
        },
    )

    result = await handle.result()
    return result
async def delete_ipmi_server(db: Session, ipmi_id: int,email: Optional[str] = None):
    """
    Start `IpmiDeletionWorkflow` for an IPMI server delete and await its result.

    Params: ipmi_id, email (optional, for the Temporal search-attribute "UserName").
    Returns: the workflow's result on success; an `error_response(404, ...)` dict if ipmi_id is not found in the DB.
    """
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
    ipmi_data= db.query(IPMIDevice).filter(IPMIDevice.id == ipmi_id).first()
    if not ipmi_data:
        return response_format.error_response(404, f"IPMI Server with ID {ipmi_id} not found.", None)
    ipmi_ip = ipmi_data.ipmi_server_ip
    userName = email if email else "UnknownUser"
 

    handle = await client.start_workflow(
        workflows_ipmi.IpmiDeletionWorkflow.run,
        ipmi_id,
        id=f'ipmi-deletion-{uniqueId}',
        task_queue="Deleteipmi-task-queue",
        # id_reuse_policy=temporalio.common.WorkflowIDReusePolicy.REJECT_DUPLICATE,
        search_attributes={
            "Entity": [ipmi_ip],
            "Action": ["IPMI-Deletion"],
            "UserName": [userName]
        },
    )

    result = await handle.result()
    return result

def get_all_ipmi_servers(db: Session, skip: int = 0, limit: int = 10):
    """
    Fetch a paginated list of IPMI devices directly from the DB (id ascending order).

    Returns: (items: list[IPMIDevice] ORM objects, total: int).
    """
    query = db.query(IPMIDevice).order_by(IPMIDevice.id.asc())
    total = query.count()
    items = query.offset(skip).limit(limit).all()
    return items, total

def get_ipmi_server_id(db: Session, ipmi_id: int):
    """
    Fetch a single IPMI device from the DB by id.

    Returns: IPMIDevice ORM object.
    Raises: HTTPException 404 if not found.
    """
    ipmi_device = db.query(IPMIDevice).filter(IPMIDevice.id == ipmi_id).first()
    if not ipmi_device:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"IPMI Server with ID {ipmi_id} not found."
        )
    return ipmi_device

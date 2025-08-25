import asyncio
from datetime import datetime
import logging
from typing import Optional
from sqlalchemy.orm import Session
from models.IPMI_models import IPMIDeviceRequest, IPMIDevice
from fastapi import HTTPException, status
from service.gucamoleService import connectionWithClient
from service.temporalResource.workflows import workflows_ipmi
from service.temporalResource.workers import workers_ipmi
def unique_id():
    unique_id = datetime.now()
    logger.info(f"Generated unique ID - {unique_id}")
    return f"{unique_id.hour }:{unique_id.minute}:{unique_id.second}"
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("create_machine_activity")
# def create_ipmi_server(db: Session, ipmi_data: IPMIDeviceRequest):
#     # Check for duplicate IPMI server IP
#     existing_device = db.query(IPMIDevice).filter(IPMIDevice.ipmi_server_ip == ipmi_data.ipmi_server_ip).first()
#     if existing_device:
#         from fastapi import HTTPException, status
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail=f"IPMI Device with IP {ipmi_data.ipmi_server_ip} already exists."
#         )
#     db_ipmi_device = IPMIDevice(**ipmi_data.dict())
#     db.add(db_ipmi_device)
#     db.commit()
#     db.refresh(db_ipmi_device)
#     return db_ipmi_device
async def create_ipmi_server(db: Session, ipmi_data: IPMIDeviceRequest):
    uniqueId = unique_id()
    client = await connectionWithClient()
    ipmi_ip = ipmi_data.get('ipmi_server_ip', "UnknownIP")
    # userName = ipmi_data.get('email', "UnknownUser")
    userName = ipmi_data.get('email')
 
    try:
        print("worker about to start...")
        asyncio.create_task(workers_ipmi.create_ipmi_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_ipmi.IpmiCreationWorkflow.run,
        ipmi_data,
        id=f'ipmi-creation-{uniqueId}',
        task_queue="Createipmi-task-queue",
        # id_reuse_policy=temporalio.common.WorkflowIDReusePolicy.REJECT_DUPLICATE,
        search_attributes={
            "Entity": [ipmi_ip],
            "Action": ["IPMI-Creation"],
            "UserName": [userName]
        },
    )
    print("Workflow started! Workflow ID:", handle.id)
    result = await handle.result()
    print("Workflow result:", result)
    return result
async def update_ipmi_server(db: Session, ipmi_id: int, ipmi_data: IPMIDeviceRequest):
    uniqueId = unique_id()
    client = await connectionWithClient()
    ipmi_ip = ipmi_data.get('ipmi_server_ip', "UnknownIP")
    # userName = ipmi_data.get('email', "UnknownUser")
    userName = ipmi_data.get('email')
 
    try:
        print("worker about to start...")
        asyncio.create_task(workers_ipmi.update_ipmi_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_ipmi.IpmiUpdateWorkflow.run,
        args=[ipmi_id, ipmi_data],
        id=f'ipmi-update-{uniqueId}',
        task_queue="Updateipmi-task-queue",
        # id_reuse_policy=temporalio.common.WorkflowIDReusePolicy.REJECT_DUPLICATE,
        search_attributes={
            "Entity": [ipmi_ip],
            "Action": ["IPMI-Update"],
            "UserName": [userName]
        },
    )
    print("Workflow started! Workflow ID:", handle.id)
    result = await handle.result()
    print("Workflow result:", result)
    return result
async def delete_ipmi_server(db: Session, ipmi_id: int,email: Optional[str] = None):
    uniqueId = unique_id()
    client = await connectionWithClient()
    ipmi_data= db.query(IPMIDevice).filter(IPMIDevice.id == ipmi_id).first()
    if not ipmi_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"IPMI Server with ID {ipmi_id} not found."
        )
    ipmi_ip = ipmi_data.ipmi_server_ip
    userName = email if email else "UnknownUser"
 
    try:
        print("worker about to start...")
        asyncio.create_task(workers_ipmi.delete_ipmi_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
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
    print("Workflow started! Workflow ID:", handle.id)
    result = await handle.result()
    print("Workflow result:", result)
    return result
def get_all_ipmi_servers(db: Session):
    return db.query(IPMIDevice).all()
def get_ipmi_server_id(db: Session, ipmi_id: int):
    ipmi_device = db.query(IPMIDevice).filter(IPMIDevice.id == ipmi_id).first()
    if not ipmi_device:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"IPMI Server with ID {ipmi_id} not found."
        )
    return ipmi_device
# def update_ipmi_server(db: Session, ipmi_id: int, ipmi_data: IPMIDeviceRequest):
#     ipmi_device = db.query(IPMIDevice).filter(IPMIDevice.id == ipmi_id).first()
#     if not ipmi_device:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND,
#             detail=f"IPMI Server with ID {ipmi_id} not found."
#         )
#     for key, value in ipmi_data.dict().items():
#         setattr(ipmi_device, key, value)
#     db.commit()
#     db.refresh(ipmi_device)
#     return ipmi_device
 
# def delete_ipmi_server(db: Session, ipmi_id: int):
#     ipmi_device = db.query(IPMIDevice).filter(IPMIDevice.id == ipmi_id).first()
#     if not ipmi_device:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND,
#             detail=f"IPMI Server with ID {ipmi_id} not found."
#         )
#     db.delete(ipmi_device)
#     db.commit()
#     return {"detail": "IPMI Server deleted successfully."}
 
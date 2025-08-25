from fastapi import HTTPException
from temporalio import  activity
from db_configuration.config import get_db
from sqlalchemy.orm import Session
from fastapi import HTTPException, status
from models.IPMI_models import IPMIDeviceRequest, IPMIDevice
# @activity.defn
# async def ipmi_activity(ipmi_data):
#     db: Session = next(get_db())
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
 
 
from models.IPMI_models import IPMIDeviceRequest
 
from fastapi.encoders import jsonable_encoder
 
from fastapi.encoders import jsonable_encoder
 
@activity.defn
async def ipmi_activity(ipmi_data):
    db: Session = next(get_db())
    ipmi_server_ip = ipmi_data["ipmi_server_ip"]
    existing_device = db.query(IPMIDevice).filter(IPMIDevice.ipmi_server_ip == ipmi_server_ip).first()
    if existing_device:
       
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"IPMI Device with IP {ipmi_server_ip} already exists."
        )
    # Only pass DB fields to SQLAlchemy
    valid_keys = IPMIDevice.__table__.columns.keys()
    filtered_data = {k: v for k, v in ipmi_data.items() if k in valid_keys}
    db_ipmi_device = IPMIDevice(**filtered_data)
    db.add(db_ipmi_device)
    db.commit()
    db.refresh(db_ipmi_device)
    # Use jsonable_encoder for serialization
    return jsonable_encoder(db_ipmi_device)
@activity.defn
async def update_ipmi_activity(ipmi_id: int, ipmi_data: IPMIDeviceRequest):
    db: Session = next(get_db())
    ipmi_device = db.query(IPMIDevice).filter(IPMIDevice.id == ipmi_id).first()
    if not ipmi_device:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"IPMI Server with ID {ipmi_id} not found."
        )
    for key, value in ipmi_data.dict().items():
        setattr(ipmi_device, key, value)
    db.commit()
    db.refresh(ipmi_device)
    return jsonable_encoder(ipmi_device)
@activity.defn
async def delete_ipmi_activity(ipmi_id: int):
    db: Session = next(get_db())
    ipmi_device = db.query(IPMIDevice).filter(IPMIDevice.id == ipmi_id).first()
    if not ipmi_device:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"IPMI Server with ID {ipmi_id} not found."
        )
    db.delete(ipmi_device)
    db.commit()
    return {"detail": "IPMI Server deleted successfully."}
 
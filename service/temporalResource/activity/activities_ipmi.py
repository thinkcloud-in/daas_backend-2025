from temporalio import  activity
from db_configuration.config import SessionLocal, get_db
from sqlalchemy.orm import Session
from models.IPMI_models import IPMIDeviceRequest, IPMIDevice
from fastapi.encoders import jsonable_encoder
 
@activity.defn
async def ipmi_activity(ipmi_data):
    db: Session = SessionLocal()
    try:
        try:
            ipmi_server_ip = ipmi_data["ipmi_server_ip"]
            existing_device = db.query(IPMIDevice).filter(IPMIDevice.ipmi_server_ip == ipmi_server_ip).first()
            if existing_device:
                return {"status": 400, "msg": f"IPMI Server with IP {ipmi_server_ip} already exists.", 'data': jsonable_encoder(existing_device)}

            # Only pass DB fields to SQLAlchemy
            valid_keys = IPMIDevice.__table__.columns.keys()
            filtered_data = {k: v for k, v in ipmi_data.items() if k in valid_keys and k != "id"}
            db_ipmi_device = IPMIDevice(**filtered_data)
            db.add(db_ipmi_device)
            db.commit()
            db.refresh(db_ipmi_device)
            data = jsonable_encoder(db_ipmi_device)
            return {"status": 200, "msg": "IPMI Server created successfully.", "data": data}
        except Exception as e:
            db.rollback()
            raise Exception("Failed to create IPMI server") from e
    finally:
        db.close()


@activity.defn
async def update_ipmi_activity(ipmi_id: int, ipmi_data: IPMIDeviceRequest):
    db: Session = SessionLocal()
    try:
        try:
            ipmi_device = db.query(IPMIDevice).filter(IPMIDevice.id == ipmi_id).first()
            if not ipmi_device:
                return {"status": 404, "msg": f"IPMI Server with ID {ipmi_id} not found.", "data": ipmi_id}
            for key, value in ipmi_data.dict().items():
                if key == "id":
                    continue
                setattr(ipmi_device, key, value)
            db.commit()
            db.refresh(ipmi_device)
            return {"status": 200, "msg": "IPMI Server updated successfully.", "data": jsonable_encoder(ipmi_device)}
        except Exception as e:
            db.rollback()
            raise Exception("Failed to update IPMI server") from e
    finally:
        db.close()



@activity.defn
async def delete_ipmi_activity(ipmi_id: int):
    db: Session = SessionLocal()
    try:
        try:
            ipmi_device = db.query(IPMIDevice).filter(IPMIDevice.id == ipmi_id).first()
            if not ipmi_device:
                return {"status": 404, "msg": f"IPMI Server with ID {ipmi_id} not found.", "data": ipmi_id}
            db.delete(ipmi_device)
            db.commit()
            return  {"status": 200, "msg": "IPMI Server deleted successfully.", "data": {"id": ipmi_id}}
        except Exception as e:
            db.rollback()
            raise Exception("Failed to delete IPMI server") from e
    finally:
        db.close()

 
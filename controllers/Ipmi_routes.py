import json
from fastapi import APIRouter, Depends, HTTPException, Request
from httpx import get
from sqlalchemy.orm import Session
from typing import List
from models.IPMI_models import IPMIDeviceRequest, IPMIDevice
from service.IPMIService import create_ipmi_server,get_all_ipmi_servers, get_ipmi_server_id, update_ipmi_server, delete_ipmi_server
from db_configuration.config import SessionLocal
from fastapi import Query
from fastapi import APIRouter
 
router = APIRouter()
# Function to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
@router.post("/add_ipmi_server", response_model=IPMIDeviceRequest)
async def create_ipmi_server_route(ipmi_data: IPMIDeviceRequest, db: Session = Depends(get_db)):
    ipmi_data_dict = ipmi_data.dict()
    return await create_ipmi_server(db, ipmi_data_dict)
@router.get("/get_all_ipmi_servers")
async def get_all_ipmi_servers_route(db: Session = Depends(get_db)):
    return get_all_ipmi_servers(db)
@router.get("/get_ipmi_server/{ipmi_id}")
def get_ipmi_server_route(ipmi_id: int, db: Session = Depends(get_db)):
    return  get_ipmi_server_id(db, ipmi_id)
# @router.put("/update_ipmi_server/{ipmi_id}")
# async def update_ipmi_server_route(ipmi_id: int, ipmi_data: IPMIDeviceRequest, db: Session = Depends(get_db)):
#     return await update_ipmi_server(db, ipmi_id, ipmi_data)
@router.put("/update_ipmi_server/{ipmi_id}", response_model=IPMIDeviceRequest)
async def update_ipmi_server_route(ipmi_id: int, ipmi_data: IPMIDeviceRequest, db: Session = Depends(get_db)):
    ipmi_data_dict = ipmi_data.dict()
    return await update_ipmi_server(db, ipmi_id, ipmi_data_dict)
# @router.delete("/delete_ipmi_server/{ipmi_id}")
# async def delete_ipmi_server_route(ipmi_id: int, db: Session = Depends(get_db)):
#     return await delete_ipmi_server(db, ipmi_id)
@router.delete("/delete_ipmi_server/{ipmi_id}", response_model=dict)
async def delete_ipmi_server_route(ipmi_id: int, request: Request, db: Session = Depends(get_db)):
    raw_body = await request.body()
    print("Received raw body:", raw_body.decode("utf-8"))
 
    if not raw_body:
        raise HTTPException(status_code=400, detail="Empty request body")
 
    try:
        body = json.loads(raw_body)
        email = body.get("email")
    except Exception as e:
        raise HTTPException(status_code=400, detail="Malformed JSON body")
 
    return await delete_ipmi_server(db, ipmi_id,email)
 
 
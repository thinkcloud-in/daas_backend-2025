import json
from fastapi import Depends, HTTPException, Request
from sqlalchemy.orm import Session
from models.IPMI_models import IPMIDeviceRequest
from service.IPMIService import create_ipmi_server,get_all_ipmi_servers, get_ipmi_server_id, update_ipmi_server, delete_ipmi_server
from db_configuration.config import SessionLocal
from utils import response_format


# Function to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

async def create_ipmi_server_route(ipmi_data, db):
    try:
        ipmi_data_dict = ipmi_data.dict()
        data = await create_ipmi_server(db, ipmi_data_dict)
        return response_format.success_response(200, "IPMI Server created successfully.", data)
    except Exception as e:
        return response_format.error_response(500, "Failed to create IPMI Server", str(e))

def model_to_dict(obj):
    return {c.name: getattr(obj, c.name) for c in obj.__table__.columns}

async def get_all_ipmi_servers_route(db):
    try:
        data = get_all_ipmi_servers(db)
        result = [model_to_dict(item) for item in data]
        return response_format.success_response(200, "IPMI Servers retrieved successfully.", result)
    except Exception as e:
        return response_format.error_response(500, "Failed to retrieve IPMI Servers", str(e))

    
async def get_ipmi_server_route(ipmi_id, db):
        data = get_ipmi_server_id(db, ipmi_id)
        return response_format.success_response(200, "IPMI Servers retrieved successfully.", data)

async def update_ipmi_server_route(ipmi_id, ipmi_data):
    ipmi_data_dict = ipmi_data.dict()
    data = await update_ipmi_server( ipmi_id, ipmi_data_dict)
    return response_format.success_response(data['status'], data['msg'], data['data'] if data else None)

async def delete_ipmi_server_route(ipmi_id, request, db):
    try:
        raw_body = await request.body()
        if not raw_body:
            return response_format.error_response(400, "Request body is empty", None).dict()
        
        body = json.loads(raw_body)
        email = body.get("email")
        data = await delete_ipmi_server(db, ipmi_id, email)

        if hasattr(data, "dict"):
            return data.dict()
        return response_format.success_response(data['status'], data['msg'], data['data'] if data else None).dict()

    except Exception as e:
        return response_format.error_response(500, "Failed to delete IPMI Server", str(e)).dict()


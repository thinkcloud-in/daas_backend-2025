import json
from service.IPMIService import create_ipmi_server,get_all_ipmi_servers, get_ipmi_server_id, update_ipmi_server, delete_ipmi_server
from utils import response_format


async def create_ipmi_server_route(ipmi_data, db):
    try:
        ipmi_data_dict = ipmi_data.dict()
        data = await create_ipmi_server(db, ipmi_data_dict)
        return response_format.success_response(200, "IPMI Server created successfully.", data)
    except Exception as e:
        return response_format.error_response(500, "Failed to create IPMI Server", str(e))

def model_to_dict(obj):
    return {c.name: getattr(obj, c.name) for c in obj.__table__.columns}

async def get_all_ipmi_servers_route(db, page: int = 1, page_size: int = 10):
    try:
        page = max(1, page)
        page_size = max(1, min(page_size, 100))
        skip = (page - 1) * page_size
        items, total = get_all_ipmi_servers(db, skip=skip, limit=page_size)
        result = [model_to_dict(item) for item in items]
        total_pages = max(1, (total + page_size - 1) // page_size)
        pagination = {
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
            "has_next": page < total_pages,
            "has_prev": page > 1,
        }
        return response_format.success_response(200, "IPMI Servers retrieved successfully.", {"items": result, "pagination": pagination})
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


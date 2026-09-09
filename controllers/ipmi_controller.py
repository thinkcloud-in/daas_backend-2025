"""
IPMI/BMC server controller — router/ipmi_router.py ("/v1/ipmi") is par
delegate karta hai. Actual DB logic service/IPMIService.py mein hai.
"""
import json
from service.IPMIService import create_ipmi_server,get_all_ipmi_servers, get_ipmi_server_id, update_ipmi_server, delete_ipmi_server
from utils import response_format


async def create_ipmi_server_route(ipmi_data, db):
    """
    Naya IPMI/BMC server record add karo.

    Used by: POST /v1/ipmi/add_ipmi_server
    Args: ipmi_data = IPMIDeviceRequest.
    Returns: 500 (error_response) agar save fail ho.
    """
    try:
        ipmi_data_dict = ipmi_data.dict()
        data = await create_ipmi_server(db, ipmi_data_dict)
        return response_format.success_response(200, "IPMI Server created successfully.", data)
    except Exception as e:
        return response_format.error_response(500, "Failed to create IPMI Server", str(e))

def model_to_dict(obj):
    """
    SQLAlchemy model instance ko plain dict mein convert karo (saare
    columns).

    ⚠ Security note: `IPMIDevice.password` column plaintext String hai (na
    encrypt hoti hai, na yahan redact) — is function se banaya har response
    isko raw expose karta hai. Cluster.password jaisa hi issue, abhi fix
    nahi kiya (documentation-only pass).
    """
    return {c.name: getattr(obj, c.name) for c in obj.__table__.columns}

async def get_all_ipmi_servers_route(db, page: int = 1, page_size: int = 10):
    """
    Saare IPMI servers list karo (paginated).

    Used by: GET /v1/ipmi/get_all_ipmi_servers
    Returns: success_response ke `data` mein {"items": [...], "pagination": {...}}
    (⚠ `password` field included — dekho `model_to_dict` note).
    """
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
    """
    Ek IPMI server ki detail lo.

    Used by: GET /v1/ipmi/get_ipmi_server/{ipmi_id}
    """
    data = get_ipmi_server_id(db, ipmi_id)
    return response_format.success_response(200, "IPMI Servers retrieved successfully.", data)

async def update_ipmi_server_route(ipmi_id, ipmi_data):
    """
    IPMI server connection details update karo.

    Used by: PUT /v1/ipmi/update_ipmi_server/{ipmi_id}
    Args: ipmi_data = IPMIDeviceRequest.
    Returns: status/msg/data seedha service ke result se aate hain (dynamic,
    fixed nahi).
    """
    ipmi_data_dict = ipmi_data.dict()
    data = await update_ipmi_server( ipmi_id, ipmi_data_dict)
    return response_format.success_response(data['status'], data['msg'], data['data'] if data else None)

async def delete_ipmi_server_route(ipmi_id, request, db):
    """
    IPMI server record delete karo.

    Used by: DELETE /v1/ipmi/delete_ipmi_server/{ipmi_id}
    Request body: {"email": str} — empty body pe 400.
    """
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

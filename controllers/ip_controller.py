"""
IP-pool controller — router/ip_router.py ("/v1/ips") is par delegate karta
hai. Actual DB logic service/IPService.py mein hai.
"""
from fastapi import Depends, Query
from sqlalchemy.orm import Session
from service.IPService import (
    create_ips,
    get_ips,
    get_all_ips,
    get_all_pool_names,
    delete_ip_pool_by_name,
    get_available_ips,
)
from utils.response_format import success_response, error_response, paginated_success_response
from db_configuration.config import get_db


def to_dict(model_instance):
    """SQLAlchemy model instance ko plain dict mein convert karo (saare columns)."""
    return {c.name: getattr(model_instance, c.name) for c in model_instance.__table__.columns}

def create_ips_routes(ips_data, db):
    """
    Naya IP pool banao.

    Used by: POST /v1/ips/add_ips
    Args: ips_data = IPSRequest (pool_name + IP list/range).
    Returns: success_response ke `data` mein saved pool record.
    """
    data = create_ips(db, ips_data)
    data_dict = to_dict(data)
    return success_response(201, "IP entry created successfully", data_dict)

def read_ips(ips_id: int, db: Session = Depends(get_db)):
    """
    Ek IP pool ki detail lo, id se.

    Used by: GET /v1/ips/ips/{ips_id}
    Returns: 404 (error_response) agar na mile.
    """
    data = get_ips(db, ips_id)
    if data is None:
        return error_response(404, "IPS entry not found")
    return success_response(200, "IPS entry retrieved successfully", data)

def read_all_ips(page: int, page_size: int, db: Session):
    """
    Saare IP pools list karo (paginated).

    Used by: GET /v1/ips/get_all_ips
    Returns: success_response ke `data` mein {"items": [...], "pagination": {...}}.
    """
    page = max(1, page)
    page_size = max(1, min(page_size, 100))
    skip = (page - 1) * page_size
    items, total = get_all_ips(db, skip=skip, limit=page_size)
    data_dict = [to_dict(item) for item in items]
    total_pages = max(1, (total + page_size - 1) // page_size)
    pagination = {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "has_next": page < total_pages,
        "has_prev": page > 1,
    }
    return success_response(200, "All IPS entries retrieved successfully", {"items": data_dict, "pagination": pagination})

def read_pool_names(db: Session):
    """
    Sirf pool names ki list lo (dropdown ke liye).

    Used by: GET /v1/ips/ip_pool_names
    Returns: success_response ke `data` mein [str, ...].
    """
    data = get_all_pool_names(db)
    return success_response(200, "All IP pool names retrieved successfully", data)

def get_available_ips_route(
    pool_id: int,
    count: int = Query(1, ge=1),
    db: Session = Depends(get_db)
):
    """
    Pool se `count` free IPs nikalo (preview — reserve nahi karta).

    Used by: GET /v1/ips/available_ips/{pool_id}
    Returns: success_response ke `data` mein [str, ...] (IP addresses),
    404 (error_response) agar itni free IPs na ho.
    """
    ips = get_available_ips(db, pool_id, count)
    if not ips:
        return error_response(404, "No available IPs found for this pool.")
    data = [ip_entry.ip for ip_entry in ips]
    return success_response(200, "Available IPs retrieved successfully", data)

def delete_pool_by_name(pool_name, db):
    """
    IP pool (aur uski saari entries) delete karo, naam se.

    Used by: DELETE /v1/ips/delete_pool_by_name/{pool_name}
    """
    data = delete_ip_pool_by_name(db, pool_name)
    return success_response(200, "IP pool deleted successfully", data)

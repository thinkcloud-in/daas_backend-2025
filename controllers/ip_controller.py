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
    return {c.name: getattr(model_instance, c.name) for c in model_instance.__table__.columns}

def create_ips_routes(ips_data, db):
    data = create_ips(db, ips_data)
    data_dict = to_dict(data)
    return success_response(201, "IP entry created successfully", data_dict)

def read_ips(ips_id: int, db: Session = Depends(get_db)):
    data = get_ips(db, ips_id)
    if data is None:
        return error_response(404, "IPS entry not found")
    return success_response(200, "IPS entry retrieved successfully", data)

def read_all_ips(skip: int, limit: int, db: Session):
    data = get_all_ips(db, skip=skip, limit=limit)
    data_dict = [to_dict(item) for item in data]
    return paginated_success_response(200, offset=skip, limit=limit, total=len(data), msg="All IPS entries retrieved successfully", data=data_dict)

def read_pool_names(db: Session):
    data = get_all_pool_names(db)
    return success_response(200, "All IP pool names retrieved successfully", data)

def get_available_ips_route(
    pool_id: int,
    count: int = Query(1, ge=1),
    db: Session = Depends(get_db)
):
    ips = get_available_ips(db, pool_id, count)
    if not ips:
        return error_response(404, "No available IPs found for this pool.")
    data = [ip_entry.ip for ip_entry in ips]
    return success_response(200, "Available IPs retrieved successfully", data)

def delete_pool_by_name(pool_name, db):
    data = delete_ip_pool_by_name(db, pool_name)
    return success_response(200, "IP pool deleted successfully", data)

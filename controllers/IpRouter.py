from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from models.IPs_model import IPSModel,IPSRequest
from service.IPService import create_ips, get_ips, get_all_ips, get_all_pool_names, delete_ip_pool_by_name, get_available_ips
from db_configuration.config import SessionLocal
from fastapi import Query

router = APIRouter()

# Function to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.post("/add_ips", response_model=IPSRequest)
def create_ips_routes(ips_data: IPSRequest, db: Session = Depends(get_db)):
    return create_ips(db, ips_data)

@router.get("/ips/{ips_id}", response_model=IPSRequest)
def read_ips(ips_id: int, db: Session = Depends(get_db)):
    db_ips = get_ips(db, ips_id)
    if db_ips is None:
        raise HTTPException(status_code=404, detail="IPS entry not found")
    return db_ips

@router.get("/get_all_ips/", response_model=List[IPSRequest])
def read_all_ips(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return get_all_ips(db, skip=skip, limit=limit)

@router.get("/ip_pool_names", response_model=List[str])
def read_pool_names(db: Session = Depends(get_db)):
    return get_all_pool_names(db)

@router.get("/available_ips/{pool_id}", response_model=List[str])
def get_available_ips_route(
    pool_id: int,
    count: int = Query(1, ge=1),
    db: Session = Depends(get_db)
) -> List[str]:
    """
    Get up to 'count' available (unused) IPs from the given pool_id.
    Returns a list of IP addresses as strings.
    """
    ips = get_available_ips(db, pool_id, count)
    if not ips:
        raise HTTPException(status_code=404, detail="No available IPs found for this pool.")
    return [ip_entry.ip for ip_entry in ips]

# NEW: Delete IP Pool by Name
@router.delete("/delete_pool_by_name/{pool_name}")
def delete_pool_by_name(pool_name: str, db: Session = Depends(get_db)):
    return delete_ip_pool_by_name(db, pool_name)


from sqlalchemy.orm import Session
from models.IPs_model import IPSModel,IPSRequest, IPEntry
import ipaddress
from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder



def generate_ip_range(start_ip: str, end_ip: str):
    start = ipaddress.IPv4Address(start_ip)
    end = ipaddress.IPv4Address(end_ip)
    if start > end:
        raise ValueError("Starting IP must be less than or equal to Ending IP")
    return [str(ipaddress.IPv4Address(ip)) for ip in range(int(start), int(end) + 1)]

def create_ips(db: Session, ips_data: IPSRequest):
    existing_pool = db.query(IPSModel).filter(IPSModel.Pool_name == ips_data.Pool_name).first()
    if existing_pool:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Pool with name '{ips_data.Pool_name}' already exists."
        )

    db_ips = IPSModel(
        Pool_name=ips_data.Pool_name,
        Starting_ip=ips_data.Starting_ip,
        Ending_ip=ips_data.Ending_ip,
        Subnet=ips_data.Subnet,
        Gateway=ips_data.Gateway,
        DNS=ips_data.DNS,
    )
    db.add(db_ips)
    db.commit()
    db.refresh(db_ips)

    ips_list = ips_data.ips if ips_data.ips is not None else generate_ip_range(ips_data.Starting_ip, ips_data.Ending_ip)
    for ip in ips_list:
        db.add(IPEntry(pool_id=db_ips.id, ip=ip, status="unused"))
    db.commit()
    return db_ips

def get_ips(db: Session, Ips_id: int):
    return db.query(IPSModel).filter(IPSModel.id == Ips_id).first()

def get_all_ips(db, skip, limit):
    return db.query(IPSModel).offset(skip).limit(limit).all()

def get_all_pool_names(db: Session):
    return [row[0] for row in db.query(IPSModel.Pool_name).distinct().all()]

def get_available_ips(db: Session, pool_id: int, count: int):
    return (
        db.query(IPEntry)
        .filter(IPEntry.pool_id == pool_id, IPEntry.status == "unused")
        .limit(count)
        .all()
    )

def delete_ip_pool_by_name(db: Session, pool_name: str):
    db_ips = db.query(IPSModel).filter(IPSModel.Pool_name == pool_name).first()
    if not db_ips:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"IP Pool with name '{pool_name}' not found."
        )
    
    used_ip = db.query(IPEntry).filter(IPEntry.pool_id == db_ips.id, IPEntry.status == "used").first()
    if used_ip:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot delete IP Pool '{pool_name}' as one or more IPs are in use."
        )
    
    db.query(IPEntry).filter(IPEntry.pool_id == db_ips.id).delete()
    db.commit()
    
    db.delete(db_ips)
    db.commit()
    return {"detail": f"IP Pool '{pool_name}' deleted successfully."}


def allocate_ips_across_pools(db: Session, pool_names, count):
    allocated = []
    remaining = count
    for pool_name in pool_names:
        pool = db.query(IPSModel).filter(IPSModel.Pool_name == pool_name).first()
        if not pool:
            continue
        ips = (
            db.query(IPEntry)
            .filter(IPEntry.pool_id == pool.id, IPEntry.status == "unused")
            .order_by(IPEntry.id)
            .limit(remaining)
            .all()
        )
        for ip_obj in ips:
            allocated.append((jsonable_encoder(ip_obj), pool_name))
            ip_obj.status = "used"
        remaining -= len(ips)
        if remaining <= 0:
            break
    db.commit()
    return jsonable_encoder(allocated) # [[{'pool_id': 14, 'vm_id': None, 'ip': '10.1.2.132', 'status': 'used', 'id': 61}, 'Proxmox-testing'], [{'pool_id': 14, 'vm_id': None, 'ip': '10.1.2.134', 'status': 'used', 'id': 63}, 'Proxmox-testing']]
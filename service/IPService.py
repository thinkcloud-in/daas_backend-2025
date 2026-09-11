"""
IPService — service layer for IP Pool management.

A "pool" (IPSModel) is a name + start/end IP range/subnet/gateway/DNS; for each pool,
individual IP entries (IPEntry) are created whose status is tracked as "used"/"unused".
Used by: the IP-pool endpoints in controllers/routes.py.
"""
from sqlalchemy.orm import Session
from models.IPs_model import IPSModel,IPSRequest, IPEntry
import ipaddress
from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder



def generate_ip_range(start_ip: str, end_ip: str):
    """Build a string list of all IPs in the start-end IPv4 range (inclusive)."""
    start = ipaddress.IPv4Address(start_ip)
    end = ipaddress.IPv4Address(end_ip)
    if start > end:
        raise ValueError("Starting IP must be less than or equal to Ending IP")
    return [str(ipaddress.IPv4Address(ip)) for ip in range(int(start), int(end) + 1)]

def create_ips(db: Session, ips_data: IPSRequest):
    """
    Create a new IP pool and seed its IPEntry rows.

    If `ips_data.ips` is given, that list is used; otherwise the
    Starting_ip..Ending_ip range is auto-generated — all entries with "unused" status.
    Returns: IPSModel ORM object (the pool record).
    Raises: HTTPException 400 if Pool_name already exists.
    """
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
    """Fetch a single IP pool from the DB by id. Returns IPSModel or None."""
    return db.query(IPSModel).filter(IPSModel.id == Ips_id).first()

def get_all_ips(db, skip, limit):
    # Previously .all() had no total-count -- the caller was faking "total"
    # as len(page_of_results), which is wrong on every page but the last.
    # Return the real total alongside the page so pagination metadata is
    # actually correct.
    """
    Fetch a paginated list of IP pools from the DB (Pool_name ascending order).
    Returns: (items: list[IPSModel], total: int).
    """
    query = db.query(IPSModel).order_by(IPSModel.Pool_name.asc())
    total = query.count()
    items = query.offset(skip).limit(limit).all()
    return items, total

def get_all_pool_names(db: Session):
    """Return the list of all distinct Pool_name values (for the dropdown)."""
    return [row[0] for row in db.query(IPSModel.Pool_name).distinct().all()]

def get_available_ips(db: Session, pool_id: int, count: int):
    """Fetch up to `count` unused IPEntry rows from a pool (a preview before allocating)."""
    return (
        db.query(IPEntry)
        .filter(IPEntry.pool_id == pool_id, IPEntry.status == "unused")
        .limit(count)
        .all()
    )

def delete_ip_pool_by_name(db: Session, pool_name: str):
    """
    Delete an IP pool by name, along with all its IPEntry rows.

    Returns: {"detail": "..."} success message.
    Raises: HTTPException 404 if the pool is not found, 400 if any IP is currently in "used" status
            (an in-use pool is not deleted).
    """
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
    """
    Allocate "unused" IPs across multiple pools (not round-robin, but draining one pool at a
    time in pool order) until `count` is reached or the pools are exhausted.
    The status of allocated IPEntry rows is set to "used" (committing is the caller's
    responsibility — there is no explicit db.commit() here).

    Returns: a list of [ip_entry_dict, pool_name] pairs, serialized via jsonable_encoder.
             Example: [[{'pool_id':14,'ip':'10.1.2.132','status':'used','id':61}, 'Proxmox-testing'], ...]
    """
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
    return jsonable_encoder(allocated) # [[{'pool_id': 14, 'vm_id': None, 'ip': '10.1.2.132', 'status': 'used', 'id': 61}, 'Proxmox-testing'], [{'pool_id': 14, 'vm_id': None, 'ip': '10.1.2.134', 'status': 'used', 'id': 63}, 'Proxmox-testing']]
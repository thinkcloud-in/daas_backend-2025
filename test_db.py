import os, sys, json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db_configuration.config import get_db
from models.models import Machine, Pool
db = next(get_db())

print("--- RECENT MACHINES ---")
machines = db.query(Machine).order_by(Machine.id.desc()).limit(5).all()
for m in machines:
    print(f"ID={m.id}, Name={m.name}, VMID={m.vm_id}, PoolID={m.pool_id}, Status={m.status}, ErrorMessage={m.error_message}")
print("---")

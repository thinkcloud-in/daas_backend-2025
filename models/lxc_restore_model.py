from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel, validator
from typing import Optional
import datetime

Base = declarative_base()


class LXCRestoreJob(Base):
    __tablename__ = "lxc_restore_jobs"

    id              = Column(Integer, primary_key=True, index=True)
    name            = Column(String, nullable=False)
    cluster_id      = Column(Integer, nullable=False)
    ip_pool_id      = Column(Integer, nullable=False)
    library_item_id = Column(Integer, nullable=False)
    node            = Column(String, nullable=True)
    vmid            = Column(Integer, nullable=True)
    ip_address      = Column(String, nullable=True)
    storage         = Column(String, nullable=True, default="local-lvm")
    status          = Column(String, nullable=False, default="provisioning")
    container_state = Column(String, nullable=True, default="unknown")
    workflow_id     = Column(String, nullable=True)
    created_at      = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at      = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)


class LXCRestoreCreate(BaseModel):
    name:        str
    ip_pool:     str          # IP pool name — static IP will be picked from here
    cluster:     str          # Proxmox cluster name
    template_id: int          # Library item ID (LXC backup .tar.zst on K8s PV)
    storage:     Optional[str] = "local-lvm"

    @validator("name")
    def name_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("name cannot be empty")
        return v.strip()

    @validator("template_id")
    def template_id_positive(cls, v):
        if v <= 0:
            raise ValueError("template_id must be a positive integer")
        return v

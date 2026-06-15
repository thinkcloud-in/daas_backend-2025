from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel
from typing import Optional
import datetime

Base = declarative_base()


class LLMInference(Base):
    __tablename__ = "llm_inferences"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, unique=True)
    ip_pool_id = Column(String, nullable=False)
    cluster_id = Column(String, nullable=False)
    node = Column(String, nullable=False)
    gpu = Column(String, nullable=False)           # e.g. "0000:01:00.0" PCI id
    base_os = Column(String, nullable=False)        # template vmid or name
    cpu = Column(Integer, nullable=False)
    ram = Column(Integer, nullable=False)           # in MB
    datastore = Column(String, nullable=False)
    os_disk_size = Column(Integer, nullable=False)  # in GB
    data_disk_size = Column(Integer, nullable=False)
    model = Column(String, nullable=False)          # e.g. openai/gpt-oss-120b
    vmid = Column(Integer, nullable=True)
    ip_address = Column(String, nullable=True)
    endpoint_url = Column(String, nullable=True)    # OpenAI-compatible endpoint
    status = Column(String, nullable=False, default="provisioning")
    workflow_id = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)


# ── Pydantic schemas ──────────────────────────────────────────────────────────

class LLMInferenceCreate(BaseModel):
    name: str
    ip_pool_id: str     # backend resolves: ip, gateway, subnet, dns
    cluster_id: str     # backend resolves: proxmox host, api token
    node: str
    gpu: str
    base_os: str
    cpu: int
    ram: int
    datastore: str
    os_disk_size: int
    data_disk_size: int
    model: str


class LLMInferenceUpdate(BaseModel):
    model: str


class LLMInferenceOut(BaseModel):
    id: int
    name: str
    ip_pool_id: str
    cluster_id: str
    node: str
    gpu: str
    base_os: str
    cpu: int
    ram: int
    datastore: str
    os_disk_size: int
    data_disk_size: int
    model: str
    vmid: Optional[int]
    ip_address: Optional[str]
    endpoint_url: Optional[str]
    status: str
    workflow_id: Optional[str]
    created_at: Optional[str]
    updated_at: Optional[str]

    class Config:
        orm_mode = True

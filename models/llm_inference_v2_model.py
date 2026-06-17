from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel
from typing import Optional, List, Literal
import datetime

Base = declarative_base()


class LLMInferenceJob(Base):
    __tablename__ = "llm_inference_jobs"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, unique=True)
    cluster_id = Column(Integer, nullable=False)
    ip_pool_ids = Column(ARRAY(Integer), nullable=False)          # multiple pools
    template = Column(String, nullable=False)
    nodes = Column(JSONB, nullable=False)                         # [{"node": "", "gpu": []}]
    machine_name = Column(String, nullable=True)
    pool_os_type = Column(String, nullable=True)
    storage = Column(String, nullable=True)
    # cpu and ram intentionally omitted — taken from template
    model = Column(String, nullable=True)                           # HuggingFace model ID
    vmids = Column(ARRAY(Integer), nullable=True)
    ip_addresses = Column(ARRAY(String), nullable=True)
    head_ip = Column(String, nullable=True)
    endpoint_url = Column(String, nullable=True)
    status = Column(String, nullable=False, default="provisioning")
    workflow_id = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)


# ── Pydantic schemas ──────────────────────────────────────────────────────────

class NodeConfig(BaseModel):
    node: str
    gpu: List[str]


class LLMInferenceJobCreate(BaseModel):
    clusterName: str
    poolName: str
    poolOSType: Optional[str] = None
    ipPools: List[str]                  # list of IPSModel.Pool_name
    template: str
    nodes: List[NodeConfig]             # [{node, gpu: []}]
    storage: Optional[str] = "local-lvm"
    machine_name: Optional[str] = None
    model: Optional[str] = None         # HuggingFace model ID e.g. "meta-llama/Llama-3-70B-Instruct"
    model_path: Optional[str] = "/vllm_data/hf_cache"
    ssh_user: Optional[str] = None      # VM SSH user  (falls back to LLM_VM_SSH_USER env var)
    ssh_pass: Optional[str] = None      # VM SSH password (falls back to LLM_VM_SSH_PASS env var)
    # ram: Optional[int] = None         # taken from template
    # cpu: Optional[int] = None         # taken from template


class LLMInferenceJobUpdate(BaseModel):
    model: Optional[str] = None
    status: Optional[str] = None


class PoolActionRequest(BaseModel):
    action: Literal["start", "stop", "shutdown", "restart"]


class LLMInferenceJobOut(BaseModel):
    id: int
    name: str
    cluster_id: int
    ip_pool_ids: List[int]
    template: str
    nodes: list
    machine_name: Optional[str]
    pool_os_type: Optional[str]
    storage: Optional[str]
    vmids: Optional[List[int]]
    ip_addresses: Optional[List[str]]
    head_ip: Optional[str]
    endpoint_url: Optional[str]
    status: str
    workflow_id: Optional[str]
    created_at: Optional[str]
    updated_at: Optional[str]

    class Config:
        orm_mode = True

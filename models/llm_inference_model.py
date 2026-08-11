from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel, validator
from typing import Optional, List, Literal
import datetime
import os

Base = declarative_base()


class LLMInferenceJob(Base):
    __tablename__ = "llm_inferences"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, unique=True)
    cluster_id = Column(Integer, nullable=False)
    ip_pool_ids = Column(ARRAY(Integer), nullable=False)          # multiple pools
    template = Column(String, nullable=False)
    nodes = Column(JSONB, nullable=False)                         # [{"node": "", "gpu": []}]
    # Actual resolved per-VM names (e.g. ["example-001", "example-002"]),
    # not the raw pattern the user typed at creation ("example-{n:fixed=3}").
    # Populated after cloning completes (see workflows_llm_inference.py Phase 2),
    # once the real names are known -- empty/null between record creation and
    # that point.
    machines_name = Column(ARRAY(String), nullable=True)
    storage = Column(String, nullable=True)
    # cpu and ram intentionally omitted — taken from template
    model = Column(String, nullable=True)                           # HuggingFace model ID
    model_type = Column(String, nullable=True)                      # text | vision_language | embeddings | audio | other
    model_type_other = Column(String, nullable=True)                # free-text label when model_type == "other"
    max_images_per_request = Column(Integer, nullable=True)         # vision_language only -> --limit-mm-per-prompt
    # Parsed (not raw text) vLLM flag overrides -- merged onto the built-in
    # defaults in _build_vllm_commands at every launch (create AND restart),
    # so this must be the source of truth read back on restart, not
    # something only used once at creation time.
    vllm_extra_params = Column(JSONB, nullable=True)
    vmids = Column(ARRAY(Integer), nullable=True)
    ip_addresses = Column(ARRAY(String), nullable=True)
    head_ip = Column(String, nullable=True)
    endpoint_url = Column(String, nullable=True)
    # Bearer token vLLM is launched with (--api-key) -- generated once at
    # creation, reused on every restart so the credential handed to
    # OpenWebUI/clients never changes underneath them.
    api_key = Column(String, nullable=True)
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
    ipPools: List[str]                  # list of IPSModel.Pool_name
    template: str
    nodes: List[NodeConfig]             # [{node, gpu: []}]
    storage: Optional[str] = "local-lvm"
    machine_name: Optional[str] = None
    model: Optional[str] = None         # HuggingFace model ID e.g. "meta-llama/Llama-3-70B-Instruct"
    model_path: Optional[str] = None
    modelType: Optional[str] = None            # text | vision_language | embeddings | audio | other
    modelTypeOther: Optional[str] = None       # free-text label when modelType == "other"
    maxImagesPerRequest: Optional[int] = None  # vision_language only
    vllmExtraParams: Optional[str] = None      # raw YAML/key:value text from the UI textarea, parsed below
    ssh_user: Optional[str] = None      # VM SSH user  (falls back to LLM_VM_SSH_USER env var)
    ssh_pass: Optional[str] = None      # VM SSH password (falls back to LLM_VM_SSH_PASS env var)
    # ram: Optional[int] = None         # taken from template
    # cpu: Optional[int] = None         # taken from template

    @validator("template")
    def template_must_not_be_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("Template VM is required. Provide a valid Proxmox template VMID or name.")
        return v.strip()

    @validator("model", pre=True, always=True)
    def set_model_from_env(cls, v):
        return v or os.getenv("LLM_MODEL_NAME")

    @validator("model_path", pre=True, always=True)
    def set_model_path_from_env(cls, v):
        return v or os.getenv("LLM_MODEL_PATH", "/vllm_data/hf_cache")

    @validator("vllmExtraParams")
    def vllm_extra_params_must_be_flat_mapping(cls, v):
        # Only validate shape here (fail fast at the API boundary with a clear
        # error) -- actual YAML->dict parsing happens in the controller right
        # before it's persisted, so both create and the future restart path
        # read the same already-parsed dict, never re-parsing raw text.
        if v is None or not v.strip():
            return v
        import yaml
        try:
            parsed = yaml.safe_load(v)
        except yaml.YAMLError as e:
            raise ValueError(f"Extra vLLM params must be valid YAML: {e}")
        if parsed is not None and not isinstance(parsed, dict):
            raise ValueError("Extra vLLM params must be a flat mapping of key: value pairs.")
        return v


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
    machines_name: Optional[List[str]]
    storage: Optional[str]
    model_type: Optional[str]
    model_type_other: Optional[str]
    max_images_per_request: Optional[int]
    vllm_extra_params: Optional[dict]
    vmids: Optional[List[int]]
    ip_addresses: Optional[List[str]]
    head_ip: Optional[str]
    endpoint_url: Optional[str]
    api_key: Optional[str]
    status: str
    workflow_id: Optional[str]
    created_at: Optional[str]
    updated_at: Optional[str]

    class Config:
        orm_mode = True

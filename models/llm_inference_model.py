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
    template_source = Column(String, nullable=True, default="proxmox")
    harbor_registry_id = Column(Integer, nullable=True)   # kubernetes_deployments.id
    harbor_template = Column(String, nullable=True)       # "repository:tag"
    harbor_model = Column(String, nullable=True)          # "repository:tag"
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
    # Harbor-source only: the VM spec the user supplied. A qcow2 disk carries
    # no VM config, so unlike a Proxmox template these have to be described at
    # create time -- and since they're null for every proxmox-source pool, they
    # live in one JSONB rather than a column each.
    #   {"cores": 8, "memory": 16384, "network": "vmbr0"}
    vm_config = Column(JSONB, nullable=True)
    vmids = Column(ARRAY(Integer), nullable=True)
    ip_addresses = Column(ARRAY(String), nullable=True)
    head_ip = Column(String, nullable=True)
    endpoint_url = Column(String, nullable=True)
    # Bearer token vLLM is launched with (--api-key) -- generated once at
    # creation, reused on every restart so the credential handed to
    # OpenWebUI/clients never changes underneath them.
    api_key = Column(String, nullable=True)
    # The only source of these credentials anywhere in the LLM inference
    # pipeline -- no env var fallback exists. Set at creation (required,
    # from the request), editable afterward via the update endpoint. A
    # later restart/power-action reads this back directly.
    ssh_user = Column(String, nullable=True)
    ssh_pass = Column(String, nullable=True)
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
    templateSource: Optional[str] = "proxmox"  # "proxmox" | "harbor"
    template: Optional[str] = None      # required when templateSource == "proxmox"
    harborRegistryId: Optional[str] = None   # kubernetes_deployments.id, required when templateSource == "harbor"
    harborTemplate: Optional[str] = None     # "repository:tag", required when templateSource == "harbor"
    harborArtifact: Optional[str] = None     # "repository:tag" of the model, required when templateSource == "harbor"
    # VM spec -- harbor source only. A qcow2 is just a disk, so the VM built
    # around it is described here; a proxmox-source pool inherits these from
    # its template instead. Persisted together in the vm_config JSONB column.
    cores: Optional[int] = None
    memory: Optional[int] = None             # MB
    network: Optional[str] = None            # bridge name, e.g. "vmbr0"
    nodes: List[NodeConfig]             # [{node, gpu: []}]
    storage: Optional[str] = "local-lvm"
    machine_name: Optional[str] = None
    model: Optional[str] = None         # HuggingFace model ID e.g. "meta-llama/Llama-3-70B-Instruct"
    model_path: Optional[str] = None
    modelType: Optional[str] = None            # text | vision_language | embeddings | audio | other
    modelTypeOther: Optional[str] = None       # free-text label when modelType == "other"
    maxImagesPerRequest: Optional[int] = None  # vision_language only
    vllmExtraParams: Optional[str] = None      # raw YAML/key:value text from the UI textarea, parsed below
    ssh_user: str                       # VM SSH user -- required, no env fallback
    ssh_pass: str                       # VM SSH password -- required, no env fallback
    # Where the Harbor template gets staged+built -- temporary, cleaned up
    # right after cloning. "auto" (default) needs no other fields; "manual"
    # requires templateStorageNode + templateStorageStorage.
    templateStorageMode: Optional[str] = "auto"      # "auto" | "manual"
    templateStorageNode: Optional[str] = None
    templateStorageStorage: Optional[str] = None
    # ram: Optional[int] = None         # taken from template
    # cpu: Optional[int] = None         # taken from template

    @validator("template", always=True)
    def template_must_not_be_empty(cls, v, values):
        if values.get("templateSource") == "harbor":
            return v
        if not v or not v.strip():
            raise ValueError("Template VM is required. Provide a valid Proxmox template VMID or name.")
        return v.strip()

    @validator("harborRegistryId", "harborTemplate", "harborArtifact", always=True)
    def harbor_fields_required_when_harbor_source(cls, v, values):
        if values.get("templateSource") == "harbor" and not v:
            raise ValueError("harborRegistryId, harborTemplate, and harborArtifact are required when templateSource is 'harbor'.")
        return v

    @validator("maxImagesPerRequest", "cores", "memory", pre=True)
    def blank_int_to_none(cls, v):
        return None if v == "" else v

    @validator("cores", "memory", "network", always=True)
    def vm_spec_required_when_harbor_source(cls, v, values):
        # A Proxmox template already carries these; a bare qcow2 does not, so
        # for harbor source there is nothing to fall back on.
        if values.get("templateSource") == "harbor" and not v:
            raise ValueError(
                "cores, memory, and network are required when templateSource is 'harbor'."
            )
        return v

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
    ssh_user: Optional[str] = None
    ssh_pass: Optional[str] = None


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

from sqlalchemy import Column, Integer, String, BigInteger, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel
from typing import Optional
import datetime

Base = declarative_base()

LIBRARY_TYPES = {
    "harbor_template", "lxc_backup",
    "base_os", "container",
    "llm_model", "llm_template",
    "podman", "devraq_agent", "general",
}

# type → pod subdirectory under /data/library/
TYPE_SUBDIR = {
    "harbor_template": "harbor",
    "lxc_backup":      "harbor",
    "base_os":         "os",
    "container":       "container",
    "llm_model":       "llm_model",
    "llm_template":    "llm_template",
    "podman":          "podman",
    "devraq_agent":    "general",
    "general":         "general",
}

# All directories present in the pod
POD_DIRS = {"harbor", "os", "container", "llm_model", "llm_template", "podman", "general"}


class LibraryItem(Base):
    __tablename__ = "library"

    id          = Column(Integer, primary_key=True, index=True)
    name        = Column(String, nullable=False)
    type        = Column(String, nullable=False)        # base_os | devraq_agent | container | llm_model | llm_template
    version     = Column(String, nullable=True)
    file_name   = Column(String, nullable=False)
    file_path   = Column(String, nullable=False)
    file_size    = Column(BigInteger, nullable=True)
    progress_pct = Column(Integer, nullable=False, default=0)          # 0-100
    status       = Column(String, nullable=False, default="uploading") # uploading | ready | failed
    workflow_id  = Column(String, nullable=True)
    # Harbor push fields (optional — sirf openwebui/vectordb/llm_model ke liye)
    harbor_registry_id = Column(Integer, nullable=True)  # kubernetes_deployments.id (harbor instance)
    k8s_cluster_id     = Column(Integer, nullable=True)  # derived from harbor_registry — legacy
    harbor_url         = Column(String, nullable=True)   # http://172.16.4.41:80
    harbor_project = Column(String, nullable=True)   # "library"
    harbor_owner   = Column(String, nullable=True)   # owner segment: harbor_host/project/owner/image:tag
    harbor_user    = Column(String, nullable=True)
    harbor_pass    = Column(String, nullable=True)
    harbor_image   = Column(Text, nullable=True)     # final pushed image ref
    push_status    = Column(String, nullable=True)   # pending|pushing|pushed|failed
    push_error     = Column(Text, nullable=True)
    push_workflow_id = Column(String, nullable=True)
    # Rich metadata (version_metadata.json se)
    display_name   = Column(String, nullable=True)  # human-readable name
    description    = Column(Text,   nullable=True)  # full description
    category       = Column(String, nullable=True)  # e.g. container-registry
    tags           = Column(Text,   nullable=True)  # JSON array as text
    metadata_json  = Column(Text,   nullable=True)  # full version_metadata.json
    created_at  = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at  = Column(DateTime, default=datetime.datetime.utcnow,
                         onupdate=datetime.datetime.utcnow)


class LibraryItemOut(BaseModel):
    id:           int
    name:         str
    type:         str
    version:      Optional[str]
    file_name:    str
    file_path:    str
    file_size:    Optional[int]
    progress_pct: int
    status:       str
    workflow_id:  Optional[str]
    created_at:   Optional[str]
    updated_at:   Optional[str]

    class Config:
        orm_mode = True

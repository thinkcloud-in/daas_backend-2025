from sqlalchemy import Column, Integer, String, BigInteger, DateTime
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel
from typing import Optional
import datetime

Base = declarative_base()

LIBRARY_TYPES = {
    "harbor_template", "lxc_backup",
    "base_os", "open_web_ui",
    "vectordb", "podman",
    "devraq_agent", "general",
}

# type → pod subdirectory under /data/library/
TYPE_SUBDIR = {
    "harbor_template": "harbor",
    "lxc_backup":      "harbor",
    "base_os":         "os",
    "open_web_ui":     "openwebui",
    "vectordb":        "vectordb",
    "podman":          "podman",
    "devraq_agent":    "general",
    "general":         "general",
}

# All directories present in the pod
POD_DIRS = {"harbor", "os", "openwebui", "vectordb", "podman", "general"}


class LibraryItem(Base):
    __tablename__ = "library"

    id          = Column(Integer, primary_key=True, index=True)
    name        = Column(String, nullable=False)
    type        = Column(String, nullable=False)        # base_os | devraq_agent | open_web_ui
    version     = Column(String, nullable=True)
    file_name   = Column(String, nullable=False)
    file_path   = Column(String, nullable=False)
    file_size    = Column(BigInteger, nullable=True)
    progress_pct = Column(Integer, nullable=False, default=0)          # 0-100
    status       = Column(String, nullable=False, default="uploading") # uploading | ready | failed
    workflow_id  = Column(String, nullable=True)
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

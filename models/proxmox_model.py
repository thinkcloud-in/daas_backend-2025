from typing import Optional
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel
 
Base = declarative_base()
 
class Proxmox(Base):
    __tablename__ = "Proxmox_Auth_table"
 
    id = Column(Integer, primary_key=True, index=False)
    cluster_name = Column(String, nullable=False)
    user = Column(String,  nullable=False)
    new_password = Column(String, nullable=False)
    token_id = Column(String, nullable=False)
    secret_key = Column(String, nullable=False)
    full_token=Column(String,nullable=False)
    api_token=Column(String,nullable=False)
    role = Column(String, nullable=False)
    path = Column(String, nullable=False)
 
class CreateBaseProxmox(BaseModel):
    id: int
    cluster_name: str
    user: str
    new_password: str
    token_id: str
    secret_key: str
    api_token:str
    full_token:str
    role: str
    path: str

class MetricServer(Base):
    __tablename__ = 'metric_servers'
 
    id = Column(Integer, primary_key=True, index=True)
    cluster_id = Column(Integer, nullable=False)
    type = Column(String, nullable=False)
    server = Column(String, nullable=False)
    port = Column(Integer, nullable=False)
    proto = Column(String, nullable=False)
    organization = Column(String, nullable=False)
    bucket = Column(String, nullable=False)
    token = Column(String, nullable=False)
    disabled = Column(Boolean, default=False)  
    monitoring = Column(Boolean, default=False)  
    is_custom_integration = Column(Boolean, nullable=False)
    workflow_id = Column(String, nullable=True)  # Optional field for workflow ID
    
    
class MetricServerBase(BaseModel):
    cluster_id: int
    type: str
    server: str
    port: int
    proto: str
    organization: str
    bucket: str
    token: str
    disabled: Optional[bool] = False
    monitoring: Optional[bool] = False
    is_custom_integration: Optional[bool] = None
    workflow_id: Optional[str] = None
 

class MigrateRequest(BaseModel):
    src_url: str
    src_token: str
    src_org: str
    src_bucket: str
    cluster_id: int
    email: Optional[str] = None
    workflow_id: Optional[str] = None
class VMPowerRequest(BaseModel):
    email: str
 
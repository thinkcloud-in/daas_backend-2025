from pydantic import BaseModel
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.declarative import declarative_base
from typing import List, Optional
Base = declarative_base()

class IPSModel(Base):
    __tablename__ = "Ips_pools"
    id = Column(Integer, primary_key=True, index=True)
    Pool_name = Column(String, nullable=False)
    Starting_ip = Column(String, nullable=False)
    Ending_ip = Column(String, nullable=False)
    Subnet = Column(String, nullable=False)
    Gateway = Column(String, nullable=False)
    DNS = Column(ARRAY(String), nullable=True)
    Status = Column(String, nullable=True)


class IPSRequest(BaseModel):
    Pool_name: str
    Starting_ip: str
    Ending_ip: str
    ips: Optional[List[str]] = None
    Subnet: str
    Gateway: str
    DNS: List[str] = []
    Status: Optional[str] = None    

class IPEntry(Base):
    __tablename__ = "Ip_Entries"
    id = Column(Integer, primary_key=True, index=True)
    pool_id = Column(Integer, ForeignKey("Ips_pools.id"), nullable=False)
    ip = Column(String, nullable=False)
    status = Column(String, nullable=False, default="unused")
    vm_id = Column(String, nullable=True)  

class IPEntryOut(BaseModel):
    id: int
    pool_id: int
    ip: str
    status: str
    vm_id: str | None = None
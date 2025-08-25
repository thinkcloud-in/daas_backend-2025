from pydantic import BaseModel
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.declarative import declarative_base
from typing import List, Optional
 
Base = declarative_base()
 
class IPMIDevice(Base):
    __tablename__ = "ipmi_servers"
 
    id = Column(Integer, primary_key=True, index=True)
    ipmi_server_ip = Column(String, index=True)
    name = Column(String, nullable=True)
    username = Column(String)
    password = Column(String)
 
class IPMIDeviceRequest(BaseModel):
    ipmi_server_ip: str
    name: Optional[str] = None
    username: str
    password: str
    email: Optional[str] = None
 
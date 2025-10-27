from pydantic import BaseModel
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from typing import  Optional
 
Base = declarative_base()
 
class IPMIDevice(Base):
    __tablename__ = "ipmi_servers"
 
    id = Column(Integer, primary_key=True, index=True)
    ipmi_server_ip = Column(String, index=True)
    name = Column(String, nullable=True)
    username = Column(String)
    password = Column(String)
 
class IPMIDeviceRequest(BaseModel):
    id : Optional[int] = None
    ipmi_server_ip: str
    name: Optional[str] = None
    username: str
    password: str
    email: Optional[str] = None
 
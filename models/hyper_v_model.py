from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel, Field

Base_task = declarative_base()

class Hyper_V(Base_task):
    __tablename__='hyper_v'
    id = Column(Integer, primary_key=True)
    vms_id = Column(String, nullable=False)
    vhd_id = Column(String, nullable=False)

class CloneVMRequest(BaseModel):
    vm_name: str = Field(..., description="Name of the new cloned VM")
    memory: int = Field(..., gt=0, description="Memory size in MB")
    vhdPath: str = Field(..., description="Path of the base VHD file")
    switch: str = Field(..., description="Virtual switch name")
    generation: int = Field(..., ge=1, le=2, description="VM generation (1 or 2)")
    PvhdPath: str = Field(..., description="Path of parent VHD")
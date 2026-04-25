from typing import Optional, Union
from pydantic import BaseModel, Field

class CloneVMRequest(BaseModel):
    cluster_id: int = Field(..., description="ID of the cluster")
    vm_name: str = Field(..., description="Name of the new cloned VM")
    memory: int = Field(..., gt=0, description="Memory size in MB")
    vhdPath: str = Field(..., description="Path of the base VHD file")
    switch: str = Field(..., description="Virtual switch name")
    generation: int = Field(..., ge=1, le=2, description="VM generation (1 or 2)")
    PvhdPath: str = Field(..., description="Path of parent VHD")
    count: int = Field(..., ge=1, le=50, description="Number of VM clones to create (required)")
    domain: Optional[str] = Field(None, description="Domain name")
    ou: Optional[str] = Field(None, description="Organizational unit")
    username: Optional[str] = Field(None, description="Username")
    domain_password: Optional[str] = Field(None, description="Domain password")
    dynamic_memory: Optional[bool] = False
    minimum_memory: Optional[int] = 0
    maximum_memory: Optional[int] = 0
    buffer_memory: Optional[int] = 0 
    processor_count: Optional[int] = 0

class HandleActionRequest(BaseModel):
    vm_id: str = Field(..., description="ID of the VM")
    action: str = Field(..., description="Action to perform on the VM")

class HandleDeleteDiskRequest(BaseModel):
    cluster_id: int = Field(..., description="ID of the cluster")
    disk_path: str = Field(..., description="Path of the disk to be deleted")

class HandleRebuildActionRequest(BaseModel):
    vm_id: Union[int, str] = Field(..., description="ID of the VM")
    pool_id: Union[int, str] = Field(..., description="ID of the pool")

class HandlePoolRebuildActionRequest(BaseModel):
    pool_id: Union[int, str] = Field(..., description="ID of the pool")
    vhdPath: str = Field(..., description="Path of the base VHD file")

class VerifyHyperVRequest(BaseModel):
    ip: str = Field(..., description="IP Address or FQDN")
    username: str = Field(..., description="Username")
    password: str = Field(..., description="Password")
    agent_port: Optional[Union[str, int]] = Field(None, description="Agent Port")
    type: str = Field(..., description="Type of the Hyper-V")
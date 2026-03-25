from typing import Optional, Union
from pydantic import BaseModel, Field

class CloneVMRequest(BaseModel):
    vm_name: str = Field(..., description="Name of the new cloned VM")
    memory: int = Field(..., gt=0, description="Memory size in MB")
    vhdPath: str = Field(..., description="Path of the base VHD file")
    switch: str = Field(..., description="Virtual switch name")
    generation: int = Field(..., ge=1, le=2, description="VM generation (1 or 2)")
    PvhdPath: str = Field(..., description="Path of parent VHD")
    count: int = Field(..., ge=1, le=50, description="Number of VM clones to create (required)")
    # os_type: str
    # mac_add: str
    # gateway: str
    # password: str
    # ip: str

class HandleActionRequest(BaseModel):
    vm_name: str = Field(..., description="Name of the VM")
    action: str = Field(..., description="Action to perform on the VM")

class HandleDeleteDiskRequest(BaseModel):
    disk_path: str = Field(..., description="Path of the disk to be deleted")

class HandleRebuildActionRequest(BaseModel):
    vm_id: Union[int, str] = Field(..., description="ID of the VM")
    pool_id: Union[int, str] = Field(..., description="ID of the pool")
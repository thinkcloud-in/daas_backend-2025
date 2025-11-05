from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel, Field
class CloneVMRequest(BaseModel):
    vm_name: str = Field(..., description="Name of the new cloned VM")
    memory: int = Field(..., gt=0, description="Memory size in MB")
    vhdPath: str = Field(..., description="Path of the base VHD file")
    switch: str = Field(..., description="Virtual switch name")
    generation: int = Field(..., ge=1, le=2, description="VM generation (1 or 2)")
    PvhdPath: str = Field(..., description="Path of parent VHD")
    failover_only: bool = Field(..., description="Indicates if the clone is for failover only")
    guacd_hostname: str = Field(..., description="Guacd hostname for remote access")
    guacd_encryption: str = Field(..., description="Guacd encryption method")
    
class HandleActionRequest(BaseModel):
    vm_name: str = Field(..., description="Name of the VM")
    action: str = Field(..., description="Action to perform on the VM")
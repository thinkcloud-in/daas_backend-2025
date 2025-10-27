from sqlalchemy import Column, Integer, String, ARRAY
from pydantic import BaseModel
from typing import List
from db_configuration.config import Base
from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.postgresql import ARRAY
 
 
 
class RBAC(Base):
    __tablename__ = "rbac_table"
    id = Column(Integer, primary_key=True, index=True)
    users = Column(ARRAY(String))
    role = Column(String)
    components = Column(ARRAY(String))
 
 
class RBACRequest(BaseModel):
    username: list[str]
    role: str
    components: List[str]
 
class RoleComponentSubmitRequest(BaseModel):
    role: str
    components: List[str]
 
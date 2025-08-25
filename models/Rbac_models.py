from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Table, DateTime, text, Boolean,ARRAY,Sequence# type: ignore
from sqlalchemy.orm import relationship, backref# type: ignore
from pydantic import BaseModel# type: ignore
from typing import List, Optional
from db_configuration.config import Base
import datetime
 
from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.postgresql import ARRAY
 
 
 
class RBAC(Base):
    __tablename__ = "rbac_table_test"
    id = Column(Integer, primary_key=True, index=True)
    users = Column(ARRAY(String))  # Use PostgreSQL-specific ARRAY type
    role = Column(String)  # Use PostgreSQL-specific ARRAY type
    components = Column(ARRAY(String))  # Adjust type based on your database schema
 
 
#creating the pydantic basemodel
class RBACRequest(BaseModel):
    username: list[str]
    role: str
    components: List[str]
 
# Add request model for submit endpoint
class RoleComponentSubmitRequest(BaseModel):
    role: str
    components: List[str]
 
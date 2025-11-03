from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base_task = declarative_base()

class Hyper_V(Base_task):
    __tablename__='hyper_v'
    id = Column(Integer, primary_key=True)
    vms_id = Column(String, nullable=False)
    vhd_id = Column(String, nullable=False)
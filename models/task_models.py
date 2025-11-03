from sqlalchemy import Column,Integer,String
from pydantic import BaseModel
from sqlalchemy.ext.declarative import declarative_base
 
Base_task=declarative_base()
 
 
class Task_DB(Base_task):
    __tablename__='task_table'
    id = Column(Integer, primary_key=True)
    workflowId = Column(String, nullable=False)
    userName = Column(String, nullable=False)
    userEmail = Column(String, nullable=False)
 
class Task_config(BaseModel):
    workflowId : str
    userName : str
    userEmail : str
from sqlalchemy import Column,Integer,String
from pydantic import BaseModel
from sqlalchemy.ext.declarative import declarative_base

Base=declarative_base()


class Schdeule(Base):
    __tablename__ = 'schedule_report_table'

    id = Column(Integer, primary_key=True)
    userEmail = Column(String(100), nullable=False)
    receiverEmail = Column(String, nullable=False)
    report = Column(String(100), nullable=False)
    reportName = Column(String(100), nullable=False)
    time= Column(String)
    schedule_date=Column(String)
    schedule_type=Column(String(100), nullable=False)
    schedule_id=Column(String)

class Schedule_report(BaseModel):
    userEmail:str
    receiverEmail:str
    report:str
    reportName:str
    time:str
    schedule_date:str
    schedule_type:str
    schedule_id:str

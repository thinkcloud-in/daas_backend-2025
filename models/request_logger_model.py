from typing import Optional
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, DateTime, Text, Float
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class RequestLog(Base):
    __tablename__ = "request_logs"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False)
    user = Column(String(255), nullable=True)
    method = Column(String(10), nullable=False)
    url = Column(Text, nullable=False)
    status = Column(String(20), nullable=False)
    duration = Column(Float, nullable=False)  
    details = Column(Text, nullable=True)


class RequestLogRequest(BaseModel):
    id: Optional[int] = None
    timestamp: Optional[datetime] = None
    user: Optional[str] = None
    method: str
    url: str
    status: str
    duration: float
    details: Optional[str] = None

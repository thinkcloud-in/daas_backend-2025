import datetime
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class RetentionSetting(Base):
    """
    Singleton row (ek hi record) — Temporal aur OpenSearch dono ke liye
    current retention configuration yahan persist hoti hai. Temporal khud
    apni current retention "describe namespace" se de deta hai, lekin
    OpenSearch ke paas aisi koi "describe policy" API nahi hai — isliye
    jo bhi retention_days set kiya gaya, use yahan record karna zaroori hai
    (daily cleanup schedule ko batane ke liye, aur UI me dikhane ke liye).
    """
    __tablename__ = "retention_settings"

    id                       = Column(Integer, primary_key=True, index=True)
    retention_days           = Column(Integer, nullable=False, default=30)
    temporal_namespace       = Column(String, nullable=False, default="default")
    opensearch_index_pattern = Column(String, nullable=False, default="backend-logs-*")
    updated_by               = Column(String, nullable=True)
    created_at               = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at               = Column(DateTime, default=datetime.datetime.utcnow,
                                      onupdate=datetime.datetime.utcnow)

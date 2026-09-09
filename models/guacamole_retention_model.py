import datetime
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class GuacamoleRetentionSetting(Base):
    """Singleton row -- Guacamole connection-history retention ke liye
    abhi kya set hai, ye track karta hai (jaise Temporal/OpenSearch
    retention_settings, lekin Guacamole ke liye alag)."""

    __tablename__ = "guacamole_retention_settings"

    id             = Column(Integer, primary_key=True, index=True)
    retention_days = Column(Integer, nullable=False, default=30)
    updated_by     = Column(String, nullable=True)
    created_at     = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at     = Column(DateTime, default=datetime.datetime.utcnow,
                             onupdate=datetime.datetime.utcnow)

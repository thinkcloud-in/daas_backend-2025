import datetime
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class GuacamoleRetentionSetting(Base):
    """Singleton row -- tracks what is currently set for Guacamole
    connection-history retention (like Temporal/OpenSearch
    retention_settings, but separate, for Guacamole)."""

    __tablename__ = "guacamole_retention_settings"

    id             = Column(Integer, primary_key=True, index=True)
    retention_days = Column(Integer, nullable=False, default=30)
    updated_by     = Column(String, nullable=True)
    created_at     = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at     = Column(DateTime, default=datetime.datetime.utcnow,
                             onupdate=datetime.datetime.utcnow)

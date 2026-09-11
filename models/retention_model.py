import datetime
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class RetentionSetting(Base):
    """
    Singleton row (a single record) — the current retention configuration
    for both Temporal and OpenSearch is persisted here. Temporal itself
    exposes its current retention via "describe namespace", but OpenSearch
    has no such "describe policy" API — so whatever retention_days is set
    must be recorded here (to tell the daily cleanup schedule, and to show
    in the UI).
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

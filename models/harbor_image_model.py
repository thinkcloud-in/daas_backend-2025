import datetime
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class HarborImage(Base):
    __tablename__ = "harbor_images"

    id           = Column(Integer, primary_key=True, index=True)
    name         = Column(String, nullable=False)          # display name
    type         = Column(String, nullable=False)          # openwebui / vectordb / etc.
    machine_name = Column(String, nullable=False)          # Harbor LXC machine name
    machine_ip   = Column(String, nullable=True)           # resolved at upload time
    image_tag    = Column(String, nullable=False, default="latest")
    project      = Column(String, nullable=False, default="library")
    harbor_image = Column(String, nullable=True)           # 127.0.0.1/project/name:tag
    file_name    = Column(String, nullable=True)
    file_size    = Column(Integer, nullable=True)
    status       = Column(String, nullable=False, default="pending")  # pending/uploading/pushing/ready/failed
    workflow_id  = Column(String, nullable=True)
    created_at   = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at   = Column(DateTime, default=datetime.datetime.utcnow,
                          onupdate=datetime.datetime.utcnow)

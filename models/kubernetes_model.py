import datetime
from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class KubernetesCluster(Base):
    __tablename__ = "kubernetes_clusters"

    id          = Column(Integer, primary_key=True, index=True)
    name        = Column(String, nullable=False, unique=True)
    control_ip  = Column(String, nullable=False)
    port        = Column(Integer, nullable=False, default=6443)
    username    = Column(String, nullable=True)
    password    = Column(String, nullable=True)
    auth_token  = Column(Text, nullable=True)
    kubeconfig  = Column(Text, nullable=True)       # full YAML content
    status      = Column(String, nullable=False, default="pending")  # pending/connected/failed
    last_tested = Column(DateTime, nullable=True)
    workflow_id = Column(String, nullable=True)
    created_at  = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at  = Column(DateTime, default=datetime.datetime.utcnow,
                         onupdate=datetime.datetime.utcnow)

import datetime
from sqlalchemy import Column, Integer, String, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class KubernetesDeployment(Base):
    __tablename__ = "kubernetes_deployments"

    id              = Column(Integer, primary_key=True, index=True)
    cluster_id      = Column(Integer, nullable=False)
    library_item_id = Column(Integer, nullable=False)
    name            = Column(String, nullable=False)
    namespace       = Column(String, nullable=False, default="harbor")
    node_name       = Column(String, nullable=True)
    node_ip         = Column(String, nullable=True)
    ssh_port        = Column(Integer, nullable=False, default=22)
    ssh_username    = Column(String, nullable=True)
    deployment_type = Column(String, nullable=False, default="kubernetes")
    deploy_dir      = Column(String, nullable=True)
    harbor_url      = Column(String, nullable=True)
    harbor_user     = Column(String, nullable=True)
    harbor_pass     = Column(String, nullable=True)
    status          = Column(String, nullable=False, default="pending")
    workflow_id     = Column(String, nullable=True)
    steps_log       = Column(Text, nullable=True)   # JSON array of step timestamps
    error_message   = Column(Text, nullable=True)
    created_at      = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at      = Column(DateTime, default=datetime.datetime.utcnow,
                             onupdate=datetime.datetime.utcnow)

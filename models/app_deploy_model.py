import datetime
from sqlalchemy import Column, Integer, String, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

VALID_DEPLOY_TYPES = {"openwebui", "vectordb"}


class AppDeployment(Base):
    __tablename__ = "app_deployments"

    id                  = Column(Integer, primary_key=True, index=True)
    name                = Column(String, nullable=False)
    deployment_type     = Column(String, nullable=False)   # "openwebui" | "vectordb"
    k8s_cluster_id      = Column(Integer, nullable=False)  # kubernetes_clusters.id
    harbor_registry_id  = Column(Integer, nullable=True)   # kubernetes_deployments.id (harbor)
    library_item_id     = Column(Integer, nullable=True)   # library.id (version)
    namespace           = Column(String, nullable=False, default="default")

    # Resolved at deploy time
    harbor_url          = Column(String, nullable=True)    # e.g. http://172.16.4.41:30080
    image               = Column(String, nullable=True)    # full harbor image ref

    # Set after deployment is ready
    external_ip         = Column(String, nullable=True)
    node_port           = Column(String, nullable=True)
    service_url         = Column(String, nullable=True)

    # VectorDB integration (openwebui only)
    linked_vectordb_id  = Column(Integer, nullable=True)   # app_deployments.id of vectordb

    # Private LLM integration (openwebui only) — JSON array of llm_inferences.id
    linked_llm_ids      = Column(Text, nullable=True)      # e.g. "[1, 3, 5]"

    # Keycloak SSO integration (openwebui only) — JSON object
    keycloak_config     = Column(Text, nullable=True)

    status              = Column(String, nullable=False, default="pending")
    # pending → connecting → deploying → waiting_ready → deployed | failed
    workflow_id         = Column(String, nullable=True)
    steps_log           = Column(Text, nullable=True)      # JSON array of step events
    error_message       = Column(Text, nullable=True)
    created_at          = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at          = Column(DateTime, default=datetime.datetime.utcnow,
                                 onupdate=datetime.datetime.utcnow)

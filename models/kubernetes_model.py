import datetime
from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base

from utils.crypto_utils import EncryptedString

Base = declarative_base()


class KubernetesCluster(Base):
    __tablename__ = "kubernetes_clusters"

    id          = Column(Integer, primary_key=True, index=True)
    name        = Column(String, nullable=False, unique=True)
    control_ip  = Column(String, nullable=False)
    port        = Column(Integer, nullable=False, default=6443)
    # kubeconfig is the ONLY supported way to authenticate to a cluster —
    # username/password/auth_token columns were removed entirely (2026-09):
    # 1. Kubernetes dropped HTTP Basic Auth from the API server in v1.19 —
    #    username/password could never actually work against any current
    #    cluster, only ones running an EOL Kubernetes version.
    # 2. Every real operation this backend performs (creating/deleting
    #    namespaces, creating PersistentVolumes, applying arbitrary
    #    manifests) needs cluster-admin-equivalent access — a narrower
    #    auth token was never a meaningful lighter-weight alternative.
    # A fresh deploy of this schema will never create these columns at all.
    # If this migration ever runs against a database that still has them
    # (from before this change), run a manual `ALTER TABLE kubernetes_clusters
    # DROP COLUMN username, DROP COLUMN password, DROP COLUMN auth_token;`
    # — not done automatically here, since dropping columns on a live
    # production database should be a deliberate, reviewed step.
    #
    # kubeconfig can independently grant full access to this cluster (often
    # carries a client cert/key or a long-lived bearer token). Encrypted at
    # rest via the same Fernet/CLUSTER_SECRET_KEY scheme already used for
    # clusters.password (see docs/cluster_password_encryption.md) — reads
    # existing code unchanged, only what's stored in Postgres is encrypted.
    # Run encrypt_kubernetes_cluster_secrets.py BEFORE deploying this change
    # against a database with any existing plaintext rows.
    kubeconfig  = Column(EncryptedString, nullable=True)       # full YAML content
    status      = Column(String, nullable=False, default="pending")  # pending/connected/failed
    last_tested = Column(DateTime, nullable=True)
    workflow_id = Column(String, nullable=True)
    created_at  = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at  = Column(DateTime, default=datetime.datetime.utcnow,
                         onupdate=datetime.datetime.utcnow)

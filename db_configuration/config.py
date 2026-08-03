from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker 
from sqlalchemy.ext.declarative import declarative_base
import os
import logging
from dotenv import load_dotenv
from utils.logging_config import setup_logging
load_dotenv()
setup_logging()
logger = logging.getLogger(__name__)


database_url=f"postgresql://{os.getenv('USER_NAME')}:{os.getenv('PASSWORD')}@{os.getenv('HOST_NAME')}/thinkclouddb"

SQLALCHEMY_DATABASE_URL = database_url

engine = create_engine(
    database_url,
    pool_size=50,        
    max_overflow=50,     
    pool_timeout=30,     
    pool_recycle=1800,   
    pool_pre_ping=True,  
    echo=False,          
    future=True          
)


try:
    with engine.begin() as connection:
        connection.execute(text("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'machine_id_seq') THEN
                    CREATE SEQUENCE machine_id_seq;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'pool_id_seq') THEN
                    CREATE SEQUENCE pool_id_seq;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'cluster_id_seq') THEN
                    CREATE SEQUENCE cluster_id_seq;
                END IF;
            END;
            $$;
        """))
except Exception as e:
    logger.error(f"Failed to ensure sequences exist: {str(e)}")

try:
    with engine.begin() as connection:
        connection.execute(text("""
            ALTER TABLE llm_inference_jobs
                ADD COLUMN IF NOT EXISTS model TEXT;
        """))
except Exception as e:
    logger.error(f"Failed to apply llm_inference_jobs migrations: {str(e)}")

try:
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS user_action_totp (
                user_id     VARCHAR PRIMARY KEY,
                totp_secret VARCHAR NOT NULL,
                username    VARCHAR,
                created_at  TIMESTAMP DEFAULT NOW()
            );
        """))
except Exception as e:
    logger.error(f"Failed to create user_action_totp table: {str(e)}")

try:
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS library (
                id           SERIAL PRIMARY KEY,
                name         VARCHAR   NOT NULL,
                type         VARCHAR   NOT NULL,
                version      VARCHAR,
                file_name    VARCHAR   NOT NULL,
                file_path    VARCHAR   NOT NULL,
                file_size    BIGINT,
                progress_pct INTEGER   NOT NULL DEFAULT 0,
                status       VARCHAR   NOT NULL DEFAULT 'uploading',
                workflow_id  VARCHAR,
                created_at   TIMESTAMP DEFAULT NOW(),
                updated_at   TIMESTAMP DEFAULT NOW()
            )
        """))
except Exception as e:
    logger.error(f"Failed to create library table: {str(e)}")

for _col_sql in [
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS status              VARCHAR  NOT NULL DEFAULT 'uploading'",
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS workflow_id         VARCHAR",
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS progress_pct        INTEGER  NOT NULL DEFAULT 0",
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS k8s_cluster_id      INTEGER",
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS harbor_registry_id  INTEGER",
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS harbor_url          VARCHAR",
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS harbor_project       VARCHAR",
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS harbor_owner         VARCHAR",
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS harbor_user          VARCHAR",
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS harbor_pass          VARCHAR",
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS harbor_image         TEXT",
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS push_status          VARCHAR",
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS push_error           TEXT",
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS push_workflow_id     VARCHAR",
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS display_name         VARCHAR",
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS description          TEXT",
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS category             VARCHAR",
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS tags                 TEXT",
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS metadata_json        TEXT",
]:
    try:
        with engine.begin() as connection:
            connection.execute(text(_col_sql))
    except Exception as e:
        logger.error(f"Failed to apply library migration [{_col_sql[:50]}]: {str(e)}")

try:
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS lxc_restore_jobs (
                id              SERIAL PRIMARY KEY,
                name            VARCHAR   NOT NULL,
                cluster_id      INTEGER   NOT NULL,
                ip_pool_id      INTEGER   NOT NULL,
                library_item_id INTEGER   NOT NULL,
                node            VARCHAR,
                vmid            INTEGER,
                ip_address      VARCHAR,
                storage         VARCHAR   DEFAULT 'local-lvm',
                status          VARCHAR   NOT NULL DEFAULT 'provisioning',
                workflow_id     VARCHAR,
                created_at      TIMESTAMP DEFAULT NOW(),
                updated_at      TIMESTAMP DEFAULT NOW()
            )
        """))
except Exception as e:
    logger.error(f"Failed to create lxc_restore_jobs table: {str(e)}")

for _lxc_col_sql in [
    "ALTER TABLE lxc_restore_jobs ADD COLUMN IF NOT EXISTS container_state VARCHAR DEFAULT 'unknown'",
]:
    try:
        with engine.begin() as connection:
            connection.execute(text(_lxc_col_sql))
    except Exception as e:
        logger.error(f"Failed to apply lxc_restore_jobs migration [{_lxc_col_sql[:60]}]: {str(e)}")


try:
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS harbor_images (
                id           SERIAL PRIMARY KEY,
                name         VARCHAR   NOT NULL,
                type         VARCHAR   NOT NULL,
                machine_name VARCHAR   NOT NULL,
                machine_ip   VARCHAR,
                image_tag    VARCHAR   NOT NULL DEFAULT 'latest',
                project      VARCHAR   NOT NULL DEFAULT 'library',
                harbor_image VARCHAR,
                file_name    VARCHAR,
                file_size    BIGINT,
                status       VARCHAR   NOT NULL DEFAULT 'pending',
                workflow_id  VARCHAR,
                created_at   TIMESTAMP DEFAULT NOW(),
                updated_at   TIMESTAMP DEFAULT NOW()
            )
        """))
except Exception as e:
    logger.error(f"Failed to create harbor_images table: {str(e)}")

try:
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS kubernetes_clusters (
                id          SERIAL PRIMARY KEY,
                name        VARCHAR   NOT NULL UNIQUE,
                control_ip  VARCHAR   NOT NULL,
                port        INTEGER   NOT NULL DEFAULT 6443,
                username    VARCHAR,
                password    VARCHAR,
                auth_token  TEXT,
                kubeconfig  TEXT,
                status      VARCHAR   NOT NULL DEFAULT 'pending',
                last_tested TIMESTAMP,
                workflow_id VARCHAR,
                created_at  TIMESTAMP DEFAULT NOW(),
                updated_at  TIMESTAMP DEFAULT NOW()
            )
        """))
except Exception as e:
    logger.error(f"Failed to create kubernetes_clusters table: {str(e)}")

for _k8s_dep_col in [
    "ALTER TABLE kubernetes_deployments ADD COLUMN IF NOT EXISTS steps_log  TEXT",
    "ALTER TABLE kubernetes_deployments ADD COLUMN IF NOT EXISTS harbor_user VARCHAR",
    "ALTER TABLE kubernetes_deployments ADD COLUMN IF NOT EXISTS harbor_pass VARCHAR",
]:
    try:
        with engine.begin() as connection:
            connection.execute(text(_k8s_dep_col))
    except Exception as e:
        logger.error(f"Failed to apply kubernetes_deployments migration: {str(e)}")

try:
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS kubernetes_deployments (
                id              SERIAL PRIMARY KEY,
                cluster_id      INTEGER   NOT NULL,
                library_item_id INTEGER   NOT NULL,
                name            VARCHAR   NOT NULL,
                namespace       VARCHAR   NOT NULL DEFAULT 'harbor',
                node_name       VARCHAR,
                node_ip         VARCHAR   NOT NULL,
                ssh_port        INTEGER   NOT NULL DEFAULT 22,
                ssh_username    VARCHAR,
                deployment_type VARCHAR   NOT NULL DEFAULT 'kubernetes',
                deploy_dir      VARCHAR,
                harbor_url      VARCHAR,
                status          VARCHAR   NOT NULL DEFAULT 'pending',
                workflow_id     VARCHAR,
                error_message   TEXT,
                created_at      TIMESTAMP DEFAULT NOW(),
                updated_at      TIMESTAMP DEFAULT NOW()
            )
        """))
except Exception as e:
    logger.error(f"Failed to create kubernetes_deployments table: {str(e)}")

try:
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS app_deployments (
                id                  SERIAL PRIMARY KEY,
                name                VARCHAR   NOT NULL,
                deployment_type     VARCHAR   NOT NULL DEFAULT 'openwebui',
                k8s_cluster_id      INTEGER   NOT NULL,
                harbor_registry_id  INTEGER,
                library_item_id     INTEGER,
                namespace           VARCHAR   NOT NULL DEFAULT 'default',
                harbor_url          VARCHAR,
                image               VARCHAR,
                external_ip         VARCHAR,
                node_port           VARCHAR,
                service_url         VARCHAR,
                status              VARCHAR   NOT NULL DEFAULT 'pending',
                workflow_id         VARCHAR,
                steps_log           TEXT,
                error_message       TEXT,
                created_at          TIMESTAMP DEFAULT NOW(),
                updated_at          TIMESTAMP DEFAULT NOW()
            )
        """))
except Exception as e:
    logger.error(f"Failed to create app_deployments table: {str(e)}")

for _app_col in [
    "ALTER TABLE app_deployments ADD COLUMN IF NOT EXISTS deployment_type   VARCHAR NOT NULL DEFAULT 'openwebui'",
    "ALTER TABLE app_deployments ADD COLUMN IF NOT EXISTS library_item_id   INTEGER",
    "ALTER TABLE app_deployments ADD COLUMN IF NOT EXISTS image             VARCHAR",
    "ALTER TABLE app_deployments ADD COLUMN IF NOT EXISTS external_ip       VARCHAR",
    "ALTER TABLE app_deployments ADD COLUMN IF NOT EXISTS node_port         VARCHAR",
    "ALTER TABLE app_deployments ADD COLUMN IF NOT EXISTS service_url       VARCHAR",
    "ALTER TABLE app_deployments ADD COLUMN IF NOT EXISTS linked_vectordb_id INTEGER",
]:
    try:
        with engine.begin() as connection:
            connection.execute(text(_app_col))
    except Exception as e:
        logger.error(f"Failed to apply app_deployments migration [{_app_col[:60]}]: {str(e)}")

SessionLocal = sessionmaker(autocommit=False, autoflush=True, bind=engine)
Base = declarative_base()
def get_db():
    db = SessionLocal()
    try:
        yield db
    except:
        db.rollback()
        raise
    finally:
        db.close()

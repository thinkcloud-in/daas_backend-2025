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
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS status       VARCHAR  NOT NULL DEFAULT 'uploading'",
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS workflow_id  VARCHAR",
    "ALTER TABLE library ADD COLUMN IF NOT EXISTS progress_pct INTEGER  NOT NULL DEFAULT 0",
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

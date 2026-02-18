from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker 
from sqlalchemy.ext.declarative import declarative_base
import os
import logging
from dotenv import load_dotenv
load_dotenv()
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
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

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker 
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()
database_url=f"postgresql://{os.getenv('USER_NAME')}:{os.getenv('PASSWORD')}@{os.getenv('HOST_NAME')}/thinkclouddb"

SQLALCHEMY_DATABASE_URL = database_url

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
        pool_size=20,
        max_overflow=30,
        pool_timeout=30,
    )


try:
    with engine.connect() as connection:
        connection = connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(text("CREATE SEQUENCE machine_id_seq;CREATE SEQUENCE pool_id_seq;CREATE SEQUENCE cluster_id_seq;"))
except Exception as e:
    logger.error(f"Failed to create database sequences: {str(e)}")

SessionLocal = sessionmaker(autocommit=False, autoflush=True, bind=engine)
Base = declarative_base()
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

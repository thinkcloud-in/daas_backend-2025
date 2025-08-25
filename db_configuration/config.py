from sqlalchemy import create_engine,text,Sequence # type: ignore
from sqlalchemy.orm import sessionmaker # type: ignore
from sqlalchemy.ext.declarative import declarative_base# type: ignore
from dotenv import load_dotenv# type: ignore
import os
from sqlalchemy.exc import SQLAlchemyError# type: ignore

# Load environment variables from .env file  ---------------------------------------------------------------------thinkclouddb
load_dotenv()
# database_url = os.getenv('DATABASE_URL', 'default_value_if_not_found')
database_url=f"postgresql://{os.getenv('USER_NAME')}:{os.getenv('PASSWORD')}@{os.getenv('HOST_NAME')}/thinkclouddb"

# print(database_url)
SQLALCHEMY_DATABASE_URL = database_url



engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
        pool_size=20,        # default is 5, increase as needed
        max_overflow=30,     # default is 10, increase if you expect bursts
        pool_timeout=30,     # seconds to wait before giving up on getting a connection
    )


try:
    # Execute the first query
    with engine.connect() as connection:
        connection = connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(text("CREATE SEQUENCE machine_id_seq;CREATE SEQUENCE pool_id_seq;CREATE SEQUENCE cluster_id_seq;"))
except SQLAlchemyError as e:
    print("An error occurred:", e)

SessionLocal = sessionmaker(autocommit=False, autoflush=True, bind=engine)
Base = declarative_base()
# Function to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

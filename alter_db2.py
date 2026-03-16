import os
from dotenv import load_dotenv
load_dotenv()
from sqlalchemy import create_engine, text

database_url = f"postgresql://{os.getenv('USER_NAME')}:{os.getenv('PASSWORD')}@{os.getenv('HOST_NAME')}/thinkclouddb"
engine = create_engine(database_url)

try:
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE pools_new_1 ADD COLUMN IF NOT EXISTS pool_ad_path VARCHAR;"))
        print('Column added successfully.')
except Exception as e:
    print('Failed:', e)

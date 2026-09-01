"""
One-time migration: encrypts any plaintext passwords already stored in the
`clusters` table's `password` column, using utils/crypto_utils.py.

Run this once (from the project root, with CLUSTER_SECRET_KEY already set in
.env):
    python encrypt_cluster_passwords.py

Safe to run more than once — for each row, it first tries to decrypt the
stored value; if that succeeds, the row is already encrypted and is left
untouched. Only rows that fail to decrypt (i.e. still plaintext) get
encrypted and written back. Talks to the DB directly via raw SQL, so it does
not depend on whether models/models.py has been updated to use
EncryptedString yet.
"""
import os
from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine, text

from utils.crypto_utils import encrypt_password, decrypt_password

database_url = f"postgresql://{os.getenv('USER_NAME')}:{os.getenv('PASSWORD')}@{os.getenv('HOST_NAME')}/thinkclouddb"
engine = create_engine(database_url)

encrypted_count = 0
already_encrypted_count = 0
skipped_count = 0

try:
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT id, password FROM clusters")).fetchall()

        for row in rows:
            cluster_id, password = row.id, row.password

            if not password:
                skipped_count += 1
                continue

            try:
                decrypt_password(password)
                already_encrypted_count += 1
                continue
            except ValueError:
                pass  # not encrypted yet — fall through and encrypt it below

            new_value = encrypt_password(password)
            conn.execute(
                text("UPDATE clusters SET password = :password WHERE id = :id"),
                {"password": new_value, "id": cluster_id},
            )
            encrypted_count += 1

    print(f"Encrypted:              {encrypted_count}")
    print(f"Already encrypted:      {already_encrypted_count}")
    print(f"Empty/null (skipped):   {skipped_count}")
except Exception as e:
    print("Failed:", e)

"""
One-time migration: encrypts any plaintext `kubeconfig` values already
stored in the `kubernetes_clusters` table, using utils/crypto_utils.py — the
exact same Fernet/CLUSTER_SECRET_KEY scheme already applied to
clusters.password (see encrypt_cluster_passwords.py / docs/cluster_password_encryption.md).

`username`/`password`/`auth_token` were removed from this table entirely
(kubeconfig is the only supported auth method now) — this script no longer
touches those columns. If your database still has them from before that
change, drop them directly once you've confirmed nothing else needs them:
    ALTER TABLE kubernetes_clusters DROP COLUMN username, DROP COLUMN password, DROP COLUMN auth_token;

Run this BEFORE deploying the model change that switches
KubernetesCluster.kubeconfig to EncryptedString — that column's
process_result_value() calls decrypt_password(), which raises on a value
that was never encrypted. Running this first means every row is already
encrypted by the time the new code ever tries to read one.

Run once (from the project root, with CLUSTER_SECRET_KEY already set in .env):
    python encrypt_kubernetes_cluster_secrets.py

Safe to run more than once — for each row, it first tries to decrypt the
stored value; if that succeeds, it's already encrypted and is left
untouched. Only a value that fails to decrypt (i.e. still plaintext) gets
encrypted and written back. Talks to the DB directly via raw SQL, so it does
not depend on whether models/kubernetes_model.py has been updated yet.
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
        rows = conn.execute(
            text("SELECT id, kubeconfig FROM kubernetes_clusters")
        ).fetchall()

        for row in rows:
            cluster_id, value = row.id, row.kubeconfig

            if not value:
                skipped_count += 1
                continue

            try:
                decrypt_password(value)
                already_encrypted_count += 1
                continue
            except ValueError:
                pass  # not encrypted yet — fall through and encrypt it below

            new_value = encrypt_password(value)
            conn.execute(
                text("UPDATE kubernetes_clusters SET kubeconfig = :kubeconfig WHERE id = :id"),
                {"kubeconfig": new_value, "id": cluster_id},
            )
            encrypted_count += 1

    print(f"Encrypted:              {encrypted_count}")
    print(f"Already encrypted:      {already_encrypted_count}")
    print(f"Empty/null (skipped):   {skipped_count}")
except Exception as e:
    print("Failed:", e)

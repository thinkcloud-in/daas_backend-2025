"""
Encrypts and decrypts cluster/deployment passwords before storing them in
the DB and when reading them back for use. These passwords are needed in
plaintext to actively authenticate (Proxmox/K8s/SSH), so one-way hashing
(like bcrypt) won't work here — reversible symmetric encryption (Fernet/AES)
is required.

Usage:
    from utils.crypto_utils import encrypt_password, decrypt_password

    cluster.password = encrypt_password(raw_password)        # on save
    real_password     = decrypt_password(cluster.password)   # on use
"""
import os
from functools import lru_cache

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy.types import TypeDecorator, String

_ENV_KEY_NAME = "CLUSTER_SECRET_KEY"


@lru_cache(maxsize=1)
def _get_fernet() -> Fernet:
    key = os.getenv(_ENV_KEY_NAME)
    if not key:
        raise RuntimeError(f"Environment variable '{_ENV_KEY_NAME}' not found.")
    return Fernet(key.encode())


def encrypt_password(plain: str | None) -> str | None:
    """Converts a plaintext password into an encrypted string safe to store in the DB."""
    if not plain:
        return plain
    return _get_fernet().encrypt(plain.encode()).decode()


def decrypt_password(cipher: str | None) -> str | None:
    """Converts an encrypted value from the DB back into the original plaintext password."""
    if not cipher:
        return cipher
    try:
        return _get_fernet().decrypt(cipher.encode()).decode()
    except InvalidToken as e:
        raise ValueError(
            f"Failed to decrypt password — wrong/rotated '{_ENV_KEY_NAME}', "
            "or the value was never encrypted."
        ) from e


class EncryptedString(TypeDecorator):
    """
    SQLAlchemy column type that transparently encrypts a value before it is
    written to the DB, and decrypts it back on read — using encrypt_password/
    decrypt_password above. The underlying DB column stays a plain VARCHAR;
    only the Python-side value changes shape.

    Use it exactly like Column(String, ...) on the model:
        password = Column(EncryptedString)

    Every existing place in the codebase that reads/writes this column via
    the ORM (e.g. `cluster.password`) keeps working unchanged — it always
    sees/sets the plaintext value, never the ciphertext.
    """
    impl = String
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return encrypt_password(value)

    def process_result_value(self, value, dialect):
        return decrypt_password(value)

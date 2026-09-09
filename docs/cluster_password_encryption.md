# Cluster Password Encryption

## Scope

Encryption/decryption has been implemented **only for the `clusters` table**
(`Cluster` model, `models/models.py` — Proxmox/LXC clusters).

The `kubernetes_clusters` table (`KubernetesCluster` model,
`models/kubernetes_model.py`) is **out of scope for now** and still stores
its `password` column as plain, unencrypted text. Apply the same pattern
there later if/when needed.

## Why reversible encryption, not hashing

These passwords are used to actively authenticate against Proxmox, SSH into
hosts, etc. — the app needs the real plaintext password back every time it
uses one. One-way hashing (bcrypt/scrypt, used for login passwords) cannot
be reversed, so it does not apply here. This uses **Fernet** (AES-based
symmetric encryption) from the `cryptography` package instead, which can be
decrypted back to the original value.

## Files changed

| File | Change |
|---|---|
| [`utils/crypto_utils.py`](../utils/crypto_utils.py) | New module: `encrypt_password()`, `decrypt_password()`, and the `EncryptedString` SQLAlchemy column type. |
| [`models/models.py`](../models/models.py) | `Cluster.password` column type changed from `Column(String)` to `Column(EncryptedString)`. One-line change, plus one import. |
| [`.env`](../.env) | New `CLUSTER_SECRET_KEY` variable — the Fernet key used for all encrypt/decrypt operations. |
| [`encrypt_cluster_passwords.py`](../encrypt_cluster_passwords.py) (repo root) | One-time, idempotent script to encrypt any passwords already sitting in the `clusters` table as plaintext. |

No other file was changed.

## How it works

`EncryptedString` is a SQLAlchemy `TypeDecorator` wrapping `String`. It hooks
into the two points where SQLAlchemy converts a Python value to/from the DB:

- **`process_bind_param`** — called right before a value is written to the
  DB. Encrypts the plaintext.
- **`process_result_value`** — called right after a value is read from the
  DB. Decrypts the ciphertext back to plaintext.

Because this happens transparently at the ORM layer, every existing place in
the codebase that does `cluster.password = "..."` or reads `cluster.password`
keeps working exactly as before — it always sees/sets plaintext in Python.
Only the bytes actually stored in Postgres are encrypted. The DB column
itself is unchanged (still `VARCHAR`) — no schema/`ALTER TABLE` migration was
needed for this.

**Confirmed unchanged (verified by code search, no edits needed):**
`service/clusterService.py`, `service/proxmoxService.py`,
`service/temporalResource/activity/activities_cluster.py`,
`service/temporalResource/activity/activities_pool.py`,
`service/temporalResource/activity/activities_lxc_restore.py`,
`service/temporalResource/activity/activities_llm_inference.py` — all the
existing `cluster.password` read/write sites.

## `CLUSTER_SECRET_KEY`

Stored in `.env`. Generated once with:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

**This key must be kept safe and backed up.** If it is lost, every password
already encrypted with it becomes permanently unrecoverable — clusters would
need their passwords re-entered from scratch. In production this should come
from a proper secret store (e.g. a Kubernetes `Secret`), not a plaintext
`.env` file committed anywhere.

## Migrating existing data

Any password already in the `clusters` table before this change is still
stored as plaintext — the `EncryptedString` type only affects values written
*after* the model change is deployed. Run the migration script once to
encrypt what's already there:

```bash
python encrypt_cluster_passwords.py
```

The script is safe to run more than once: for each row it first tries to
decrypt the stored value — if that succeeds, the row is already encrypted
and is left alone; only rows that fail to decrypt (still plaintext) get
encrypted and written back. It talks to the DB directly over raw SQL, so it
works regardless of whether the model change has been deployed yet.

## Known separate issue — not fixed by this change

Several API endpoints currently return the `Cluster` row's `password` field
in plaintext in the JSON response body (`GET /cluster/clusters`,
`GET /cluster/{cluster_id}`, and the create/update/delete cluster responses
in `controllers/routes.py` / `service/temporalResource/activity/activities_cluster.py`).
Encrypting the column **does not fix this** — by the time the row is
serialized to JSON, the ORM has already decrypted `password` back to
plaintext in memory. This needs a separate fix (excluding `password` from
those response payloads) and has not been done as part of this change.

## Testing performed

- Round-trip unit test of `encrypt_password`/`decrypt_password`.
- `EncryptedString.process_bind_param` / `process_result_value` tested in
  isolation.
- Full ORM round-trip test against an in-memory SQLite DB using the same
  `Column(EncryptedString)` pattern as the real `Cluster` model: wrote a row
  the same way existing code does (`cluster.password = "..."`), confirmed
  the raw stored value is ciphertext (not plaintext), and confirmed reading
  it back via the ORM (`cluster.password`) returns the original plaintext.
- Not run against the real Postgres database — `encrypt_cluster_passwords.py`
  should be run manually against the actual DB when ready to apply this.

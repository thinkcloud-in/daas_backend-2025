# DaaS Backend (DevRaq)

FastAPI backend powering the DevRaq Desktop-as-a-Service (DaaS) platform — virtual desktop/VDI provisioning, infrastructure management (Proxmox, Hyper-V, Kubernetes), and a library of container/model artifact pipelines, all orchestrated with [Temporal](https://temporal.io/).

## What this service does

- **VDI / remote desktop management** — machines, pools, and sessions delivered through [Apache Guacamole](https://guacamole.apache.org/), including active-session monitoring and recording playback.
- **Infrastructure integrations** — Proxmox VE, Hyper-V, Kubernetes clusters, IPMI hardware management, LXC container restore, SSL certificate lifecycle, IP pool management.
- **Library & artifact pipeline** — chunked/resumable file uploads (containers, LLM models/templates, base OS images) with live progress tracking, pushed to a Harbor registry via OCI/skopeo.
- **App deployment** — deploying applications onto Kubernetes clusters, including Keycloak-backed app connections.
- **Retention management** — a single API that sets both Temporal namespace retention and OpenSearch log retention (via ISM policies) for the platform's own operational data.
- **Observability** — structured JSON logging (`structlog`), Grafana/InfluxDB metrics wiring, and a Help & Support module for searching/downloading backend logs from OpenSearch.
- **Reporting & notifications** — scheduled usage reports emailed via SMTP, generated and tracked as Temporal workflows.
- **RBAC & auth** — Keycloak-backed authentication and role/permission management.

Almost every long-running or multi-step operation (provisioning a machine, pushing an image, generating a report, cleaning up old data) is modeled as a **Temporal workflow + activity**, with a dedicated worker per domain area running inside this same process.

## Tech stack

| Layer | Technology |
|---|---|
| API framework | FastAPI (Starlette), Uvicorn |
| Workflow orchestration | Temporal.io (`temporalio` SDK) — workflows, activities, workers, schedules |
| Database | PostgreSQL via SQLAlchemy (`psycopg2` / `asyncpg`) — no Alembic; tables are created/altered idempotently at startup |
| Auth | Keycloak (JWT, RBAC) |
| Remote desktop | Apache Guacamole REST API |
| Search/logs | OpenSearch (log search, download, ISM-based retention) |
| Container/artifact registry | Harbor (OCI push via `oras`/`skopeo`) |
| Infra APIs | Proxmox VE, Kubernetes (`kubernetes` client), Hyper-V, IPMI |
| Metrics | InfluxDB, Grafana, Telegraf |
| Logging | `structlog` (JSON in production, console-friendly in dev) |

## Project structure

```
main.py                      # FastAPI app assembly — routers, middleware, worker startup
controllers/                 # Request handlers (business logic called by routers)
router/                      # FastAPI APIRouter definitions per feature area
service/                     # Domain services
service/temporalResource/    # Temporal workflows/, activities/, workers/ (one set per feature)
models/                      # SQLAlchemy models
middleware/                  # DB init, request logging, auth/RBAC, CORS, Grafana init
utils/                       # Shared helpers (crypto, logging config, k8s exec, response format)
db_configuration/            # Engine/session setup + idempotent schema migrations
keycloak_configration/       # Keycloak client setup
dto/                         # Data-transfer / request objects
tests/                       # Test scripts
docs/                        # Feature-specific design notes
k8s/                         # Kubernetes manifests used by the platform
```

## Getting started

### Prerequisites

- Python 3.12
- PostgreSQL
- A reachable Temporal server (self-hosted or Temporal Cloud)
- Access to the platform's Keycloak, OpenSearch, and Guacamole instances (for the features that depend on them)

### Local setup

```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Create a `.env` file in the project root (see [Configuration](#configuration) below), then run:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

On startup, the app also spins up all the Temporal workers (grouped by domain — Core, Identity, Infrastructure, Reporting, LibraryUpload, HarborPush, AppDeploy, etc.) as background asyncio tasks in the same process.

Interactive API docs are available at `http://localhost:8000/docs` once the server is running.

## Configuration

Configuration is read entirely from environment variables (`.env`, loaded via `python-dotenv`). Key groups:

- **Database**: standard `PG*`/SQLAlchemy connection settings
- **Temporal**: `TEMPORAL_SERVER`, `TEMPORAL_NAMESPACE`
- **Keycloak**: `KEYCLOAK_REALM`, `KEYCLOAK_ROOT_URL`, `KEYCLOAK_PUBLIC_URL`, `KEYCLOAK_ADMIN`, `KEYCLOAK_PASSWORD`, `CLIENT_ID`, `DEVRAQ_BACKEND_CLIENT_SECRET`
- **Guacamole**: `GUCAMOLE_BASE_URL`, `USER_GUACA`, `GUACA_PASS`, `GUACAMOLE_DATASOURCE`, `GUACAMOLE_REPORT_URL`
- **OpenSearch**: `OPENSEARCH_URL`, `OPENSEARCH_USER`, `OPENSEARCH_PASSWORD`, `OPENSEARCH_INDEX`
- **Harbor / artifact storage**: `HARBOR_ADMIN_PASSWORD`, `PUSH_IMAGE_BASE_URL`, `STORAGE_BASE_URL`, `STORAGE_INTERNAL_URL`, `STORAGE_TEMP_DIR`, `LIBRARY_BASE_PATH`, `LIBRARY_TEMP_PATH`
- **Proxmox / Hyper-V / LXC**: `PROXMOX_HOST`, `PROXMOX_USER`, `PROXMOX_PASS`, `PROXMOX_SSH_USER`, `PROXMOX_SSH_PASS`, `PROXMOX_STORAGE`, `LXC_SSH_USER`, `LXC_SSH_PASS`, `LXC_BRIDGE`
- **Metrics**: `INFLUXDB_URL`, `INFLUXDB_TOKEN`, `INFLUXDB_ORG`, `INFLUXDB_BUCKET`, `GRAFANA_URL`, `GRAFANA_TOKEN`
- **Security**: `CLUSTER_SECRET_KEY` (Fernet key encrypting stored cluster credentials — see `docs/cluster_password_encryption.md`)
- **CORS**: `CORS_ALLOWED_ORIGINS`, `CORS_ALLOWED_METHODS`, `CORS_ALLOWED_HEADERS`, `CORS_ALLOW_CREDENTIALS`, `CORS_MAX_AGE`

`.env` is git-ignored — never commit real credentials. Ask a team member for a working configuration for your target environment.

## Database schema

There is no Alembic/migration framework. Tables are declared as SQLAlchemy models and created idempotently on startup (`middleware/DB_init.py`), with a small number of additive `ALTER TABLE IF NOT EXISTS` statements in `db_configuration/config.py` for columns added after initial release.

## Background workers (Temporal)

Every feature area registers its own Temporal worker(s) on dedicated task queues (see `service/temporalResource/workers/`). They all start together when the FastAPI app starts (`main.py`) and run for the lifetime of the process — there is no separate worker deployment. If you need to test a single workflow in isolation, you can still start just its worker function directly.

## Testing

Test scripts live under `tests/` and are written with `unittest`. Run them with:

```bash
python -m unittest discover -s tests
```

## Docker

```bash
docker build -t devraq-backend:latest .
docker run -p 8000:8000 --env-file .env devraq-backend:latest
```

The image is based on `python:3.12.1-slim-bookworm` and runs `uvicorn main:app --host 0.0.0.0 --port 8000`.

## Deployment

Deployment is automated via Jenkins (`Jenkinsfile`, not committed to git): build → save image as a tar → copy to the target server → run the deployment script there. Ask a team member for access to the Jenkins job and target-server details.

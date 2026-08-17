import json
import logging
import os
import re
import tempfile
import time

from temporalio import activity

from db_configuration.config import SessionLocal
from models.app_deploy_model import AppDeployment
from models.kubernetes_model import KubernetesCluster

logger = logging.getLogger(__name__)

_KEYCLOAK_ENV_KEYS = {
    "ENABLE_OAUTH_SIGNUP", "OAUTH_PROVIDER_NAME", "OPENID_PROVIDER_URL",
    "OAUTH_CLIENT_ID", "OAUTH_CLIENT_SECRET", "OAUTH_MERGE_ACCOUNTS_BY_EMAIL",
    "ENABLE_LOGIN_FORM", "OAUTH_SCOPES", "DEFAULT_USER_ROLE",
    "OAUTH_ROLES_CLAIM", "OAUTH_ADMIN_ROLES",
}

_LLM_ENV_KEYS = {
    "OPENAI_API_BASE_URL", "OPENAI_API_BASE_URLS",
    "OPENAI_API_KEY",      "OPENAI_API_KEYS",
}

_DAAS_OW_API_KEY = "daas-openwebui-api-key"


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def _log_step(deploy_id: int, step: str):
    db = SessionLocal()
    try:
        d = db.query(AppDeployment).filter(AppDeployment.id == deploy_id).first()
        if d:
            try:
                logs = json.loads(d.steps_log) if d.steps_log else []
            except Exception:
                logs = []
            logs.append({"step": step, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
            d.steps_log = json.dumps(logs)
            db.commit()
    finally:
        db.close()


def _db_update(deploy_id: int, **kwargs):
    db = SessionLocal()
    try:
        d = db.query(AppDeployment).filter(AppDeployment.id == deploy_id).first()
        if d:
            for k, v in kwargs.items():
                setattr(d, k, v)
            db.commit()
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Keycloak Admin API helpers
# ─────────────────────────────────────────────────────────────────────────────

def _kc_admin_token(kc_url: str, kc_admin: str, kc_pass: str) -> str:
    import httpx
    r = httpx.post(
        f"{kc_url}/realms/master/protocol/openid-connect/token",
        data={"grant_type": "password", "client_id": "admin-cli",
              "username": kc_admin, "password": kc_pass},
        timeout=15, verify=False,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def _kc_ensure_client(kc_url: str, token: str, realm: str,
                      client_id: str, redirect_uris: list) -> tuple[str, str]:
    """Create or find Keycloak client, return (client_uuid, client_secret)."""
    import httpx
    hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Check if client exists
    r = httpx.get(f"{kc_url}/admin/realms/{realm}/clients",
                  params={"clientId": client_id}, headers=hdrs, timeout=15, verify=False)
    r.raise_for_status()
    existing = r.json()

    if existing:
        uuid = existing[0]["id"]
        # Update redirect URIs
        httpx.put(f"{kc_url}/admin/realms/{realm}/clients/{uuid}",
                  headers=hdrs, timeout=15, verify=False,
                  json={**existing[0], "redirectUris": redirect_uris, "webOrigins": ["*"]})
    else:
        # Create new client
        cr = httpx.post(
            f"{kc_url}/admin/realms/{realm}/clients",
            headers=hdrs, timeout=15, verify=False,
            json={
                "clientId":                 client_id,
                "enabled":                  True,
                "protocol":                 "openid-connect",
                "publicClient":             False,
                "standardFlowEnabled":      True,
                "directAccessGrantsEnabled": True,
                "redirectUris":             redirect_uris,
                "webOrigins":               ["*"],
            },
        )
        cr.raise_for_status()
        loc = cr.headers.get("Location", "")
        uuid = loc.rstrip("/").split("/")[-1]

    # Get client secret
    sr = httpx.post(f"{kc_url}/admin/realms/{realm}/clients/{uuid}/client-secret",
                    headers=hdrs, timeout=15, verify=False)
    if sr.status_code not in (200, 201):
        sr = httpx.get(f"{kc_url}/admin/realms/{realm}/clients/{uuid}/client-secret",
                       headers=hdrs, timeout=15, verify=False)
    sr.raise_for_status()
    secret = sr.json().get("value", "")
    return uuid, secret


# ─────────────────────────────────────────────────────────────────────────────
# OpenWebUI API helpers
# ─────────────────────────────────────────────────────────────────────────────

def _ow_admin_token(svc: str, email: str, password: str) -> str | None:
    import httpx
    r = httpx.post(f"{svc}/api/v1/auths/signin",
                   json={"email": email, "password": password}, timeout=10)
    if r.status_code == 200:
        return r.json().get("token")
    return None


def _ow_set_auth_config(svc: str, token: str, patch: dict) -> str | None:
    """POST /api/v1/auths/config/update — returns None on success, error string on failure."""
    import httpx
    hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    r = httpx.post(f"{svc}/api/v1/auths/config/update", headers=hdrs, json=patch, timeout=10)
    if r.status_code < 400:
        return None
    return f"auths/config/update → {r.status_code}: {r.text[:120]}"


# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL direct config helper
# ─────────────────────────────────────────────────────────────────────────────

def _pg_set_config(ow_id: int, postgresql_deploy_id, patch: dict) -> str | None:
    """Update OpenWebUI PostgreSQL config table directly. Returns None on success."""
    import json as _j, time as _t

    if not postgresql_deploy_id:
        return "postgresql_deploy_id not set"

    try:
        import psycopg2
    except ImportError:
        return "psycopg2 not installed"

    db = SessionLocal()
    try:
        pg = db.query(AppDeployment).filter(AppDeployment.id == postgresql_deploy_id).first()
        if not pg:
            return f"PostgreSQL deployment id={postgresql_deploy_id} not found"
        if not pg.external_ip or not pg.node_port:
            return "PostgreSQL has no external_ip/node_port"
        _host, _port = pg.external_ip, int(pg.node_port)
    finally:
        db.close()

    _dbname = f"openwebui_{ow_id}"
    try:
        conn = psycopg2.connect(host=_host, port=_port, dbname=_dbname,
                                user="postgres", password="postgres123", connect_timeout=10)
        cur = conn.cursor()
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name='config'"
        )
        if not cur.fetchone():
            conn.close()
            return f"'config' table not found in {_dbname}"

        cur.execute("SELECT id, data FROM config ORDER BY id LIMIT 1")
        row = cur.fetchone()
        if row:
            try:
                data = _j.loads(row[1]) if row[1] else {}
            except Exception:
                data = {}
            data.update(patch)
            cur.execute("UPDATE config SET data = %s, updated_at = %s WHERE id = %s",
                        (_j.dumps(data), int(_t.time() * 1000), row[0]))
        else:
            _ts = int(_t.time() * 1000)
            cur.execute("INSERT INTO config (data, version, created_at, updated_at) VALUES (%s, 1, %s, %s)",
                        (_j.dumps(patch), _ts, _ts))
        conn.commit()
        cur.close()
        conn.close()
        return None
    except Exception as e:
        return f"PG error: {str(e)[:200]}"


# ─────────────────────────────────────────────────────────────────────────────
# K8s helpers
# ─────────────────────────────────────────────────────────────────────────────

def _load_k8s_client(k8s_cluster_id: int):
    """Load kubeconfig from DB, return (apps_v1, cluster_name)."""
    import yaml
    from kubernetes import client as kc, config as kcfg

    db = SessionLocal()
    try:
        cluster = db.query(KubernetesCluster).filter(KubernetesCluster.id == k8s_cluster_id).first()
        if not cluster or not cluster.kubeconfig:
            raise RuntimeError(f"K8s cluster id={k8s_cluster_id} not found or kubeconfig missing")
        kubeconfig_yaml = cluster.kubeconfig
        control_ip      = cluster.control_ip
        cluster_name    = cluster.name or str(k8s_cluster_id)
    finally:
        db.close()

    kc_dict = yaml.safe_load(kubeconfig_yaml)
    if control_ip:
        for ce in kc_dict.get("clusters", []):
            srv = ce.get("cluster", {}).get("server", "")
            if srv:
                ce["cluster"]["server"] = re.sub(r"https://[^:/]+", f"https://{control_ip}", srv)

    kc_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(kc_dict, f)
            kc_path = f.name
        kcfg.load_kube_config(config_file=kc_path)
    finally:
        if kc_path:
            try:
                os.unlink(kc_path)
            except OSError:
                pass

    return kc.AppsV1Api(), cluster_name


def _wait_rollout(apps_v1, dep_name: str, namespace: str,
                  expected_gen: int, timeout: int = 300) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        dep       = apps_v1.read_namespaced_deployment(name=dep_name, namespace=namespace)
        obs_gen   = dep.status.observed_generation or 0
        upd_rep   = dep.status.updated_replicas    or 0
        ready_rep = dep.status.ready_replicas      or 0
        wanted    = dep.spec.replicas              or 1
        activity.logger.info(
            f"[KcRollout] obs_gen={obs_gen}/{expected_gen} "
            f"updated={upd_rep} ready={ready_rep}/{wanted}"
        )
        if obs_gen >= expected_gen and upd_rep == wanted and ready_rep == wanted:
            return True
        time.sleep(8)
    return False


def _get_llm_urls_from_ids(linked_llm_ids: list) -> list[str]:
    """DB se LLM ids ke corresponding URLs lo."""
    from models.llm_inference_model import LLMInferenceJob
    urls = []
    if not linked_llm_ids:
        return urls
    db = SessionLocal()
    try:
        for lid in linked_llm_ids:
            llm = db.query(LLMInferenceJob).filter(LLMInferenceJob.id == lid).first()
            if llm:
                url = llm.endpoint_url or (f"http://{llm.head_ip}:8000" if llm.head_ip else None)
                if url:
                    if not url.endswith("/v1"):
                        url = f"{url}/v1"
                    urls.append(url)
    finally:
        db.close()
    return urls


# ─────────────────────────────────────────────────────────────────────────────
# Connect Keycloak Activity
# ─────────────────────────────────────────────────────────────────────────────

@activity.defn(name="connect_keycloak_activity")
def connect_keycloak_activity(payload: dict) -> dict:
    import datetime as _dt
    from kubernetes.client.models import (
        V1EnvVar, V1ObjectMeta, V1DeploymentStrategy, V1RollingUpdateDeployment,
        V1Probe, V1HTTPGetAction,
    )

    ow_id                = payload["openwebui_id"]
    kc_url               = payload["kc_url"]
    kc_admin             = payload["kc_admin"]
    kc_pass              = payload["kc_pass"]
    kc_realm             = payload["kc_realm"]
    client_id            = payload["client_id"]
    provider_name        = payload["provider_name"]
    oauth_scopes         = payload["oauth_scopes"]
    redirect_uris        = payload["redirect_uris"]
    service_url          = payload.get("service_url", "")
    admin_email          = payload.get("admin_email", "")
    admin_password       = payload.get("admin_password", "")
    postgresql_deploy_id = payload.get("postgresql_deploy_id")
    k8s_cluster_id       = payload["k8s_cluster_id"]
    dep_name             = payload["dep_name"]
    namespace            = payload["namespace"]
    linked_llm_ids       = payload.get("linked_llm_ids", [])

    try:
        _log_step(ow_id, f"ConnectKeycloak started — realm={kc_realm} client={client_id}")

        # ── Stage 1: Keycloak Admin Token ─────────────────────────────────────
        activity.logger.info("[ConnectKeycloak] Stage 1 — getting Keycloak admin token")
        _log_step(ow_id, "Stage 1: Authenticating with Keycloak admin API ...")
        token = _kc_admin_token(kc_url, kc_admin, kc_pass)
        _log_step(ow_id, "Stage 1: Keycloak admin token obtained ✓")

        # ── Stage 2: Keycloak Client Create / Find ────────────────────────────
        activity.logger.info(f"[ConnectKeycloak] Stage 2 — ensure client '{client_id}' in realm '{kc_realm}'")
        _log_step(ow_id, f"Stage 2: Creating/finding Keycloak client '{client_id}' in realm '{kc_realm}' ...")
        client_uuid, client_secret = _kc_ensure_client(kc_url, token, kc_realm, client_id, redirect_uris)
        _log_step(ow_id, f"Stage 2: Keycloak client ready — uuid={client_uuid[:8]}... ✓")

        openid_url = f"{kc_url}/realms/{kc_realm}/.well-known/openid-configuration"

        # ── Stage 3: OpenWebUI HTTP API — login form disable ──────────────────
        activity.logger.info("[ConnectKeycloak] Stage 3 — disabling OW login form via HTTP API")
        _log_step(ow_id, "Stage 3: Disabling OpenWebUI login form via admin API ...")
        _api_err = "credentials not set"
        if service_url and admin_email and admin_password:
            svc = service_url.rstrip("/")
            _tok = _ow_admin_token(svc, admin_email, admin_password)
            if _tok:
                _api_err = _ow_set_auth_config(svc, _tok, {
                    "enable_login_form":             False,
                    "enable_signup":                 False,
                    "enable_oauth_signup":           False,
                    "oauth_provider_name":           provider_name,
                    "openid_provider_url":           openid_url,
                    "oauth_client_id":               client_id,
                    "oauth_client_secret":           client_secret,
                    "oauth_scopes":                  oauth_scopes,
                    "oauth_merge_accounts_by_email": True,
                })
                if _api_err is None:
                    _log_step(ow_id, "Stage 3: OpenWebUI HTTP API login form disabled ✓")
                else:
                    _log_step(ow_id, f"Stage 3: HTTP API failed ({_api_err[:100]}) — will use PG fallback")
            else:
                _log_step(ow_id, "Stage 3: Admin signin failed — will use PG fallback")
        else:
            _log_step(ow_id, "Stage 3: Credentials not set — skipping HTTP API")

        # ── Stage 4: PostgreSQL direct update ────────────────────────────────
        activity.logger.info("[ConnectKeycloak] Stage 4 — updating OW config via PostgreSQL direct")
        _log_step(ow_id, "Stage 4: Updating OpenWebUI config in PostgreSQL (ENABLE_LOGIN_FORM=false) ...")
        _pg_patch = {
            "ENABLE_LOGIN_FORM":             False,
            "ENABLE_SIGNUP":                 False,
            "ENABLE_OAUTH_SIGNUP":           False,
            "OAUTH_PROVIDER_NAME":           provider_name,
            "OPENID_PROVIDER_URL":           openid_url,
            "OAUTH_CLIENT_ID":               client_id,
            "OAUTH_CLIENT_SECRET":           client_secret,
            "OAUTH_SCOPES":                  oauth_scopes,
            "OAUTH_MERGE_ACCOUNTS_BY_EMAIL": True,
        }
        _pg_err = _pg_set_config(ow_id, postgresql_deploy_id, _pg_patch)
        if _pg_err is None:
            _log_step(ow_id, "Stage 4: PostgreSQL config updated — login form disabled in DB ✓ (takes effect on next page load)")
        else:
            _log_step(ow_id, f"Stage 4: PostgreSQL update failed: {_pg_err[:120]}")

        # ── Stage 5: Load K8s cluster config ─────────────────────────────────
        activity.logger.info(f"[ConnectKeycloak] Stage 5 — loading K8s cluster (id={k8s_cluster_id})")
        _log_step(ow_id, "Stage 5: Loading K8s cluster configuration from DB ...")
        apps_v1, cluster_name = _load_k8s_client(k8s_cluster_id)
        _log_step(ow_id, f"Stage 5: K8s cluster '{cluster_name}' loaded ✓")

        # ── Stage 6: Read deployment ──────────────────────────────────────────
        activity.logger.info(f"[ConnectKeycloak] Stage 6 — reading deployment '{dep_name}'")
        _log_step(ow_id, f"Stage 6: Reading K8s deployment '{dep_name}' (namespace: {namespace}) ...")
        deployment = apps_v1.read_namespaced_deployment(name=dep_name, namespace=namespace)

        target = None
        for c in (deployment.spec.template.spec.containers or []):
            if c.name == "openwebui":
                target = c
                break
        if target is None:
            raise RuntimeError(
                f"Container 'openwebui' not found in deployment '{dep_name}' — "
                f"containers: {[c.name for c in deployment.spec.template.spec.containers]}"
            )
        _log_step(ow_id, f"Stage 6: Deployment found — {len(target.env or [])} env var(s) currently ✓")

        # ── Stage 7: Inject Keycloak env vars ────────────────────────────────
        activity.logger.info("[ConnectKeycloak] Stage 7 — injecting Keycloak env vars")
        _log_step(ow_id, "Stage 7: Injecting Keycloak OAuth env vars into pod spec ...")

        oauth_env_vars = {
            "ENABLE_OAUTH_SIGNUP":           "false",
            "OAUTH_PROVIDER_NAME":           provider_name,
            "OPENID_PROVIDER_URL":           openid_url,
            "OAUTH_CLIENT_ID":               client_id,
            "OAUTH_CLIENT_SECRET":           client_secret,
            "OAUTH_MERGE_ACCOUNTS_BY_EMAIL": "true",
            "ENABLE_LOGIN_FORM":             "false",
            "OAUTH_SCOPES":                  oauth_scopes,
            "DEFAULT_USER_ROLE":             "user",
            "OAUTH_ROLES_CLAIM":             "realm_access.roles",
            "OAUTH_ADMIN_ROLES":             "admin",
        }

        clean_env = [e for e in (target.env or []) if e.name not in _KEYCLOAK_ENV_KEYS]
        for k, v in oauth_env_vars.items():
            clean_env.append(V1EnvVar(name=k, value=str(v)))

        # Preserve linked LLM env vars
        llm_urls = _get_llm_urls_from_ids(linked_llm_ids)
        if llm_urls:
            clean_env = [e for e in clean_env if e.name not in _LLM_ENV_KEYS]
            clean_env.append(V1EnvVar(name="OPENAI_API_BASE_URL",  value=llm_urls[0]))
            clean_env.append(V1EnvVar(name="OPENAI_API_BASE_URLS", value=";".join(llm_urls)))
            clean_env.append(V1EnvVar(name="OPENAI_API_KEY",       value="sk-EMPTY"))
            clean_env.append(V1EnvVar(name="OPENAI_API_KEYS",      value=";".join(["sk-EMPTY"] * len(llm_urls))))
        if not any(e.name == "WEBUI_API_KEY" for e in clean_env):
            clean_env.append(V1EnvVar(name="WEBUI_API_KEY", value=_DAAS_OW_API_KEY))
        target.env = clean_env

        if deployment.spec.template.metadata is None:
            deployment.spec.template.metadata = V1ObjectMeta()
        if deployment.spec.template.metadata.annotations is None:
            deployment.spec.template.metadata.annotations = {}
        deployment.spec.template.metadata.annotations["kubectl.kubernetes.io/restartedAt"] = \
            _dt.datetime.utcnow().isoformat()

        deployment.spec.strategy = V1DeploymentStrategy(
            type="RollingUpdate",
            rolling_update=V1RollingUpdateDeployment(max_unavailable=1, max_surge=0),
        )

        _log_step(ow_id, f"Stage 7: {len(oauth_env_vars)} Keycloak env var(s) injected ✓")

        # ── Stage 8: Apply patch ──────────────────────────────────────────────
        activity.logger.info("[ConnectKeycloak] Stage 8 — applying K8s deployment patch")
        _log_step(ow_id, "Stage 8: Applying K8s deployment patch (pod restart triggered) ...")
        patched      = apps_v1.replace_namespaced_deployment(name=dep_name, namespace=namespace, body=deployment)
        expected_gen = patched.metadata.generation or 1
        _log_step(ow_id, f"Stage 8: Deployment replaced (generation → {expected_gen}) — pod restarting ✓")

        # ── Stage 9: Wait for rollout ─────────────────────────────────────────
        activity.logger.info(f"[ConnectKeycloak] Stage 9 — waiting for rollout gen={expected_gen} (max 300s)")
        _log_step(ow_id, f"Stage 9: Waiting for pod rollout gen={expected_gen} (max 300s) ...")
        rollout_ok = _wait_rollout(apps_v1, dep_name, namespace, expected_gen)
        if rollout_ok:
            _log_step(ow_id, "Stage 9: Pod rollout complete — Keycloak SSO active ✓")
        else:
            _log_step(ow_id, "Stage 9: WARNING — rollout timed out after 300s (pod may still be starting)")

        # ── Stage 10: Save keycloak_config to DB ─────────────────────────────
        activity.logger.info("[ConnectKeycloak] Stage 10 — saving keycloak_config to DB")
        _log_step(ow_id, "Stage 10: Saving Keycloak config to database ...")
        _db_update(ow_id, keycloak_config=json.dumps({
            "keycloak_url":  kc_url,
            "realm":         kc_realm,
            "client_id":     client_id,
            "client_uuid":   client_uuid,
            "client_secret": client_secret,
            "provider_name": provider_name,
            "oauth_scopes":  oauth_scopes,
        }))
        _log_step(ow_id, f"Stage 10: Done ✓ — Keycloak SSO connected (realm={kc_realm} client={client_id})")

        return {
            "status":       "connected",
            "openwebui_id": ow_id,
            "client_id":    client_id,
            "realm":        kc_realm,
            "rollout_ok":   rollout_ok,
            "pg_updated":   _pg_err is None,
        }

    except Exception as e:
        logger.error(f"[ConnectKeycloak] Failed ow_id={ow_id}: {e}", exc_info=True)
        _log_step(ow_id, f"ERROR: {str(e)[:300]}")
        raise


# ─────────────────────────────────────────────────────────────────────────────
# Disconnect Keycloak Activity
# ─────────────────────────────────────────────────────────────────────────────

@activity.defn(name="disconnect_keycloak_activity")
def disconnect_keycloak_activity(payload: dict) -> dict:
    import datetime as _dt
    import httpx as _hx
    from kubernetes.client.models import (
        V1EnvVar, V1ObjectMeta, V1DeploymentStrategy, V1RollingUpdateDeployment,
    )

    ow_id                = payload["openwebui_id"]
    keycloak_config_raw  = payload.get("keycloak_config", "")
    service_url          = payload.get("service_url", "")
    admin_email          = payload.get("admin_email", "")
    admin_password       = payload.get("admin_password", "")
    postgresql_deploy_id = payload.get("postgresql_deploy_id")
    k8s_cluster_id       = payload["k8s_cluster_id"]
    dep_name             = payload["dep_name"]
    namespace            = payload["namespace"]
    linked_llm_ids       = payload.get("linked_llm_ids", [])
    kc_url               = payload.get("kc_url", "")
    kc_admin             = payload.get("kc_admin", "admin")
    kc_pass              = payload.get("kc_pass", "admin")
    kc_realm             = payload.get("kc_realm", "")

    try:
        _log_step(ow_id, "DisconnectKeycloak started")

        # ── Stage 1: Capture Keycloak user emails (before pod restart) ────────
        activity.logger.info("[DisconnectKeycloak] Stage 1 — capturing Keycloak user emails")
        _log_step(ow_id, "Stage 1: Capturing Keycloak user emails (before pod restart) ...")
        _kc_emails: set = set()
        if kc_url and kc_realm:
            try:
                _kt = _kc_admin_token(kc_url, kc_admin, kc_pass)
                _ku_r = _hx.get(
                    f"{kc_url}/admin/realms/{kc_realm}/users",
                    params={"max": 1000},
                    headers={"Authorization": f"Bearer {_kt}"},
                    timeout=15, verify=False,
                )
                if _ku_r.status_code == 200:
                    _kc_emails = {(u.get("email") or "").lower()
                                  for u in _ku_r.json() if u.get("email")}
                _log_step(ow_id, f"Stage 1: {len(_kc_emails)} Keycloak email(s) captured ✓")
            except Exception as _ke:
                _log_step(ow_id, f"Stage 1: Keycloak email capture failed: {str(_ke)[:100]} — user deletion skipped")
        else:
            _log_step(ow_id, "Stage 1: Keycloak URL/realm not set — skipping email capture")

        # ── Stage 2: OpenWebUI HTTP API — re-enable login form ────────────────
        activity.logger.info("[DisconnectKeycloak] Stage 2 — re-enabling login form via HTTP API")
        _log_step(ow_id, "Stage 2: Re-enabling OpenWebUI login form via admin API ...")
        if service_url and admin_email and admin_password:
            svc  = service_url.rstrip("/")
            _tok = _ow_admin_token(svc, admin_email, admin_password)
            if _tok:
                _re_err = _ow_set_auth_config(svc, _tok, {
                    "enable_login_form": True,
                    "enable_signup":     False,
                })
                if _re_err is None:
                    _log_step(ow_id, "Stage 2: Login form re-enabled via HTTP API ✓")
                else:
                    _log_step(ow_id, f"Stage 2: HTTP API failed ({_re_err[:100]}) — PG fallback will handle")
            else:
                _log_step(ow_id, "Stage 2: Admin signin failed — PG fallback will handle")
        else:
            _log_step(ow_id, "Stage 2: Credentials not set — skipping HTTP API")

        # ── Stage 3: PostgreSQL direct — restore login form ───────────────────
        activity.logger.info("[DisconnectKeycloak] Stage 3 — restoring login form in PostgreSQL")
        _log_step(ow_id, "Stage 3: Restoring ENABLE_LOGIN_FORM=true in PostgreSQL config ...")
        _pg_err = _pg_set_config(ow_id, postgresql_deploy_id, {
            "ENABLE_LOGIN_FORM":   True,
            "ENABLE_SIGNUP":       False,
            "ENABLE_OAUTH_SIGNUP": False,
        })
        if _pg_err is None:
            _log_step(ow_id, "Stage 3: PostgreSQL login form restored ✓ (takes effect on next page load)")
        else:
            _log_step(ow_id, f"Stage 3: PostgreSQL restore failed: {_pg_err[:120]}")

        # ── Stage 4: Load K8s cluster config ─────────────────────────────────
        activity.logger.info(f"[DisconnectKeycloak] Stage 4 — loading K8s cluster (id={k8s_cluster_id})")
        _log_step(ow_id, "Stage 4: Loading K8s cluster configuration from DB ...")
        apps_v1, cluster_name = _load_k8s_client(k8s_cluster_id)
        _log_step(ow_id, f"Stage 4: K8s cluster '{cluster_name}' loaded ✓")

        # ── Stage 5: Read deployment ──────────────────────────────────────────
        activity.logger.info(f"[DisconnectKeycloak] Stage 5 — reading deployment '{dep_name}'")
        _log_step(ow_id, f"Stage 5: Reading K8s deployment '{dep_name}' (namespace: {namespace}) ...")
        deployment = apps_v1.read_namespaced_deployment(name=dep_name, namespace=namespace)

        target = None
        for c in (deployment.spec.template.spec.containers or []):
            if c.name == "openwebui":
                target = c
                break
        if target is None:
            raise RuntimeError(f"Container 'openwebui' not found in deployment '{dep_name}'")
        _log_step(ow_id, f"Stage 5: Deployment found ✓")

        # ── Stage 6: Remove Keycloak env vars ────────────────────────────────
        activity.logger.info("[DisconnectKeycloak] Stage 6 — removing Keycloak env vars")
        _log_step(ow_id, "Stage 6: Removing Keycloak OAuth env vars from pod spec ...")

        clean_env = [e for e in (target.env or []) if e.name not in _KEYCLOAK_ENV_KEYS]

        # Preserve linked LLM env vars
        llm_urls = _get_llm_urls_from_ids(linked_llm_ids)
        if llm_urls:
            clean_env = [e for e in clean_env if e.name not in _LLM_ENV_KEYS]
            clean_env.append(V1EnvVar(name="OPENAI_API_BASE_URL",  value=llm_urls[0]))
            clean_env.append(V1EnvVar(name="OPENAI_API_BASE_URLS", value=";".join(llm_urls)))
            clean_env.append(V1EnvVar(name="OPENAI_API_KEY",       value="sk-EMPTY"))
            clean_env.append(V1EnvVar(name="OPENAI_API_KEYS",      value=";".join(["sk-EMPTY"] * len(llm_urls))))
        if not any(e.name == "WEBUI_API_KEY" for e in clean_env):
            clean_env.append(V1EnvVar(name="WEBUI_API_KEY", value=_DAAS_OW_API_KEY))
        target.env = clean_env

        if deployment.spec.template.metadata is None:
            deployment.spec.template.metadata = V1ObjectMeta()
        if deployment.spec.template.metadata.annotations is None:
            deployment.spec.template.metadata.annotations = {}
        deployment.spec.template.metadata.annotations["kubectl.kubernetes.io/restartedAt"] = \
            _dt.datetime.utcnow().isoformat()

        deployment.spec.strategy = V1DeploymentStrategy(
            type="RollingUpdate",
            rolling_update=V1RollingUpdateDeployment(max_unavailable=1, max_surge=0),
        )
        _log_step(ow_id, "Stage 6: Keycloak env vars removed from pod spec ✓")

        # ── Stage 7: Apply patch ──────────────────────────────────────────────
        activity.logger.info("[DisconnectKeycloak] Stage 7 — applying K8s deployment patch")
        _log_step(ow_id, "Stage 7: Applying K8s deployment patch (pod restart triggered) ...")
        patched      = apps_v1.replace_namespaced_deployment(name=dep_name, namespace=namespace, body=deployment)
        expected_gen = patched.metadata.generation or 1
        _log_step(ow_id, f"Stage 7: Deployment replaced (generation → {expected_gen}) — pod restarting ✓")

        # ── Stage 8: Wait for rollout ─────────────────────────────────────────
        activity.logger.info(f"[DisconnectKeycloak] Stage 8 — waiting for rollout gen={expected_gen}")
        _log_step(ow_id, f"Stage 8: Waiting for pod rollout gen={expected_gen} (max 300s) ...")
        rollout_ok = _wait_rollout(apps_v1, dep_name, namespace, expected_gen)
        if rollout_ok:
            _log_step(ow_id, "Stage 8: Pod rollout complete ✓")
        else:
            _log_step(ow_id, "Stage 8: WARNING — rollout timed out after 300s")

        # ── Stage 9: Delete Keycloak users from OpenWebUI ────────────────────
        _deleted = 0
        _admin_lower = admin_email.lower()
        if _kc_emails and service_url and admin_email and admin_password:
            activity.logger.info("[DisconnectKeycloak] Stage 9 — deleting Keycloak users from OpenWebUI")
            _log_step(ow_id, f"Stage 9: Deleting {len(_kc_emails)} Keycloak user(s) from OpenWebUI ...")
            try:
                svc  = service_url.rstrip("/")
                _tok = _ow_admin_token(svc, admin_email, admin_password)
                if _tok:
                    _hdrs = {"Authorization": f"Bearer {_tok}"}
                    _ur = _hx.get(f"{svc}/api/v1/users/", params={"skip": 0, "limit": 10000},
                                  headers=_hdrs, timeout=15)
                    _ow_users = []
                    if _ur.status_code == 200:
                        _raw = _ur.json()
                        _ow_users = _raw.get("users", _raw) if isinstance(_raw, dict) else _raw

                    for _u in _ow_users:
                        if not isinstance(_u, dict):
                            continue
                        _email = (_u.get("email") or "").lower()
                        _uid   = _u.get("id")
                        if not _uid or _email == _admin_lower or _email not in _kc_emails:
                            continue
                        _dr = _hx.delete(f"{svc}/api/v1/users/{_uid}", headers=_hdrs, timeout=10)
                        if _dr.status_code in (200, 204):
                            _deleted += 1
                            _log_step(ow_id, f"Stage 9: Deleted OW user — {_email}")
                        else:
                            _log_step(ow_id, f"Stage 9: Delete failed {_email}: {_dr.status_code}")
                    _log_step(ow_id, f"Stage 9: {_deleted} Keycloak user(s) deleted from OpenWebUI ✓")
                else:
                    _log_step(ow_id, "Stage 9: Admin signin failed — users not deleted")
            except Exception as _ue:
                _log_step(ow_id, f"Stage 9: User deletion error: {str(_ue)[:120]}")
        else:
            _log_step(ow_id, "Stage 9: Skipped — no Keycloak emails captured or credentials not set")

        # ── Stage 10: Clear keycloak_config from DB ───────────────────────────
        activity.logger.info("[DisconnectKeycloak] Stage 10 — clearing keycloak_config from DB")
        _log_step(ow_id, "Stage 10: Clearing Keycloak config from database ...")
        _db_update(ow_id, keycloak_config=None)
        _log_step(ow_id, "Stage 10: Done ✓ — Keycloak SSO disconnected")

        return {
            "status":           "disconnected",
            "openwebui_id":     ow_id,
            "rollout_ok":       rollout_ok,
            "users_deleted":    _deleted,
            "kc_emails_count":  len(_kc_emails),
            "pg_restored":      _pg_err is None,
        }

    except Exception as e:
        logger.error(f"[DisconnectKeycloak] Failed ow_id={ow_id}: {e}", exc_info=True)
        _log_step(ow_id, f"ERROR: {str(e)[:300]}")
        raise

import json
import os
import re
import tempfile
import time

from temporalio import activity

from db_configuration.config import SessionLocal
from models.app_deploy_model import AppDeployment
from models.kubernetes_model import KubernetesCluster

logger = activity.logger
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
# Stage 1 helper — update the config via the OpenWebUI HTTP API
# ─────────────────────────────────────────────────────────────────────────────

def _try_http_sync(service_url: str, urls: list, admin_email: str, admin_password: str,
                   keys: list | None = None) -> str | None:
    """
    Update the config via the OpenWebUI /openai/config/update API.
    Returns None on success, an error string on failure.
    """
    import httpx

    svc = service_url.rstrip("/")
    resolved_keys = keys if (keys and len(keys) == len(urls)) else ["sk-EMPTY"] * len(urls)
    try:
        # get an admin JWT
        token = None
        if admin_email and admin_password:
            sr = httpx.post(
                f"{svc}/api/v1/auths/signin",
                json={"email": admin_email, "password": admin_password},
                timeout=10,
            )
            if sr.status_code == 200:
                token = sr.json().get("token")
            else:
                return f"Admin signin failed ({sr.status_code}): {sr.text[:100]}"

        if not token:
            return "Admin email/password not set — cannot get admin JWT"

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        # OPENAI_API_CONFIGS: one key per URL, value exactly like the working kubectl command
        api_configs = {url: {"enable": True, "prefix_id": None} for url in urls}

        payload = {
            "ENABLE_OPENAI_API":    True,
            "OPENAI_API_BASE_URLS": urls,
            "OPENAI_API_KEYS":      resolved_keys,
            "OPENAI_API_CONFIGS":   api_configs,
        }
        wr = httpx.post(f"{svc}/openai/config/update", headers=headers, json=payload, timeout=10)
        if wr.status_code < 400:
            return None
        return f"POST /openai/config/update → {wr.status_code}: {wr.text[:100]}"

    except Exception as e:
        return f"HTTP API error: {str(e)[:200]}"


# ─────────────────────────────────────────────────────────────────────────────
# Stage 1.5 helper — update the config directly in the PostgreSQL DB
# ─────────────────────────────────────────────────────────────────────────────

def _try_pg_sync(deploy_id: int, postgresql_deploy_id: int | None, urls: list,
                 keys: list | None = None) -> str | None:
    """
    Update OpenWebUI's PostgreSQL DB directly.
    Returns None on success, an error string on failure.
    """
    import json as _j, time as _t

    if not postgresql_deploy_id:
        return "postgresql_deploy_id not set on this deployment"

    try:
        import psycopg2
    except ImportError:
        return "psycopg2 not installed"

    resolved_keys = keys if (keys and len(keys) == len(urls)) else ["sk-EMPTY"] * len(urls)

    db = SessionLocal()
    try:
        pg = db.query(AppDeployment).filter(AppDeployment.id == postgresql_deploy_id).first()
        if not pg:
            return f"PostgreSQL AppDeployment id={postgresql_deploy_id} not found"
        if not pg.external_ip or not pg.node_port:
            return f"PostgreSQL has no external_ip/node_port (external_ip={pg.external_ip} node_port={pg.node_port})"

        _dbname = f"openwebui_{deploy_id}"
        conn = psycopg2.connect(
            host=pg.external_ip,
            port=int(pg.node_port),
            dbname=_dbname,
            user="postgres",
            password="postgres123",
            connect_timeout=10,
        )
        cur = conn.cursor()

        _new_vals = {
            "ENABLE_OPENAI_API":    True,
            "OPENAI_API_BASE_URLS": urls,
            "OPENAI_API_KEYS":      resolved_keys,
            "OPENAI_API_CONFIGS":   {u: {"enable": True, "prefix_id": None} for u in urls},
        }

        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name='config'"
        )
        if not cur.fetchone():
            conn.close()
            return f"'config' table not found in {_dbname}"

        cur.execute("SELECT id, data FROM config ORDER BY id LIMIT 1")
        _row = cur.fetchone()
        if _row:
            try:
                _data = _j.loads(_row[1]) if _row[1] else {}
            except Exception:
                _data = {}
            _data.update(_new_vals)
            cur.execute(
                "UPDATE config SET data = %s, updated_at = %s WHERE id = %s",
                (_j.dumps(_data), int(_t.time() * 1000), _row[0]),
            )
        else:
            _ts = int(_t.time() * 1000)
            cur.execute(
                "INSERT INTO config (data, version, created_at, updated_at) VALUES (%s, 1, %s, %s)",
                (_j.dumps(_new_vals), _ts, _ts),
            )

        conn.commit()
        cur.close()
        conn.close()
        return None

    except Exception as e:
        return f"PostgreSQL connect/update failed: {str(e)[:200]}"
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Connect LLM Activity
# ─────────────────────────────────────────────────────────────────────────────

@activity.defn(name="connect_llm_activity")
def connect_llm_activity(payload: dict) -> dict:
    import yaml
    from kubernetes import client as kc, config as kcfg
    from kubernetes.client.models import V1EnvVar

    deploy_id            = payload["deploy_id"]
    llm_ids              = payload["llm_ids"]
    new_ids              = payload["new_ids"]
    base_urls            = payload["base_urls"]
    api_keys             = payload.get("api_keys") or ["sk-EMPTY"] * len(base_urls)
    if len(api_keys) != len(base_urls):
        api_keys = ["sk-EMPTY"] * len(base_urls)
    k8s_cluster_id       = payload["k8s_cluster_id"]
    dep_name             = payload["dep_name"]
    namespace            = payload["namespace"]
    service_url          = payload.get("service_url", "")
    admin_email          = payload.get("admin_email", "")
    admin_password       = payload.get("admin_password", "")
    postgresql_deploy_id = payload.get("postgresql_deploy_id")

    import json as _json

    rollout_ok  = True
    _k8s_needed = True

    try:
        _log_step(deploy_id, f"ConnectLLM started — adding {len(llm_ids)} LLM(s), total after={len(new_ids)}")

        # ── Stage 1: HTTP API se instant config update ────────────────────────
        activity.logger.info("[ConnectLLM] Stage 1 — trying HTTP API config update")
        _log_step(deploy_id, "Stage 1: Trying OpenWebUI HTTP API (admin JWT + /openai/config/update) ...")

        if service_url and admin_email and admin_password:
            _http_err = _try_http_sync(service_url, base_urls, admin_email, admin_password, keys=api_keys)
            if _http_err is None:
                _log_step(deploy_id,
                    f"Stage 1: HTTP API update SUCCESS ✓ — model visible immediately (no pod restart needed)")
                activity.logger.info("[ConnectLLM] Stage 1 — HTTP API success, skipping K8s restart")
                _k8s_needed = False
            else:
                _log_step(deploy_id,
                    f"Stage 1: HTTP API failed ({_http_err[:120]}) — trying PostgreSQL direct update")
                activity.logger.warning(f"[ConnectLLM] Stage 1 HTTP error: {_http_err}")
        else:
            _log_step(deploy_id,
                "Stage 1: service_url or admin credentials not set — skipping HTTP API")
            activity.logger.info("[ConnectLLM] Stage 1 — skipped (no service_url/credentials)")

        # ── Stage 1.5: PostgreSQL direct update (if HTTP failed) ──────────────
        if _k8s_needed:
            activity.logger.info("[ConnectLLM] Stage 1.5 — trying PostgreSQL direct update")
            _log_step(deploy_id, "Stage 1.5: Trying PostgreSQL direct config update ...")

            _pg_err = _try_pg_sync(deploy_id, postgresql_deploy_id, base_urls, keys=api_keys)
            if _pg_err is None:
                _log_step(deploy_id,
                    "Stage 1.5: PostgreSQL config updated SUCCESS ✓ — "
                    "model visible after pod loads config from DB")
                activity.logger.info("[ConnectLLM] Stage 1.5 — PG sync success, skipping K8s restart")
                _k8s_needed = False
            else:
                _log_step(deploy_id,
                    f"Stage 1.5: PostgreSQL update failed ({_pg_err[:120]}) — falling back to K8s restart")
                activity.logger.warning(f"[ConnectLLM] Stage 1.5 PG error: {_pg_err}")

        # ── Stages 2-6: K8s pod restart (only if HTTP API and PG both failed) ─
        if _k8s_needed:
            # ── Stage 2: Load kubeconfig from DB ─────────────────────────────
            activity.logger.info(f"[ConnectLLM] Stage 2 — loading cluster config (cluster_id={k8s_cluster_id})")
            _log_step(deploy_id, "Stage 2: Loading K8s cluster configuration from DB ...")

            db = SessionLocal()
            try:
                cluster = db.query(KubernetesCluster).filter(
                    KubernetesCluster.id == k8s_cluster_id
                ).first()
                if not cluster or not cluster.kubeconfig:
                    raise RuntimeError(
                        f"K8s cluster id={k8s_cluster_id} not found or kubeconfig missing"
                    )
                kubeconfig_yaml = cluster.kubeconfig
                control_ip      = cluster.control_ip
                cluster_name    = cluster.name or str(k8s_cluster_id)
            finally:
                db.close()

            _log_step(deploy_id, f"Stage 2: Cluster '{cluster_name}' config loaded")

            # ── Stage 3: Kubernetes API client init ──────────────────────────
            activity.logger.info("[ConnectLLM] Stage 3 — initializing Kubernetes API client")
            _log_step(deploy_id, "Stage 3: Initializing Kubernetes API client ...")

            kc_dict = yaml.safe_load(kubeconfig_yaml)
            if control_ip:
                for ce in kc_dict.get("clusters", []):
                    srv = ce.get("cluster", {}).get("server", "")
                    if srv:
                        ce["cluster"]["server"] = re.sub(
                            r"https://[^:/]+", f"https://{control_ip}", srv
                        )

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

            apps_v1 = kc.AppsV1Api()
            _log_step(deploy_id, "Stage 3: Kubernetes API client ready")

            # ── Stage 4: Read current deployment spec ─────────────────────────
            activity.logger.info(
                f"[ConnectLLM] Stage 4 — reading deployment '{dep_name}' ns='{namespace}'"
            )
            _log_step(deploy_id, f"Stage 4: Reading deployment '{dep_name}' (namespace: {namespace}) ...")

            deployment = apps_v1.read_namespaced_deployment(name=dep_name, namespace=namespace)

            target = None
            for c in (deployment.spec.template.spec.containers or []):
                if c.name == "openwebui":
                    target = c
                    break

            if target is None:
                raise RuntimeError(
                    f"Container 'openwebui' not found in deployment '{dep_name}' — "
                    f"containers present: {[c.name for c in deployment.spec.template.spec.containers]}"
                )

            _log_step(
                deploy_id,
                f"Stage 4: Deployment found — {len(deployment.spec.template.spec.containers)} container(s), "
                f"current env count: {len(target.env or [])}"
            )

            # ── Stage 5: Inject env vars + force restart annotation ───────────
            urls_str = ";".join(base_urls)
            keys_str = ";".join(api_keys)
            activity.logger.info(
                f"[ConnectLLM] Stage 5 — patching {len(base_urls)} URL(s): {urls_str}"
            )
            _log_step(
                deploy_id,
                f"Stage 5: Injecting {len(base_urls)} LLM URL(s) → OPENAI_API_BASE_URLS={urls_str} ..."
            )

            import datetime as _dt
            from kubernetes.client.models import (
                V1ObjectMeta, V1DeploymentStrategy, V1RollingUpdateDeployment,
                V1Probe, V1HTTPGetAction,
            )

            _DAAS_OW_API_KEY = "daas-openwebui-api-key"
            llm_keys  = {"OPENAI_API_BASE_URL", "OPENAI_API_BASE_URLS",
                         "OPENAI_API_KEY",       "OPENAI_API_KEYS"}
            ssl_keys  = {"AIOHTTP_CLIENT_SESSION_SSL", "REQUESTS_VERIFY"}
            clean_env = [e for e in (target.env or []) if e.name not in llm_keys | ssl_keys]
            clean_env.append(V1EnvVar(name="OPENAI_API_BASE_URL",  value=base_urls[0]))
            clean_env.append(V1EnvVar(name="OPENAI_API_BASE_URLS", value=urls_str))
            clean_env.append(V1EnvVar(name="OPENAI_API_KEY",       value=api_keys[0]))
            clean_env.append(V1EnvVar(name="OPENAI_API_KEYS",      value=keys_str))
            clean_env.append(V1EnvVar(name="AIOHTTP_CLIENT_SESSION_SSL", value="false"))
            clean_env.append(V1EnvVar(name="REQUESTS_VERIFY",           value="false"))
            if not any(e.name == "WEBUI_API_KEY" for e in clean_env):
                clean_env.append(V1EnvVar(name="WEBUI_API_KEY", value=_DAAS_OW_API_KEY))
            target.env = clean_env

            if target.readiness_probe is None:
                target.readiness_probe = V1Probe(
                    http_get=V1HTTPGetAction(path="/health", port=8080),
                    initial_delay_seconds=10,
                    period_seconds=5,
                    failure_threshold=12,
                )
            if target.liveness_probe is None:
                target.liveness_probe = V1Probe(
                    http_get=V1HTTPGetAction(path="/health", port=8080),
                    initial_delay_seconds=30,
                    period_seconds=10,
                    failure_threshold=3,
                )

            deployment.spec.strategy = V1DeploymentStrategy(
                type="RollingUpdate",
                rolling_update=V1RollingUpdateDeployment(max_unavailable=1, max_surge=0),
            )

            if deployment.spec.template.metadata is None:
                deployment.spec.template.metadata = V1ObjectMeta()
            if deployment.spec.template.metadata.annotations is None:
                deployment.spec.template.metadata.annotations = {}
            deployment.spec.template.metadata.annotations["kubectl.kubernetes.io/restartedAt"] = \
                _dt.datetime.utcnow().isoformat()

            patched      = apps_v1.replace_namespaced_deployment(name=dep_name, namespace=namespace, body=deployment)
            expected_gen = patched.metadata.generation or 1

            _log_step(
                deploy_id,
                f"Stage 5: Deployment replaced (generation → {expected_gen}) — pod restart triggered"
            )

            # ── Stage 6: Wait for rollout ─────────────────────────────────────
            activity.logger.info(f"[ConnectLLM] Stage 6 — waiting for rollout gen={expected_gen} (max 300s)")
            _log_step(deploy_id, f"Stage 6: Waiting for pod rollout gen={expected_gen} (max 300s) ...")

            rollout_ok = False
            deadline   = time.time() + 300
            while time.time() < deadline:
                dep       = apps_v1.read_namespaced_deployment(name=dep_name, namespace=namespace)
                obs_gen   = dep.status.observed_generation or 0
                upd_rep   = dep.status.updated_replicas    or 0
                ready_rep = dep.status.ready_replicas      or 0
                wanted    = dep.spec.replicas              or 1
                activity.logger.info(
                    f"[ConnectLLM] rollout: obs_gen={obs_gen}/{expected_gen} "
                    f"updated={upd_rep} ready={ready_rep}/{wanted}"
                )
                if obs_gen >= expected_gen and upd_rep == wanted and ready_rep == wanted:
                    rollout_ok = True
                    break
                time.sleep(8)

            if rollout_ok:
                _log_step(deploy_id, "Stage 6: Pod rollout complete — new pod is running ✓")
            else:
                _log_step(deploy_id, "Stage 6: WARNING — rollout timed out after 300s")

            # ── Stage 6.5: sync the OpenWebUI config via the admin JWT (after restart) ─
            activity.logger.info("[ConnectLLM] Stage 6.5 — syncing OpenWebUI config via admin API")
            _log_step(deploy_id, "Stage 6.5: Syncing OpenWebUI config via admin API after pod restart (3 retries) ...")

            if service_url and admin_email and admin_password:
                time.sleep(15)  # pod fully init hone do

                _synced = False
                for _att in range(1, 4):
                    try:
                        _http_err2 = _try_http_sync(service_url, base_urls, admin_email, admin_password)
                        if _http_err2 is None:
                            _log_step(deploy_id,
                                f"Stage 6.5: Config synced via admin API (attempt {_att}) — model visible ✓")
                            _synced = True
                            break
                        raise ValueError(_http_err2)
                    except Exception as _e65:
                        activity.logger.warning(f"[ConnectLLM] Stage 6.5 attempt {_att} failed: {_e65}")
                        if _att < 3:
                            time.sleep(10)

                if not _synced:
                    _log_step(deploy_id,
                        "Stage 6.5: All attempts failed — env vars active, model visible after browser refresh")
            else:
                _log_step(deploy_id, "Stage 6.5: service_url/credentials not set — skipping")

        # ── Stage 7: Persist linked_llm_ids to DB ─────────────────────────────
        activity.logger.info(f"[ConnectLLM] Stage 7 — saving linked_llm_ids={new_ids} (added={llm_ids})")
        _log_step(deploy_id, f"Stage 7: Saving linked_llm_ids={new_ids} to database ...")

        db = SessionLocal()
        try:
            ow = db.query(AppDeployment).filter(AppDeployment.id == deploy_id).first()
            if ow:
                ow.linked_llm_ids = _json.dumps(new_ids)
                ow.status         = "deployed"
                ow.error_message  = None
                db.commit()
        finally:
            db.close()

        method = "http_api" if not _k8s_needed else "k8s_restart"
        _log_step(
            deploy_id,
            f"Stage 7: Done — added LLM id(s) {llm_ids}, total {len(new_ids)} LLM(s) active. Method: {method}"
        )
        activity.logger.info(
            f"[ConnectLLM] All stages complete. deploy_id={deploy_id} "
            f"added={llm_ids} total={new_ids} method={method} rollout_ok={rollout_ok}"
        )

        return {
            "status":     "connected",
            "deploy_id":  deploy_id,
            "llm_ids":    llm_ids,
            "all_ids":    new_ids,
            "base_urls":  base_urls,
            "rollout_ok": rollout_ok,
            "method":     method,
        }

    except Exception as e:
        logger.error(f"[ConnectLLM] Failed deploy_id={deploy_id}: {e}", exc_info=True)
        rollback_ids = payload.get("current_ids", [])
        _db_update(
            deploy_id,
            status         = "llm_connect_failed",
            error_message  = str(e)[:500],
            linked_llm_ids = _json.dumps(rollback_ids),
        )
        _log_step(deploy_id, f"ERROR: {str(e)[:300]}")
        raise


# ─────────────────────────────────────────────────────────────────────────────
# Disconnect LLM Activity
# ─────────────────────────────────────────────────────────────────────────────

@activity.defn(name="disconnect_llm_activity")
def disconnect_llm_activity(payload: dict) -> dict:
    import yaml
    from kubernetes import client as kc, config as kcfg
    from kubernetes.client.models import (
        V1EnvVar, V1ObjectMeta, V1DeploymentStrategy, V1RollingUpdateDeployment,
        V1Probe, V1HTTPGetAction,
    )

    deploy_id            = payload["deploy_id"]
    remaining_ids        = payload["remaining_ids"]
    remaining_urls       = payload["remaining_urls"]
    disconnected_id      = payload.get("disconnected_id")
    k8s_cluster_id       = payload["k8s_cluster_id"]
    dep_name             = payload["dep_name"]
    namespace            = payload["namespace"]
    service_url          = payload.get("service_url", "")
    admin_email          = payload.get("admin_email", "")
    admin_password       = payload.get("admin_password", "")
    postgresql_deploy_id = payload.get("postgresql_deploy_id")

    import json as _json

    rollout_ok  = True
    _k8s_needed = True

    try:
        _dis_label = f"id={disconnected_id}" if disconnected_id else "all"
        _log_step(deploy_id,
            f"DisconnectLLM started — disconnecting {_dis_label}, "
            f"{len(remaining_ids)} LLM(s) remaining after")

        # ── Stage 1: HTTP API se instant config update ────────────────────────
        activity.logger.info("[DisconnectLLM] Stage 1 — trying HTTP API config update")
        _log_step(deploy_id, "Stage 1: Trying OpenWebUI HTTP API (admin JWT + /openai/config/update) ...")

        if service_url and admin_email and admin_password:
            _http_err = _try_http_sync(service_url, remaining_urls, admin_email, admin_password)
            if _http_err is None:
                _log_step(deploy_id,
                    "Stage 1: HTTP API update SUCCESS ✓ — model removed immediately (no pod restart needed)")
                activity.logger.info("[DisconnectLLM] Stage 1 — HTTP API success, skipping K8s restart")
                _k8s_needed = False
            else:
                _log_step(deploy_id,
                    f"Stage 1: HTTP API failed ({_http_err[:120]}) — trying PostgreSQL direct update")
                activity.logger.warning(f"[DisconnectLLM] Stage 1 HTTP error: {_http_err}")
        else:
            _log_step(deploy_id,
                "Stage 1: service_url or admin credentials not set — skipping HTTP API")

        # ── Stage 1.5: PostgreSQL direct update (if HTTP failed) ──────────────
        if _k8s_needed:
            activity.logger.info("[DisconnectLLM] Stage 1.5 — trying PostgreSQL direct update")
            _log_step(deploy_id, "Stage 1.5: Trying PostgreSQL direct config update ...")

            _pg_err = _try_pg_sync(deploy_id, postgresql_deploy_id, remaining_urls)
            if _pg_err is None:
                _log_step(deploy_id,
                    "Stage 1.5: PostgreSQL config updated SUCCESS ✓ — "
                    "changes visible after pod reloads config from DB")
                activity.logger.info("[DisconnectLLM] Stage 1.5 — PG sync success, skipping K8s restart")
                _k8s_needed = False
            else:
                _log_step(deploy_id,
                    f"Stage 1.5: PostgreSQL update failed ({_pg_err[:120]}) — falling back to K8s restart")
                activity.logger.warning(f"[DisconnectLLM] Stage 1.5 PG error: {_pg_err}")

        # ── Stages 2-6: K8s pod restart (only if HTTP API and PG both failed) ─
        if _k8s_needed:
            # ── Stage 2: Load kubeconfig ──────────────────────────────────────
            activity.logger.info(f"[DisconnectLLM] Stage 2 — loading cluster config (cluster_id={k8s_cluster_id})")
            _log_step(deploy_id, "Stage 2: Loading K8s cluster configuration ...")

            db = SessionLocal()
            try:
                cluster = db.query(KubernetesCluster).filter(
                    KubernetesCluster.id == k8s_cluster_id
                ).first()
                if not cluster or not cluster.kubeconfig:
                    raise RuntimeError(f"K8s cluster id={k8s_cluster_id} not found or kubeconfig missing")
                kubeconfig_yaml = cluster.kubeconfig
                control_ip      = cluster.control_ip
                cluster_name    = cluster.name or str(k8s_cluster_id)
            finally:
                db.close()

            _log_step(deploy_id, f"Stage 2: Cluster '{cluster_name}' config loaded")

            # ── Stage 3: K8s client init ──────────────────────────────────────
            activity.logger.info("[DisconnectLLM] Stage 3 — initializing Kubernetes API client")
            _log_step(deploy_id, "Stage 3: Initializing Kubernetes API client ...")

            kc_dict = yaml.safe_load(kubeconfig_yaml)
            if control_ip:
                for ce in kc_dict.get("clusters", []):
                    srv = ce.get("cluster", {}).get("server", "")
                    if srv:
                        ce["cluster"]["server"] = re.sub(
                            r"https://[^:/]+", f"https://{control_ip}", srv
                        )

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

            apps_v1 = kc.AppsV1Api()
            _log_step(deploy_id, "Stage 3: Kubernetes API client ready")

            # ── Stage 4: Read deployment ──────────────────────────────────────
            activity.logger.info(f"[DisconnectLLM] Stage 4 — reading deployment '{dep_name}'")
            _log_step(deploy_id, f"Stage 4: Reading deployment '{dep_name}' (namespace: {namespace}) ...")

            deployment = apps_v1.read_namespaced_deployment(name=dep_name, namespace=namespace)
            target = None
            for c in (deployment.spec.template.spec.containers or []):
                if c.name == "openwebui":
                    target = c
                    break

            if target is None:
                raise RuntimeError(f"Container 'openwebui' not found in deployment '{dep_name}'")

            _log_step(deploy_id, f"Stage 4: Deployment found — {len(deployment.spec.template.spec.containers)} container(s)")

            # ── Stage 5: Update env vars ──────────────────────────────────────
            _DAAS_OW_API_KEY = "daas-openwebui-api-key"
            llm_keys  = {"OPENAI_API_BASE_URL", "OPENAI_API_BASE_URLS", "OPENAI_API_KEY", "OPENAI_API_KEYS"}
            clean_env = [e for e in (target.env or []) if e.name not in llm_keys]

            if remaining_urls:
                urls_str = ";".join(remaining_urls)
                keys_str = ";".join(["sk-EMPTY"] * len(remaining_urls))
                clean_env.append(V1EnvVar(name="OPENAI_API_BASE_URL",  value=remaining_urls[0]))
                clean_env.append(V1EnvVar(name="OPENAI_API_BASE_URLS", value=urls_str))
                clean_env.append(V1EnvVar(name="OPENAI_API_KEY",       value="sk-EMPTY"))
                clean_env.append(V1EnvVar(name="OPENAI_API_KEYS",      value=keys_str))
                action = f"updated to {len(remaining_urls)} remaining URL(s): {urls_str}"
            else:
                action = "all LLM env vars removed (no remaining LLMs)"

            if not any(e.name == "WEBUI_API_KEY" for e in clean_env):
                clean_env.append(V1EnvVar(name="WEBUI_API_KEY", value=_DAAS_OW_API_KEY))

            target.env = clean_env

            activity.logger.info(f"[DisconnectLLM] Stage 5 — {action}")
            _log_step(deploy_id, f"Stage 5: Env vars {action} ...")

            import datetime as _dt
            if target.readiness_probe is None:
                target.readiness_probe = V1Probe(
                    http_get=V1HTTPGetAction(path="/health", port=8080),
                    initial_delay_seconds=10, period_seconds=5, failure_threshold=12,
                )
            if target.liveness_probe is None:
                target.liveness_probe = V1Probe(
                    http_get=V1HTTPGetAction(path="/health", port=8080),
                    initial_delay_seconds=30, period_seconds=10, failure_threshold=3,
                )
            deployment.spec.strategy = V1DeploymentStrategy(
                type="RollingUpdate",
                rolling_update=V1RollingUpdateDeployment(max_unavailable=1, max_surge=0),
            )
            if deployment.spec.template.metadata is None:
                deployment.spec.template.metadata = V1ObjectMeta()
            if deployment.spec.template.metadata.annotations is None:
                deployment.spec.template.metadata.annotations = {}
            deployment.spec.template.metadata.annotations["kubectl.kubernetes.io/restartedAt"] = \
                _dt.datetime.utcnow().isoformat()

            patched      = apps_v1.replace_namespaced_deployment(name=dep_name, namespace=namespace, body=deployment)
            expected_gen = patched.metadata.generation or 1
            _log_step(deploy_id, f"Stage 5: Deployment replaced (generation → {expected_gen}) — pod restart triggered")

            # ── Stage 6: Wait for rollout ─────────────────────────────────────
            activity.logger.info(f"[DisconnectLLM] Stage 6 — waiting for rollout gen={expected_gen} (max 300s)")
            _log_step(deploy_id, f"Stage 6: Waiting for pod rollout (max 300s) ...")

            rollout_ok = False
            deadline   = time.time() + 300
            while time.time() < deadline:
                dep       = apps_v1.read_namespaced_deployment(name=dep_name, namespace=namespace)
                obs_gen   = dep.status.observed_generation or 0
                upd_rep   = dep.status.updated_replicas    or 0
                ready_rep = dep.status.ready_replicas      or 0
                wanted    = dep.spec.replicas              or 1
                if obs_gen >= expected_gen and upd_rep == wanted and ready_rep == wanted:
                    rollout_ok = True
                    break
                time.sleep(8)

            if rollout_ok:
                _log_step(deploy_id, "Stage 6: Pod rollout complete ✓")
            else:
                _log_step(deploy_id, "Stage 6: WARNING — rollout timed out after 300s")

            # ── Stage 6.5: Admin API sync after pod restart ────────────────────
            activity.logger.info("[DisconnectLLM] Stage 6.5 — syncing OpenWebUI config via admin API")
            _log_step(deploy_id, "Stage 6.5: Syncing OpenWebUI config via admin API after pod restart (3 retries) ...")

            if service_url and admin_email and admin_password:
                time.sleep(15)

                _synced = False
                for _att in range(1, 4):
                    try:
                        _http_err2 = _try_http_sync(service_url, remaining_urls, admin_email, admin_password)
                        if _http_err2 is None:
                            _msg = "cleared" if not remaining_urls else f"{len(remaining_urls)} LLM(s) remaining"
                            _log_step(deploy_id,
                                f"Stage 6.5: Config synced via admin API (attempt {_att}) — {_msg} ✓")
                            _synced = True
                            break
                        raise ValueError(_http_err2)
                    except Exception as _e65:
                        activity.logger.warning(f"[DisconnectLLM] Stage 6.5 attempt {_att} failed: {_e65}")
                        if _att < 3:
                            time.sleep(10)

                if not _synced:
                    _log_step(deploy_id, "Stage 6.5: All attempts failed — env vars active")
            else:
                _log_step(deploy_id, "Stage 6.5: service_url/credentials not set — skipping")

        # ── Stage 7: DB update ────────────────────────────────────────────────
        activity.logger.info(f"[DisconnectLLM] Stage 7 — saving remaining_ids={remaining_ids}")
        _log_step(deploy_id, f"Stage 7: Saving remaining_llm_ids={remaining_ids} to database ...")

        db = SessionLocal()
        try:
            ow = db.query(AppDeployment).filter(AppDeployment.id == deploy_id).first()
            if ow:
                ow.linked_llm_ids = _json.dumps(remaining_ids)
                ow.status         = "deployed"
                ow.error_message  = None
                db.commit()
        finally:
            db.close()

        method = "http_api" if not _k8s_needed else "k8s_restart"
        _log_step(deploy_id,
            f"Stage 7: Done — disconnected {_dis_label}, {len(remaining_ids)} LLM(s) remaining. Method: {method}")
        activity.logger.info(
            f"[DisconnectLLM] Complete — deploy_id={deploy_id} remaining={remaining_ids} "
            f"method={method} rollout_ok={rollout_ok}"
        )

        return {
            "status":          "disconnected",
            "deploy_id":       deploy_id,
            "disconnected_id": disconnected_id,
            "remaining_ids":   remaining_ids,
            "rollout_ok":      rollout_ok,
            "method":          method,
        }

    except Exception as e:
        logger.error(f"[DisconnectLLM] Failed deploy_id={deploy_id}: {e}", exc_info=True)
        _db_update(deploy_id, status="deployed", error_message=str(e)[:500])
        _log_step(deploy_id, f"ERROR: {str(e)[:300]}")
        raise

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


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers (same pattern as activities_app_deploy.py)
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
# Activity
# ─────────────────────────────────────────────────────────────────────────────

@activity.defn(name="connect_llm_activity")
def connect_llm_activity(payload: dict) -> dict:
    import yaml
    from kubernetes import client as kc, config as kcfg
    from kubernetes.client.models import V1EnvVar

    deploy_id      = payload["deploy_id"]
    llm_ids        = payload["llm_ids"]          # newly added IDs
    new_ids        = payload["new_ids"]          # full list of llm_ids after connect
    base_urls      = payload["base_urls"]        # full list of endpoint URLs
    k8s_cluster_id = payload["k8s_cluster_id"]
    dep_name       = payload["dep_name"]
    namespace      = payload["namespace"]

    try:
        # ── Stage 1: Load kubeconfig from DB ─────────────────────────────────
        activity.logger.info(f"[ConnectLLM] Stage 1 — loading cluster config (cluster_id={k8s_cluster_id})")
        _log_step(deploy_id, "Stage 1: Loading K8s cluster configuration from DB ...")

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

        _log_step(deploy_id, f"Stage 1: Cluster '{cluster_name}' config loaded")

        # ── Stage 2: Kubernetes API client init ──────────────────────────────
        activity.logger.info("[ConnectLLM] Stage 2 — initializing Kubernetes API client")
        _log_step(deploy_id, "Stage 2: Initializing Kubernetes API client ...")

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
        _log_step(deploy_id, "Stage 2: Kubernetes API client ready")

        # ── Stage 3: Read current deployment spec ────────────────────────────
        activity.logger.info(
            f"[ConnectLLM] Stage 3 — reading deployment '{dep_name}' ns='{namespace}'"
        )
        _log_step(deploy_id, f"Stage 3: Reading deployment '{dep_name}' (namespace: {namespace}) ...")

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
            f"Stage 3: Deployment found — {len(deployment.spec.template.spec.containers)} container(s), "
            f"current env count: {len(target.env or [])}"
        )

        # ── Stage 4: Inject env vars + force restart annotation ──────────────
        urls_str = ";".join(base_urls)
        keys_str = ";".join(["none"] * len(base_urls))
        activity.logger.info(
            f"[ConnectLLM] Stage 4 — patching {len(base_urls)} URL(s): {urls_str}"
        )
        _log_step(
            deploy_id,
            f"Stage 4: Injecting {len(base_urls)} LLM URL(s) → OPENAI_API_BASE_URLS={urls_str} ..."
        )

        import datetime as _dt
        from kubernetes.client.models import (
            V1ObjectMeta, V1DeploymentStrategy, V1RollingUpdateDeployment,
            V1Probe, V1HTTPGetAction,
        )

        _DAAS_OW_API_KEY = "daas-openwebui-api-key"
        llm_keys  = {"OPENAI_API_BASE_URL", "OPENAI_API_BASE_URLS",
                     "OPENAI_API_KEY",       "OPENAI_API_KEYS"}
        clean_env = [e for e in (target.env or []) if e.name not in llm_keys]
        clean_env.append(V1EnvVar(name="OPENAI_API_BASE_URL",  value=base_urls[0]))
        clean_env.append(V1EnvVar(name="OPENAI_API_BASE_URLS", value=urls_str))
        clean_env.append(V1EnvVar(name="OPENAI_API_KEY",       value="none"))
        clean_env.append(V1EnvVar(name="OPENAI_API_KEYS",      value=keys_str))
        # Inject API key so disconnect can call OpenWebUI REST API directly — no pod restart needed
        if not any(e.name == "WEBUI_API_KEY" for e in clean_env):
            clean_env.append(V1EnvVar(name="WEBUI_API_KEY", value=_DAAS_OW_API_KEY))
        target.env = clean_env

        # Ensure readinessProbe is set — without it K8s marks the pod Ready the moment
        # the container process starts, before Uvicorn is actually serving. This causes
        # the old pod to be terminated while the new one is still warming up → brief 503.
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

        # Ensure zero-downtime rolling update strategy (maxUnavailable:0, maxSurge:1).
        # Old pod stays alive until new pod passes readinessProbe → no gap in service.
        deployment.spec.strategy = V1DeploymentStrategy(
            type="RollingUpdate",
            rolling_update=V1RollingUpdateDeployment(max_unavailable=0, max_surge=1),
        )

        # Restart annotation forces a new generation even when env var values are unchanged
        # (re-connecting the same LLM). Without this, K8s skips the rollout entirely.
        if deployment.spec.template.metadata is None:
            deployment.spec.template.metadata = V1ObjectMeta()
        if deployment.spec.template.metadata.annotations is None:
            deployment.spec.template.metadata.annotations = {}
        deployment.spec.template.metadata.annotations["kubectl.kubernetes.io/restartedAt"] = \
            _dt.datetime.utcnow().isoformat()

        # replace (PUT) karo patch (PATCH/strategic-merge) ki jagah —
        # strategic merge patch env vars omit hone par preserve karta hai delete nahi karta
        patched = apps_v1.replace_namespaced_deployment(name=dep_name, namespace=namespace, body=deployment)
        expected_gen = patched.metadata.generation or 1

        _log_step(
            deploy_id,
            f"Stage 4: Deployment replaced (generation → {expected_gen}) — pod restart triggered"
        )

        # ── Stage 5: Wait for rollout ─────────────────────────────────────────
        activity.logger.info(f"[ConnectLLM] Stage 5 — waiting for rollout gen={expected_gen} (max 300s)")
        _log_step(deploy_id, f"Stage 5: Waiting for pod rollout gen={expected_gen} (max 300s) ...")

        rollout_ok = False
        deadline   = time.time() + 300
        while time.time() < deadline:
            dep = apps_v1.read_namespaced_deployment(name=dep_name, namespace=namespace)
            obs_gen     = dep.status.observed_generation or 0
            upd_rep     = dep.status.updated_replicas    or 0
            ready_rep   = dep.status.ready_replicas      or 0
            wanted_rep  = dep.spec.replicas              or 1
            activity.logger.info(
                f"[ConnectLLM] rollout check: obs_gen={obs_gen}/{expected_gen} "
                f"updated={upd_rep} ready={ready_rep}/{wanted_rep}"
            )
            if obs_gen >= expected_gen and upd_rep == wanted_rep and ready_rep == wanted_rep:
                rollout_ok = True
                break
            time.sleep(8)

        if rollout_ok:
            activity.logger.info("[ConnectLLM] Stage 5 — rollout complete")
            _log_step(deploy_id, "Stage 5: Pod rollout complete — new pod running with LLM env vars")
        else:
            activity.logger.warning("[ConnectLLM] Stage 5 — rollout timeout (300s)")
            _log_step(
                deploy_id,
                "Stage 5: WARNING — rollout timed out after 300s. Check pod events for errors."
            )

        # ── Stage 6: Persist linked_llm_id to DB ─────────────────────────────
        activity.logger.info(f"[ConnectLLM] Stage 6 — saving linked_llm_ids={new_ids} (added={llm_ids})")
        _log_step(deploy_id, f"Stage 6: Saving linked_llm_ids={new_ids} to database ...")

        import json as _json
        db = SessionLocal()
        try:
            ow = db.query(AppDeployment).filter(AppDeployment.id == deploy_id).first()
            if ow:
                ow.linked_llm_ids = _json.dumps(new_ids)
                ow.status         = "deployed"
                db.commit()
        finally:
            db.close()

        _log_step(
            deploy_id,
            f"Stage 6: Done — added LLM id(s) {llm_ids}. "
            f"Total {len(new_ids)} LLM(s) active on this OpenWebUI."
        )
        activity.logger.info(
            f"[ConnectLLM] All 6 stages complete. deploy_id={deploy_id} "
            f"added={llm_ids} total={new_ids} rollout_ok={rollout_ok}"
        )

        return {
            "status":     "connected",
            "deploy_id":  deploy_id,
            "llm_ids":    llm_ids,
            "all_ids":    new_ids,
            "base_urls":  base_urls,
            "rollout_ok": rollout_ok,
        }

    except Exception as e:
        logger.error(f"[ConnectLLM] Failed deploy_id={deploy_id}: {e}", exc_info=True)
        import json as _j
        # Rollback linked_llm_ids to the state before this connect attempt
        rollback_ids = payload.get("current_ids", [])
        _db_update(
            deploy_id,
            status        = "llm_connect_failed",
            error_message = str(e)[:500],
            linked_llm_ids= _j.dumps(rollback_ids),
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

    deploy_id       = payload["deploy_id"]
    remaining_ids   = payload["remaining_ids"]
    remaining_urls  = payload["remaining_urls"]
    disconnected_id = payload.get("disconnected_id")
    k8s_cluster_id  = payload["k8s_cluster_id"]
    dep_name        = payload["dep_name"]
    namespace       = payload["namespace"]

    try:
        # ── Stage 1: Load kubeconfig ──────────────────────────────────────────
        activity.logger.info(f"[DisconnectLLM] Stage 1 — loading cluster config (cluster_id={k8s_cluster_id})")
        _log_step(deploy_id, "Stage 1: Loading K8s cluster configuration ...")

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

        _log_step(deploy_id, f"Stage 1: Cluster '{cluster_name}' config loaded")

        # ── Stage 2: K8s client init ──────────────────────────────────────────
        activity.logger.info("[DisconnectLLM] Stage 2 — initializing Kubernetes API client")
        _log_step(deploy_id, "Stage 2: Initializing Kubernetes API client ...")

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
        _log_step(deploy_id, "Stage 2: Kubernetes API client ready")

        # ── Stage 3: Read deployment ──────────────────────────────────────────
        activity.logger.info(f"[DisconnectLLM] Stage 3 — reading deployment '{dep_name}'")
        _log_step(deploy_id, f"Stage 3: Reading deployment '{dep_name}' (namespace: {namespace}) ...")

        deployment = apps_v1.read_namespaced_deployment(name=dep_name, namespace=namespace)
        target = None
        for c in (deployment.spec.template.spec.containers or []):
            if c.name == "openwebui":
                target = c
                break

        if target is None:
            raise RuntimeError(f"Container 'openwebui' not found in deployment '{dep_name}'")

        _log_step(deploy_id, f"Stage 3: Deployment found — {len(deployment.spec.template.spec.containers)} container(s)")

        # ── Stage 4: Update env vars ──────────────────────────────────────────
        _DAAS_OW_API_KEY = "daas-openwebui-api-key"
        llm_keys  = {"OPENAI_API_BASE_URL", "OPENAI_API_BASE_URLS", "OPENAI_API_KEY", "OPENAI_API_KEYS"}
        clean_env = [e for e in (target.env or []) if e.name not in llm_keys]

        if remaining_urls:
            urls_str = ";".join(remaining_urls)
            keys_str = ";".join(["none"] * len(remaining_urls))
            clean_env.append(V1EnvVar(name="OPENAI_API_BASE_URL",  value=remaining_urls[0]))
            clean_env.append(V1EnvVar(name="OPENAI_API_BASE_URLS", value=urls_str))
            clean_env.append(V1EnvVar(name="OPENAI_API_KEY",       value="none"))
            clean_env.append(V1EnvVar(name="OPENAI_API_KEYS",      value=keys_str))
            action = f"updated to {len(remaining_urls)} remaining URL(s)"
        else:
            action = "all LLM env vars removed (no remaining LLMs)"

        if not any(e.name == "WEBUI_API_KEY" for e in clean_env):
            clean_env.append(V1EnvVar(name="WEBUI_API_KEY", value=_DAAS_OW_API_KEY))

        target.env = clean_env

        activity.logger.info(f"[DisconnectLLM] Stage 4 — {action}")
        _log_step(deploy_id, f"Stage 4: Env vars {action} ...")

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
            rolling_update=V1RollingUpdateDeployment(max_unavailable=0, max_surge=1),
        )
        if deployment.spec.template.metadata is None:
            deployment.spec.template.metadata = V1ObjectMeta()
        if deployment.spec.template.metadata.annotations is None:
            deployment.spec.template.metadata.annotations = {}
        deployment.spec.template.metadata.annotations["kubectl.kubernetes.io/restartedAt"] = \
            _dt.datetime.utcnow().isoformat()

        patched       = apps_v1.replace_namespaced_deployment(name=dep_name, namespace=namespace, body=deployment)
        expected_gen  = patched.metadata.generation or 1
        _log_step(deploy_id, f"Stage 4: Deployment replaced (generation → {expected_gen}) — pod restart triggered")

        # ── Stage 5: Wait for rollout ─────────────────────────────────────────
        activity.logger.info(f"[DisconnectLLM] Stage 5 — waiting for rollout gen={expected_gen} (max 300s)")
        _log_step(deploy_id, f"Stage 5: Waiting for pod rollout (max 300s) ...")

        rollout_ok = False
        deadline   = time.time() + 300
        while time.time() < deadline:
            dep        = apps_v1.read_namespaced_deployment(name=dep_name, namespace=namespace)
            obs_gen    = dep.status.observed_generation or 0
            upd_rep    = dep.status.updated_replicas    or 0
            ready_rep  = dep.status.ready_replicas      or 0
            wanted_rep = dep.spec.replicas              or 1
            if obs_gen >= expected_gen and upd_rep == wanted_rep and ready_rep == wanted_rep:
                rollout_ok = True
                break
            time.sleep(8)

        if rollout_ok:
            _log_step(deploy_id, "Stage 5: Pod rollout complete")
        else:
            _log_step(deploy_id, "Stage 5: WARNING — rollout timed out after 300s")

        # ── Stage 6: DB update ────────────────────────────────────────────────
        activity.logger.info(f"[DisconnectLLM] Stage 6 — saving remaining_ids={remaining_ids}")
        _log_step(deploy_id, f"Stage 6: Saving remaining_llm_ids={remaining_ids} to database ...")

        import json as _json
        db = SessionLocal()
        try:
            ow = db.query(AppDeployment).filter(AppDeployment.id == deploy_id).first()
            if ow:
                ow.linked_llm_ids = _json.dumps(remaining_ids)
                ow.status         = "deployed"
                db.commit()
        finally:
            db.close()

        _log_step(deploy_id, f"Stage 6: Done — disconnected id={disconnected_id}, {len(remaining_ids)} LLM(s) remaining")
        activity.logger.info(f"[DisconnectLLM] Complete — deploy_id={deploy_id} remaining={remaining_ids} rollout_ok={rollout_ok}")

        return {
            "status":         "disconnected",
            "deploy_id":      deploy_id,
            "disconnected_id": disconnected_id,
            "remaining_ids":  remaining_ids,
            "rollout_ok":     rollout_ok,
        }

    except Exception as e:
        logger.error(f"[DisconnectLLM] Failed deploy_id={deploy_id}: {e}", exc_info=True)
        _db_update(deploy_id, status="deployed", error_message=str(e)[:500])
        _log_step(deploy_id, f"ERROR: {str(e)[:300]}")
        raise

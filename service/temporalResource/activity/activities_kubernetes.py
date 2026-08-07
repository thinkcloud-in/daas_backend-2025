import datetime
import logging

import requests
import urllib3
from temporalio import activity
from sqlalchemy.orm import Session

from db_configuration.config import SessionLocal
from models.kubernetes_model import KubernetesCluster

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)


@activity.defn(name="k8s-test-connection")
def test_k8s_connection_activity(payload: dict) -> dict:
    """
    Kubernetes cluster ka connection test karo:
    Priority: kubeconfig > auth_token > username/password
    /healthz aur /version hit karo.
    cluster_id optional hai — agar diya toh DB status update hoga,
    agar None toh sirf test karke result return karo (pre-save test).
    """
    cluster_id = payload.get("cluster_id")   # None = pre-save test
    control_ip = payload["control_ip"]
    port       = payload.get("port", 6443)
    username   = payload.get("username")
    password   = payload.get("password")
    auth_token = payload.get("auth_token")
    kubeconfig = payload.get("kubeconfig")

    base_url = f"https://{control_ip}:{port}"
    headers  = {}
    auth     = None

    # kubeconfig se server URL aur token extract karo
    if kubeconfig:
        try:
            import yaml
            kc = yaml.safe_load(kubeconfig)
            clusters_cfg = kc.get("clusters", [])
            if clusters_cfg:
                server = clusters_cfg[0].get("cluster", {}).get("server", "")
                if server:
                    base_url = server.rstrip("/")
            users_cfg = kc.get("users", [])
            if users_cfg:
                user_data = users_cfg[0].get("user", {})
                if "token" in user_data and user_data["token"]:
                    auth_token = user_data["token"]
            logger.info(f"[K8s] kubeconfig parsed: server={base_url}")
        except Exception as e:
            logger.warning(f"[K8s] kubeconfig parse failed: {e}, falling back to IP/port")

    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"
    elif username and password:
        auth = (username, password)

    activity.heartbeat(f"Testing connection to {base_url} ...")

    def _update_db(status: str):
        """Agar cluster DB me saved hai toh status update karo."""
        if cluster_id is None:
            return
        db: Session = SessionLocal()
        try:
            c = db.query(KubernetesCluster).filter(KubernetesCluster.id == cluster_id).first()
            if c:
                c.status      = status
                c.last_tested = datetime.datetime.utcnow()
                db.commit()
        except Exception as e:
            logger.warning(f"[K8s] DB update failed: {e}")
        finally:
            db.close()

    try:
        resp = requests.get(
            f"{base_url}/healthz",
            headers=headers,
            auth=auth,
            verify=False,
            timeout=15,
        )

        if resp.status_code == 200 and resp.text.strip() == "ok":
            version_info = {}
            try:
                ver = requests.get(
                    f"{base_url}/version",
                    headers=headers,
                    auth=auth,
                    verify=False,
                    timeout=10,
                )
                if ver.status_code == 200:
                    version_info = ver.json()
            except Exception:
                pass

            node_count = None
            try:
                nodes_resp = requests.get(
                    f"{base_url}/api/v1/nodes",
                    headers=headers,
                    auth=auth,
                    verify=False,
                    timeout=10,
                )
                if nodes_resp.status_code == 200:
                    node_count = len(nodes_resp.json().get("items", []))
            except Exception:
                pass

            _update_db("connected")
            logger.info(f"[K8s] Connection OK → {base_url} (cluster_id={cluster_id})")
            return {
                "status":     "connected",
                "message":    "Kubernetes cluster reachable",
                "server":     base_url,
                "version":    version_info,
                "node_count": node_count,
            }

        else:
            error_msg = f"/healthz returned HTTP {resp.status_code}: {resp.text[:300]}"
            _update_db("failed")
            logger.warning(f"[K8s] healthz failed (cluster_id={cluster_id}): {error_msg}")
            return {"status": "failed", "error": error_msg}

    except requests.exceptions.ConnectTimeout:
        err = f"Connection timed out to {base_url}"
    except requests.exceptions.ConnectionError as e:
        err = f"Connection refused/unreachable: {str(e)[:300]}"
    except requests.exceptions.Timeout:
        err = "Request timed out after 15s"
    except Exception as e:
        err = str(e)[:300]

    _update_db("failed")
    logger.warning(f"[K8s] Connection failed (cluster_id={cluster_id}): {err}")
    return {"status": "failed", "error": err}

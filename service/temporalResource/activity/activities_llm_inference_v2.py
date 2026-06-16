import os
import time
import ipaddress
import logging
import requests

from temporalio import activity
from sqlalchemy.orm import Session

from db_configuration.config import SessionLocal
from models.models import Cluster, Machine
from models.IPs_model import IPEntry, IPSModel
from models.llm_inference_v2_model import LLMInferenceJob
from service import proxmoxService
from service.clusterService import get_api_token, getting_Proxmox_host
from utils.ssh_client import run_commands, reboot_and_wait

import dotenv
dotenv.load_dotenv()

logger = logging.getLogger(__name__)

_SSH_USER = os.getenv("LLM_VM_SSH_USER", "vllm")
_SSH_PASS  = os.getenv("LLM_VM_SSH_PASS", "Teamw0rk@1")


# ── helpers ───────────────────────────────────────────────────────────────────

def _netmask_to_cidr(netmask: str) -> int:
    return ipaddress.IPv4Network(f"0.0.0.0/{netmask}").prefixlen


def _wait_for_task(host: str, headers: dict, node: str, upid: str, timeout: int = 300):
    import time
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = requests.get(
            f"{host}/api2/json/nodes/{node}/tasks/{upid}/status",
            headers=headers, verify=False, timeout=10
        )
        if resp.ok:
            data = resp.json().get("data", {})
            if data.get("status") == "stopped":
                if data.get("exitstatus", "OK") != "OK":
                    raise RuntimeError(f"Proxmox task {upid} failed: {data.get('exitstatus')}")
                return
        time.sleep(5)
    raise RuntimeError(f"Proxmox task {upid} timed out after {timeout}s")


# ── Activities ────────────────────────────────────────────────────────────────

@activity.defn
async def clone_and_configure_vm_activity(payload: dict) -> dict:
    """
    For one node:
      1. Clone template VM onto the target node
      2. Attach GPU, set CPU/RAM
      3. Apply cloud-init (ip/cidr/gateway/dns/hostname/user/pass)
      4. Boot the VM
      5. Mark IPEntry as used
    Returns: vmid, node, ip_address
    """
    db: Session = SessionLocal()
    try:
        cluster_id  = payload["cluster_id"]
        node        = payload["node"]
        gpus        = payload["gpus"]          # list of GPU mapping names
        reserved_ip = payload["reserved_ip"]
        ip_pool_id  = payload["ip_pool_id"]
        template    = str(payload["template"])
        datastore    = payload.get("storage", os.getenv("PROXMOX_STORAGE", "local-lvm"))
        pool_name    = payload["pool_name"]
        ssh_user     = payload.get("ssh_user", _SSH_USER)
        ssh_pass     = payload.get("ssh_pass", _SSH_PASS)

        # ── Resolve cluster ───────────────────────────────────────────────
        cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster_data:
            raise RuntimeError(f"Cluster {cluster_id} not found")

        api_token    = get_api_token(db, cluster_data.name)
        headers      = {"Authorization": f"PVEAPIToken={api_token}"}
        PROXMOX_HOST = getting_Proxmox_host(cluster_data)
        if not PROXMOX_HOST:
            raise RuntimeError("No reachable Proxmox host")

        # ── Generate unique VM name ───────────────────────────────────────
        all_vms        = proxmoxService.get_all_cluster_vms(db, cluster_data)
        existing_names = [vm["name"] for vm in all_vms if "name" in vm and vm["name"]]
        db_names       = [m.name for m in db.query(Machine).all()]
        all_names      = set(existing_names) | set(db_names)
        name_template  = f"{pool_name}-{{n:fixed=3}}"
        vm_name        = proxmoxService.generate_machine_name(name_template, list(all_names), 1)[0]

        # ── Resolve IP details from pool ──────────────────────────────────
        ip_entry = db.query(IPEntry).filter(IPEntry.ip == reserved_ip).first()
        if not ip_entry:
            raise RuntimeError(f"IPEntry not found for {reserved_ip}")

        ip_pool = db.query(IPSModel).filter(IPSModel.id == ip_entry.pool_id).first()
        if not ip_pool:
            raise RuntimeError("IP pool not found")

        cidr         = _netmask_to_cidr(ip_pool.Subnet)
        ip_with_cidr = f"{reserved_ip}/{cidr}"
        gateway      = ip_pool.Gateway
        dns          = ip_pool.DNS[0] if ip_pool.DNS else "8.8.8.8"

        # ── Find template VM node ─────────────────────────────────────────
        template_node = None
        for vm in all_vms:
            if str(vm.get("vmid")) == template:
                if int(vm.get("template", 0)) == 1:
                    template_node = vm.get("node")
                    break

        if not template_node:
            all_vmids = [str(vm.get("vmid")) for vm in all_vms if vm.get("vmid")]
            raise RuntimeError(f"Template VM {template} not found. Available vmids: {all_vmids}")

        # ── Get free VMID ─────────────────────────────────────────────────
        resp = requests.get(
            f"{PROXMOX_HOST}/api2/json/cluster/nextid",
            headers=headers, verify=False, timeout=10
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"Failed to get next VMID: {resp.text}")
        vmid = int(resp.json()["data"])

        # ── Clone from template ───────────────────────────────────────────
        clone_data = {"newid": vmid, "name": vm_name, "full": 1}
        if node != template_node:
            clone_data["target"]  = node
            clone_data["storage"] = datastore
        resp = requests.post(
            f"{PROXMOX_HOST}/api2/json/nodes/{template_node}/qemu/{template}/clone",
            headers=headers,
            data=clone_data,
            verify=False, timeout=30
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"Clone failed: {resp.text}")
        upid = resp.json()["data"]
        _wait_for_task(PROXMOX_HOST, headers, template_node, upid, timeout=4800)

        # ── Attach GPU + set CPU/RAM + cloud-init in one PUT ──────────────
        config_url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/config"
        resp = requests.put(
            config_url, headers=headers,
            data={
                **{f"hostpci{i}": f"mapping={g}" if ":" not in g else f"{g},pcie=1,x-vga=0"
                   for i, g in enumerate(gpus)},
                "ipconfig0":    f"ip={ip_with_cidr},gw={gateway}",
                "nameserver":   dns,
                "searchdomain": vm_name,
                "ciuser":       ssh_user,
                "cipassword":   ssh_pass,
            },
            verify=False, timeout=30
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"VM config failed: {resp.text}")

        # ── Boot VM ───────────────────────────────────────────────────────
        resp = requests.post(
            f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/status/start",
            headers=headers, verify=False, timeout=30
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"VM start failed: {resp.text}")
        upid = resp.json()["data"]
        _wait_for_task(PROXMOX_HOST, headers, node, upid, timeout=120)

        # ── Mark IP as used ───────────────────────────────────────────────
        ip_entry.status = "used"
        ip_entry.vm_id  = str(vmid)
        db.commit()

        logger.info(f"VM {vmid} ({vm_name}) cloned on {node} — IP: {ip_with_cidr}, GPUs: {gpus}")
        return {"vmid": vmid, "node": node, "ip_address": reserved_ip}

    finally:
        db.close()


@activity.defn
async def launch_vllm_from_template_activity(payload: dict) -> dict:

    try:
        import time
        ip       = payload["ip_address"]
        ssh_user = payload.get("ssh_user", _SSH_USER)
        ssh_pass = payload.get("ssh_pass", _SSH_PASS)
        tp_size  = payload.get("tensor_parallel_size", 1)
        pp_size  = payload.get("pipeline_parallel_size", 1)

        vllm_cmd = (
            "source /etc/environment; "
            "export VLLM_USE_V1=1; "
            "nohup /root/vllm-ray-env/bin/python3 -m vllm.entrypoints.openai.api_server "
            f"  --model $LLM_MODEL_NAME "
            "  --served-model-name $LLM_MODEL_NAME "
            "  --download-dir $LLM_MODEL_PATH "
            "  --distributed-executor-backend ray "
            f" --tensor-parallel-size {tp_size} "
            f" --pipeline-parallel-size {pp_size} "
            "  --max-model-len 32768 "
            "  --gpu-memory-utilization 0.90 "
            "  --enable-chunked-prefill "
            "  --trust-remote-code "
            "  --host 0.0.0.0 --port 8000 "
            "  --enable-auto-tool-choice "
            "  --tool-call-parser openai "
            "  > /root/vllm_server.log 2>&1 &"
        )

        health_poll = (
            "for i in $(seq 1 90); do "
            "  curl -sf http://localhost:8000/health && echo 'vllm ready' && break; "
            "  echo \"Waiting for vllm... $i/90\"; sleep 10; "
            "done"
        )

        run_commands(ip, ssh_user, ssh_pass, [vllm_cmd, health_poll], timeout=900)

        endpoint = f"http://{ip}:8000/v1"
        logger.info(f"vLLM started on {ip} — endpoint: {endpoint}")
        return {"endpoint_url": endpoint}

    except Exception as e:
        raise RuntimeError(str(e))


@activity.defn
async def restore_llm_services_activity(payload: dict) -> dict:
    """
    Called after VM start/restart.
    1. Waits for ray-head/ray-worker systemd service to become active (Restart=always handles it).
    2. Waits for Ray cluster to be healthy (ray status).
    3. Kills any stale vLLM process, then relaunches vLLM from /etc/environment config.
    """
    ip       = payload["ip_address"]
    ssh_user = payload.get("ssh_user", _SSH_USER)
    ssh_pass = payload.get("ssh_pass", _SSH_PASS)
    role     = payload.get("role", "head")   # "head" | "worker"
    tp_size  = payload.get("tensor_parallel_size", 1)
    pp_size  = payload.get("pipeline_parallel_size", 1)
    venv_bin = "/root/vllm-ray-env/bin"
    service  = "ray-head" if role == "head" else "ray-worker"

    # ── Step 1: Wait for systemd Ray service to be active (max 3 min) ─────────
    wait_ray_service = (
        f"for i in $(seq 1 36); do "
        f"  systemctl is-active {service}.service && echo '{service} active' && break; "
        f"  echo \"Waiting for {service}... $i/36\"; sleep 5; "
        f"done; "
        f"systemctl is-active {service}.service"
    )
    run_commands(ip, ssh_user, ssh_pass, [wait_ray_service], timeout=200)
    logger.info(f"[{ip}] {service}.service is active")

    # ── Step 2: Wait for Ray cluster to be healthy (head node only) ───────────
    if role == "head":
        wait_ray_cluster = (
            f"for i in $(seq 1 30); do "
            f"  {venv_bin}/ray status 2>/dev/null && echo 'ray cluster ready' && break; "
            f"  echo \"Waiting for ray cluster... $i/30\"; sleep 5; "
            f"done; "
            f"{venv_bin}/ray status"
        )
        run_commands(ip, ssh_user, ssh_pass, [wait_ray_cluster], timeout=180)
        logger.info(f"[{ip}] Ray cluster is healthy")

        # ── Step 3: Kill stale vLLM + relaunch ───────────────────────────────
        vllm_cmd = (
            "source /etc/environment; "
            "export VLLM_USE_V1=1; "
            # Kill any existing vLLM process first
            "pkill -f 'vllm.entrypoints.openai.api_server' 2>/dev/null || true; "
            "sleep 3; "
            f"nohup {venv_bin}/python3 -m vllm.entrypoints.openai.api_server "
            f"  --model $LLM_MODEL_NAME "
            "  --served-model-name $LLM_MODEL_NAME "
            "  --download-dir $LLM_MODEL_PATH "
            "  --distributed-executor-backend ray "
            f"  --tensor-parallel-size {tp_size} "
            f"  --pipeline-parallel-size {pp_size} "
            "  --max-model-len 32768 "
            "  --gpu-memory-utilization 0.90 "
            "  --enable-chunked-prefill "
            "  --trust-remote-code "
            "  --host 0.0.0.0 --port 8000 "
            "  --enable-auto-tool-choice "
            "  --tool-call-parser openai "
            "  > /root/vllm_server.log 2>&1 &"
        )
        health_poll = (
            "for i in $(seq 1 90); do "
            "  curl -sf http://localhost:8000/health && echo 'vllm ready' && break; "
            "  echo \"Waiting for vllm... $i/90\"; sleep 10; "
            "done; "
            "curl -sf http://localhost:8000/health"
        )
        run_commands(ip, ssh_user, ssh_pass, [vllm_cmd, health_poll], timeout=960)
        logger.info(f"[{ip}] vLLM relaunched and healthy")

    return {"ip_address": ip, "role": role, "step": "services_restored"}


_ACTION_PATH = {
    "start":    "start",
    "stop":     "stop",
    "shutdown": "shutdown",
    "restart":  "reboot",
}
_TARGET_STATE = {
    "start":    "running",
    "stop":     "stopped",
    "shutdown": "stopped",
    "restart":  "running",
}


@activity.defn
async def vm_power_action_activity(payload: dict) -> dict:
    """
    Performs start / stop / shutdown / restart on a single VM via Proxmox API,
    then polls until the VM reaches the expected state.
    """
    cluster_id = payload["cluster_id"]
    vmid       = payload["vmid"]
    node       = payload["node"]
    action     = payload["action"]

    db: Session = SessionLocal()
    try:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster:
            raise RuntimeError(f"Cluster {cluster_id} not found")

        api_token    = get_api_token(db, cluster.name)
        headers      = {"Authorization": f"PVEAPIToken={api_token}"}
        PROXMOX_HOST = getting_Proxmox_host(cluster)

        # Trigger the action
        action_url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/status/{_ACTION_PATH[action]}"
        resp = requests.post(action_url, headers=headers, verify=False, timeout=30)
        if resp.status_code >= 400:
            raise RuntimeError(f"VM {vmid} {action} failed: {resp.text}")

        # Poll until target state is reached (max 5 min)
        target   = _TARGET_STATE[action]
        deadline = time.time() + 300
        while time.time() < deadline:
            st = requests.get(
                f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/status/current",
                headers=headers, verify=False, timeout=10
            )
            if st.ok and st.json().get("data", {}).get("status") == target:
                logger.info(f"VM {vmid} reached '{target}' after {action}")
                return {"vmid": vmid, "node": node, "action": action, "vm_status": target}
            time.sleep(5)

        raise RuntimeError(f"VM {vmid} did not reach '{target}' within 5 minutes after {action}")
    finally:
        db.close()


@activity.defn
async def update_llm_inference_job_activity(payload: dict) -> dict:
    """Update LLMInferenceJob record fields."""
    db: Session = SessionLocal()
    try:
        job_id = payload["job_id"]
        record = db.query(LLMInferenceJob).filter(LLMInferenceJob.id == job_id).first()
        if not record:
            raise RuntimeError(f"LLMInferenceJob {job_id} not found")

        for field in ("status", "vmids", "ip_addresses", "head_ip", "endpoint_url", "workflow_id"):
            if field in payload:
                setattr(record, field, payload[field])

        db.commit()
        return {"ok": True}
    finally:
        db.close()

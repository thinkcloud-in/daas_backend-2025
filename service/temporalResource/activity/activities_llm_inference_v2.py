import os
import re
import time
import ipaddress
import logging
import requests
import paramiko
from urllib.parse import quote

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

_SSH_USER = os.getenv("LLM_VM_SSH_USER", "root")
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


def _wait_for_ssh_with_key(host: str, username: str, pkey, retries: int = 40, interval: int = 15):
    """SSH with RSA key auth — works even when PasswordAuthentication is disabled."""
    for attempt in range(1, retries + 1):
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(host, username=username, pkey=pkey, timeout=10, look_for_keys=False)
            client.close()
            return
        except Exception as exc:
            logger.debug(f"SSH key attempt {attempt}/{retries} to {host}: {exc}")
            if attempt < retries:
                time.sleep(interval)
    raise RuntimeError(f"Could not SSH (key auth) into {host} after {retries} attempts")


def _run_commands_with_key(host: str, username: str, pkey, commands: list):
    """Run shell commands via key-based SSH session."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=username, pkey=pkey, timeout=30, look_for_keys=False)
    try:
        for cmd in commands:
            _, stdout, stderr = client.exec_command(cmd)
            exit_code = stdout.channel.recv_exit_status()
            if exit_code != 0:
                err = stderr.read().decode(errors="replace").strip()
                logger.warning(f"Command '{cmd}' exited {exit_code}: {err}")
    finally:
        client.close()


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

        # ── Fetch PCI hardware mappings (PCI addr → mapping name) ─────────
        # Avoids guessing the mapping name from the bus number.
        pci_to_mapping: dict = {}
        try:
            map_resp = requests.get(
                f"{PROXMOX_HOST}/api2/json/cluster/mapping/pci",
                headers=headers, verify=False, timeout=10
            )
            if map_resp.ok:
                for m in map_resp.json().get("data", []):
                    for map_str in m.get("map", []):
                        parts = dict(p.split("=", 1) for p in map_str.split(";") if "=" in p)
                        path = parts.get("path", "")
                        if path:
                            pci_to_mapping[path] = m["id"]
            logger.info(f"[cluster {cluster_id}] PCI mappings: {pci_to_mapping}")
        except Exception as exc:
            logger.warning(f"[cluster {cluster_id}] PCI mapping fetch failed (name-derived fallback active): {exc}")

        # ── Generate unique VM name ───────────────────────────────────────
        all_vms        = proxmoxService.get_all_cluster_vms(db, cluster_data)
        existing_names = [vm["name"] for vm in all_vms if "name" in vm and vm["name"]]
        db_names       = [m.name for m in db.query(Machine).all()]
        all_names      = set(existing_names) | set(db_names)
        name_template  = f"{pool_name}-{{n:fixed=3}}"
        vm_name        = proxmoxService.generate_machine_name(name_template, list(all_names), 1)[0]
        # Proxmox requires DNS-valid hostnames: lowercase, alphanumeric + hyphens only
        vm_name = re.sub(r'[^a-zA-Z0-9-]', '-', vm_name)
        vm_name = re.sub(r'-+', '-', vm_name).strip('-').lower()
        if vm_name and not vm_name[0].isalpha():
            vm_name = 'vm-' + vm_name
        vm_name = vm_name[:63]

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

        # ── Strip hostpci from template before clone ──────────────────────
        # If the template has raw PCI devices configured, Proxmox will try
        # to copy them during clone — API tokens with privilege_separation=1
        # cannot do that. Remove them first; we add the correct mapped
        # devices after clone.
        try:
            tpl_cfg_resp = requests.get(
                f"{PROXMOX_HOST}/api2/json/nodes/{template_node}/qemu/{template}/config",
                headers=headers, verify=False, timeout=10
            )
            if tpl_cfg_resp.ok:
                tpl_cfg = tpl_cfg_resp.json().get("data", {})
                hostpci_keys = [k for k in tpl_cfg if k.startswith("hostpci")]
                if hostpci_keys:
                    del_resp = requests.put(
                        f"{PROXMOX_HOST}/api2/json/nodes/{template_node}/qemu/{template}/config",
                        headers=headers,
                        data={"delete": ",".join(hostpci_keys)},
                        verify=False, timeout=10
                    )
                    logger.info(
                        f"Template {template} hostpci cleanup "
                        f"({'ok' if del_resp.ok else del_resp.text}): removed {hostpci_keys}"
                    )
        except Exception as exc:
            logger.warning(f"Template hostpci cleanup failed (proceeding anyway): {exc}")

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

        def _resolve_hostpci(g: str) -> str:
            if ":" not in g:
                # Already a mapping name e.g. "gpu-41"
                return f"mapping={g},pcie=1"
            # PCI address: look up actual mapping name from fetched cluster mappings
            mapping_name = pci_to_mapping.get(g)
            if mapping_name:
                return f"mapping={mapping_name},pcie=1"
            # Fallback: derive from bus segment (assumes naming convention gpu-{bus})
            bus_match = re.match(r'^[0-9a-fA-F]{4}:([0-9a-fA-F]+):', g)
            if bus_match:
                return f"mapping=gpu-{bus_match.group(1)},pcie=1"
            return f"mapping={g},pcie=1"

        hostpci_data = {f"hostpci{i}": _resolve_hostpci(g) for i, g in enumerate(gpus)}
        logger.info(f"[{vmid}] Resolved hostpci: {hostpci_data}")

        # ── Generate temp RSA key for initial key-based SSH ───────────────
        # sshkeys cloud-init param adds this key to ciuser's authorized_keys.
        # Allows SSH even when PasswordAuthentication is disabled in template.
        temp_rsa_key = paramiko.RSAKey.generate(2048)
        sshkeys_param = quote(
            f"{temp_rsa_key.get_name()} {temp_rsa_key.get_base64()} llm-temp\n",
            safe=""
        )

        # ── Attach GPU + set CPU/RAM + cloud-init in one PUT ──────────────
        # machine=q35 is required for pcie=1 PCI passthrough
        config_url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/config"
        resp = requests.put(
            config_url, headers=headers,
            data={
                **hostpci_data,
                "machine":      "q35",
                "sshkeys":      sshkeys_param,
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
        # Proxmox may return a UPID when cloud-init image needs regeneration.
        # Not waiting for it causes "VM is locked (cloudinit)" on the start call.
        config_upid = resp.json().get("data")
        if config_upid:
            logger.info(f"[{vmid}] Waiting for config task {config_upid}...")
            _wait_for_task(PROXMOX_HOST, headers, node, config_upid, timeout=60)

        # ── Boot VM ───────────────────────────────────────────────────────
        # Retry start up to 5 times with increasing delay to ride out any
        # transient "VM is locked" states (cloud-init regen, storage settle).
        start_url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/status/start"
        for attempt in range(1, 6):
            resp = requests.post(start_url, headers=headers, verify=False, timeout=30)
            if resp.status_code < 400:
                break
            err_text = resp.text
            if attempt == 5:
                raise RuntimeError(f"VM start failed after {attempt} attempts: {err_text}")
            logger.warning(f"[{vmid}] Start attempt {attempt}/5 failed ({resp.status_code}): {err_text} — retrying in {attempt * 3}s")
            time.sleep(attempt * 3)
        upid = resp.json().get("data")
        if upid:
            _wait_for_task(PROXMOX_HOST, headers, node, upid, timeout=120)
        else:
            # Some Proxmox versions return null for synchronous start; poll until running
            logger.info(f"[{vmid}] Start returned no UPID — polling VM status...")
            for _ in range(24):
                time.sleep(5)
                s = requests.get(
                    f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/status/current",
                    headers=headers, verify=False, timeout=10
                )
                if s.ok and s.json().get("data", {}).get("status") == "running":
                    break
            else:
                raise RuntimeError(f"VM {vmid} did not reach running state after start")

        # ── Wait for VM SSH (key auth) then enable PasswordAuthentication ──
        # Use key-based SSH first — works even if PasswordAuthentication is
        # disabled in the template (sshkeys cloud-init param adds our key).
        logger.info(f"[{vmid}] VM started — waiting for SSH (key auth) at {reserved_ip} (up to 10 min)...")
        _wait_for_ssh_with_key(reserved_ip, ssh_user, temp_rsa_key)
        logger.info(f"[{vmid}] SSH up — enabling PasswordAuthentication...")
        _run_commands_with_key(reserved_ip, ssh_user, temp_rsa_key, [
            "sudo sed -i 's/^#*PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config",
            "sudo find /etc/ssh/sshd_config.d/ -name '*.conf' "
            "  -exec sed -i 's/^PasswordAuthentication no/PasswordAuthentication yes/' {} \\; 2>/dev/null || true",
            # Restart sshd in background after a delay — avoids killing this connection
            # (systemd KillMode=control-group would terminate active sessions on restart)
            "sudo sh -c 'sleep 2 && systemctl restart sshd' &",
        ])
        # Wait for background sshd restart to complete
        time.sleep(5)
        logger.info(f"[{vmid}] PasswordAuthentication enabled at {reserved_ip}")

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

        vllm_launch = (
            # Load env vars written during install_ray_vllm_activity
            "set -a; source /etc/environment; set +a; "
            # Fallback: if LLM_MODEL_NAME empty, try template's LLM_NAME
            "[ -n \"$LLM_MODEL_NAME\" ] || LLM_MODEL_NAME=\"$LLM_NAME\"; "
            # Fallback: if LLM_MODEL_PATH empty, try template's LLM_PATH
            "[ -n \"$LLM_MODEL_PATH\" ] || LLM_MODEL_PATH=\"${LLM_PATH:-/vllm_data/hf_cache}\"; "
            # No model configured → skip gracefully (exit 0 so Temporal doesn't retry)
            "if [ -z \"$LLM_MODEL_NAME\" ]; then "
            "  echo 'VLLM_SKIP: no model name in LLM_MODEL_NAME or LLM_NAME — skipping vLLM launch'; "
            "  exit 0; "
            "fi; "
            # Kill any existing vLLM process
            "pkill -f 'vllm.entrypoints.openai.api_server' 2>/dev/null || true; "
            "sleep 2; "
            # Launch vLLM in background
            f"nohup /root/vllm-ray-env/bin/python3 -m vllm.entrypoints.openai.api_server "
            f"  --model \"$LLM_MODEL_NAME\" "
            f"  --served-model-name \"$LLM_MODEL_NAME\" "
            f"  --download-dir \"$LLM_MODEL_PATH\" "
            f"  --distributed-executor-backend ray "
            f"  --tensor-parallel-size {tp_size} "
            f"  --pipeline-parallel-size {pp_size} "
            f"  --max-model-len 32768 "
            f"  --gpu-memory-utilization 0.90 "
            f"  --enable-chunked-prefill "
            f"  --trust-remote-code "
            f"  --host 0.0.0.0 --port 8000 "
            f"  > /root/vllm_server.log 2>&1 & "
            # Brief wait then verify process actually started
            "sleep 8; "
            "pgrep -f 'vllm.entrypoints.openai.api_server' > /dev/null || "
            "  { echo 'ERROR: vLLM process failed to start'; cat /root/vllm_server.log; exit 1; }"
        )

        health_poll = (
            "for i in $(seq 1 90); do "
            "  curl -sf http://localhost:8000/health && echo 'vllm ready' && exit 0; "
            "  echo \"Waiting for vllm... $i/90\"; sleep 10; "
            "done; "
            "echo 'ERROR: vLLM did not become healthy in 15 min'; "
            "tail -50 /root/vllm_server.log; "
            "exit 1"
        )

        launch_results = run_commands(ip, ssh_user, ssh_pass, [vllm_launch], timeout=60)
        launch_stdout = launch_results[0]["stdout"] if launch_results else ""

        if "VLLM_SKIP" in launch_stdout:
            logger.info(f"[{ip}] vLLM launch skipped — no model configured in /etc/environment")
            return {"endpoint_url": None}

        run_commands(ip, ssh_user, ssh_pass, [health_poll], timeout=960)

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
        vllm_launch = (
            "set -a; source /etc/environment; set +a; "
            "[ -n \"$LLM_MODEL_NAME\" ] || "
            "  { echo 'ERROR: LLM_MODEL_NAME not set in /etc/environment'; exit 1; }; "
            "pkill -f 'vllm.entrypoints.openai.api_server' 2>/dev/null || true; "
            "sleep 3; "
            f"nohup {venv_bin}/python3 -m vllm.entrypoints.openai.api_server "
            f"  --model \"$LLM_MODEL_NAME\" "
            f"  --served-model-name \"$LLM_MODEL_NAME\" "
            f"  --download-dir \"$LLM_MODEL_PATH\" "
            f"  --distributed-executor-backend ray "
            f"  --tensor-parallel-size {tp_size} "
            f"  --pipeline-parallel-size {pp_size} "
            f"  --max-model-len 32768 "
            f"  --gpu-memory-utilization 0.90 "
            f"  --enable-chunked-prefill "
            f"  --trust-remote-code "
            f"  --host 0.0.0.0 --port 8000 "
            f"  > /root/vllm_server.log 2>&1 & "
            "sleep 8; "
            "pgrep -f 'vllm.entrypoints.openai.api_server' > /dev/null || "
            "  { echo 'ERROR: vLLM process failed to start'; cat /root/vllm_server.log; exit 1; }"
        )
        health_poll = (
            "for i in $(seq 1 90); do "
            "  curl -sf http://localhost:8000/health && echo 'vllm ready' && exit 0; "
            "  echo \"Waiting for vllm... $i/90\"; sleep 10; "
            "done; "
            "echo 'ERROR: vLLM did not become healthy in 15 min'; "
            "tail -50 /root/vllm_server.log; "
            "exit 1"
        )
        run_commands(ip, ssh_user, ssh_pass, [vllm_launch, health_poll], timeout=960)
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


@activity.defn
async def delete_llm_pool_activity(payload: dict) -> dict:
    """
    Delete all VMs for an LLM inference job:
      1. Remove HA resources / groups
      2. Strip GPU config
      3. Stop each VM, wait, then permanently delete it
      4. Release IPs back to unused
      5. Remove the DB job record
    """
    import time as _time
    db: Session = SessionLocal()
    try:
        job_id       = payload["job_id"]
        vmids        = payload.get("vmids") or []
        nodes        = payload.get("nodes") or []
        cluster_id   = payload["cluster_id"]
        ip_addresses = payload.get("ip_addresses") or []

        from models.llm_inference_v2_model import LLMInferenceJob
        record = db.query(LLMInferenceJob).filter(LLMInferenceJob.id == job_id).first()
        if not record:
            logger.warning(f"[delete] Job {job_id} not found — may already be deleted")
            return {"ok": True, "skipped": True}

        # ── Mark as deleting ─────────────────────────────────────────────
        record.status = "deleting"
        db.commit()

        # ── Delete VMs from Proxmox ───────────────────────────────────────
        if vmids:
            cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
            if cluster_data:
                from service.clusterService import get_api_token, getting_Proxmox_host
                api_token    = get_api_token(db, cluster_data.name)
                headers      = {"Authorization": f"PVEAPIToken={api_token}"}
                PROXMOX_HOST = getting_Proxmox_host(cluster_data)

                for i, vmid in enumerate(vmids):
                    node = nodes[i]["node"] if i < len(nodes) else None
                    if not node:
                        continue
                    logger.info(f"[delete] Removing VM {vmid} from node {node}")

                    # Remove HA resources
                    requests.delete(
                        f"{PROXMOX_HOST}/api2/json/cluster/ha/resources/vm%3A{vmid}",
                        headers=headers, verify=False, timeout=15
                    )
                    # Remove HA group
                    requests.delete(
                        f"{PROXMOX_HOST}/api2/json/cluster/ha/groups/llm-{vmid}",
                        headers=headers, verify=False, timeout=15
                    )
                    # Strip GPU passthrough config
                    requests.put(
                        f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/config",
                        headers=headers,
                        data={"delete": "hostpci0,hostpci1,hostpci2,hostpci3"},
                        verify=False, timeout=15
                    )
                    # Stop VM
                    requests.post(
                        f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/status/stop",
                        headers=headers, verify=False, timeout=30
                    )
                    # Wait for stopped state (max 60 s)
                    for _ in range(12):
                        _time.sleep(5)
                        st = requests.get(
                            f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/status/current",
                            headers=headers, verify=False, timeout=10
                        )
                        if st.ok and st.json().get("data", {}).get("status") == "stopped":
                            break
                    # Permanently delete VM + disks
                    requests.delete(
                        f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}",
                        headers=headers,
                        params={"purge": 1, "destroy-unreferenced-disks": 1},
                        verify=False, timeout=60
                    )
                    logger.info(f"[delete] VM {vmid} deleted")

        # ── Release IPs ───────────────────────────────────────────────────
        from models.IPs_model import IPEntry
        for ip in ip_addresses:
            entry = db.query(IPEntry).filter(IPEntry.ip == ip).first()
            if entry:
                entry.status = "unused"
                entry.vm_id  = None

        # ── Remove DB record ──────────────────────────────────────────────
        db.delete(record)
        db.commit()
        logger.info(f"[delete] Job {job_id} fully deleted")
        return {"ok": True, "job_id": job_id}

    except Exception as e:
        raise RuntimeError(str(e))
    finally:
        db.close()

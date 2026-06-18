import os
import base64
import time
import logging
import requests
import dotenv

from temporalio import activity
from sqlalchemy.orm import Session

from db_configuration.config import SessionLocal
from models.models import Cluster
from models.llm_inference_model import LLMInference
from service.clusterService import get_api_token, getting_Proxmox_host
import ipaddress
from utils.ssh_client import run_commands, reboot_and_wait
from models.IPs_model import IPEntry, IPSModel

dotenv.load_dotenv()
logger = logging.getLogger(__name__)

VERIFY_SSL = False
_SSH_USER = os.getenv("LLM_VM_SSH_USER", "root")
_SSH_PASS = os.getenv("LLM_VM_SSH_PASS", "Teamw0rk@1")
_DEVRAQ_RPM_URL = os.getenv("DEVRAQ_RPM_URL", "")  # URL to devraq-agent RPM


# ── helpers ───────────────────────────────────────────────────────────────────

def _get_proxmox_ctx(db: Session, cluster_id: str):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise RuntimeError(f"Cluster {cluster_id} not found")
    token = get_api_token(db, cluster.name)
    host = getting_Proxmox_host(cluster)
    if not host:
        raise RuntimeError(f"No reachable Proxmox host for cluster {cluster_id}")
    headers = {
        "Authorization": f"PVEAPIToken={token}",
        "Content-Type": "application/json",
    }
    return cluster, host, headers


def _netmask_to_cidr(netmask: str) -> int:
    return ipaddress.IPv4Network(f"0.0.0.0/{netmask}").prefixlen


def _next_free_vmid(host: str, headers: dict) -> int:
    resp = requests.get(f"{host}/api2/json/cluster/nextid", headers=headers, verify=VERIFY_SSL, timeout=10)
    resp.raise_for_status()
    return int(resp.json()["data"])


def _wait_for_task(host: str, headers: dict, node: str, upid: str, timeout: int = 600):
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = requests.get(
            f"{host}/api2/json/nodes/{node}/tasks/{upid}/status",
            headers=headers, verify=VERIFY_SSL, timeout=10,
        )
        resp.raise_for_status()
        status = resp.json()["data"].get("status")
        if status == "stopped":
            exit_status = resp.json()["data"].get("exitstatus", "")
            if exit_status != "OK":
                raise RuntimeError(f"Proxmox task {upid} failed: {exit_status}")
            return
        time.sleep(5)
    raise RuntimeError(f"Proxmox task {upid} timed out after {timeout}s")


# ── activities ────────────────────────────────────────────────────────────────

@activity.defn
async def create_vm_with_gpu_activity(payload: dict) -> dict:
    db: Session = SessionLocal()
    try:
        try:
            cluster_id = payload["cluster_id"]
            node = payload["node"]
            base_os_url = payload["base_os"]
            name = payload["name"]
            cpu = int(payload["cpu"])
            ram = int(payload["ram"])
            datastore = payload["datastore"]
            os_disk_size = int(payload["os_disk_size"])
            data_disk_size = int(payload["data_disk_size"])
            gpu_pci_id = payload["gpu"]
            ip_pool_id = payload["ip_pool_id"]
            reserved_ip = payload["reserved_ip"]
            ssh_user = payload.get("ssh_user", _SSH_USER)
            ssh_pass = payload.get("ssh_pass", _SSH_PASS)
            pve_ssh_user = payload.get("pve_ssh_user", os.getenv("PVE_SSH_USER", "root"))
            pve_ssh_pass = payload.get("pve_ssh_pass", os.getenv("PVE_SSH_PASS", ""))

            # ── Resolve cluster → proxmox host + api token ────────────────────
            cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
            if not cluster_data:
                return {"error": f"Cluster {cluster_id} not found", "error_type": "cluster_missing"}
            api_token = get_api_token(db, cluster_data.name)
            headers = {"Authorization": f"PVEAPIToken={api_token}"}
            PROXMOX_HOST = getting_Proxmox_host(cluster_data)
            if not PROXMOX_HOST:
                return {"error": "No reachable Proxmox host found", "error_type": "host_unreachable"}

            # ── Resolve ip/subnet/gateway/dns from ip_pool_id + reserved_ip ──
            ip_entry = db.query(IPEntry).filter(IPEntry.ip == reserved_ip).first()
            if not ip_entry:
                return {"error": f"IPEntry not found for {reserved_ip}", "error_type": "ip_missing"}
            ip_pool = db.query(IPSModel).filter(IPSModel.id == ip_entry.pool_id).first()
            if not ip_pool:
                return {"error": f"IP pool not found", "error_type": "pool_missing"}

            cidr = _netmask_to_cidr(ip_pool.Subnet)
            ip_with_cidr = f"{reserved_ip}/{cidr}"
            gateway = ip_pool.Gateway
            dns = ip_pool.DNS[0] if ip_pool.DNS else "8.8.8.8"

            # ── Get next free VMID ────────────────────────────────────────────
            resp = requests.get(f"{PROXMOX_HOST}/api2/json/cluster/nextid", headers=headers, verify=False, timeout=10)
            resp.raise_for_status()
            vmid = int(resp.json()["data"])

            # ── Step 1: Create blank VM ───────────────────────────────────────
            create_payload = {
                "vmid": vmid,
                "name": name,
                "cores": cpu,
                "memory": ram,
                "sockets": 1,
                "cpu": "host",
                "net0": "virtio,bridge=vmbr0",
                "ostype": "l26",
                "hostpci0": f"{gpu_pci_id},pcie=1,x-vga=0",
                "machine": "q35",
                "bios": "ovmf",
                "efidisk0": f"{datastore}:1",
            }
            resp = requests.post(
                f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu",
                headers=headers, data=create_payload, verify=False, timeout=30
            )
            if resp.status_code >= 400:
                return {"error": f"VM creation failed: {resp.status_code} {resp.text}", "error_type": "create_failed"}
            upid = resp.json()["data"]
            _wait_for_task(PROXMOX_HOST, headers, node, upid, timeout=120)

            # ── Step 2: SSH into PVE node → wget qcow2 → qm importdisk ───────
            pve_node_ip = cluster_data.ip.split(",")[0].strip()
            qcow2_tmp = f"/tmp/base_os_{vmid}.qcow2"
            run_commands(pve_node_ip, pve_ssh_user, pve_ssh_pass, [
                f"wget -q --show-progress --no-check-certificate -O {qcow2_tmp} '{base_os_url}'",
                f"qm importdisk {vmid} {qcow2_tmp} {datastore} --format qcow2",
                f"rm -f {qcow2_tmp}",
            ])

            # ── Step 3: Attach boot disk + data disk + cloud-init drive ───────
            config_url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/config"
            resp = requests.put(config_url, headers=headers, data={
                "scsi0": f"{datastore}:vm-{vmid}-disk-1,size={os_disk_size}G",
                "scsi1": f"{datastore}:{data_disk_size}",
                "ide2": f"{datastore}:cloudinit",
                "boot": "order=scsi0",
                "scsihw": "virtio-scsi-pci",
            }, verify=False, timeout=30)
            if resp.status_code >= 400:
                return {"error": f"Disk config failed: {resp.text}", "error_type": "config_failed"}

            # ── Step 4: Set cloud-init — ip, gateway, dns, hostname, user ─────
            resp = requests.put(config_url, headers=headers, data={
                "ipconfig0": f"ip={ip_with_cidr},gw={gateway}",
                "nameserver": dns,
                "searchdomain": name,
                "ciuser": ssh_user,
                "cipassword": ssh_pass,
            }, verify=False, timeout=30)
            if resp.status_code >= 400:
                return {"error": f"Cloud-init config failed: {resp.text}", "error_type": "cloudinit_failed"}

            logger.info(f"VM {vmid} ({name}) created — IP: {ip_with_cidr}, GW: {gateway}, DNS: {dns}")
            return {"vmid": vmid, "node": node, "pve_node_ip": pve_node_ip, "ip_address": reserved_ip}

        except Exception as e:
            return {"error": str(e), "error_type": "activity_exception"}
    finally:
        db.close()


@activity.defn
async def add_affinity_rule_activity(payload: dict) -> dict:
    """
    Adds a HA affinity rule to keep the LLM VM on its designated node.
    Proxmox HA rules are node-pinned via the 'ha' group resource.
    """
    db: Session = SessionLocal()
    try:
        cluster_id = payload["cluster_id"]
        vmid = payload["vmid"]
        node = payload["node"]

        cluster, host, headers = _get_proxmox_ctx(db, cluster_id)

        group_id = f"llm-{vmid}"
        # Create HA group pinned to the node
        resp = requests.post(
            f"{host}/api2/json/cluster/ha/groups",
            headers=headers,
            json={"group": group_id, "nodes": node, "restricted": 1},
            verify=VERIFY_SSL, timeout=15,
        )
        # skip if already exists (Proxmox returns 500 with "already defined")
        if resp.status_code not in (200, 400) and "already defined" not in resp.text:
            resp.raise_for_status()

        # Add VM to HA resource under that group
        resp = requests.post(
            f"{host}/api2/json/cluster/ha/resources",
            headers=headers,
            json={"sid": f"vm:{vmid}", "group": group_id, "state": "started"},
            verify=VERIFY_SSL, timeout=15,
        )
        if resp.status_code not in (200, 400) and "already defined" not in resp.text:
            resp.raise_for_status()

        logger.info(f"Affinity rule set: VM {vmid} pinned to node {node}")
        return {"vmid": vmid, "ha_group": group_id}
    finally:
        db.close()


@activity.defn
async def configure_cloudinit_activity(payload: dict) -> dict:
    """
    Cloud-init config is already set during VM creation.
    This activity only starts the VM and returns the IP address.
    """
    db: Session = SessionLocal()
    try:
        try:
            cluster_id = payload["cluster_id"]
            vmid = payload["vmid"]
            node = payload["node"]
            reserved_ip = payload["reserved_ip"]

            cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
            if not cluster_data:
                return {"error": f"Cluster {cluster_id} not found", "error_type": "cluster_missing"}
            api_token = get_api_token(db, cluster_data.name)
            headers = {"Authorization": f"PVEAPIToken={api_token}"}
            PROXMOX_HOST = getting_Proxmox_host(cluster_data)
            if not PROXMOX_HOST:
                return {"error": "No reachable Proxmox host found", "error_type": "host_unreachable"}

            start_url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/status/start"
            resp = requests.post(start_url, headers=headers, verify=False, timeout=30)
            if resp.status_code >= 400:
                return {"error": f"VM start failed: {resp.text}", "error_type": "start_failed"}
            upid = resp.json()["data"]
            _wait_for_task(PROXMOX_HOST, headers, node, upid, timeout=120)

            logger.info(f"VM {vmid} started — IP: {reserved_ip}")
            return {"vmid": vmid, "ip_address": reserved_ip}

        except Exception as e:
            return {"error": str(e), "error_type": "activity_exception"}
    finally:
        db.close()


@activity.defn
async def install_devraq_agent_activity(payload: dict) -> dict:
    """
    SSHes into the VM and installs the devraq guest agent RPM.
    """
    ip = payload["ip_address"]
    ssh_user = payload.get("ssh_user", _SSH_USER)
    ssh_pass = payload.get("ssh_pass", _SSH_PASS)
    rpm_url = payload.get("devraq_rpm_url", _DEVRAQ_RPM_URL)

    cmds = [
        f"curl -fsSL -o /tmp/devraq-agent.rpm '{rpm_url}'",
        "dnf install -y /tmp/devraq-agent.rpm",
        "systemctl enable --now devraq-agent",
    ]
    run_commands(ip, ssh_user, ssh_pass, cmds)
    logger.info(f"devraq-agent installed on {ip}")
    return {"ip_address": ip, "step": "devraq_installed"}


@activity.defn
async def install_ray_vllm_activity(payload: dict) -> dict:
    """
    Idempotent setup: NVIDIA driver + CUDA + Ray + vLLM on Rocky Linux 9.
    Each phase is skipped if already installed — safe to re-run on existing VMs.
      Phase 1 (skipped if nvidia-smi works): NVIDIA driver install + reboot
      Phase 2 (skipped if venv/ray/vllm exist): CUDA, Python venv, ray, vllm
      Phase 3 (always runs): firewall rules + dirs (idempotent)
    """
    ip = payload["ip_address"]
    ssh_user = payload.get("ssh_user", _SSH_USER)
    ssh_pass = payload.get("ssh_pass", _SSH_PASS)
    hostname = payload.get("name", "llm-node")
    subnet = payload.get("subnet", "192.168.100.0/24")
    net_iface = payload.get("net_iface", "ens18")
    model = payload.get("model", "")
    model_path = payload.get("model_path", "/vllm_data/hf_cache")

    # Always set hostname
    run_commands(ip, ssh_user, ssh_pass, [f"sudo hostnamectl set-hostname '{hostname}'"], timeout=30)

    # ── Check: NVIDIA driver already installed? ───────────────────────────────
    try:
        run_commands(ip, ssh_user, ssh_pass, ["nvidia-smi"], timeout=30)
        nvidia_installed = True
        logger.info(f"[{ip}] NVIDIA driver already installed — skipping Phase 1 + reboot")
    except RuntimeError:
        nvidia_installed = False

    # ── Phase 1: NVIDIA driver (only if not already installed) ───────────────
    if not nvidia_installed:
        phase1 = [
            "sudo dnf update -y",
            "sudo dnf config-manager --set-enabled crb",
            "sudo dnf install -y epel-release",
            "sudo dnf install -y kernel-devel-$(uname -r) kernel-headers-$(uname -r) make gcc dkms",
            "sudo dnf config-manager --add-repo https://developer.download.nvidia.com/compute/cuda/repos/rhel9/x86_64/cuda-rhel9.repo",
            "sudo dnf clean expire-cache",
            "sudo dnf module install -y nvidia-driver:latest-dkms",
        ]
        run_commands(ip, ssh_user, ssh_pass, phase1, timeout=900)
        reboot_and_wait(ip, ssh_user, ssh_pass, wait_before_retry=60)
        logger.info(f"[{ip}] NVIDIA driver installed and VM rebooted")

    # ── Check: Ray + vLLM venv already installed? ────────────────────────────
    try:
        run_commands(ip, ssh_user, ssh_pass, [
            "test -f ~/vllm-ray-env/bin/ray && test -f ~/vllm-ray-env/bin/vllm"
        ], timeout=15)
        venv_installed = True
        logger.info(f"[{ip}] Ray + vLLM venv already exists — skipping Phase 2 installs")
    except RuntimeError:
        venv_installed = False

    # ── Phase 2: CUDA + Python venv + Ray + vLLM (only if not installed) ─────
    if not venv_installed:
        phase2 = [
            "nvidia-smi",
            "sudo dnf install -y cuda-toolkit",
            "sudo dnf install -y python3.11 python3.11-devel",
            "python3.11 -m venv ~/vllm-ray-env",
            "~/vllm-ray-env/bin/pip install --upgrade pip",
            '~/vllm-ray-env/bin/pip install "ray[default]"',
            "~/vllm-ray-env/bin/pip install vllm --upgrade",
        ]
        run_commands(ip, ssh_user, ssh_pass, phase2, timeout=1800)
        logger.info(f"[{ip}] CUDA + Ray + vLLM installed")

    # ── Phase 3: Env vars + firewall + dirs (always — all idempotent) ────────
    phase3 = [
        # ~/.bashrc env vars (for interactive shells)
        "grep -qF 'PATH=/usr/local/cuda/bin' ~/.bashrc || echo 'export PATH=/usr/local/cuda/bin:$PATH' >> ~/.bashrc",
        "grep -qF 'LD_LIBRARY_PATH=/usr/local/cuda/lib64' ~/.bashrc || echo 'export LD_LIBRARY_PATH=/usr/local/cuda/lib64:$LD_LIBRARY_PATH' >> ~/.bashrc",
        "grep -qF 'VLLM_USE_V1' ~/.bashrc || echo 'export VLLM_USE_V1=1' >> ~/.bashrc",
        f"grep -qF 'NCCL_SOCKET_IFNAME' ~/.bashrc || echo 'export NCCL_SOCKET_IFNAME={net_iface}' >> ~/.bashrc",
        "grep -qF 'vllm-ray-env/bin' ~/.bashrc || echo 'export PATH=\"$HOME/vllm-ray-env/bin:$PATH\"' >> ~/.bashrc",

        # /etc/environment — used by systemd services + vLLM launch (survives reboot)
        # Always overwrite model vars so any update takes effect
        "sudo sed -i '/^LLM_MODEL_NAME=/d' /etc/environment",
        "sudo sed -i '/^LLM_MODEL_PATH=/d' /etc/environment",
        "sudo sed -i '/^VLLM_USE_V1=/d' /etc/environment",
        "sudo sed -i '/^NCCL_SOCKET_IFNAME=/d' /etc/environment",
        # LLM_MODEL_NAME: use API-provided model; if empty fall back to template's LLM_NAME
        (
            f'echo \'LLM_MODEL_NAME={model}\' | sudo tee -a /etc/environment > /dev/null'
            if model else
            'LLM_NAME_VAL=$(grep "^LLM_NAME=" /etc/environment | cut -d= -f2- | tr -d \'"\'); '
            '[ -n "$LLM_NAME_VAL" ] && echo "LLM_MODEL_NAME=$LLM_NAME_VAL" | sudo tee -a /etc/environment > /dev/null || true'
        ),
        # LLM_MODEL_PATH: use API-provided path; if default fall back to template's LLM_PATH
        (
            f'echo \'LLM_MODEL_PATH={model_path}\' | sudo tee -a /etc/environment > /dev/null'
            if model_path and model_path != "/vllm_data/hf_cache" else
            'LLM_PATH_VAL=$(grep "^LLM_PATH=" /etc/environment | cut -d= -f2- | tr -d \'"\'); '
            f'[ -n "$LLM_PATH_VAL" ] && echo "LLM_MODEL_PATH=$LLM_PATH_VAL" | sudo tee -a /etc/environment > /dev/null '
            f'|| echo \'LLM_MODEL_PATH={model_path}\' | sudo tee -a /etc/environment > /dev/null'
        ),
        'echo \'VLLM_USE_V1=1\' | sudo tee -a /etc/environment > /dev/null',
        f'echo \'NCCL_SOCKET_IFNAME={net_iface}\' | sudo tee -a /etc/environment > /dev/null',

        # Firewall — start firewalld if not running, then configure ports
        (
            f"sudo firewall-cmd --state 2>/dev/null | grep -q running || "
            f"  {{ sudo systemctl enable firewalld && sudo systemctl start firewalld && sleep 3; }}; "
            f"sudo firewall-cmd --permanent --add-rich-rule='rule family=\"ipv4\" source address=\"{subnet}\" accept' 2>/dev/null || true; "
            f"sudo firewall-cmd --permanent --add-port=8000/tcp; "
            f"sudo firewall-cmd --permanent --add-port=6379/tcp; "
            f"sudo firewall-cmd --permanent --add-port=8265/tcp; "
            f"sudo firewall-cmd --permanent --add-port=10001/tcp; "
            f"sudo firewall-cmd --permanent --add-port=10002-19999/tcp; "
            f"sudo firewall-cmd --reload"
        ),

        # Model cache dir
        "sudo mkdir -p /vllm_data/hf_cache",
        "sudo chmod 777 /vllm_data/hf_cache",

        # Confirm env + versions
        "cat /etc/environment",
        "~/vllm-ray-env/bin/ray --version",
        "~/vllm-ray-env/bin/vllm --version",
    ]
    run_commands(ip, ssh_user, ssh_pass, phase3, timeout=120)
    logger.info(f"[{ip}] Ray + vLLM ready — model: {model}")
    return {"ip_address": ip, "step": "ray_vllm_installed"}


@activity.defn
async def configure_ray_activity(payload: dict) -> dict:
    """
    Creates ray-head.service (head node) or ray-worker.service (worker node)
    systemd unit file, then starts and enables it.

    payload must include:
      role         : "head" | "worker"
      ip_address   : this node's IP
      head_ip      : head node IP (only needed when role == "worker")
    """
    ip = payload["ip_address"]
    ssh_user = payload.get("ssh_user", _SSH_USER)
    ssh_pass = payload.get("ssh_pass", _SSH_PASS)
    role = payload.get("role", "head")
    head_ip = payload.get("head_ip", ip)
    num_gpus = payload.get("num_gpus", 1)
    net_iface = payload.get("net_iface", "ens18")

    venv_bin = "/root/vllm-ray-env/bin"
    cuda_path = "/usr/local/cuda/bin"

    if role == "head":
        service_name = "ray-head"
        exec_start = (
            f"{venv_bin}/ray start --head --port=6379 "
            f"--num-gpus={num_gpus} "
            "--dashboard-host=0.0.0.0 --include-dashboard=true "
            "--block"
        )
    else:
        service_name = "ray-worker"
        exec_start = (
            f"{venv_bin}/ray start --address={head_ip}:6379 "
            f"--num-gpus={num_gpus} "
            "--block"
        )

    # Build service file with real newlines
    # Type=simple + --block: ray runs in foreground, systemd tracks the process directly
    service_content = "\n".join([
        "[Unit]",
        f"Description=Ray {role.capitalize()} Node Daemon",
        "After=network.target",
        "Wants=network-online.target",
        "",
        "[Service]",
        "Type=simple",
        "User=root",
        f'Environment="PATH={venv_bin}:{cuda_path}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin"',
        f'Environment="NCCL_SOCKET_IFNAME={net_iface}"',
        'Environment="VLLM_USE_V1=1"',
        'Environment="LD_LIBRARY_PATH=/usr/local/cuda/lib64"',
        f"ExecStart={exec_start}",
        f"ExecStop={venv_bin}/ray stop --force",
        "Restart=always",
        "RestartSec=10",
        "",
        "[Install]",
        "WantedBy=multi-user.target",
        "",
    ])
    encoded = base64.b64encode(service_content.encode()).decode()

    cmds = [
        # Fail fast if ray binary missing
        f"test -f {venv_bin}/ray || {{ echo 'ERROR: Ray binary not found at {venv_bin}/ray. Run install step first.'; exit 1; }}",

        # Stop existing ray + service cleanly
        f"sudo systemctl stop {service_name}.service 2>/dev/null || true",
        f"{venv_bin}/ray stop --force 2>/dev/null || true",
        "sleep 3",

        # Write service file via base64 — no quoting issues
        f"echo '{encoded}' | base64 -d | sudo tee /etc/systemd/system/{service_name}.service > /dev/null",
        "sudo systemctl daemon-reload",

        # Enable + start
        f"sudo systemctl enable {service_name}.service",
        f"sudo systemctl start {service_name}.service",

        # Give ray a moment then verify
        "sleep 5",
        f"{venv_bin}/ray status",
    ]
    run_commands(ip, ssh_user, ssh_pass, cmds, timeout=120)
    logger.info(f"Ray {role} started on {ip} via {service_name}.service")
    return {"ip_address": ip, "role": role, "step": "ray_configured"}


@activity.defn
async def launch_vllm_model_activity(payload: dict) -> dict:
    """
    Launches vLLM OpenAI-compatible server on the head node.
    Model is downloaded from HuggingFace into /vllm_data/hf_cache.
    Uses Ray as distributed executor with pipeline parallelism across nodes.

    Key payload fields:
      model               : HuggingFace model ID (e.g. "openai/gpt-oss-120b")
      served_model_name   : alias shown in API responses (defaults to last part of model ID)
      tensor_parallel_size: GPUs per node (default 1)
      pipeline_parallel_size: number of nodes / pipeline stages (default 1)
      quantization        : e.g. "mxfp4", "awq", "gptq" (optional)
      max_model_len       : max context length in tokens (default 32768)
      gpu_memory_utilization: fraction of GPU VRAM to use (default 0.90)
    """
    ip = payload["ip_address"]
    ssh_user = payload.get("ssh_user", _SSH_USER)
    ssh_pass = payload.get("ssh_pass", _SSH_PASS)
    model = payload["model"]
    hf_token = payload.get("hf_token", os.getenv("HF_TOKEN", ""))

    served_model_name = payload.get("served_model_name", model.split("/")[-1])
    tensor_parallel_size = payload.get("tensor_parallel_size", 1)
    pipeline_parallel_size = payload.get("pipeline_parallel_size", 1)
    quantization = payload.get("quantization", "")
    max_model_len = payload.get("max_model_len", 32768)
    gpu_memory_utilization = payload.get("gpu_memory_utilization", 0.90)

    venv_python = "/root/vllm-ray-env/bin/python3"

    quantization_flag = f"--quantization {quantization} \\" if quantization else ""

    hf_export = f"export HF_TOKEN={hf_token}" if hf_token else "true"

    vllm_cmd = (
        f"VLLM_USE_V1=1 \\\n"
        f"nohup {venv_python} -m vllm.entrypoints.openai.api_server \\\n"
        f"  --model {model} \\\n"
        f"  --served-model-name {served_model_name} \\\n"
        f"  --download-dir /vllm_data/hf_cache \\\n"
        f"  --distributed-executor-backend ray \\\n"
        f"  --tensor-parallel-size {tensor_parallel_size} \\\n"
        f"  --pipeline-parallel-size {pipeline_parallel_size} \\\n"
        f"  {quantization_flag}\n"
        f"  --max-model-len {max_model_len} \\\n"
        f"  --gpu-memory-utilization {gpu_memory_utilization} \\\n"
        f"  --enable-chunked-prefill \\\n"
        f"  --trust-remote-code \\\n"
        f"  --host 0.0.0.0 \\\n"
        f"  --port 8000 \\\n"
        f"  --enable-auto-tool-choice \\\n"
        f"  --tool-call-parser openai \\\n"
        f"> /root/vllm_server.log 2>&1 &"
    )

    cmds = [
        hf_export,
        "export VLLM_USE_V1=1",
        # Stop any existing vLLM process
        "pkill -f 'vllm.entrypoints.openai.api_server' 2>/dev/null || true",
        "sleep 3",
        # Launch vLLM — HuggingFace download happens automatically on first run
        vllm_cmd,
        # Poll /health every 10s — large models take several minutes to download + load
        (
            "for i in $(seq 1 90); do "
            "  curl -sf http://localhost:8000/health && echo 'vllm ready' && break; "
            "  echo \"Waiting for vllm... attempt $i/90 (check /root/vllm_server.log)\"; "
            "  sleep 10; "
            "done"
        ),
    ]
    run_commands(ip, ssh_user, ssh_pass, cmds, timeout=1800)
    endpoint = f"http://{ip}:8000/v1"
    logger.info(f"vllm model {model} launched on {ip}, endpoint: {endpoint}")
    return {"ip_address": ip, "endpoint_url": endpoint, "step": "vllm_launched"}


@activity.defn
async def update_llm_inference_status_activity(payload: dict) -> dict:
    """
    Persists vmid, ip_address, endpoint_url, and status back to the DB record.
    """
    db: Session = SessionLocal()
    try:
        inference_id = payload["inference_id"]
        record = db.query(LLMInference).filter(LLMInference.id == inference_id).first()
        if not record:
            raise RuntimeError(f"LLMInference record {inference_id} not found")

        for field in ("vmid", "ip_address", "endpoint_url", "status"):
            if field in payload:
                setattr(record, field, payload[field])

        db.commit()
        logger.info(f"LLMInference {inference_id} updated: status={payload.get('status')}")
        return {"inference_id": inference_id, "status": payload.get("status")}
    finally:
        db.close()

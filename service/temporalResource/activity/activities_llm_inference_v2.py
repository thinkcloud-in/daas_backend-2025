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


def _all_hosts(cluster_data) -> list:
    """Build all candidate 'https://ip:port' URLs for a cluster (no probing)."""
    ip_field = cluster_data.ip
    port     = cluster_data.port
    if isinstance(ip_field, str):
        ips = [ip.strip() for ip in ip_field.split(",") if ip.strip()]
    elif isinstance(ip_field, list):
        ips = [ip.strip() for ip in ip_field if isinstance(ip, str) and ip.strip()]
    else:
        ips = []
    return [f"https://{ip}:{port}" for ip in ips]


def _wait_for_task(hosts, headers: dict, node: str, upid: str, timeout: int = 300):
    """
    Poll a Proxmox task until it stops. `hosts` may be a single URL or a list of
    cluster host URLs — on a connection failure we rotate to the next host, since
    any node in a Proxmox cluster can answer a cluster-wide task-status query.
    """
    import time
    if isinstance(hosts, str):
        hosts = [hosts]
    deadline   = time.time() + timeout
    poll_count = 0
    hi         = 0  # current host index
    conn_fail  = 0  # consecutive connection failures

    while time.time() < deadline:
        poll_count += 1
        host = hosts[hi % len(hosts)]
        try:
            resp = requests.get(
                f"{host}/api2/json/nodes/{node}/tasks/{quote(upid, safe='')}/status",
                headers=headers, verify=False, timeout=10
            )
            conn_fail = 0  # reachable again
            if resp.ok:
                data = resp.json().get("data", {})
                if data.get("status") == "stopped":
                    if data.get("exitstatus", "OK") != "OK":
                        raise RuntimeError(f"Proxmox task {upid} failed: {data.get('exitstatus')}")
                    return
                if poll_count % 12 == 0:  # log every ~1 min
                    logger.info(f"Task {upid} on {node}: still {data.get('status')} (via {host})")
            else:
                logger.warning(f"Task status poll failed ({resp.status_code}) for {upid} via {host}: {resp.text[:200]}")
                if resp.status_code in (401, 403, 404) and poll_count >= 6:
                    raise RuntimeError(
                        f"Cannot read status of task {upid} on {node} "
                        f"(HTTP {resp.status_code} after {poll_count} attempts): {resp.text[:300]}"
                    )
        except requests.exceptions.RequestException as exc:
            # Connection dropped — rotate to the next cluster host and keep polling
            conn_fail += 1
            logger.warning(f"Task poll connection error via {host} (fail #{conn_fail}): {exc}")
            hi += 1
            if conn_fail >= len(hosts) * 6:
                raise RuntimeError(
                    f"All cluster hosts unreachable while polling task {upid} "
                    f"(tried {hosts}, {conn_fail} consecutive failures)"
                )
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
def clone_and_configure_vm_activity(payload: dict) -> dict:
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
        # All cluster hosts — used for task-status failover if one node drops mid-clone
        ALL_HOSTS = _all_hosts(cluster_data) or [PROXMOX_HOST]

        logger.info(f"[cluster {cluster_id}] GPUs (raw PCI): {gpus}")

        # ── Generate unique VM name ───────────────────────────────────────
        all_vms        = proxmoxService.get_all_cluster_vms(db, cluster_data)
        existing_names = [vm["name"] for vm in all_vms if "name" in vm and vm["name"]]
        db_names       = [m.name for m in db.query(Machine).all()]
        all_names      = set(existing_names) | set(db_names)
        # Use name_template from payload if provided (e.g. "lucky-{n:fixed=3}"),
        # otherwise fall back to "{pool_name}-{n:fixed=3}".
        name_template  = payload.get("name_template") or f"{pool_name}-{{n:fixed=3}}"
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

        # ── Release the DB connection BEFORE the long clone ───────────────
        # All DB reads are done. The clone + boot + SSH below take ~40 min, and
        # Postgres/poolers reap idle connections in that window → the later
        # db.close() would blow up with "server closed the connection
        # unexpectedly". We reopen a fresh short-lived session at the very end
        # only to mark the IP as used.
        db.close()
        db = None

        # ── Clone from template ───────────────────────────────────────────
        # Always set target explicitly — if omitted, Proxmox places the clone
        # on the template's node regardless of which node the user selected.
        clone_data = {
            "newid":   vmid,
            "name":    vm_name,
            "full":    1,
            "target":  node,
            "storage": datastore,
        }
        resp = requests.post(
            f"{PROXMOX_HOST}/api2/json/nodes/{template_node}/qemu/{template}/clone",
            headers=headers,
            data=clone_data,
            verify=False, timeout=30
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"Clone failed: {resp.text}")
        upid = resp.json()["data"]
        _wait_for_task(ALL_HOSTS, headers, template_node, upid, timeout=10800)

        hostpci_data = {
            f"hostpci{i}": f"{pci},pcie=1"
            for i, pci in enumerate(gpus)
        }
        logger.info(f"[{vmid}] hostpci config: {hostpci_data}")

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
            _wait_for_task(ALL_HOSTS, headers, node, config_upid, timeout=60)

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
            _wait_for_task(ALL_HOSTS, headers, node, upid, timeout=120)
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

        # ── Mark IP as used (fresh short-lived session) ───────────────────
        # The original session was closed before the clone; open a new one now
        # and re-query the IPEntry so we commit on a live connection.
        mark_db: Session = SessionLocal()
        try:
            ip_entry = mark_db.query(IPEntry).filter(IPEntry.ip == reserved_ip).first()
            if ip_entry:
                ip_entry.status = "used"
                ip_entry.vm_id  = str(vmid)
                mark_db.commit()
            else:
                logger.warning(f"[{vmid}] IPEntry for {reserved_ip} not found when marking used")
        finally:
            mark_db.close()

        logger.info(f"VM {vmid} ({vm_name}) cloned on {node} — IP: {ip_with_cidr}, GPUs: {gpus}")
        return {"vmid": vmid, "node": node, "ip_address": reserved_ip}

    finally:
        # db may already be closed (set to None before the clone) — guard it.
        if db is not None:
            db.close()


@activity.defn
def launch_vllm_from_template_activity(payload: dict) -> dict:

    try:
        import time
        ip       = payload["ip_address"]
        ssh_user = payload.get("ssh_user", _SSH_USER)
        ssh_pass = payload.get("ssh_pass", _SSH_PASS)
        tp_size  = payload.get("tensor_parallel_size", 1)
        pp_size  = payload.get("pipeline_parallel_size", 1)

        # ── Step 0: Reboot VM ─────────────────────────────────────────────────
        # Fresh clone ke baad GPU drivers properly initialize nahi hote.
        # Reboot ensures clean GPU state before vLLM load.
        # skip_reboot lets a test jump straight to the launch+health step (the
        # part we're debugging) without the ~3 min reboot.
        if payload.get("skip_reboot"):
            logger.info(f"[{ip}] skip_reboot=True — skipping reboot, going straight to launch")
        else:
            logger.info(f"[{ip}] Rebooting VM to ensure clean GPU initialization...")
            reboot_and_wait(ip, ssh_user, ssh_pass, wait_before_retry=90)
            logger.info(f"[{ip}] VM back online after reboot")

        # ── Step 0.1: Wait for NVIDIA driver/NVML to be ready ─────────────────
        # After reboot the nvidia kernel modules take time to initialize. If vLLM
        # launches before NVML responds it fails with "Driver Not Loaded /
        # Failed to infer device type". Gate on nvidia-smi returning cleanly.
        _wait_gpu = (
            "source /etc/profile || true; "
            "source ~/.bash_profile || true; "
            "source ~/.bashrc || true; "
            "for i in $(seq 1 30); do "
            "  nvidia-smi > /dev/null 2>&1 && echo 'gpu ready' && exit 0; "
            "  echo \"Waiting for GPU/NVML... $i/30\"; sleep 5; "
            "done; "
            "echo 'ERROR: GPU/NVML not ready after 150s'; nvidia-smi; exit 1"
        )
        run_commands(ip, ssh_user, ssh_pass, [_wait_gpu], timeout=180)
        logger.info(f"[{ip}] GPU/NVML ready")

        # ── Step 0.5: Compute if multi-node (determines Ray vLLM params) ──────
        is_multinode = pp_size > 1

        # ── Step 0.6: Wait for Ray head service (always, but only vLLM uses it if multi-node) ──
        # Reboot ke baad Ray head service auto-start hoti hai — wait karo ready hone ka
        _wait_ray_head = (
            "source /etc/profile || true; "
            "source ~/.bash_profile || true; "
            "source ~/.bashrc || true; "
            "for i in $(seq 1 48); do "
            "  systemctl is-active ray-head.service && echo 'ray-head active' && exit 0; "
            "  echo \"Waiting for ray-head... $i/48\"; sleep 5; "
            "done; "
            "echo 'WARNING: ray-head not active after 4 min'; "
            "systemctl status ray-head.service --no-pager || true; "
            "exit 0"  # non-fatal — vLLM launch will tell us if Ray is actually broken
        )
        try:
            run_commands(ip, ssh_user, ssh_pass, [_wait_ray_head], timeout=260)
            logger.info(f"[{ip}] Ray head service confirmed active")
        except RuntimeError as _ray_err:
            logger.warning(f"[{ip}] Ray head wait encountered error (non-fatal): {_ray_err}")

        home_dir = "/root" if ssh_user == "root" else f"/home/{ssh_user}"
        _vllm_bin = f"{home_dir}/vllm-ray-env/bin/python3 -m vllm.entrypoints.openai.api_server"
        # Single-node inference (pp_size=1) → no Ray, simpler & faster.
        # Multi-node inference (pp_size>1) → Ray for pipeline parallelism.
        if is_multinode:
            _vllm_common_args = (
                f" --distributed-executor-backend ray"
                f" --tensor-parallel-size {tp_size}"
                f" --pipeline-parallel-size {pp_size}"
                f" --max-model-len 4096"
                f" --gpu-memory-utilization 0.90"
                f" --enable-chunked-prefill"
                f" --trust-remote-code"
                f" --host 0.0.0.0 --port 8000"
            )
        else:
            # Single node: no Ray overhead, plain in-process inference
            _vllm_common_args = (
                f" --max-model-len 4096"
                f" --gpu-memory-utilization 0.90"
                f" --enable-chunked-prefill"
                f" --trust-remote-code"
                f" --host 0.0.0.0 --port 8000"
            )

        _vllm_env_setup = (
            f"source {home_dir}/vllm-ray-env/bin/activate; "
            f"export CUDA_HOME=/usr/local/cuda; "
            f"export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/cuda/lib64:/usr/lib64:/usr/lib/x86_64-linux-gnu; "
            f"export PATH=$PATH:/usr/local/cuda/bin; "
            f"export VLLM_DEVICE=cuda; "
            f"export CUDA_VISIBLE_DEVICES=0; "
        )

        vllm_launch = (
            # ── 0.5: Load CUDA & vLLM env ────────────────────────────────────────
            "source /etc/profile || true; "
            "source ~/.bash_profile || true; "
            "source ~/.bashrc || true; "
            # set -a/source /etc/environment is unreliable in non-login SSH shells.
            # Parse and export each key=value line explicitly instead.
            "while IFS='=' read -r _k _v; do "
            "  case \"$_k\" in '#'*|'') continue;; esac; "
            "  export \"$_k=$_v\"; "
            "done < /etc/environment; "
            + _vllm_env_setup +
            # ── 2. Resolve model path from LLM_MODEL_PATH + LLM_MODEL_NAME ────
            # LLM_MODEL_PATH = base dir  e.g. /vllm_data/hf_cache
            # LLM_MODEL_NAME = model dir e.g. ArtLLM
            # Combined → /vllm_data/hf_cache/ArtLLM
            #
            # Also supports:
            #   LLM_MODEL_NAME as absolute path  (/some/path/model)
            #   LLM_MODEL_NAME as HF ID          (org/model-name)
            "RESOLVED_MODEL=''; "

            # Case A: LLM_MODEL_PATH + LLM_MODEL_NAME both set → combine them
            "if [ -n \"${LLM_MODEL_PATH:-}\" ] && [ -n \"${LLM_MODEL_NAME:-}\" ]; then "
            "  _combined=\"${LLM_MODEL_PATH}/${LLM_MODEL_NAME}\"; "
            "  if [ -f \"${_combined}/config.json\" ]; then "
            "    RESOLVED_MODEL=\"$_combined\"; "
            "    echo \"[vLLM] Model resolved: $RESOLVED_MODEL (LLM_MODEL_PATH + LLM_MODEL_NAME)\"; "
            "  else "
            "    echo \"[vLLM] WARNING: ${_combined}/config.json not found\"; "
            "  fi; "
            "fi; "

            # Case B: LLM_MODEL_NAME alone is an absolute path or HF ID (has slash)
            "if [ -z \"$RESOLVED_MODEL\" ] && [ -n \"${LLM_MODEL_NAME:-}\" ]; then "
            "  case \"$LLM_MODEL_NAME\" in "
            "    /*) "  # absolute path
            "      [ -f \"${LLM_MODEL_NAME}/config.json\" ] && RESOLVED_MODEL=\"$LLM_MODEL_NAME\" "
            "        && echo \"[vLLM] Model resolved: $RESOLVED_MODEL (absolute path)\"; ;; "
            "    */*) "  # HF ID like org/model
            "      RESOLVED_MODEL=\"$LLM_MODEL_NAME\"; "
            "      echo \"[vLLM] Model resolved: $RESOLVED_MODEL (HuggingFace ID)\"; ;; "
            "  esac; "
            "fi; "

            # Case C: Fallback scan inside LLM_MODEL_PATH if name not set
            "if [ -z \"$RESOLVED_MODEL\" ] && [ -n \"${LLM_MODEL_PATH:-}\" ]; then "
            "  for _cfg in \"$LLM_MODEL_PATH\"/*/config.json \"$LLM_MODEL_PATH\"/config.json; do "
            "    [ -f \"$_cfg\" ] && { RESOLVED_MODEL=$(dirname \"$_cfg\"); "
            "      echo \"[vLLM] Found model in LLM_MODEL_PATH: $RESOLVED_MODEL\"; break; }; "
            "  done; "
            "fi; "

            # Case D: Final fallback — scan /vllm_data/hf_cache
            "if [ -z \"$RESOLVED_MODEL\" ]; then "
            "  for _cfg in /vllm_data/hf_cache/*/config.json "
            "             /vllm_data/hf_cache/models--*/snapshots/*/config.json; do "
            "    [ -f \"$_cfg\" ] && { RESOLVED_MODEL=$(dirname \"$_cfg\"); "
            "      echo \"[vLLM] Found model in hf_cache fallback: $RESOLVED_MODEL\"; break; }; "
            "  done; "
            "fi; "

            # ── 3. Nothing found → fail loudly with env dump ─────────────────
            "if [ -z \"$RESOLVED_MODEL\" ]; then "
            "  echo 'VLLM_SKIP: LLM_MODEL_PATH/LLM_MODEL_NAME not set and no model found in /vllm_data/hf_cache'; "
            "  echo \"  LLM_MODEL_PATH=${LLM_MODEL_PATH:-<unset>}\"; "
            "  echo \"  LLM_MODEL_NAME=${LLM_MODEL_NAME:-<unset>}\"; "
            "  echo \"  /etc/environment contents:\"; cat /etc/environment 2>/dev/null || echo '  (not found)'; "
            "  echo \"  /vllm_data/hf_cache listing:\"; ls /vllm_data/hf_cache/ 2>/dev/null || echo '  (not found)'; "
            "  exit 1; "  # fail instead of silently skipping so health_poll never runs
            "fi; "

            # ── 4. Kill stale vLLM process + TRUNCATE old log ─────────────────
            # Overwrite the log with a dated marker so we never read a stale log
            # left over from template prep. Everything after appends (>>).
            "echo \"[vLLM] Launching: $RESOLVED_MODEL\"; "
            "pgrep -f 'vllm.entrypoints.openai.api_server' | grep -v $$ | xargs -r kill 2>/dev/null || true; sleep 2; "
            f"echo \"===== vLLM launch attempt $(date -u) — model=$RESOLVED_MODEL =====\" > {home_dir}/vllm_server.log; "

            # ── 5. Fire-and-forget launch ─────────────────────────────────────
            # env VAR=value prefix guarantees vars reach the nohup subprocess
            # even if the SSH channel closes before shell exports are inherited.
            f"_VLLM_ENV=\"VLLM_DEVICE=cuda CUDA_VISIBLE_DEVICES=0 CUDA_HOME=/usr/local/cuda\"; "

            # Log exact env + command to vllm_server.log before launching
            f"echo \"[vLLM-env] VLLM_DEVICE=$VLLM_DEVICE\" >> {home_dir}/vllm_server.log; "
            f"echo \"[vLLM-env] CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES\" >> {home_dir}/vllm_server.log; "
            f"echo \"[vLLM-env] CUDA_HOME=$CUDA_HOME\" >> {home_dir}/vllm_server.log; "
            f"echo \"[vLLM-env] LD_LIBRARY_PATH=$LD_LIBRARY_PATH\" >> {home_dir}/vllm_server.log; "
            f"echo \"[vLLM-env] _VLLM_ENV=$_VLLM_ENV\" >> {home_dir}/vllm_server.log; "
            f"echo \"[vLLM-env] /dev/nvidia* = $(ls /dev/nvidia* 2>/dev/null || echo MISSING)\" >> {home_dir}/vllm_server.log; "

            "if [ \"${RESOLVED_MODEL:0:1}\" = \"/\" ]; then "
            f"  echo \"[vLLM-cmd] nohup env $_VLLM_ENV {_vllm_bin} --model $RESOLVED_MODEL {_vllm_common_args}\" >> {home_dir}/vllm_server.log; "
            f"  nohup env $_VLLM_ENV {_vllm_bin}"
            f"    --model \"$RESOLVED_MODEL\""
            f"    --served-model-name \"$RESOLVED_MODEL\""
            f"    {_vllm_common_args}"
            f"    >> {home_dir}/vllm_server.log 2>&1 & "
            "else "
            f"  echo \"[vLLM-cmd] nohup env $_VLLM_ENV {_vllm_bin} --model $RESOLVED_MODEL --download-dir ${{LLM_MODEL_PATH:-/vllm_data/hf_cache}} {_vllm_common_args}\" >> {home_dir}/vllm_server.log; "
            f"  nohup env $_VLLM_ENV {_vllm_bin}"
            f"    --model \"$RESOLVED_MODEL\""
            f"    --served-model-name \"$RESOLVED_MODEL\""
            f"    --download-dir \"${{LLM_MODEL_PATH:-/vllm_data/hf_cache}}\""
            f"    {_vllm_common_args}"
            f"    >> {home_dir}/vllm_server.log 2>&1 & "
            "fi; "
            "echo \"[vLLM] Process launched in background\""
        )

        health_poll = (
            "source /etc/profile || true; "
            "source ~/.bash_profile || true; "
            "source ~/.bashrc || true; "
            "for i in $(seq 1 90); do "
            "  curl -sf http://localhost:8000/health && echo 'vllm ready' && exit 0; "
            "  echo \"Waiting for vllm... $i/90\"; "
            "  pgrep -f 'vllm.entrypoints.openai.api_server' > /dev/null || { echo 'ERROR: vLLM process died' >&2; break; }; "
            "  sleep 10; "
            "done; "
            "{ "
            "  echo '=== nvidia-smi ==='; nvidia-smi 2>/dev/null || echo 'nvidia-smi failed'; "
            "  echo '=== ray status ==='; ray status 2>/dev/null || echo 'ray status failed'; "
            "  echo '=== vllm_server.log (last 80 lines) ==='; "
            f"  [ -f {home_dir}/vllm_server.log ] && tail -80 {home_dir}/vllm_server.log || echo 'Log not found'; "
            "} >&2; "
            "exit 1"
        )

        try:
            launch_results = run_commands(ip, ssh_user, ssh_pass, [vllm_launch], timeout=120)
            launch_stdout = launch_results[0]["stdout"] if launch_results else ""
        except RuntimeError as launch_err:
            if "exit -1" in str(launch_err):
                # SSH channel dropped — exit -1 means channel closed without exit status.
                # This can happen BEFORE or AFTER nohup was submitted, so we cannot assume
                # the process is running. Wait 30s then verify with a fresh SSH connection.
                logger.warning(f"[{ip}] SSH dropped during vLLM launch (exit -1) — verifying process...")
                time.sleep(30)

                # Fresh SSH: check if vLLM process is actually running
                _check_cmd = (
                    "pgrep -fl 'vllm.entrypoints.openai.api_server' "
                    "&& echo VLLM_RUNNING || echo VLLM_NOT_RUNNING"
                )
                try:
                    _chk = run_commands(ip, ssh_user, ssh_pass, [_check_cmd], timeout=30)
                    _chk_out = _chk[0]["stdout"] if _chk else "VLLM_NOT_RUNNING"
                except Exception:
                    _chk_out = "VLLM_NOT_RUNNING"

                if "VLLM_NOT_RUNNING" in _chk_out:
                    # nohup never ran — re-launch now
                    logger.warning(f"[{ip}] vLLM process not found after exit -1 — re-launching...")
                    try:
                        re_results = run_commands(ip, ssh_user, ssh_pass, [vllm_launch], timeout=60)
                        launch_stdout = re_results[0]["stdout"] if re_results else ""
                    except RuntimeError as relaunch_err:
                        if "exit -1" in str(relaunch_err):
                            logger.warning(f"[{ip}] Re-launch also got exit -1 — assuming nohup submitted, waiting 60s")
                            time.sleep(60)
                            launch_stdout = "ASSUMED_LAUNCHED"
                        else:
                            raise
                else:
                    logger.info(f"[{ip}] vLLM confirmed running after exit -1 — proceeding to health poll")
                    launch_stdout = "ASSUMED_LAUNCHED"
            else:
                raise

        if "VLLM_SKIP" in launch_stdout:
            logger.info(f"[{ip}] vLLM launch skipped — no model found in any configured path")
            return {"endpoint_url": None}

        run_commands(ip, ssh_user, ssh_pass, [health_poll], timeout=960)

        endpoint = f"http://{ip}:8000/v1"
        logger.info(f"vLLM started on {ip} — endpoint: {endpoint}")
        return {"endpoint_url": endpoint}

    except Exception as e:
        raise RuntimeError(str(e))


@activity.defn
def restore_llm_services_activity(payload: dict) -> dict:
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
    home_dir = "/root" if ssh_user == "root" else f"/home/{ssh_user}"
    venv_bin = f"{home_dir}/vllm-ray-env/bin"
    service  = "ray-head" if role == "head" else "ray-worker"

    # ── Step 1: Wait for systemd Ray service to be active (max 3 min) ─────────
    wait_ray_service = (
        f"source /etc/profile || true; "
        f"source ~/.bash_profile || true; "
        f"source ~/.bashrc || true; "
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
            f"source /etc/profile || true; "
            f"source ~/.bash_profile || true; "
            f"source ~/.bashrc || true; "
            f"for i in $(seq 1 30); do "
            f"  {venv_bin}/ray status 2>/dev/null && echo 'ray cluster ready' && break; "
            f"  echo \"Waiting for ray cluster... $i/30\"; sleep 5; "
            f"done; "
            f"{venv_bin}/ray status"
        )
        run_commands(ip, ssh_user, ssh_pass, [wait_ray_cluster], timeout=180)
        logger.info(f"[{ip}] Ray cluster is healthy")

        # ── Step 3: Kill stale vLLM + relaunch ───────────────────────────────
        _vllm_bin_r = f"{venv_bin}/python3 -m vllm.entrypoints.openai.api_server"
        _vllm_args_r = (
            f" --distributed-executor-backend ray"
            f" --tensor-parallel-size {tp_size}"
            f" --pipeline-parallel-size {pp_size}"
            f" --max-model-len 4096"
            f" --gpu-memory-utilization 0.90"
            f" --enable-chunked-prefill"
            f" --trust-remote-code"
            f" --host 0.0.0.0 --port 8000"
        )
        vllm_launch = (
            "source /etc/profile || true; "
            "source ~/.bash_profile || true; "
            "source ~/.bashrc || true; "
            "set -a; source /etc/environment; set +a; "
            f"export CUDA_HOME=/usr/local/cuda; "
            f"export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/cuda/lib64:/usr/lib64:/usr/lib/x86_64-linux-gnu; "
            f"export PATH=$PATH:/usr/local/cuda/bin; "
            f"export VLLM_TARGET_DEVICE=cuda; "

            # Resolve model: LLM_MODEL_PATH + LLM_MODEL_NAME → combined path
            "RESOLVED_MODEL=''; "
            "if [ -n \"${LLM_MODEL_PATH:-}\" ] && [ -n \"${LLM_MODEL_NAME:-}\" ]; then "
            "  _combined=\"${LLM_MODEL_PATH}/${LLM_MODEL_NAME}\"; "
            "  [ -f \"${_combined}/config.json\" ] && RESOLVED_MODEL=\"$_combined\" "
            "    && echo \"[vLLM-restore] Model: $RESOLVED_MODEL\"; "
            "fi; "
            "if [ -z \"$RESOLVED_MODEL\" ] && [ -n \"${LLM_MODEL_NAME:-}\" ]; then "
            "  case \"$LLM_MODEL_NAME\" in "
            "    /*) [ -f \"${LLM_MODEL_NAME}/config.json\" ] && RESOLVED_MODEL=\"$LLM_MODEL_NAME\"; ;; "
            "    */*) RESOLVED_MODEL=\"$LLM_MODEL_NAME\"; ;; "
            "  esac; "
            "fi; "
            "if [ -z \"$RESOLVED_MODEL\" ]; then "
            "  echo 'ERROR: Cannot resolve model — set LLM_MODEL_PATH and LLM_MODEL_NAME in /etc/environment'; exit 1; "
            "fi; "

            "pgrep -f 'vllm.entrypoints.openai.api_server' | grep -v $$ | xargs -r kill 2>/dev/null || true; sleep 3; "
            "if [ \"${RESOLVED_MODEL:0:1}\" = \"/\" ]; then "
            f"  nohup {_vllm_bin_r} --model \"$RESOLVED_MODEL\" --served-model-name \"$RESOLVED_MODEL\"{_vllm_args_r} > {home_dir}/vllm_server.log 2>&1 & disown; "
            "else "
            f"  nohup {_vllm_bin_r} --model \"$RESOLVED_MODEL\" --served-model-name \"$RESOLVED_MODEL\" --download-dir \"${{LLM_MODEL_PATH:-/vllm_data/hf_cache}}\"{_vllm_args_r} > {home_dir}/vllm_server.log 2>&1 & disown; "
            "fi; "
            "sleep 8; "
            "pgrep -f 'vllm.entrypoints.openai.api_server' > /dev/null || "
            f"  {{ echo 'ERROR: vLLM process failed to start'; cat {home_dir}/vllm_server.log; exit 1; }}"
        )
        health_poll = (
            "source /etc/profile || true; "
            "source ~/.bash_profile || true; "
            "source ~/.bashrc || true; "
            "for i in $(seq 1 90); do "
            "  curl -sf http://localhost:8000/health && echo 'vllm ready' && exit 0; "
            "  echo \"Waiting for vllm... $i/90\"; "
            "  pgrep -f 'vllm.entrypoints.openai.api_server' > /dev/null || { echo 'ERROR: vLLM process died' >&2; break; }; "
            "  sleep 10; "
            "done; "
            "{ "
            "  echo '=== nvidia-smi ==='; nvidia-smi 2>/dev/null || echo 'nvidia-smi failed'; "
            "  echo '=== ray status ==='; ray status 2>/dev/null || echo 'ray status failed'; "
            "  echo '=== vllm_server.log (last 80 lines) ==='; "
            f"  [ -f {home_dir}/vllm_server.log ] && tail -80 {home_dir}/vllm_server.log || echo 'Log not found'; "
            "} >&2; "
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
def vm_power_action_activity(payload: dict) -> dict:
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
def update_llm_inference_job_activity(payload: dict) -> dict:
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
def delete_llm_pool_activity(payload: dict) -> dict:
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

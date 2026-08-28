import os
import re
import time
import ipaddress
import requests
import paramiko

from temporalio import activity
from sqlalchemy.orm import Session

from db_configuration.config import SessionLocal
from models.models import Cluster
from models.IPs_model import IPEntry, IPSModel
from models.library_model import LibraryItem
from models.lxc_restore_model import LXCRestoreJob
from service.clusterService import get_api_token, getting_Proxmox_host
from utils.ssh_client import run_commands

logger = activity.logger
_PROXMOX_SSH_USER = os.getenv("PROXMOX_SSH_USER", "root")
_PROXMOX_SSH_PASS = os.getenv("PROXMOX_SSH_PASS", "")


# ── helpers ───────────────────────────────────────────────────────────────────

def _netmask_to_cidr(netmask: str) -> int:
    return ipaddress.IPv4Network(f"0.0.0.0/{netmask}").prefixlen


def _proxmox_ssh_creds(cluster) -> tuple:
    """Return (proxmox_ip, ssh_user, ssh_pass) for direct SSH into Proxmox node."""
    proxmox_url = getting_Proxmox_host(cluster)
    proxmox_ip  = proxmox_url.replace("https://", "").replace("http://", "").split(":")[0]
    user        = cluster.username.split("@")[0] if "@" in (cluster.username or "") else (cluster.username or _PROXMOX_SSH_USER)
    password    = cluster.password or _PROXMOX_SSH_PASS
    return proxmox_ip, user, password


# ── Activities ────────────────────────────────────────────────────────────────

@activity.defn(name="LXC-Upload-Template-to-Proxmox")
def upload_lxc_to_proxmox_activity(payload: dict) -> dict:
    """
    Step 1 — HTTP GET se file download karo (devraq.dev.team/{dir}/{file}) → /tmp
    Step 2 — /tmp se SFTP karke Proxmox /var/lib/vz/dump/ pe upload karo
    """
    db: Session = SessionLocal()
    try:
        cluster_id      = payload["cluster_id"]
        library_item_id = payload["library_item_id"]

        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster:
            raise RuntimeError(f"Cluster {cluster_id} not found")

        item = db.query(LibraryItem).filter(LibraryItem.id == library_item_id).first()
        if not item:
            raise RuntimeError(f"Library item {library_item_id} not found")
        if item.status != "ready":
            raise RuntimeError(f"Library item {library_item_id} not ready (status={item.status})")

        # ── Step 1: HTTP GET with Range resume → temp file ───────────────────
        # item.file_path = WebDAV URL (e.g. https://devraq.dev.team/library/harbor/file)
        # Firewall har 30s pe connection kaate to bhi Range header se resume ho jaata hai.
        file_url   = item.file_path
        temp_path  = f"/tmp/lxc_deploy_{item.id}_{item.file_name}"
        chunk_size = 5 * 1024 * 1024  # 5 MB
        max_retries = 200              # 200 x 5MB = 1TB tak handle ho sakta hai

        # Total size HEAD request se pata karo
        try:
            head_resp  = requests.head(file_url, verify=False, timeout=30)
            total_size = int(head_resp.headers.get("content-length", 0))
        except Exception as exc:
            raise RuntimeError(f"HEAD request failed {file_url}: {exc}")

        activity.heartbeat(f"Download start: {file_url} ({total_size:,} bytes)")
        logger.info(f"[LXC] Resume download: {file_url} → {temp_path} total={total_size:,}")

        bytes_done = 0

        for attempt in range(1, max_retries + 1):
            # Partial file check — resume karo agar pehle se kuch liya hua hai
            if os.path.exists(temp_path):
                bytes_done = os.path.getsize(temp_path)

            if total_size > 0 and bytes_done >= total_size:
                break  # poora aa gaya

            headers = {}
            if bytes_done > 0:
                headers["Range"] = f"bytes={bytes_done}-"
                logger.info(f"[LXC] Resuming from {bytes_done:,} bytes (attempt {attempt})")

            try:
                with requests.get(
                    file_url,
                    headers=headers,
                    stream=True,
                    verify=False,
                    timeout=60,          # 60s inactivity timeout — connection drop pata chalega
                ) as resp:
                    if resp.status_code not in (200, 206):
                        raise RuntimeError(f"Server returned {resp.status_code}")

                    with open(temp_path, "ab") as f:
                        for chunk in resp.iter_content(chunk_size=chunk_size):
                            if not chunk:
                                continue
                            f.write(chunk)
                            bytes_done += len(chunk)
                            pct = int(bytes_done * 100 / total_size) if total_size else 0
                            activity.heartbeat(
                                f"Download {pct}% ({bytes_done:,}/{total_size:,} bytes) attempt={attempt}"
                            )

            except Exception as exc:
                logger.warning(f"[LXC] Download interrupted at {bytes_done:,} bytes: {exc} — retry in 5s")
                time.sleep(5)
                continue

            # Agar yahan pahunche to is attempt mein connection normal end hua
            break

        file_size = os.path.getsize(temp_path) if os.path.exists(temp_path) else 0
        if total_size > 0 and file_size < total_size:
            raise RuntimeError(
                f"Download incomplete: got {file_size:,} of {total_size:,} bytes after {max_retries} attempts"
            )

        # ── Integrity check — corrupt file Proxmox pe jaane se pehle pakad lo ──
        # zstd valid file ke pehle 4 bytes hamesha 0x28 0xB5 0x2F 0xFD hote hain.
        # Agar file incomplete/html-error/empty hai to ye bytes match nahi honge.
        activity.heartbeat("Verifying archive integrity...")
        _ZSTD_MAGIC = b'\x28\xb5\x2f\xfd'
        with open(temp_path, 'rb') as _f:
            _magic = _f.read(4)
        if _magic != _ZSTD_MAGIC:
            try:
                os.remove(temp_path)
            except OSError:
                pass
            raise RuntimeError(
                f"Downloaded file is not a valid zstd archive "
                f"(magic bytes={_magic.hex()}, expected=28b52ffd). "
                f"Source file may be corrupt — re-upload karo."
            )
        logger.info(f"[LXC] Download complete + verified: {file_size:,} bytes → {temp_path}")

        # ── Step 2: SFTP temp → Proxmox /var/lib/vz/dump/ ───────────────────
        # set_pipelined(True) large files mein silently truncate karta hai —
        # isliye sftp.put() use karo jo internally reliable chunked transfer karta hai.
        proxmox_ip, ssh_user, ssh_pass = _proxmox_ssh_creds(cluster)
        remote_path = f"/var/lib/vz/dump/{item.file_name}"

        activity.heartbeat(f"SFTP {item.file_name} ({file_size:,} bytes) → Proxmox {proxmox_ip}")
        logger.info(f"[LXC] SFTP {temp_path} → {proxmox_ip}:{remote_path} ({file_size:,} bytes)")

        prx_ssh = paramiko.SSHClient()
        prx_ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        prx_ssh.connect(proxmox_ip, username=ssh_user, password=ssh_pass, timeout=30)

        try:
            _, stdout, _ = prx_ssh.exec_command("mkdir -p /var/lib/vz/dump && echo ok")
            stdout.channel.recv_exit_status()

            sftp = prx_ssh.open_sftp()

            def _sftp_progress(transferred: int, total: int):
                pct = int(transferred * 100 / total) if total else 0
                activity.heartbeat(f"SFTP to Proxmox {pct}% ({transferred:,}/{total:,} bytes)")

            # sftp.put() — pipelining nahi, ACK ka wait karta hai, large files safe
            sftp.put(temp_path, remote_path, callback=_sftp_progress)

            # Transfer ke baad remote file size verify karo
            remote_size = sftp.stat(remote_path).st_size
            if remote_size != file_size:
                raise RuntimeError(
                    f"SFTP transfer incomplete: Proxmox pe {remote_size:,} bytes, "
                    f"expected {file_size:,} bytes"
                )

            sftp.close()
            logger.info(f"[LXC] SFTP complete + verified → {remote_path} ({remote_size:,} bytes)")
        finally:
            prx_ssh.close()
            try:
                os.remove(temp_path)
            except OSError:
                pass

        return {
            "dump_path":  remote_path,
            "proxmox_ip": proxmox_ip,
            "ssh_user":   ssh_user,
            "ssh_pass":   ssh_pass,
        }
    finally:
        db.close()


@activity.defn(name="LXC-Restore-and-Clone-Container")
def restore_lxc_container_activity(payload: dict) -> dict:
    """
    Template dump ko Proxmox pe deploy karta hai (clone flow):
      1. pct restore <tmp_vmid> <dump>  → template CT banta hai
      2. pct clone <tmp_vmid> <clone_vmid> --full 1  → actual running container
      3. pct destroy <tmp_vmid>  → temp template hatao
      4. pct set <clone_vmid> network + features
      5. pct start <clone_vmid>
    """
    db: Session = SessionLocal()
    try:
        cluster_id      = payload["cluster_id"]
        job_id          = payload["job_id"]
        dump_path       = payload["dump_path"]
        proxmox_ip      = payload["proxmox_ip"]
        prx_ssh_user    = payload["proxmox_ssh_user"]
        prx_ssh_pass    = payload["proxmox_ssh_pass"]
        ip_address      = payload["ip_address"]
        ip_pool_id      = payload["ip_pool_id"]
        name            = payload["name"]
        storage         = payload.get("storage", "local-lvm")
        bridge          = payload.get("bridge", "vmbr0")

        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster:
            raise RuntimeError(f"Cluster {cluster_id} not found")

        api_token    = get_api_token(db, cluster.name)
        headers      = {"Authorization": f"PVEAPIToken={api_token}"}
        PROXMOX_HOST = getting_Proxmox_host(cluster)

        # Get IP pool info for CIDR / gateway / DNS
        pool = db.query(IPSModel).filter(IPSModel.id == ip_pool_id).first()
        if not pool:
            raise RuntimeError(f"IP pool {ip_pool_id} not found")
        cidr    = _netmask_to_cidr(pool.Subnet)
        gateway = pool.Gateway
        dns     = pool.DNS[0] if pool.DNS else "8.8.8.8"

        # Discover Proxmox node name from API
        node_resp = requests.get(
            f"{PROXMOX_HOST}/api2/json/nodes",
            headers=headers, verify=False, timeout=10,
        )
        nodes    = node_resp.json().get("data", [])
        pve_node = nodes[0]["node"] if nodes else "pve"

        # Sanitize hostname
        hostname = re.sub(r"[^a-zA-Z0-9-]", "-", name).strip("-").lower()[:63] or "lxc-container"
        if hostname[0].isdigit():
            hostname = "lxc-" + hostname

        # ── Step 1: VMID for temp template restore ────────────────────────────
        vmid_resp = requests.get(
            f"{PROXMOX_HOST}/api2/json/cluster/nextid",
            headers=headers, verify=False, timeout=10,
        )
        tmp_vmid = int(vmid_resp.json()["data"])
        logger.info(f"[LXC] Restoring dump as template VMID={tmp_vmid} node={pve_node}")

        # ── Pre-restore: file verify + LVM space check + cleanup ────────────
        activity.heartbeat("Verifying dump + disk space before restore...")
        pre_checks = [
            # File exist + zstd valid?
            f"test -f {dump_path} || (echo 'ERROR: dump not found' && exit 1)",
            f"zstd -t {dump_path} && echo '[OK] zstd valid' || (echo '[FAIL] zstd corrupt' && exit 1)",
            # LVM free space — pct restore ke liye kam se kam uncompressed size chahiye
            "vgdisplay pve 2>/dev/null | grep 'Free' || true",
            f"df -h /var/lib/vz/ | tail -1",
            # tmp_vmid pehle se exist kare to destroy karo (previous failed attempt cleanup)
            f"pct status {tmp_vmid} 2>/dev/null && "
            f"(echo 'Cleaning up stale VMID {tmp_vmid}...' && "
            f"pct stop {tmp_vmid} 2>/dev/null; "
            f"pct destroy {tmp_vmid} --force 1 --purge 1 2>/dev/null) || true",
        ]
        run_commands(proxmox_ip, prx_ssh_user, prx_ssh_pass, pre_checks, timeout=120)
        logger.info(f"[LXC] Pre-restore verification passed for {dump_path}")

        activity.heartbeat(f"Restoring dump as template (VMID {tmp_vmid})")
        restore_cmd = (
            f"pct restore {tmp_vmid} {dump_path} "
            f"--storage {storage} "
            f"--force 1"
        )
        run_commands(proxmox_ip, prx_ssh_user, prx_ssh_pass, [restore_cmd], timeout=7200)
        logger.info(f"[LXC] Template restore complete VMID={tmp_vmid}")

        # ── Step 2: Get fresh VMID for clone (tmp_vmid now in use) ───────────
        vmid_resp2 = requests.get(
            f"{PROXMOX_HOST}/api2/json/cluster/nextid",
            headers=headers, verify=False, timeout=10,
        )
        clone_vmid = int(vmid_resp2.json()["data"])
        logger.info(f"[LXC] Cloning template {tmp_vmid} → container {clone_vmid}")

        activity.heartbeat(f"Cloning template {tmp_vmid} → container {clone_vmid}")
        clone_cmd = (
            f"pct clone {tmp_vmid} {clone_vmid} "
            f"--full 1 "
            f"--hostname {hostname} "
            f"--storage {storage}"
        )
        run_commands(proxmox_ip, prx_ssh_user, prx_ssh_pass, [clone_cmd], timeout=3600)
        logger.info(f"[LXC] Clone complete VMID={clone_vmid}")

        # ── Step 3: Destroy temp template ────────────────────────────────────
        activity.heartbeat(f"Removing temp template VMID {tmp_vmid}")
        run_commands(proxmox_ip, prx_ssh_user, prx_ssh_pass,
                     [f"pct destroy {tmp_vmid} --force 1 --purge 1"], timeout=120)

        # ── Step 4: Configure clone + start ──────────────────────────────────
        activity.heartbeat(f"Configuring and starting container {clone_vmid}")
        run_commands(proxmox_ip, prx_ssh_user, prx_ssh_pass, [
            f"pct set {clone_vmid} --features nesting=1,keyctl=1",
            f"pct set {clone_vmid} --net0 name=eth0,bridge={bridge},ip={ip_address}/{cidr},gw={gateway},firewall=0",
            f"pct set {clone_vmid} --nameserver {dns}",
            f"pct start {clone_vmid}",
        ], timeout=300)
        logger.info(f"[LXC] Container {clone_vmid} started at {ip_address}")

        # ── DB: update job with clone VMID ────────────────────────────────────
        job = db.query(LXCRestoreJob).filter(LXCRestoreJob.id == job_id).first()
        if job:
            job.vmid   = clone_vmid
            job.node   = pve_node
            job.status = "container_started"
            db.commit()

        ip_entry = db.query(IPEntry).filter(IPEntry.ip == ip_address).first()
        if ip_entry:
            ip_entry.vm_id = str(clone_vmid)
            db.commit()

        return {"vmid": clone_vmid, "node": pve_node}
    finally:
        db.close()


@activity.defn(name="LXC-Setup-SSH-Iptables-and-Services")
def setup_lxc_container_activity(payload: dict) -> dict:
    """
    1. Enable SSH via pct exec from Proxmox host
    2. Start stopped podman containers (Harbor services may not auto-start after clone)
    3. Wait for all containers to be Up/healthy
    4. Reset iptables
    5. Restart nginx container (find by name dynamically — could be 'nginx', 'harbor-nginx', etc.)
    6. ORAS login to local Harbor (read admin password from harbor.yml inside container)
    """
    proxmox_ip     = payload["proxmox_ip"]
    prx_ssh_user   = payload["proxmox_ssh_user"]
    prx_ssh_pass   = payload["proxmox_ssh_pass"]
    vmid           = payload["vmid"]
    container_ip   = payload["container_ip"]
    container_user = payload.get("container_ssh_user", "root")
    container_pass = payload.get("container_ssh_pass", "")

    activity.heartbeat("Enabling SSH inside container via pct exec")

    # ── Step 1: Enable SSH from Proxmox host ──────────────────────────────────
    wait_running = (
        f"for i in $(seq 1 30); do "
        f"  pct status {vmid} 2>/dev/null | grep -q running && echo running && break; "
        f"  sleep 3; "
        f"done"
    )
    enable_ssh = (
        f"pct exec {vmid} -- bash -c "
        f"'systemctl enable ssh --now 2>/dev/null || systemctl enable sshd --now 2>/dev/null || true'"
    )
    allow_root = (
        f"pct exec {vmid} -- bash -c "
        f"'sed -i \"s/^#*PermitRootLogin.*/PermitRootLogin yes/\" /etc/ssh/sshd_config; "
        f"sed -i \"s/^PasswordAuthentication no/PasswordAuthentication yes/\" /etc/ssh/sshd_config; "
        f"systemctl restart ssh 2>/dev/null || systemctl restart sshd 2>/dev/null || true'"
    )

    pct_cmds = [wait_running, enable_ssh, allow_root]
    if container_pass:
        pct_cmds.append(
            f"pct exec {vmid} -- bash -c 'echo \"{container_user}:{container_pass}\" | chpasswd'"
        )
    run_commands(proxmox_ip, prx_ssh_user, prx_ssh_pass, pct_cmds, timeout=120)
    logger.info(f"[LXC] SSH enabled in container {vmid}")
    activity.heartbeat(f"SSH ready — waiting for Harbor containers at {container_ip}")

    # ── Step 2: /run/user/0 fix → stale cleanup → prepare → harbor-start → wait
    wait_healthy = (
        # /run/user/0 is tmpfs — wiped on clone/restart → stale podman netns.
        "echo '=== [1] Ensuring /run/user/0 ==='; "
        "mkdir -p /run/user/0; chmod 700 /run/user/0; "
        "loginctl enable-linger root 2>/dev/null || true; "

        # Remove stale containers (bypass podman ps — it fails on stale netns)
        "echo '=== [2] Removing stale containers ==='; "
        "podman rm --all --force 2>/dev/null || true; "

        # harbor.yml mein hostname AND external_url dono update karo.
        # external_url overrides hostname for EXT_ENDPOINT — agar sirf hostname
        # update karo aur external_url purana rahe to token URL abhi bhi purana IP use karta hai.
        # Fallback: common/config/core/env mein EXT_ENDPOINT directly bhi update karo.
        f"NEW_IP='{container_ip}'; "
        # Search /usr/local/src/harbor first (confirmed install path), then fallbacks
        "HARBOR_YML=$(find /usr/local/src/harbor /opt/harbor /root/harbor /etc/harbor "
        "  -maxdepth 3 -name 'harbor.yml' 2>/dev/null | head -1); "
        "if [ -n \"$HARBOR_YML\" ]; then "
        "  HARBOR_DIR=$(dirname \"$HARBOR_YML\"); "
        "  echo \"=== [3] harbor.yml: $HARBOR_YML → hostname+external_url=$NEW_IP ===\"; "
        "  sed -i \"s|^hostname:.*|hostname: $NEW_IP|\" \"$HARBOR_YML\"; "
        "  sed -i \"s|^external_url:.*|external_url: http://$NEW_IP|\" \"$HARBOR_YML\"; "
        "  sed -i \"s|^#\\s*external_url:.*|external_url: http://$NEW_IP|\" \"$HARBOR_YML\"; "
        "  if [ -f \"$HARBOR_DIR/prepare\" ]; then "
        "    echo '=== [4] Running prepare ==='; "
        "    cd \"$HARBOR_DIR\" && python3 prepare 2>&1 || ./prepare 2>&1; "
        "  fi; "
        "  CORE_ENV=\"$HARBOR_DIR/common/config/core/env\"; "
        "  if [ -f \"$CORE_ENV\" ]; then "
        "    sed -i \"s|EXT_ENDPOINT=http://[^/]*|EXT_ENDPOINT=http://$NEW_IP|g\" \"$CORE_ENV\"; "
        "    echo \"  EXT_ENDPOINT: $(grep EXT_ENDPOINT $CORE_ENV)\"; "
        "  fi; "
        "else "
        "  echo 'WARNING: harbor.yml not found'; "
        "fi; "

        # Start Harbor fresh with updated configs
        "echo '=== [5] Running harbor-start.sh ==='; "
        "chmod +x /usr/local/bin/harbor-start.sh; "
        "/usr/local/bin/harbor-start.sh 2>&1; "
        "sleep 5; "

        # Wait for all containers Up (30 x 10s = 5 min max)
        "for i in $(seq 1 30); do "
        "  total=$(podman ps -a --format '{{.Names}}' 2>/dev/null | wc -l); "
        "  not_up=$(podman ps -a --format '{{.Status}}' 2>/dev/null | grep -v '^Up' | wc -l); "
        "  if [ \"$total\" -gt 0 ] && [ \"$not_up\" -eq 0 ]; then "
        "    echo \"All $total containers healthy\"; break; "
        "  fi; "
        "  echo \"Waiting... total=$total not_up=$not_up attempt=$i/30\"; "
        "  sleep 10; "
        "done; "
        "podman ps 2>/dev/null"
    )

    # ── Step 3: Restart harbor-jobservice (starts before harbor-core is ready) -
    restart_jobservice = (
        "echo 'Restarting harbor-jobservice...'; "
        "podman restart harbor-jobservice 2>&1; "
        "sleep 5; "
        "podman logs --tail 5 harbor-jobservice 2>/dev/null || true"
    )

    activity.heartbeat("Step 2/2 — Prepare + Harbor start + waiting healthy (max 5 min)")
    run_commands(container_ip, container_user, container_pass, [wait_healthy], timeout=500)
    logger.info(f"[LXC] Containers healthy on {container_ip}")

    activity.heartbeat("Step 2/2 — Restarting harbor-jobservice")
    run_commands(container_ip, container_user, container_pass, [restart_jobservice], timeout=60)

    logger.info(f"[LXC] Setup complete — container at {container_ip}")
    return {"container_ip": container_ip, "status": "running"}


@activity.defn(name="Harbor-ORAS-Login")
def oras_login_to_harbor_activity(payload: dict) -> dict:
    """
    1. /usr/local/src/harbor/common/config/core/env mein EXT_ENDPOINT update karo
    2. harbor-core restart karo
    3. HARBOR_ADMIN_PASSWORD wahan se padh ke oras login karo
    """
    container_ip   = payload["container_ip"]
    container_user = payload.get("container_ssh_user", "root")
    container_pass = payload.get("container_ssh_pass", "")

    activity.heartbeat(f"Fixing EXT_ENDPOINT and ORAS login to {container_ip}")

    oras_cmd = (
        f"HARBOR_IP='{container_ip}'; "

        # core/env find karo (primary: /usr/local/src/harbor, fallback: /opt/harbor)
        "CORE_ENV=$(find /usr/local/src/harbor /opt/harbor /root/harbor "
        "  -path '*/common/config/core/env' 2>/dev/null | head -1); "
        "if [ -z \"$CORE_ENV\" ]; then "
        "  echo 'ERROR: core/env not found'; exit 1; "
        "fi; "
        "echo \"core/env: $CORE_ENV\"; "

        # EXT_ENDPOINT update karo — koi bhi purana IP replace hoga
        "sed -i \"s|EXT_ENDPOINT=http://[^/]*|EXT_ENDPOINT=http://$HARBOR_IP|g\" \"$CORE_ENV\"; "
        "echo \"Updated: $(grep EXT_ENDPOINT $CORE_ENV)\"; "

        # harbor-core restart karo nayi config ke saath
        "echo 'Restarting harbor-core...'; "
        "podman restart harbor-core 2>&1; "
        "echo 'Waiting 60s for harbor-core to become healthy...'; "
        "sleep 60; "

        # HARBOR_ADMIN_PASSWORD core/env se padho
        "HARBOR_PASS=$(grep 'HARBOR_ADMIN_PASSWORD' \"$CORE_ENV\" | cut -d'=' -f2 | tr -d '\\r\\n'); "
        "HARBOR_PASS=${HARBOR_PASS:-Harbor12345}; "
        "echo \"Password found: $([ -n \"$HARBOR_PASS\" ] && echo yes || echo no)\"; "

        # ORAS login
        "echo \"ORAS login → $HARBOR_IP\"; "
        "oras login \"$HARBOR_IP\" -u admin -p \"$HARBOR_PASS\" --plain-http 2>&1"
    )

    run_commands(container_ip, container_user, container_pass, [oras_cmd], timeout=90)
    logger.info(f"[LXC] ORAS login done for {container_ip}")
    return {"harbor_ip": container_ip, "status": "logged_in"}


_FAILURE_STATUSES = {
    "UPLOAD_FAILED",
    "RESTORE_FAILED",
    "SETUP_FAILED",
    "ORAS_FAILED",
}


@activity.defn(name="LXC-Update-Job-Status-in-DB")
def update_lxc_job_status_activity(payload: dict) -> None:
    """Final DB status update for the LXC restore job.
    Failure statuses pe IP automatically release ho jaata hai Ip_Entries table mein."""
    db: Session = SessionLocal()
    try:
        job = db.query(LXCRestoreJob).filter(LXCRestoreJob.id == payload["job_id"]).first()
        if not job:
            return

        for key, val in payload.items():
            if key != "job_id" and hasattr(job, key):
                setattr(job, key, val)
        db.commit()

        # Failure pe IP wapas "unused" mark karo taaki dobaara allocate ho sake
        new_status = payload.get("status", "")
        if new_status in _FAILURE_STATUSES and job.ip_address:
            ip_entry = (
                db.query(IPEntry)
                .filter(IPEntry.ip == job.ip_address)
                .first()
            )
            if ip_entry:
                ip_entry.status = "unused"
                ip_entry.vm_id  = None
                db.commit()
                logger.info(
                    f"[LXC] IP {job.ip_address} released (job={job.id} status={new_status})"
                )
    finally:
        db.close()

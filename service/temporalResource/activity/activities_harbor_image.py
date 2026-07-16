import logging
import os

import paramiko
from temporalio import activity
from sqlalchemy.orm import Session

from db_configuration.config import SessionLocal
from models.harbor_image_model import HarborImage
from utils.ssh_client import run_commands

logger = logging.getLogger(__name__)


@activity.defn(name="Harbor-Image-SFTP-and-Push")
def sftp_and_push_harbor_image_activity(payload: dict) -> dict:
    """
    1. Temp file SFTP karo Harbor LXC container pe
    2. Harbor project create karo (agar exist nahi karta)
    3. podman load + tag + push
    4. Temp file cleanup (local + remote)
    """
    image_id     = payload["image_id"]
    temp_path    = payload["temp_path"]       # local temp file path
    machine_ip   = payload["machine_ip"]
    ssh_user     = payload["ssh_user"]
    ssh_pass     = payload["ssh_pass"]
    image_name   = payload["image_name"]      # e.g. "openwebui"
    image_tag    = payload.get("image_tag", "latest")
    project      = payload.get("project", "library")
    file_name    = payload.get("file_name", os.path.basename(temp_path))

    remote_temp  = f"/tmp/harbor_img_{image_id}_{file_name}"
    harbor_image = f"127.0.0.1/{project}/{image_name}:{image_tag}"
    file_size    = os.path.getsize(temp_path)

    db: Session = SessionLocal()
    try:
        # ── Step 1: SFTP local temp → Harbor container ────────────────────────
        activity.heartbeat(f"SFTP {file_name} ({file_size:,} bytes) → {machine_ip}")
        logger.info(f"[HarborImage] SFTP {temp_path} → {machine_ip}:{remote_temp}")

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(machine_ip, username=ssh_user, password=ssh_pass, timeout=30)

        try:
            sftp = ssh.open_sftp()

            def _progress(transferred: int, total: int):
                pct = int(transferred * 100 / total) if total else 0
                activity.heartbeat(f"SFTP {pct}% ({transferred:,}/{total:,} bytes)")

            sftp.put(temp_path, remote_temp, callback=_progress)

            remote_size = sftp.stat(remote_temp).st_size
            if remote_size != file_size:
                raise RuntimeError(
                    f"SFTP incomplete: container pe {remote_size:,}, expected {file_size:,}"
                )
            sftp.close()
            logger.info(f"[HarborImage] SFTP complete: {remote_temp} ({remote_size:,} bytes)")
        finally:
            ssh.close()

        # Local temp file ab kaam nahi — delete karo
        try:
            os.remove(temp_path)
        except OSError:
            pass

        # ── Step 2: Harbor project create + podman load + push ────────────────
        activity.heartbeat("Harbor project create + podman load + push...")

        push_script = (
            # Harbor admin password
            "CORE_ENV=$(find /usr/local/src/harbor /opt/harbor /root/harbor "
            "  -path '*/common/config/core/env' 2>/dev/null | head -1); "
            "HARBOR_PASS=$(grep 'HARBOR_ADMIN_PASSWORD' \"$CORE_ENV\" 2>/dev/null "
            "  | cut -d'=' -f2 | tr -d '\\r\\n'); "
            "HARBOR_PASS=${HARBOR_PASS:-Harbor12345}; "

            # Harbor project create karo (ignore 409 = already exists)
            f"echo '=== [1] Creating Harbor project: {project} ==='; "
            f"HTTP_CODE=$(curl -s -o /dev/null -w '%{{http_code}}' "
            f"  -X POST 'http://127.0.0.1/api/v2.0/projects' "
            f"  -H 'Content-Type: application/json' "
            f"  -u \"admin:$HARBOR_PASS\" "
            f"  -d '{{\"project_name\":\"{project}\",\"public\":true}}'); "
            f"echo \"Harbor project create response: $HTTP_CODE\"; "

            # podman login
            f"echo '=== [2] podman login ==='; "
            f"podman login 127.0.0.1 -u admin -p \"$HARBOR_PASS\" --tls-verify=false 2>&1 || "
            f"(echo '[FAIL] podman login failed' && exit 1); "

            # podman load
            f"echo '=== [3] podman load {file_name} ==='; "
            f"LOAD_OUT=$(podman load -i '{remote_temp}' 2>&1); "
            f"echo \"$LOAD_OUT\"; "
            f"LOADED_REF=$(echo \"$LOAD_OUT\" | grep -i 'Loaded image' | "
            f"  sed 's/.*Loaded image[s]*: //' | head -1 | tr -d '\\r\\n'); "
            f"if [ -z \"$LOADED_REF\" ]; then "
            # fallback: latest image from list
            f"  LOADED_REF=$(podman images --format '{{{{.Repository}}}}:{{{{.Tag}}}}' | head -1); "
            f"fi; "
            f"echo \"Loaded: $LOADED_REF\"; "
            f"[ -n \"$LOADED_REF\" ] || (echo '[FAIL] No image loaded' && exit 1); "

            # tag
            f"echo '=== [4] podman tag → {harbor_image} ==='; "
            f"podman tag \"$LOADED_REF\" '{harbor_image}' 2>&1 || "
            f"(echo '[FAIL] tag failed' && exit 1); "

            # push
            f"echo '=== [5] podman push → {harbor_image} ==='; "
            f"podman push '{harbor_image}' --tls-verify=false 2>&1 || "
            f"(echo '[FAIL] push failed' && exit 1); "
            f"echo '[OK] Image pushed: {harbor_image}'; "

            # cleanup remote temp
            f"rm -f '{remote_temp}' && echo '[OK] Remote temp cleaned'"
        )

        run_commands(machine_ip, ssh_user, ssh_pass, [push_script], timeout=600)
        logger.info(f"[HarborImage] Push complete: {harbor_image} on {machine_ip}")

        # ── Step 3: DB update ─────────────────────────────────────────────────
        img = db.query(HarborImage).filter(HarborImage.id == image_id).first()
        if img:
            img.status       = "ready"
            img.harbor_image = harbor_image
            db.commit()

        return {"harbor_image": harbor_image, "machine_ip": machine_ip, "status": "ready"}

    except Exception:
        try:
            img = db.query(HarborImage).filter(HarborImage.id == image_id).first()
            if img:
                img.status = "failed"
                db.commit()
        except Exception:
            pass
        # local temp cleanup on failure too
        try:
            os.remove(temp_path)
        except OSError:
            pass
        raise
    finally:
        db.close()

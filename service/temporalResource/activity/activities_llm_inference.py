import os
import re
import json
import base64
import math
import shlex
import time
import threading
import ipaddress
import requests
import paramiko
from urllib.parse import quote

from temporalio import activity
from sqlalchemy.orm import Session

from db_configuration.config import SessionLocal
from models.models import Cluster, Machine
from models.IPs_model import IPEntry, IPSModel
from models.llm_inference_model import LLMInferenceJob
from service import proxmoxService
from service.clusterService import get_api_token, getting_Proxmox_host, root_proxmox_login
from utils.ssh_client import run_commands, reboot_and_wait

import dotenv
dotenv.load_dotenv()

logger = activity.logger
# ssh_user/ssh_pass are NOT sourced from the environment anywhere in this
# file -- every activity requires them as a real key in its own payload
# (payload["ssh_user"]/payload["ssh_pass"]), threaded through from the
# per-pool value persisted on LLMInferenceJob.ssh_user/ssh_pass (see
# llm_inference_controller.py). A missing key raises KeyError immediately
# rather than silently falling back to a shared credential.
_VLLM_LOG_FILE = "vllm_provisioning.log"  # kept on the head node across launches/restarts
# Room on the model volume beyond the weights themselves -- vLLM writes
# compiled kernels, torch caches and tokenizer artifacts alongside them.
_MODEL_VOLUME_HEADROOM_GB = 100
# In-cluster Service DNS name for the artifacts-controller pod (devraq-oras-skopeo-api),
# which does the actual Harbor pull + Proxmox push -- this backend's own network
# position cannot complete that upload reliably (confirmed extensively).
ARTIFACTS_CONTROLLER_URL = os.getenv(
    "ARTIFACTS_CONTROLLER_URL",
    "http://devraq-oras-skopeo-api-svc.thinkcloud.svc.cluster.local:8009",
)


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


def _post_with_heartbeat(url: str, json_body: dict, timeout: int, heartbeat_msg: str,
                          heartbeat_interval: int = 20) -> requests.Response:
    """
    Runs a blocking requests.post() in a background thread while the main
    thread keeps calling activity.heartbeat() every heartbeat_interval
    seconds. A single blocking HTTP call has no opportunity to heartbeat
    while it's waiting on a response -- for a long-running call (the pod
    pulling/pushing a multi-GB file can easily take minutes), that silence
    exceeds Temporal's heartbeat_timeout and the activity gets killed as
    presumed-dead even though it's still working fine.
    """
    return _request_with_heartbeat("POST", url, timeout, heartbeat_msg, heartbeat_interval, json=json_body, verify=False)


def _request_with_heartbeat(method: str, url: str, timeout: int, heartbeat_msg: str,
                             heartbeat_interval: int = 20, **kwargs) -> requests.Response:
    """
    Same as _post_with_heartbeat but for any requests.request() call (e.g. a
    form-encoded POST with headers/verify) -- e.g. Proxmox's own VM-create
    call, which imports the disk synchronously and can block for minutes,
    missing every heartbeat during that window otherwise.
    """
    result = {}

    def _do_request():
        try:
            result["response"] = requests.request(method, url, timeout=timeout, **kwargs)
        except Exception as e:
            result["error"] = e

    thread = threading.Thread(target=_do_request, daemon=True)
    thread.start()

    while thread.is_alive():
        thread.join(timeout=heartbeat_interval)
        activity.heartbeat(heartbeat_msg)

    if "error" in result:
        raise result["error"]
    return result["response"]


# ── Activities ────────────────────────────────────────────────────────────────

@activity.defn
def reserve_vmids_activity(payload: dict) -> dict:
    """
    Reserve N unique VMIDs + N unique VM names for a pool BEFORE the per-node
    clones fire in parallel.

    Both /cluster/nextid and generate_machine_name() work off a snapshot of
    "what already exists" -- they don't reserve anything. If each parallel
    clone activity independently computes its own next-id / next-name, two
    nodes cloning at the same moment can both compute the SAME id (e.g. both
    109) and the SAME name (e.g. both "lucky-001"), since neither sees the
    other's not-yet-created VM. Doing both allocations once, sequentially,
    up front — before any parallel clone starts — avoids that race entirely.
    """
    db: Session = SessionLocal()
    try:
        cluster_id    = payload["cluster_id"]
        count         = int(payload["count"])
        pool_name     = payload.get("pool_name", "vm")
        name_template = payload.get("name_template") or f"{pool_name}-{{n:fixed=3}}"

        cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster_data:
            raise RuntimeError(f"Cluster {cluster_id} not found")

        api_token    = get_api_token(db, cluster_data.name)
        headers      = {"Authorization": f"PVEAPIToken={api_token}"}
        PROXMOX_HOST = getting_Proxmox_host(cluster_data)
        if not PROXMOX_HOST:
            raise RuntimeError("No reachable Proxmox host")

        all_vms   = proxmoxService.get_all_cluster_vms(db, cluster_data)
        used_ids  = {int(vm["vmid"]) for vm in all_vms if vm.get("vmid")}

        resp = requests.get(
            f"{PROXMOX_HOST}/api2/json/cluster/nextid",
            headers=headers, verify=False, timeout=10
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"Failed to get next VMID: {resp.text}")
        candidate = int(resp.json()["data"])

        reserved = []
        while len(reserved) < count:
            if candidate not in used_ids:
                reserved.append(candidate)
                used_ids.add(candidate)
            candidate += 1

        existing_names = [vm["name"] for vm in all_vms if "name" in vm and vm["name"]]
        db_names       = [m.name for m in db.query(Machine).all()]
        all_names      = set(existing_names) | set(db_names)
        raw_names      = proxmoxService.generate_machine_name(name_template, list(all_names), count)

        vm_names = []
        for n in raw_names:
            n = re.sub(r'[^a-zA-Z0-9-]', '-', n)
            n = re.sub(r'-+', '-', n).strip('-').lower()
            if n and not n[0].isalpha():
                n = 'vm-' + n
            vm_names.append(n[:63])

        logger.info(f"[cluster {cluster_id}] Reserved VMIDs: {reserved}, names: {vm_names}")
        return {"vmids": reserved, "vm_names": vm_names}
    finally:
        db.close()


def _resolve_harbor_blob(harbor_url, harbor_user, harbor_pass, project, repository, tag):
    """
    OCI manifest fetch -> (digest, layer_size, annotations) (see list_harbor_artifacts).

    Annotations come back from the same response the digest does, so anything
    needing artifact metadata (e.g. size_bytes for volume sizing)
    reads it from here rather than fetching the manifest a second time.

    Note layer_size is the COMPRESSED transfer size -- for provisioning against
    what the artifact occupies on disk, use the size_bytes
    annotation instead (see _model_volume_gb).
    """
    full_name = f"{project}/{repository}"
    resp = requests.get(
        f"{harbor_url.rstrip('/')}/v2/{full_name}/manifests/{tag}",
        auth=(harbor_user, harbor_pass), verify=False, timeout=30,
        headers={"Accept": "application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json"},
    )
    if not resp.ok:
        raise RuntimeError(f"Harbor manifest fetch failed ({resp.status_code}): {resp.text[:300]}")
    manifest = resp.json()
    layers = manifest.get("layers") or []
    if not layers:
        raise RuntimeError(f"No layers found in manifest for {full_name}:{tag}")
    return layers[0]["digest"], layers[0]["size"], (manifest.get("annotations") or {})


def _model_volume_gb(annotations, repository, tag, headroom_gb=_MODEL_VOLUME_HEADROOM_GB):
    """
    Volume size for a model artifact: its on-disk size plus room to work in.

    Sized from size_bytes, not the manifest's layer size -- that one
    is the compressed transfer size and can be several times smaller than what
    lands on disk, which would undersize the volume.
    """
    raw = (annotations or {}).get("size_bytes")
    if not raw:
        raise RuntimeError(
            f"Model '{repository}:{tag}' has no size_bytes annotation -- "
            f"its volume cannot be sized without it."
        )
    model_gb = math.ceil(int(str(raw).strip()) / (1024 ** 3))
    return model_gb + headroom_gb


def _node_api_host(hosts, headers, node_name, fallback):
    """
    The node's OWN API URL, resolved via /cluster/status.

    Uploads must go straight to the node that will hold the file. Sending them
    to another node makes its pveproxy relay the entire body onward, and that
    relay hop is where every large upload stalled during testing.
    """
    for host in hosts:
        try:
            resp = requests.get(f"{host}/api2/json/cluster/status",
                                headers=headers, verify=False, timeout=10)
            if not resp.ok:
                continue
            for entry in resp.json().get("data", []):
                if entry.get("type") == "node" and entry.get("name") == node_name and entry.get("ip"):
                    port = fallback.rsplit(":", 1)[-1]
                    return f"https://{entry['ip']}:{port}"
        except Exception:
            continue
    return fallback


def _assert_storage_space(host, headers, node, storage, needed_bytes, margin=1.05):
    """
    Refuse a doomed upload up front. Running out of space 20 minutes into a
    multi-GB transfer costs the whole transfer; checking first costs a second.
    Unreadable status is not treated as failure -- we only block on a definite no.
    """
    try:
        resp = requests.get(f"{host}/api2/json/nodes/{node}/storage/{storage}/status",
                            headers=headers, verify=False, timeout=15)
        if not resp.ok:
            return
        avail = resp.json().get("data", {}).get("avail")
        if avail is None:
            return
        required = int(needed_bytes * margin)
        if avail < required:
            raise RuntimeError(
                f"Not enough space on {node}:{storage} -- need ~{required:,} bytes, "
                f"only {avail:,} available. Free space or pick a different node."
            )
    except RuntimeError:
        raise
    except Exception:
        return


@activity.defn
def pull_harbor_template_to_pv_activity(payload: dict) -> dict:
    """
    Harbor -> artifacts-controller pod's PV only. Deliberately split from
    the push/VM-create steps (see push_and_create_template_activity) so a
    failure on the Proxmox side (chunked-encoding quirks, space issues,
    VMID collisions) retries just the push -- not a full re-pull of a
    40-50GB+ file from Harbor, which is what happened when this was one
    combined activity.
    """
    db: Session = SessionLocal()
    try:
        cluster_id     = payload["cluster_id"]
        template_storage = payload["template_storage"]
        target_node    = template_storage["node"]
        import_storage = template_storage["storage"]
        harbor_url     = payload["harbor_url"]
        harbor_user    = payload["harbor_user"]
        harbor_pass    = payload["harbor_pass"]
        project        = payload["project"]
        repository     = payload["repository"]
        tag            = payload["tag"]

        cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster_data:
            raise RuntimeError(f"Cluster {cluster_id} not found")

        api_token    = get_api_token(db, cluster_data.name)
        headers      = {"Authorization": f"PVEAPIToken={api_token}"}
        PROXMOX_HOST = getting_Proxmox_host(cluster_data)
        if not PROXMOX_HOST:
            raise RuntimeError("No reachable Proxmox host")
        ALL_HOSTS = _all_hosts(cluster_data) or [PROXMOX_HOST]
        node_host = _node_api_host(ALL_HOSTS, headers, target_node, PROXMOX_HOST)

        digest, blob_size, _ann = _resolve_harbor_blob(harbor_url, harbor_user, harbor_pass, project, repository, tag)
        _assert_storage_space(node_host, headers, target_node, import_storage, blob_size)

        resp = requests.get(f"{node_host}/api2/json/cluster/nextid", headers=headers, verify=False, timeout=10)
        if resp.status_code >= 400:
            raise RuntimeError(f"Failed to get next VMID: {resp.text}")
        new_vmid = int(resp.json()["data"])

        filename = f"llm-template-{new_vmid}-{int(time.time())}.qcow2"
        activity.heartbeat(f"Pulling {repository}:{tag} -> PV")

        harbor_plain_http = harbor_url.startswith("http://")
        harbor_host = harbor_url.split("://", 1)[-1].rstrip("/")

        pull_resp = _post_with_heartbeat(
            f"{ARTIFACTS_CONTROLLER_URL}/pull/artifact",
            json_body={
                "harbor_url": harbor_host,
                "username": harbor_user,
                "password": harbor_pass,
                "project": project,
                "artifact_name": repository,
                "tag": tag,
                "plain_http": harbor_plain_http,
                # Unique per pull so concurrent pool creations (multi-node)
                # can't collide on the same staged file.
                "dest_dir": f"/library/harbor/pull_artifacts/{new_vmid}",
            },
            timeout=7200,
            heartbeat_msg=f"Pulling {repository}:{tag} -> PV",
        )
        if pull_resp.status_code >= 400:
            raise RuntimeError(f"Artifact pull failed ({pull_resp.status_code}): {pull_resp.text[:500]}")
        pull_data = pull_resp.json()
        pulled_files = pull_data.get("files") or []
        if not pulled_files:
            raise RuntimeError(f"Artifact pull returned no files: {pull_data}")
        staged_path = f"{pull_data['dest_dir']}/{pulled_files[0]}"
        logger.info(f"[cluster {cluster_id}] Pulled {repository}:{tag} to {staged_path}")

        return {"staged_path": staged_path, "new_vmid": new_vmid, "filename": filename}
    finally:
        db.close()


def _resolve_node_host(db, cluster_id, target_node):
    """Shared setup for the push/create/convert activities below."""
    cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster_data:
        raise RuntimeError(f"Cluster {cluster_id} not found")
    api_token    = get_api_token(db, cluster_data.name)
    headers      = {"Authorization": f"PVEAPIToken={api_token}"}
    PROXMOX_HOST = getting_Proxmox_host(cluster_data)
    if not PROXMOX_HOST:
        raise RuntimeError("No reachable Proxmox host")
    ALL_HOSTS = _all_hosts(cluster_data) or [PROXMOX_HOST]
    node_host = _node_api_host(ALL_HOSTS, headers, target_node, PROXMOX_HOST)
    return headers, api_token, node_host, ALL_HOSTS


@activity.defn
def push_template_to_proxmox_activity(payload: dict) -> dict:
    """
    PV -> Proxmox storage only (upload, no VM involved yet). Split out on
    its own -- and given a more generous retry policy in the workflow --
    because this is specifically the step that hit the chunked-encoding/
    HTTP1.0 quirks during testing; retrying it never re-touches Harbor NOR
    re-runs VM creation once this step itself succeeds.
    """
    db: Session = SessionLocal()
    try:
        cluster_id  = payload["cluster_id"]
        staged_path = payload["staged_path"]
        filename    = payload["filename"]
        template_storage = payload["template_storage"]
        target_node    = template_storage["node"]
        import_storage = template_storage["storage"]

        headers, api_token, node_host, _ = _resolve_node_host(db, cluster_id, target_node)
        node_ip = node_host.split("://", 1)[-1].split(":", 1)[0]

        push_resp = _post_with_heartbeat(
            f"{ARTIFACTS_CONTROLLER_URL}/push/artifact/proxmox",
            json_body={
                "file_path": staged_path,
                "node_host": node_ip,
                "node_name": target_node,
                "filename": filename,
                "proxmox_token": api_token,
                "storage": import_storage,
                "tls_verify": False,
                "poll_timeout": 3600,
            },
            timeout=7500,
            heartbeat_msg=f"Pushing {filename} -> {target_node}:{import_storage}",
        )
        if push_resp.status_code >= 400:
            raise RuntimeError(f"Proxmox push failed ({push_resp.status_code}): {push_resp.text[:500]}")
        logger.info(f"[cluster {cluster_id}] Push to {target_node}:{import_storage} completed")

        return {"import_volid": f"{import_storage}:import/{filename}"}
    finally:
        db.close()


@activity.defn
def create_vm_from_import_activity(payload: dict) -> dict:
    """
    Creates the VM, importing the already-uploaded disk. Split from the
    push above and the template-conversion below so a failure here doesn't
    re-upload (push already succeeded) and a failure in conversion doesn't
    redo this (VM-create is itself the expensive Proxmox-side step, and
    already has its own internal collision-retry loop).
    """
    db: Session = SessionLocal()
    new_vmid = payload["new_vmid"]
    node_host = None
    headers = None
    template_storage = payload["template_storage"]
    target_node = template_storage["node"]
    import_storage = template_storage["storage"]
    import_volid = payload["import_volid"]
    try:
        cluster_id = payload["cluster_id"]
        storage    = payload.get("storage", "local-lvm")
        cores      = int(payload.get("cores") or 4)
        memory     = int(payload.get("memory") or 8192)
        bridge     = payload.get("network") or "vmbr0"

        headers, api_token, node_host, ALL_HOSTS = _resolve_node_host(db, cluster_id, target_node)

        # ── One call: create the VM and pull the disk into real storage ────
        # Proxmox's /cluster/nextid has no locking -- another workflow
        # (a different pool's template, or its own reserve_vmids_activity)
        # can independently be handed the same "next free" id if neither
        # VM has actually been created yet at the moment each call happens.
        # Rather than trying to prevent that race upfront (not fully
        # possible against an unlocked counter), retry specifically on the
        # collision it produces: the push already happened and doesn't need
        # repeating, so on a "config file already exists" failure this just
        # grabs a fresh id and retries the create call alone.
        max_create_attempts = 5
        for create_attempt in range(1, max_create_attempts + 1):
            activity.heartbeat(f"Creating vmid {new_vmid} from {import_volid}")
            logger.info(f"[cluster {cluster_id}] Creating vmid={new_vmid} import-from={import_volid} "
                        f"(attempt {create_attempt}/{max_create_attempts})")
            create_resp = _request_with_heartbeat(
                "POST", f"{node_host}/api2/json/nodes/{target_node}/qemu",
                timeout=1800, heartbeat_msg=f"Creating vmid {new_vmid} from {import_volid}",
                headers=headers, verify=False,
                data={
                    "vmid":   new_vmid,
                    "name":   f"llm-template-{new_vmid}",
                    "cores":  cores,
                    "memory": memory,
                    "sockets": 1,
                    "cpu":    "host",
                    "numa":   0,
                    "ostype": "l26",
                    "scsihw": "virtio-scsi-single",
                    "agent":  1,
                    "net0":   f"virtio,bridge={bridge},firewall=1",
                    "scsi0":  f"{storage}:0,import-from={import_volid},iothread=1",
                    "ide0":   f"{storage}:cloudinit",
                    "ide2":   "none,media=cdrom",
                    "boot":   "order=scsi0;ide2;net0",
                },
            )
            logger.info(f"[cluster {cluster_id}] Create returned {create_resp.status_code} for vmid={new_vmid}")
            if create_resp.status_code >= 400:
                is_collision = "already exists" in create_resp.text.lower()
                if is_collision and create_attempt < max_create_attempts:
                    logger.warning(f"[cluster {cluster_id}] vmid {new_vmid} collided with another "
                                    f"concurrently-created VM, picking a new one and retrying")
                    resp = requests.get(f"{node_host}/api2/json/cluster/nextid", headers=headers, verify=False, timeout=10)
                    if resp.status_code >= 400:
                        raise RuntimeError(f"Failed to get next VMID after collision: {resp.text}")
                    new_vmid = int(resp.json()["data"])
                    continue
                raise RuntimeError(f"VM create/import failed for vmid {new_vmid}: {create_resp.text}")
            break

        create_upid = create_resp.json().get("data")
        if create_upid:
            _wait_for_task(ALL_HOSTS, headers, target_node, create_upid, timeout=10800)

        logger.info(f"[cluster {cluster_id}] VM created: vmid={new_vmid} node={target_node}")
        return {"vmid": new_vmid}
    except Exception:
        # The VM either doesn't exist yet or import failed part way --
        # either way this attempt made no completed progress worth keeping,
        # so it's safe to clean up and let the retry start fresh. (Unlike
        # the old combined activity, there's no "leave it, it's expensive
        # work" case here -- that case is now template-conversion's
        # problem, once this activity has actually returned successfully.)
        if new_vmid and node_host:
            try:
                requests.delete(
                    f"{node_host}/api2/json/nodes/{target_node}/qemu/{new_vmid}",
                    headers=headers, verify=False, timeout=30,
                )
            except Exception:
                pass
        # Matches the original combined activity's behavior: on any failure
        # after the push succeeded, also remove the staged upload -- it's
        # otherwise orphaned on Proxmox's import storage with nothing left
        # to reference or clean it up later.
        if import_volid and node_host:
            try:
                requests.delete(
                    f"{node_host}/api2/json/nodes/{target_node}/storage/{import_storage}/content/{quote(import_volid, safe='')}",
                    headers=headers, verify=False, timeout=30,
                )
            except Exception:
                pass
        raise
    finally:
        db.close()


@activity.defn
def convert_to_template_activity(payload: dict) -> dict:
    """
    Cleans up the now-redundant staged import file, then converts the
    created VM into a Proxmox template. Split out so a failure here (cheap,
    fast call) never redoes the upload or the VM-create/import, which are
    the two expensive steps.
    """
    db: Session = SessionLocal()
    try:
        cluster_id = payload["cluster_id"]
        new_vmid   = payload["vmid"]
        template_storage = payload["template_storage"]
        target_node    = template_storage["node"]
        import_storage = template_storage["storage"]
        import_volid   = payload["import_volid"]

        headers, api_token, node_host, ALL_HOSTS = _resolve_node_host(db, cluster_id, target_node)

        # ── The staged file is dead weight the moment the disk is imported ─
        # Freeing it here rather than at pool-completion keeps node-local space
        # occupied for minutes instead of the hour of driver/Ray/vLLM setup
        # that follows -- that space is what the next pull needs.
        try:
            requests.delete(
                f"{node_host}/api2/json/nodes/{target_node}/storage/{import_storage}/content/{quote(import_volid, safe='')}",
                headers=headers, verify=False, timeout=60,
            )
        except Exception as cleanup_err:
            logger.warning(f"Could not remove staged disk {import_volid} (non-fatal): {cleanup_err}")

        tmpl_resp = requests.post(
            f"{node_host}/api2/json/nodes/{target_node}/qemu/{new_vmid}/template",
            headers=headers, verify=False, timeout=60,
        )
        if tmpl_resp.status_code >= 400:
            raise RuntimeError(f"qm template failed for vmid {new_vmid}: {tmpl_resp.text}")
        upid = tmpl_resp.json().get("data")
        if upid:
            _wait_for_task(ALL_HOSTS, headers, target_node, upid, timeout=300)

        logger.info(f"[cluster {cluster_id}] Harbor template ready: vmid={new_vmid} node={target_node}")
        return {"template_vmid": new_vmid, "template_node": target_node}
    finally:
        db.close()


def _delete_proxmox_vm(db: Session, cluster_id: int, vmid: int) -> bool:
    """Find and permanently purge a Proxmox VM/template by vmid. True if removed or already absent."""
    cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster_data:
        logger.warning(f"[template-cleanup] Cluster {cluster_id} not found — cannot clean up vmid {vmid}")
        return False
    api_token    = get_api_token(db, cluster_data.name)
    headers      = {"Authorization": f"PVEAPIToken={api_token}"}
    PROXMOX_HOST = getting_Proxmox_host(cluster_data)
    if not PROXMOX_HOST:
        return False
    node = next(
        (vm.get("node") for vm in proxmoxService.get_all_cluster_vms(db, cluster_data)
         if str(vm.get("vmid")) == str(vmid)),
        None,
    )
    if not node:
        logger.info(f"[template-cleanup] vmid {vmid} already absent")
        return True
    resp = requests.delete(
        f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}",
        headers=headers, params={"purge": 1, "destroy-unreferenced-disks": 1},
        verify=False, timeout=60,
    )
    if resp.status_code >= 400 and resp.status_code != 404:
        logger.error(f"[template-cleanup] Could not remove vmid {vmid} on {node}: {resp.text}")
        return False
    logger.info(f"[template-cleanup] vmid {vmid} removed from {node}")
    return True


@activity.defn
def delete_harbor_template_activity(payload: dict) -> dict:
    """
    Removes the intermediate Proxmox template restored from Harbor, right
    after every node has finished cloning from it -- clones are full
    (independent-disk) clones, so nothing depends on the template staying
    around. Runs at the end of a successful clone phase rather than waiting
    for pool deletion, so a Harbor-sourced template never sits around as
    standing storage.
    """
    db: Session = SessionLocal()
    try:
        ok = _delete_proxmox_vm(db, payload["cluster_id"], int(payload["template_vmid"]))
        return {"ok": ok, "template_vmid": payload["template_vmid"]}
    finally:
        db.close()


@activity.defn
def provision_model_volume_activity(payload: dict) -> dict:
    """
    Give the Ray head VM a dedicated volume for the model, mounted at the path
    the model pull already writes to.

    Sized model + headroom so the OS disk never has to grow to fit a model --
    which matters because the OS disk arrives as a fixed-size qcow2 from Harbor
    and enlarging it would mean growing the guest filesystem too.

    Attaches to the head VM only; workers reach the same files over NFS.
    """
    db: Session = SessionLocal()
    try:
        cluster_id = payload["cluster_id"]
        node       = payload["node"]
        vmid       = int(payload["vmid"])
        ip         = payload["ip_address"]
        storage    = payload.get("storage", "local-lvm")
        slot       = payload.get("slot", "scsi1")
        mount_path = payload.get("mount_path", "/vllm_data/hf_cache")
        ssh_user   = payload["ssh_user"]
        ssh_pass   = payload["ssh_pass"]

        # Sized here rather than at create time so a slow or briefly unreachable
        # Harbor is retried by Temporal instead of failing the API request.
        _d, _s, annotations = _resolve_harbor_blob(
            payload["harbor_url"], payload["harbor_user"], payload["harbor_pass"],
            payload["project"], payload["repository"], payload["tag"],
        )
        size_gb = _model_volume_gb(annotations, payload["repository"], payload["tag"])

        cluster_data = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster_data:
            raise RuntimeError(f"Cluster {cluster_id} not found")
        api_token    = get_api_token(db, cluster_data.name)
        headers      = {"Authorization": f"PVEAPIToken={api_token}"}
        PROXMOX_HOST = getting_Proxmox_host(cluster_data)
        ALL_HOSTS    = _all_hosts(cluster_data) or [PROXMOX_HOST]

        # ── Attach the volume ────────────────────────────────────────────
        activity.heartbeat(f"Attaching {size_gb}GB model volume to vmid {vmid}")
        resp = requests.put(
            f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/config",
            headers=headers, verify=False, timeout=120,
            data={slot: f"{storage}:{size_gb}"},
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"Could not attach model volume to vmid {vmid}: {resp.text}")
        upid = resp.json().get("data")
        if upid:
            _wait_for_task(ALL_HOSTS, headers, node, upid, timeout=600)

        # ── Format and mount it in the guest ─────────────────────────────
        # The device letter isn't predictable (it depends on how many disks
        # the guest already sees), so find the one disk that has no partitions
        # and no filesystem signature rather than assuming /dev/sdb.
        activity.heartbeat(f"Formatting model volume on {ip}")
        find_dev = (
            "for d in $(lsblk -dnpo NAME,TYPE | awk '$2==\"disk\"{print $1}'); do "
            "  if [ -z \"$(lsblk -no NAME \"$d\" | tail -n +2)\" ] && "
            "     [ -z \"$(sudo blkid -o value -s TYPE \"$d\" 2>/dev/null)\" ]; then "
            "    echo \"$d\"; break; "
            "  fi; "
            "done"
        )
        results = run_commands(ip, ssh_user, ssh_pass, [
            "sudo partprobe >/dev/null 2>&1 || true",
            find_dev,
        ], timeout=120)
        device = results[-1]["stdout"].strip().split("\n")[-1].strip()
        if not device.startswith("/dev/"):
            raise RuntimeError(
                f"No unformatted disk found on {ip} after attaching {slot} "
                f"-- got {device!r}. The volume may not have been picked up by the guest."
            )
        logger.info(f"[{ip}] Model volume detected as {device}")

        # fstab entry keyed on UUID, not the device name -- device letters can
        # shift across reboots once the GPU or other disks change.
        run_commands(ip, ssh_user, ssh_pass, [
            f"sudo mkfs.ext4 -F -L llm-models {device}",
            f"sudo mkdir -p {mount_path}",
            f"sudo mount {device} {mount_path}",
            (
                f"UUID=$(sudo blkid -o value -s UUID {device}); "
                f"grep -q \"$UUID\" /etc/fstab || "
                f"echo \"UUID=$UUID {mount_path} ext4 defaults,nofail 0 2\" | sudo tee -a /etc/fstab > /dev/null"
            ),
            f"df -h {mount_path}",
        ], timeout=900)

        logger.info(f"[{ip}] Model volume ready: {device} ({size_gb}GB) -> {mount_path}")
        return {"slot": slot, "device": device, "size_gb": size_gb,
                "storage": storage, "mount_path": mount_path}
    finally:
        db.close()


@activity.defn
def pull_harbor_model_to_vm_activity(payload: dict) -> dict:
    """
    Pull a full HuggingFace-format model artifact (a folder: config.json,
    tokenizer files, safetensors shards) from Harbor onto one VM. Returns
    (model_dir, model_file) shaped exactly for configure_llm_node_activity's
    existing LLM_MODEL_PATH + LLM_MODEL_NAME combination logic -- model_dir
    is the BASE directory, model_file is the model's own folder name inside
    it, so LLM_MODEL_PATH/LLM_MODEL_NAME combined lands on the folder that
    actually contains config.json.

    Same relay pattern as pull_harbor_template_to_pv_activity: this
    backend's own network position can't move a multi-GB transfer reliably,
    so the artifacts-controller pod does the actual work -- pulling Harbor ->
    its own PV, then transferring PV -> the VM over SFTP (the whole folder,
    not a single file). This activity just triggers each step and waits.
    """
    ip          = payload["ip_address"]
    ssh_user    = payload["ssh_user"]
    ssh_pass    = payload["ssh_pass"]
    harbor_url  = payload["harbor_url"]
    harbor_user = payload["harbor_user"]
    harbor_pass = payload["harbor_pass"]
    project     = payload["project"]
    repository  = payload["repository"]
    tag         = payload["tag"]
    # Base dir only -- the model's own folder name comes from what Harbor
    # actually returns below, not assumed/constructed here.
    model_base_dir = payload.get("model_dir", "/vllm_data/hf_cache")

    harbor_plain_http = harbor_url.startswith("http://")
    harbor_host = harbor_url.split("://", 1)[-1].rstrip("/")

    activity.heartbeat(f"Pulling model {repository}:{tag} -> PV")
    pull_resp = _post_with_heartbeat(
        f"{ARTIFACTS_CONTROLLER_URL}/pull/artifact",
        json_body={
            "harbor_url": harbor_host,
            "username": harbor_user,
            "password": harbor_pass,
            "project": project,
            "artifact_name": repository,
            "tag": tag,
            "plain_http": harbor_plain_http,
            # Unique per pull so concurrent model pulls can't collide on
            # the same staged folder.
            "dest_dir": f"/library/harbor/pull_artifacts/model-{ip.replace('.', '-')}-{int(time.time())}",
        },
        timeout=7200,
        heartbeat_msg=f"Pulling model {repository}:{tag} -> PV",
    )
    if pull_resp.status_code >= 400:
        raise RuntimeError(f"Model pull failed ({pull_resp.status_code}): {pull_resp.text[:500]}")
    pull_data = pull_resp.json()
    pulled_files = pull_data.get("files") or []
    if not pulled_files:
        raise RuntimeError(f"Model pull returned no files: {pull_data}")
    # The artifact is expected to be one folder (e.g. "qwen2.5-0.5b-instruct")
    # containing everything -- oras pull recreates that structure under
    # dest_dir, so dest_dir's one top-level entry IS that folder.
    model_name = pulled_files[0]
    staged_path = f"{pull_data['dest_dir']}/{model_name}"
    logger.info(f"[{ip}] Pulled model {repository}:{tag} to {staged_path}")

    run_commands(ip, ssh_user, ssh_pass, [f"sudo mkdir -p {model_base_dir}", f"sudo chmod 777 {model_base_dir}"], timeout=60)

    remote_path = f"{model_base_dir}/{model_name}"
    push_resp = _post_with_heartbeat(
        f"{ARTIFACTS_CONTROLLER_URL}/push/artifact/vm",
        json_body={
            "file_path": staged_path,
            "vm_host": ip,
            "vm_ssh_user": ssh_user,
            "vm_ssh_pass": ssh_pass,
            "dest_path": remote_path,
        },
        timeout=7500,
        heartbeat_msg=f"Pushing model {model_name} -> {ip}:{model_base_dir}",
    )
    if push_resp.status_code >= 400:
        raise RuntimeError(f"Model push to VM failed ({push_resp.status_code}): {push_resp.text[:500]}")

    # Files land root:root from the sudo mkdir above; readable/traversable
    # for everyone (a+rX: read on files, execute-only on dirs -- doesn't
    # accidentally make a regular file executable) rather than a single
    # 644 chmod, since this is now a whole directory tree.
    run_commands(ip, ssh_user, ssh_pass, [f"sudo chmod -R a+rX {remote_path}"], timeout=60)

    logger.info(f"[{ip}] Harbor model pulled: {model_base_dir}/{model_name}")
    return {"model_dir": model_base_dir, "model_file": model_name}


@activity.defn
def share_model_nfs_activity(payload: dict) -> dict:
    """Export the head node's pulled-model folder over NFS, scoped to the pool subnet."""
    ip        = payload["ip_address"]
    ssh_user  = payload["ssh_user"]
    ssh_pass  = payload["ssh_pass"]
    model_dir = payload["model_dir"]
    subnet    = payload["subnet"]

    run_commands(ip, ssh_user, ssh_pass, [
        "sudo dnf install -y nfs-utils",
        f"sudo mkdir -p {model_dir}",
        f"grep -qF '{model_dir} ' /etc/exports || "
        f"echo '{model_dir} {subnet}(ro,sync,no_subtree_check,no_root_squash)' | sudo tee -a /etc/exports > /dev/null",
        "sudo exportfs -ra",
        "sudo systemctl enable --now nfs-server",
    ], timeout=120)

    logger.info(f"[{ip}] NFS export ready: {model_dir} -> {subnet}")
    return {"exported_path": model_dir}


@activity.defn
def mount_model_nfs_activity(payload: dict) -> dict:
    """Mount the head node's exported model folder on a worker, at the same local path."""
    ip        = payload["ip_address"]
    ssh_user  = payload["ssh_user"]
    ssh_pass  = payload["ssh_pass"]
    head_ip   = payload["head_ip"]
    model_dir = payload["model_dir"]

    run_commands(ip, ssh_user, ssh_pass, [
        "sudo dnf install -y nfs-utils",
        f"sudo mkdir -p {model_dir}",
        f"grep -qF '{head_ip}:{model_dir}' /etc/fstab || "
        f"echo '{head_ip}:{model_dir} {model_dir} nfs defaults 0 0' | sudo tee -a /etc/fstab > /dev/null",
        f"mountpoint -q {model_dir} || sudo mount {model_dir}",
    ], timeout=120)

    logger.info(f"[{ip}] Mounted model share from {head_ip}:{model_dir}")
    return {"mounted_path": model_dir}


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
        ssh_user     = payload["ssh_user"]
        ssh_pass     = payload["ssh_pass"]

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

        all_vms = proxmoxService.get_all_cluster_vms(db, cluster_data)

        # ── Resolve VM name ───────────────────────────────────────────────
        # Prefer a pre-reserved name (reserve_vmids_activity, called once
        # sequentially before parallel clones fire) — same rationale as the
        # pre-reserved vmid: computing "next free name" independently inside
        # each parallel clone let two nodes both land on e.g. "lucky-001".
        # Fall back to computing one here only if none was reserved.
        if payload.get("vm_name"):
            vm_name = payload["vm_name"]
        else:
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
        # Proxmox's /cluster/resources is backed by pvestatd, which refreshes
        # its cached view on its own ~10s cycle -- immediately after a VM is
        # converted to a template (in the prior activity), this can briefly
        # still show the pre-conversion state. Retry a few times instead of
        # failing on what's usually just propagation lag, not a real absence.
        template_node = None
        for lookup_attempt in range(6):
            for vm in all_vms:
                if str(vm.get("vmid")) == template:
                    if int(vm.get("template", 0)) == 1:
                        template_node = vm.get("node")
                        break
            if template_node:
                break
            time.sleep(5)
            all_vms = proxmoxService.get_all_cluster_vms(db, cluster_data)

        if not template_node:
            all_vmids = [str(vm.get("vmid")) for vm in all_vms if vm.get("vmid")]
            raise RuntimeError(f"Template VM {template} not found. Available vmids: {all_vmids}")

        # ── Get free VMID ─────────────────────────────────────────────────
        # Prefer a pre-reserved id (reserve_vmids_activity, called once
        # sequentially before parallel clones fire) to avoid the race where
        # two nodes cloning in parallel both call /cluster/nextid and get the
        # same id. Fall back to a live nextid call only if none was reserved
        # (e.g. single-node / older callers).
        if payload.get("vmid"):
            vmid = int(payload["vmid"])
        else:
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

        config_url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/config"

        # ── Attach raw PCI GPU(s) ──────────────────────────────────────────
        # Proxmox hard-restricts setting hostpci for non-mapped (raw) devices
        # to root, regardless of the API token's assigned role/ACL — so this
        # one call must use the cluster's root session, not the dedicated
        # per-cluster token used everywhere else.
        root_headers, root_cookies = root_proxmox_login(
            PROXMOX_HOST, cluster_data.username, cluster_data.password
        )
        resp = requests.put(
            config_url, headers=root_headers, cookies=root_cookies,
            data={**hostpci_data, "machine": "q35"},
            verify=False, timeout=30
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"GPU attach failed: {resp.text}")

        # ── Set CPU/RAM + cloud-init (non-root token) ─────────────────────
        resp = requests.put(
            config_url, headers=headers,
            data={
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

        # ── Reboot once after first boot with GPU attached ─────────────────
        # Confirmed via real logs: the head node (which already gets an
        # explicit reboot later, in launch_vllm_from_template_activity)
        # consistently stays GPU-healthy; a worker node that ran continuously
        # since its very first boot (no reboot at all) had its GPU driver
        # break LIVE, with no reboot or update involved on our side. A GPU
        # attached via PCI passthrough initializing cleanly on the VM's
        # first-ever boot is less reliable than a normal boot with the
        # device already present from POST onward. Doing this here, for
        # every node (not just the head), closes that gap.
        logger.info(f"[{vmid}] Rebooting once after first boot to ensure clean GPU init...")
        reboot_and_wait(reserved_ip, ssh_user, ssh_pass, wait_before_retry=60)
        logger.info(f"[{vmid}] Back online after post-attach reboot")

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
def verify_gpu_health_activity(payload: dict) -> dict:
    """
    Confirms nvidia-smi actually works on this node, with one automatic
    reboot-and-retry if it doesn't.

    This exists because launch_vllm_from_template_activity's own GPU/NVML
    wait only ever ran on the HEAD node -- worker nodes were never checked
    at all, so a broken driver on a worker (e.g. "Driver/library version
    mismatch" -- kernel module and userspace library out of sync, usually
    from a driver update that hasn't been rebooted into yet) went completely
    undetected until vLLM itself failed deep inside Ray's actor init, many
    steps later, with a confusing multi-layer error. Running this on every
    node up front catches it immediately and attempts the same fix a human
    would reach for first (reboot), before giving up with a clear message.
    """
    ip       = payload["ip_address"]
    ssh_user = payload["ssh_user"]
    ssh_pass = payload["ssh_pass"]

    _check = "nvidia-smi 2>&1"

    result = run_commands(ip, ssh_user, ssh_pass, [_check + " || true"], timeout=30)
    output = result[0]["stdout"] if result else ""

    if "Driver/library version mismatch" in output or "NVML" in output and "Failed" in output:
        logger.warning(
            f"[{ip}] nvidia-smi reports a driver/library mismatch — "
            f"rebooting once to reload the correct kernel module: {output[:200]!r}"
        )
        reboot_and_wait(ip, ssh_user, ssh_pass, wait_before_retry=60)

        result = run_commands(ip, ssh_user, ssh_pass, [_check + " || true"], timeout=30)
        output = result[0]["stdout"] if result else ""

        if "Driver/library version mismatch" in output or ("NVML" in output and "Failed" in output):
            raise RuntimeError(
                f"[{ip}] GPU driver still broken after reboot-and-retry: {output[:300]!r}. "
                f"This needs a manual driver rebuild/reinstall (dkms) on this VM — "
                f"not something a reboot alone can fix."
            )
        logger.info(f"[{ip}] GPU healthy after reboot")
    else:
        logger.info(f"[{ip}] GPU/nvidia-smi healthy")

    return {"ip_address": ip, "gpu_healthy": True}


@activity.defn
def verify_ray_cluster_gpu_activity(payload: dict) -> dict:
    """
    Confirms the Ray cluster itself (not just each node's own nvidia-smi)
    actually reports the expected total GPU count before we ever attempt to
    launch vLLM. This is the check that would have caught the earlier
    "Pending Demands: {'GPU': 1.0} * 2" stuck-forever scenario immediately,
    instead of only discovering it after a 900s health-poll timeout with an
    unhelpful "Log not found".
    """
    ip            = payload["ip_address"]  # head node
    ssh_user      = payload["ssh_user"]
    ssh_pass      = payload["ssh_pass"]
    expected_gpus = int(payload["expected_gpus"])
    home_dir = "/root" if ssh_user == "root" else f"/home/{ssh_user}"
    venv_bin = f"{home_dir}/vllm-ray-env/bin"

    _check = (
        "for i in $(seq 1 12); do "
        f"  {venv_bin}/ray status 2>/dev/null | grep -oP '(?<=/)[0-9.]+(?= GPU)'; "
        "  sleep 5; "
        "done"
    )
    result = run_commands(ip, ssh_user, ssh_pass, [_check], timeout=90)
    out = (result[0]["stdout"] if result else "").strip()

    # grep runs 12 times in the loop; take the last (most recent) reading.
    lines = [l for l in out.splitlines() if l.strip()]
    last_total = None
    if lines:
        try:
            last_total = float(lines[-1])
        except ValueError:
            pass

    if last_total is None or last_total < expected_gpus:
        raise RuntimeError(
            f"[{ip}] Ray cluster reports {last_total} total GPU(s), expected "
            f"{expected_gpus}. Not proceeding to vLLM launch -- a node likely "
            f"failed to register its GPU with the cluster (check ray status "
            f"and each node's ray-worker.service manually)."
        )

    logger.info(f"[{ip}] Ray cluster confirms {last_total} GPU(s) available (expected {expected_gpus})")
    return {"ip_address": ip, "total_gpus": last_total}


def _render_vllm_arg(key: str, value) -> str:
    """
    Render one (key, value) pair as a vLLM CLI flag. Keys use snake_case
    (matching the UI's YAML field names) and get translated to vLLM's
    dashed flag form.

    vLLM flags come in two shapes and must be told apart:
      - boolean/store_true flags (--enable-chunked-prefill, --trust-remote-code)
        take NO value on the command line -- their presence alone means "on".
        Passing e.g. "--enable-chunked-prefill true" is not how argparse
        store_true works and can be rejected as an unrecognized argument.
      - value flags (--max-model-len 4096) need "--flag value".
    True  -> bare flag, no value.
    False -> omitted entirely (there's no negated form for these flags).
    Anything else -> "--flag <shell-quoted value>".
    """
    flag = "--" + key.replace("_", "-")
    if isinstance(value, bool):
        return f" {flag}" if value else ""
    return f" {flag} {shlex.quote(str(value))}"


def _build_vllm_commands(
    home_dir: str,
    tp_size: int,
    pp_size: int,
    model_type: str | None = None,
    max_images_per_request: int | None = None,
    extra_params: dict | None = None,
    api_key: str | None = None,
) -> tuple[str, str]:
    """
    Build the vLLM launch + health-poll shell commands. Shared by
    launch_vllm_from_template_activity (first boot) and
    restore_llm_services_activity (start/restart) so the two code paths can
    never drift apart -- a prior version of restore_llm_services_activity
    had its own simplified copy of this logic that silently diverged
    (wrong env vars, no single-node/multi-node branching), causing restarts
    to launch vLLM differently than the original provisioning did.

    extra_params (parsed from the UI's "Extra vLLM Params" textarea, already
    a dict by the time it gets here -- never raw YAML) is merged on top of
    the built-in defaults: same key overrides, new key gets added. A
    "served_model_name" key is special-cased out of the dict since it
    replaces the default (path-derived) --served-model-name value rather
    than being a generic flag.
    """
    is_multinode = pp_size > 1
    _vllm_bin = f"{home_dir}/vllm-ray-env/bin/python3 -m vllm.entrypoints.openai.api_server"

    # ── Base args as a dict, not a hand-built string -- this is what makes
    # "same key overrides, new key gets added" possible via dict.update().
    base_args: dict = {}
    # Single-node inference (pp_size=1) -> no Ray, simpler & faster.
    # Multi-node inference (pp_size>1) -> Ray for pipeline parallelism.
    if is_multinode:
        base_args["distributed_executor_backend"] = "ray"
        base_args["tensor_parallel_size"] = tp_size
        base_args["pipeline_parallel_size"] = pp_size
    elif tp_size > 1:
        # Still need --tensor-parallel-size when a single node has multiple
        # GPUs -- without it vLLM defaults to TP=1 and silently only uses
        # one of the attached GPUs.
        base_args["tensor_parallel_size"] = tp_size
    base_args.update({
        "max_model_len": 4096,
        "gpu_memory_utilization": 0.90,
        "enable_chunked_prefill": True,
        "trust_remote_code": True,
        "host": "0.0.0.0",
        "port": 8000,
    })

    if model_type == "vision_language" and max_images_per_request:
        # vLLM parses --limit-mm-per-prompt with json.loads -- it needs a JSON
        # object, not the old "image=4" key=value shorthand.
        base_args["limit_mm_per_prompt"] = json.dumps({"image": int(max_images_per_request)})

    # ── TLS + bearer-auth: vLLM's uvicorn server does both natively, no
    # separate reverse proxy needed. Cert is self-signed and generated (once,
    # on first launch -- restarts reuse the existing files) directly on the
    # VM, so the endpoint is served as https://<ip>:8000/v1.
    _ssl_dir = f"{home_dir}/.vllm_ssl"
    _ssl_keyfile = f"{_ssl_dir}/key.pem"
    _ssl_certfile = f"{_ssl_dir}/cert.pem"
    base_args["ssl_keyfile"] = _ssl_keyfile
    base_args["ssl_certfile"] = _ssl_certfile
    if api_key:
        base_args["api_key"] = api_key

    served_model_name_override = None
    if extra_params:
        extra_params = dict(extra_params)  # don't mutate the caller's dict
        if "served_model_name" in extra_params:
            served_model_name_override = str(extra_params.pop("served_model_name")).strip()
        base_args.update(extra_params)

    _vllm_common_args = "".join(_render_vllm_arg(k, v) for k, v in base_args.items())
    _served_model_name_expr = (
        shlex.quote(served_model_name_override) if served_model_name_override else '"$RESOLVED_MODEL"'
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
        # ── 1.5: Ensure a self-signed TLS cert exists for vLLM's HTTPS server ──
        # Generated once and reused across restarts (same cert => no new
        # browser/client trust prompt every relaunch). 10-year validity since
        # this is an internal, self-signed cert with no rotation process.
        f"mkdir -p {_ssl_dir}; "
        f"if [ ! -f {_ssl_certfile} ] || [ ! -f {_ssl_keyfile} ]; then "
        f"  echo '[vLLM] Generating self-signed TLS cert...'; "
        f"  openssl req -x509 -newkey rsa:2048 -nodes "
        f"    -keyout {_ssl_keyfile} -out {_ssl_certfile} "
        f"    -days 3650 -subj \"/CN=$(hostname -I | awk '{{print $1}}')\"; "
        f"  chmod 600 {_ssl_keyfile}; "
        f"fi; "
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
        "pgrep -f 'vllm.entrypoints.openai.api_server' | grep -v $$ | xargs -r kill 2>/dev/null || true; "
        # Also clear any leftover Ray actor/worker processes from a prior
        # launch attempt (e.g. EngineCore, RayWorkerP). Without this, a
        # retry can hit "ActorHandleNotFoundError: ... not valid across
        # Ray sessions" because vLLM still holds a handle to an actor
        # from the previous session. NOT `ray stop` -- that would tear
        # down the whole cluster (this runs on the head); this only
        # kills vLLM's own leftover worker processes.
        # grep -v $$ is required -- without it, pgrep matches the shell
        # running THIS SCRIPT ITSELF (its command line literally contains
        # this search text), and xargs kills it, terminating the whole
        # launch after only a fraction of a second (confirmed via logs:
        # channel dropped in 0.22s, before the script could possibly have
        # reached model resolution).
        "pgrep -f 'ray::RayWorkerP|EngineCore' | grep -v $$ | xargs -r kill 2>/dev/null || true; "
        "sleep 2; "
        f"echo \"===== vLLM launch attempt $(date -u) — model=$RESOLVED_MODEL =====\" > {home_dir}/{_VLLM_LOG_FILE}; "

        # ── 5. Fire-and-forget launch ─────────────────────────────────────
        # env VAR=value prefix guarantees vars reach the nohup subprocess
        # even if the SSH channel closes before shell exports are inherited.
        f"_VLLM_ENV=\"VLLM_DEVICE=cuda CUDA_VISIBLE_DEVICES=0 CUDA_HOME=/usr/local/cuda\"; "

        "if [ \"${RESOLVED_MODEL:0:1}\" = \"/\" ]; then "
        f"  echo \"[vLLM-cmd] nohup env $_VLLM_ENV {_vllm_bin} --model $RESOLVED_MODEL --served-model-name {_served_model_name_expr} {_vllm_common_args}\" >> {home_dir}/{_VLLM_LOG_FILE}; "
        f"  nohup env $_VLLM_ENV {_vllm_bin}"
        f"    --model \"$RESOLVED_MODEL\""
        f"    --served-model-name {_served_model_name_expr}"
        f"    {_vllm_common_args}"
        f"    >> {home_dir}/{_VLLM_LOG_FILE} 2>&1 & "
        "else "
        f"  echo \"[vLLM-cmd] nohup env $_VLLM_ENV {_vllm_bin} --model $RESOLVED_MODEL --served-model-name {_served_model_name_expr} --download-dir ${{LLM_MODEL_PATH:-/vllm_data/hf_cache}} {_vllm_common_args}\" >> {home_dir}/{_VLLM_LOG_FILE}; "
        f"  nohup env $_VLLM_ENV {_vllm_bin}"
        f"    --model \"$RESOLVED_MODEL\""
        f"    --served-model-name {_served_model_name_expr}"
        f"    --download-dir \"${{LLM_MODEL_PATH:-/vllm_data/hf_cache}}\""
        f"    {_vllm_common_args}"
        f"    >> {home_dir}/{_VLLM_LOG_FILE} 2>&1 & "
        "fi; "
        "echo \"[vLLM] Process launched in background\""
    )

    _health_curl_auth = f' -H "Authorization: Bearer {api_key}"' if api_key else ""
    health_poll = (
        "source /etc/profile || true; "
        "source ~/.bash_profile || true; "
        "source ~/.bashrc || true; "
        "for i in $(seq 1 90); do "
        f"  curl -sfk{_health_curl_auth} https://localhost:8000/health && echo 'vllm ready' && exit 0; "
        "  echo \"Waiting for vllm... $i/90\"; "
        "  pgrep -f 'vllm.entrypoints.openai.api_server' > /dev/null || { echo 'ERROR: vLLM process died' >&2; break; }; "
        "  sleep 10; "
        "done; "
        "{ "
        "  echo '=== nvidia-smi ==='; nvidia-smi 2>/dev/null || echo 'nvidia-smi failed'; "
        "  echo '=== ray status ==='; ray status 2>/dev/null || echo 'ray status failed'; "
        f"  echo '=== {_VLLM_LOG_FILE} (last 80 lines) ==='; "
        f"  [ -f {home_dir}/{_VLLM_LOG_FILE} ] && tail -80 {home_dir}/{_VLLM_LOG_FILE} || echo 'Log not found'; "
        "} >&2; "
        "exit 1"
    )

    return vllm_launch, health_poll

def _sed_escape(value: str) -> str:
    """Escape a value for safe use as a sed s|||  replacement (delimiter '|')."""
    return value.replace("\\", "\\\\").replace("|", "\\|").replace("&", "\\&")


_TELEGRAF_CONF_PATH = os.getenv("LLM_VM_TELEGRAF_CONF_PATH", "/etc/telegraf/telegraf.conf")


@activity.defn
def lunch_configure_influxdb_activity(payload: dict) -> dict:
    """
    Points this VM's pre-installed Telegraf agent (baked into the template's
    .deb alongside the user's own Python service) at this environment's
    InfluxDB by rewriting the [[outputs.influxdb_v2]] stanza in
    /etc/telegraf/telegraf.conf, then restarts telegraf.service to pick it
    up. Runs once per VM -- the workflow fans this out over every node in
    the pool (head + workers), same pattern as configure_llm_node_activity, so
    Task Manager's CPU/process monitoring works for all machines, not just
    the head node.
    """
    ip       = payload["ip_address"]
    ssh_user = payload["ssh_user"]
    ssh_pass = payload["ssh_pass"]

    influxdb_url    = payload.get("influxdb_url")
    influxdb_token  = payload.get("influxdb_token")
    influxdb_org    = payload.get("influxdb_org")
    influxdb_bucket = payload.get("influxdb_bucket")

    missing = [
        name for name, val in (
            ("influxdb_url", influxdb_url),
            ("influxdb_token", influxdb_token),
            ("influxdb_org", influxdb_org),
            ("influxdb_bucket", influxdb_bucket),
        ) if not val
    ]
    if missing:
        raise RuntimeError(
            f"[{ip}] Missing InfluxDB config: {', '.join(missing)} -- check "
            f"INFLUXDB_URL / INFLUXDB_TOKEN / INFLUXDB_ORG / "
            f"INFLUXDB_LINUX_METRICS_BUCKET env vars on the backend."
        )

    # Only touches keys inside the [[outputs.influxdb_v2]] stanza (range-bound
    # between that header and the next [[...]] header or EOF) so a same-named
    # key under a different plugin stanza is never accidentally rewritten.
    sed_script = (
        r'/^\[\[outputs\.influxdb_v2\]\]/,/^\[\[/{'
        rf's|^\([[:space:]]*urls[[:space:]]*=\).*|\1 ["{_sed_escape(influxdb_url)}"]|; '
        rf's|^\([[:space:]]*token[[:space:]]*=\).*|\1 "{_sed_escape(influxdb_token)}"|; '
        rf's|^\([[:space:]]*organization[[:space:]]*=\).*|\1 "{_sed_escape(influxdb_org)}"|; '
        rf's|^\([[:space:]]*bucket[[:space:]]*=\).*|\1 "{_sed_escape(influxdb_bucket)}"|'
        r'}'
    )

    cmd = (
        f"grep -q '\\[\\[outputs.influxdb_v2\\]\\]' {_TELEGRAF_CONF_PATH} "
        f"|| {{ echo 'ERROR: [[outputs.influxdb_v2]] stanza not found in {_TELEGRAF_CONF_PATH} "
        f"-- is the Telegraf .deb actually installed on this template?' >&2; exit 1; }}; "
        f"cp {_TELEGRAF_CONF_PATH} {_TELEGRAF_CONF_PATH}.bak.$(date +%s); "
        f"sed -i {shlex.quote(sed_script)} {_TELEGRAF_CONF_PATH} && "
        f"systemctl restart telegraf && "
        f"sleep 2 && "
        f"systemctl is-active --quiet telegraf"
    )

    try:
        run_commands(ip, ssh_user, ssh_pass, [cmd], timeout=60)
        logger.info(f"[{ip}] Telegraf configured -- org={influxdb_org} bucket={influxdb_bucket}")
        return {"ip_address": ip, "status": "configured"}
    except Exception as e:
        raise RuntimeError(f"[{ip}] Failed to configure Telegraf InfluxDB output: {e}")

@activity.defn
def launch_vllm_from_template_activity(payload: dict) -> dict:

    try:
        import time
        ip       = payload["ip_address"]
        ssh_user = payload["ssh_user"]
        ssh_pass = payload["ssh_pass"]
        tp_size  = payload.get("tensor_parallel_size", 1)
        pp_size  = payload.get("pipeline_parallel_size", 1)
        model_type             = payload.get("model_type")
        max_images_per_request = payload.get("max_images_per_request")
        vllm_extra_params      = payload.get("vllm_extra_params")
        api_key                = payload.get("api_key")

        # ── Step 0: Reboot VM ─────────────────────────────────────────────────
        # After a fresh clone the GPU drivers do not initialize properly.
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

        # ── Step 0.6: Wait for Ray head service (always, but only vLLM uses it if multi-node) ──
        # After reboot the Ray head service auto-starts — wait for it to become ready
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
        vllm_launch, health_poll = _build_vllm_commands(
            home_dir, tp_size, pp_size,
            model_type=model_type,
            max_images_per_request=max_images_per_request,
            extra_params=vllm_extra_params,
            api_key=api_key,
        )

        try:
            logger.info(f"[{ip}] Executing vLLM launch script over SSH...")
            launch_results = run_commands(ip, ssh_user, ssh_pass, [vllm_launch], timeout=120)
            exit_code = launch_results[0].get("exit_code") if launch_results else None

            # run_commands treats exit_code -1 (SSH channel closed without a real
            # exit status) as if it were a clean success -- it does NOT raise for
            # it. That silently let a mid-script channel drop (before the launch
            # script ever reached the log-write/nohup lines) look like a normal
            # launch, sending us straight into a doomed 900s health poll with no
            # process and no log ("Log not found"). Detect it explicitly here and
            # route into the same verify/retry logic below instead of trusting it.
            if launch_results and exit_code == -1:
                logger.error(f"[{ip}] SSH connection dropped mid-command (exit_code=-1)")
                raise RuntimeError(f"Command failed (exit -1) on {ip}: channel closed without exit status")
            launch_stdout = launch_results[0]["stdout"] if launch_results else ""
        except RuntimeError as launch_err:
            if "exit -1" in str(launch_err):
                # SSH channel dropped — exit -1 means channel closed without exit status.
                # This can happen BEFORE or AFTER nohup was submitted, so we cannot assume
                # the process is running. Wait 30s then verify with a fresh SSH connection.
                logger.warning(f"[{ip}] SSH dropped during vLLM launch (exit -1) — verifying process...")
                time.sleep(30)

                # Fresh SSH: check if vLLM process is actually running.
                # NOTE the [v]llm bracket trick: pgrep -f matches every process's
                # FULL command line, including the shell invoking pgrep itself --
                # since that shell's own command line literally contains the
                # search text, a plain 'vllm.entrypoints...' pattern always
                # self-matches and reports RUNNING even when nothing is actually
                # running (confirmed via logs: "confirmed running" after only
                # 0.22s, before the launch script could possibly have started
                # vLLM). [v]llm as a regex still matches a real process's plain
                # "vllm..." text, but not its own invocation's literal "[v]llm...".
                _check_cmd = (
                    "pgrep -fl '[v]llm.entrypoints.openai.api_server' "
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
                            # Previously this just waited 60s and assumed success with
                            # no verification -- if the process genuinely never started
                            # (e.g. a real failure inside vllm_launch got masked by the
                            # channel drop), that silently produced a doomed 900s health
                            # poll with no useful diagnostics ("Log not found" and
                            # nothing else). Actually verify before proceeding.
                            logger.warning(f"[{ip}] Re-launch also got exit -1 — waiting 60s then verifying...")
                            time.sleep(60)
                            try:
                                _chk2 = run_commands(ip, ssh_user, ssh_pass, [_check_cmd], timeout=30)
                                _chk2_out = _chk2[0]["stdout"] if _chk2 else "VLLM_NOT_RUNNING"
                            except Exception:
                                _chk2_out = "VLLM_NOT_RUNNING"
                            if "VLLM_NOT_RUNNING" in _chk2_out:
                                raise RuntimeError(
                                    f"[{ip}] vLLM process could not be confirmed running after "
                                    f"2 launch attempts + SSH channel drops. Not proceeding to "
                                    f"health poll — check /etc/environment model config and "
                                    f"SSH/network stability to this VM."
                                ) from relaunch_err
                            logger.info(f"[{ip}] vLLM confirmed running after re-verification")
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

        endpoint = f"https://{ip}:8000/v1"
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
    ssh_user = payload["ssh_user"]
    ssh_pass = payload["ssh_pass"]
    role     = payload.get("role", "head")   # "head" | "worker"
    tp_size  = payload.get("tensor_parallel_size", 1)
    pp_size  = payload.get("pipeline_parallel_size", 1)
    model_type             = payload.get("model_type")
    max_images_per_request = payload.get("max_images_per_request")
    vllm_extra_params      = payload.get("vllm_extra_params")
    api_key                = payload.get("api_key")
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
        # Uses the exact same command-building logic as the original
        # provisioning launch (launch_vllm_from_template_activity), so a
        # restart can never diverge from how the pool was first launched
        # (single-node vs multi-node branching, env vars, model resolution).
        vllm_launch, health_poll = _build_vllm_commands(
            home_dir, tp_size, pp_size,
            model_type=model_type,
            max_images_per_request=max_images_per_request,
            extra_params=vllm_extra_params,
            api_key=api_key,
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

        for field in ("status", "vmids", "ip_addresses", "head_ip", "endpoint_url", "workflow_id", "machines_name", "template"):
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

        from models.llm_inference_model import LLMInferenceJob
        record = db.query(LLMInferenceJob).filter(LLMInferenceJob.id == job_id).first()
        if not record:
            logger.warning(f"[delete] Job {job_id} not found — may already be deleted")
            return {"ok": True, "skipped": True}

        # ── Mark as deleting ─────────────────────────────────────────────
        record.status = "deleting"
        db.commit()

        # ── Delete VMs from Proxmox ───────────────────────────────────────
        failed_vmids = []
        deleted_vmids = []
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
                        logger.warning(f"[delete] No node recorded for VM {vmid} — cannot target delete, marking failed")
                        failed_vmids.append(vmid)
                        continue
                    logger.info(f"[delete] Removing VM {vmid} from node {node}")

                    # Remove HA resources (best-effort — VM purge below removes
                    # these anyway; not fatal if the VM/rule is already gone)
                    requests.delete(
                        f"{PROXMOX_HOST}/api2/json/cluster/ha/resources/vm%3A{vmid}",
                        headers=headers, verify=False, timeout=15
                    )
                    # Remove HA node-affinity rule (groups were migrated to rules;
                    # the old 'cluster/ha/groups' endpoint no longer exists)
                    requests.delete(
                        f"{PROXMOX_HOST}/api2/json/cluster/ha/rules/llm-{vmid}",
                        headers=headers, verify=False, timeout=15
                    )
                    # Strip GPU passthrough config (raw hostpci — root-only on this
                    # Proxmox version, same restriction as attaching it; best-effort
                    # since the VM purge below removes the config file regardless)
                    requests.put(
                        f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/config",
                        headers=headers,
                        data={"delete": "hostpci0,hostpci1,hostpci2,hostpci3"},
                        verify=False, timeout=15
                    )
                    # Stop VM
                    stop_resp = requests.post(
                        f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/status/stop",
                        headers=headers, verify=False, timeout=30
                    )
                    if stop_resp.status_code >= 400 and stop_resp.status_code != 404:
                        logger.warning(f"[delete] Stop request for VM {vmid} failed: {stop_resp.text}")

                    # Wait for stopped state (max 60 s) — track whether it actually stopped
                    stopped = False
                    for _ in range(12):
                        _time.sleep(5)
                        st = requests.get(
                            f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}/status/current",
                            headers=headers, verify=False, timeout=10
                        )
                        if st.status_code == 404:
                            # VM already gone (e.g. never existed / already deleted)
                            stopped = True
                            break
                        if st.ok and st.json().get("data", {}).get("status") == "stopped":
                            stopped = True
                            break

                    # Permanently delete VM + disks — verify the response before
                    # trusting this VM is actually gone.
                    del_resp = requests.delete(
                        f"{PROXMOX_HOST}/api2/json/nodes/{node}/qemu/{vmid}",
                        headers=headers,
                        params={"purge": 1, "destroy-unreferenced-disks": 1},
                        verify=False, timeout=60
                    )
                    if del_resp.status_code == 404:
                        # Already gone — treat as success
                        logger.info(f"[delete] VM {vmid} already absent from Proxmox")
                        deleted_vmids.append(vmid)
                    elif del_resp.status_code >= 400:
                        logger.error(
                            f"[delete] Failed to delete VM {vmid} on node {node} "
                            f"(stopped={stopped}): {del_resp.text}"
                        )
                        failed_vmids.append(vmid)
                    else:
                        # Proxmox may return a UPID (async task) — wait for it so we
                        # know the purge actually finished before trusting it.
                        upid = del_resp.json().get("data")
                        delete_confirmed = True
                        if upid:
                            try:
                                _wait_for_task(_all_hosts(cluster_data) or [PROXMOX_HOST], headers, node, upid, timeout=120)
                            except Exception as wait_err:
                                logger.error(f"[delete] VM {vmid} delete task did not confirm complete: {wait_err}")
                                delete_confirmed = False
                        if delete_confirmed:
                            logger.info(f"[delete] VM {vmid} deleted")
                            deleted_vmids.append(vmid)
                        else:
                            failed_vmids.append(vmid)

        if failed_vmids:
            # Do NOT release IPs or drop the DB record when some VMs failed to
            # delete -- doing so would let those IPs be reassigned to a new VM
            # while the old one (and its config file) is still sitting on
            # Proxmox, causing exactly the "config file already exists" clone
            # collision this guard is meant to prevent.
            record.status = "delete_failed"
            db.commit()
            logger.error(
                f"[delete] Job {job_id}: {len(failed_vmids)} VM(s) failed to delete "
                f"({failed_vmids}) — job kept as 'delete_failed' for retry, "
                f"IPs and DB record NOT released."
            )
            return {
                "ok": False,
                "job_id": job_id,
                "deleted_vmids": deleted_vmids,
                "failed_vmids": failed_vmids,
            }

        # ── Safety net: remove the Harbor-restored template if it's somehow
        # still around (normally delete_harbor_template_activity already
        # removed it right after cloning finished). A "proxmox" source means
        # the user picked a pre-existing cluster template that may back other
        # pools -- deleting it would break them, so only ever touch "harbor".
        # Best-effort: an orphan template is untidy, but not a reason to hold
        # the pool's IPs and DB record hostage the way an undeleted VM is.
        if record.template_source == "harbor" and str(record.template or "").isdigit():
            try:
                _delete_proxmox_vm(db, cluster_id, int(record.template))
            except Exception as tmpl_err:
                logger.error(f"[delete] Harbor template {record.template} cleanup failed: {tmpl_err}")

        # ── Release IPs (only reached if every VM was confirmed deleted) ──
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
        return {"ok": True, "job_id": job_id, "deleted_vmids": deleted_vmids}

    except Exception as e:
        raise RuntimeError(str(e))
    finally:
        db.close()


# ── Ray/host-setup helpers ──────────────────────────────────────────────────
# Moved here from the retired single-VM "v1" activities file -- these three
# activities (configure_llm_node_activity, add_affinity_rule_activity,
# configure_ray_activity) are the same ones the multi-node workflow above has
# always called; they just used to live in a separate module alongside dead
# v1-only code. Kept byte-for-byte identical to avoid changing behavior.

def _detect_net_iface(ip: str, ssh_user: str, ssh_pass: str, fallback: str = "eth0") -> str:
    """
    Auto-detect the VM's actual primary network interface name instead of
    assuming one. Interface naming (eth0, ens18, enp0s18, ...) is decided by
    the guest OS/kernel at boot, not by us -- a hardcoded guess breaks the
    moment a template is rebuilt with a different naming scheme. Detecting it
    live, per-VM, means this keeps working regardless of what the template
    happens to use.
    """
    try:
        result = run_commands(
            ip, ssh_user, ssh_pass,
            ["ip route get 8.8.8.8 2>/dev/null | awk '{for(i=1;i<=NF;i++) if ($i==\"dev\") {print $(i+1); exit}}'"],
            timeout=15,
        )
        iface = result[0]["stdout"].strip()
        if iface:
            return iface
        logger.warning(f"[{ip}] Could not detect network interface (empty output) — falling back to '{fallback}'")
    except Exception as exc:
        logger.warning(f"[{ip}] Network interface detection failed ({exc}) — falling back to '{fallback}'")
    return fallback


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


@activity.defn
def add_affinity_rule_activity(payload: dict) -> dict:
    """
    Adds a HA affinity rule to keep the LLM VM on its designated node.
    Proxmox migrated HA groups to HA rules (node-affinity type) -- the old
    'cluster/ha/groups' endpoint now 500s with "ha groups have been migrated
    to rules", so pinning is done via 'cluster/ha/rules' instead.
    """
    db: Session = SessionLocal()
    try:
        cluster_id = payload["cluster_id"]
        vmid = payload["vmid"]
        node = payload["node"]

        cluster, host, headers = _get_proxmox_ctx(db, cluster_id)

        rule_id = f"llm-{vmid}"

        # Add VM to HA resource management FIRST -- the rules API rejects
        # rules that reference an "unmanaged" resource, so the vm:<id> sid
        # must already exist under HA before a node-affinity rule can cite it.
        resp = requests.post(
            f"{host}/api2/json/cluster/ha/resources",
            headers=headers,
            json={"sid": f"vm:{vmid}", "state": "started"},
            verify=False, timeout=15,
        )
        if resp.status_code not in (200, 400) and "already defined" not in resp.text:
            resp.raise_for_status()

        # Create node-affinity rule pinning this VM to its node
        resp = requests.post(
            f"{host}/api2/json/cluster/ha/rules",
            headers=headers,
            json={
                "rule":      rule_id,
                "type":      "node-affinity",
                "resources": f"vm:{vmid}",
                "nodes":     node,
                "strict":    1,
            },
            verify=False, timeout=15,
        )
        # skip if already exists (Proxmox returns 400/500 with "already exists")
        if resp.status_code not in (200, 400) and "already exists" not in resp.text:
            resp.raise_for_status()

        logger.info(f"Affinity rule set: VM {vmid} pinned to node {node}")
        return {"vmid": vmid, "ha_rule": rule_id}
    finally:
        db.close()


@activity.defn
def configure_llm_node_activity(payload: dict) -> dict:
    """
    Idempotent node setup -- does NOT install anything (client sites may have
    no internet access). The Harbor template is required to already carry
    the NVIDIA driver + CUDA + the ~/vllm-ray-env venv (ray, vllm); this
    activity fails fast if either is missing rather than falling back to a
    dnf/pip install. What it actually does, every run:
      - Verify NVIDIA driver present (fail if not)
      - Verify Ray + vLLM venv present (fail if not)
      - Hostname, /etc/hosts, SELinux permissive, env vars, firewall, dirs
    """
    ip = payload["ip_address"]
    ssh_user = payload["ssh_user"]
    ssh_pass = payload["ssh_pass"]
    hostname = payload.get("name", "llm-node")
    role = payload.get("role", "head")   # "head" | "worker"
    subnet = payload.get("subnet")
    if not subnet:
        raise RuntimeError(
            "configure_llm_node_activity requires 'subnet' (the real cluster subnet, "
            "e.g. '172.16.4.0/24') to open the inter-node firewall rule. "
            "A wrong/default subnet here silently leaves node-to-node traffic "
            "(e.g. the PyTorch/NCCL rendezvous port) unprotected by the firewall's "
            "accept-rule, causing 'No route to host' failures between GPU workers."
        )
    net_iface = payload.get("net_iface") or _detect_net_iface(ip, ssh_user, ssh_pass)
    model = payload.get("model", "")
    model_path = payload.get("model_path", "/vllm_data/hf_cache")

    # Log exactly what we received so we can prove whether this activity is the
    # one touching /etc/environment's LLM_MODEL_NAME. Empty model => we leave the
    # template's value untouched; non-empty => we delete + rewrite it.
    logger.info(
        f"[{ip}] install_ray_vllm: model={model!r} (will_overwrite_name={bool(model)}), "
        f"model_path={model_path!r} (will_overwrite_path={bool(model_path and model_path != '/vllm_data/hf_cache')})"
    )

    # Always set hostname -- and a matching /etc/hosts entry. hostnamectl
    # alone only sets the kernel hostname; it never touches /etc/hosts. NSS
    # resolution order (nsswitch.conf) checks /etc/hosts ("files") before
    # DNS, so without this entry, any FQDN lookup (socket.getfqdn(), used
    # internally by vLLM/Ray's Prometheus exporters for their "instance"
    # label) falls through to DNS -- which was producing a doubled
    # "host.host" tag value in InfluxDB (e.g. "lucky001.lucky001") instead
    # of the plain hostname every other Telegraf input tags metrics with.
    # sed first removes any stale line for this hostname so re-running this
    # (idempotent) activity never appends duplicate /etc/hosts lines.
    run_commands(ip, ssh_user, ssh_pass, [
        f"sudo hostnamectl set-hostname '{hostname}'",
        f"sudo sed -i '/\\b{hostname}\\b/d' /etc/hosts",
        f"echo '{ip} {hostname}' | sudo tee -a /etc/hosts > /dev/null",
    ], timeout=30)

    # ── SELinux → permissive ──────────────────────────────────────────────────
    # Enforcing SELinux blocks systemd from exec'ing the Ray binary in /home
    # (user_home_t context) → ray-head.service crash-loops with 203/EXEC.
    # setenforce 0 fixes the running mode now; the config edit makes it survive
    # reboots. Idempotent — safe to re-run.
    run_commands(ip, ssh_user, ssh_pass, [
        "sudo setenforce 0 2>/dev/null || true",
        "sudo sed -i 's/^SELINUX=enforcing/SELINUX=permissive/' /etc/selinux/config 2>/dev/null || true",
    ], timeout=30)

    # ── Require: NVIDIA driver already present ────────────────────────────────
    # Deliberately NOT falling back to installing it here -- client sites may
    # have no internet access, and dnf/pip installs would just hang or fail
    # with a confusing network error deep inside a 900s+ block instead of
    # failing fast with a clear cause. The Harbor template is expected to
    # already carry the driver; if it doesn't, that's a template problem to
    # fix at build time, not something this activity should paper over.
    try:
        run_commands(ip, ssh_user, ssh_pass, ["nvidia-smi"], timeout=30)
        logger.info(f"[{ip}] NVIDIA driver present")
    except RuntimeError:
        raise RuntimeError(
            f"[{ip}] NVIDIA driver not found on this VM. This activity will not "
            f"install it (client sites may have no internet access) -- rebuild "
            f"the Harbor template with the NVIDIA driver pre-installed."
        )

    # ── Require: Ray + vLLM venv already present ───────────────────────────────
    # Same reasoning as the driver check above -- no install fallback.
    try:
        run_commands(ip, ssh_user, ssh_pass, [
            "test -f ~/vllm-ray-env/bin/ray && test -f ~/vllm-ray-env/bin/vllm"
        ], timeout=15)
        logger.info(f"[{ip}] Ray + vLLM venv present")
    except RuntimeError:
        raise RuntimeError(
            f"[{ip}] Ray/vLLM venv (~/vllm-ray-env) not found on this VM. This "
            f"activity will not install it (client sites may have no internet "
            f"access) -- rebuild the Harbor template with CUDA + the "
            f"~/vllm-ray-env venv (ray, vllm) pre-installed."
        )

    # ── Phase 3: Env vars + firewall + dirs (always — all idempotent) ────────
    phase3 = [
        # ~/.bashrc env vars (for interactive shells)
        "grep -qF 'PATH=/usr/local/cuda/bin' ~/.bashrc || echo 'export PATH=/usr/local/cuda/bin:$PATH' >> ~/.bashrc",
        "grep -qF 'LD_LIBRARY_PATH=/usr/local/cuda/lib64' ~/.bashrc || echo 'export LD_LIBRARY_PATH=/usr/local/cuda/lib64:$LD_LIBRARY_PATH' >> ~/.bashrc",
        "grep -qF 'VLLM_USE_V1' ~/.bashrc || echo 'export VLLM_USE_V1=1' >> ~/.bashrc",
        f"grep -qF 'NCCL_SOCKET_IFNAME' ~/.bashrc || echo 'export NCCL_SOCKET_IFNAME={net_iface}' >> ~/.bashrc",
        f"grep -qF 'GLOO_SOCKET_IFNAME' ~/.bashrc || echo 'export GLOO_SOCKET_IFNAME={net_iface}' >> ~/.bashrc",
        "grep -qF 'vllm-ray-env/bin' ~/.bashrc || echo 'export PATH=\"$HOME/vllm-ray-env/bin:$PATH\"' >> ~/.bashrc",

        # /etc/environment — used by systemd services + vLLM launch (survives reboot)
        # MODEL vars come from the TEMPLATE's /etc/environment by default.
        # We only OVERWRITE them when the API request explicitly provides a value;
        # if the request leaves model / model_path empty, the template's existing
        # LLM_MODEL_NAME and LLM_MODEL_PATH are left untouched.

        # ── Guard: ensure /etc/environment ends with a newline BEFORE any append.
        # If the template's file has no trailing newline on its last line (e.g.
        # "LLM_MODEL_NAME=..."), the first `tee -a` below would glue onto it
        # ("LLM_MODEL_NAME=ArtLLMVLLM_USE_V1=1") — silently corrupting/erasing
        # that line. This appends a single newline only when the last byte isn't one.
        "sudo test -s /etc/environment && "
        "[ -n \"$(sudo tail -c1 /etc/environment)\" ] && "
        "echo | sudo tee -a /etc/environment > /dev/null || true",

        # LLM_MODEL_NAME: overwrite only if API provided a real model (HF ID / path).
        # If empty → keep the template's LLM_MODEL_NAME from /etc/environment.
        *(
            [
                "sudo sed -i '/^LLM_MODEL_NAME=/d' /etc/environment",
                f'echo \'LLM_MODEL_NAME={model}\' | sudo tee -a /etc/environment > /dev/null',
            ]
            if model else []
        ),
        # LLM_MODEL_PATH: overwrite only if API provided a non-default path.
        # If empty/default → keep the template's LLM_MODEL_PATH from /etc/environment.
        *(
            [
                "sudo sed -i '/^LLM_MODEL_PATH=/d' /etc/environment",
                f'echo \'LLM_MODEL_PATH={model_path}\' | sudo tee -a /etc/environment > /dev/null',
            ]
            if model_path and model_path != "/vllm_data/hf_cache" else []
        ),
        # Runtime vars — safe to always refresh (not model-related)
        "sudo sed -i '/^VLLM_USE_V1=/d' /etc/environment",
        "sudo sed -i '/^NCCL_SOCKET_IFNAME=/d' /etc/environment",
        "sudo sed -i '/^GLOO_SOCKET_IFNAME=/d' /etc/environment",
        'echo \'VLLM_USE_V1=1\' | sudo tee -a /etc/environment > /dev/null',
        f'echo \'NCCL_SOCKET_IFNAME={net_iface}\' | sudo tee -a /etc/environment > /dev/null',
        f'echo \'GLOO_SOCKET_IFNAME={net_iface}\' | sudo tee -a /etc/environment > /dev/null',

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

        # Model cache dir -- head only. On a worker node this path is an NFS
        # mount of the head's own directory, exported read-only
        # (share_model_nfs_activity's "ro" export) -- there's nothing to
        # create or chmod there; the head already set correct permissions
        # on the files (chmod -R a+rX) before sharing them out, and NFS
        # preserves that. Skipped entirely on workers rather than attempted
        # and swallowed, since it's not a "might fail" case there -- it's
        # simply not applicable.
        *(["sudo mkdir -p /vllm_data/hf_cache", "sudo chmod 777 /vllm_data/hf_cache"] if role == "head" else []),

        # Confirm env + versions
        "cat /etc/environment",
        "~/vllm-ray-env/bin/ray --version",
        "~/vllm-ray-env/bin/vllm --version",
    ]
    run_commands(ip, ssh_user, ssh_pass, phase3, timeout=120)
    logger.info(f"[{ip}] Ray + vLLM ready — model: {model}")
    return {"ip_address": ip, "step": "ray_vllm_installed"}


@activity.defn
def configure_ray_activity(payload: dict) -> dict:
    """
    Creates ray-head.service (head node) or ray-worker.service (worker node)
    systemd unit file, then starts and enables it.

    payload must include:
      role         : "head" | "worker"
      ip_address   : this node's IP
      head_ip      : head node IP (only needed when role == "worker")
    """
    ip = payload["ip_address"]
    ssh_user = payload["ssh_user"]
    ssh_pass = payload["ssh_pass"]
    role = payload.get("role", "head")
    head_ip = payload.get("head_ip", ip)
    num_gpus = payload.get("num_gpus", 1)
    net_iface = payload.get("net_iface") or _detect_net_iface(ip, ssh_user, ssh_pass)

    home_dir = "/root" if ssh_user == "root" else f"/home/{ssh_user}"
    venv_bin = f"{home_dir}/vllm-ray-env/bin"
    cuda_path = "/usr/local/cuda/bin"

    # Ray binds several internal services (node-manager, object-manager,
    # runtime-env agent, dashboard agent, metrics) to RANDOM ephemeral ports
    # by default -- only the well-known ports (6379 GCS, 8265 dashboard,
    # 10001 client) were ever opened in the firewall. That gap is what was
    # silently breaking the raylet<->GCS connection: confirmed via `ss
    # -tulnp` that raylet/DashboardAgent/RuntimeEnvAgent were bound to
    # random ports (e.g. 34499, 52365, 37513) outside the opened
    # 10002-19999 range, invisible to the firewall rule written for it.
    # Pinning them to fixed values inside that already-open range makes
    # every port Ray actually uses match what's permitted, so the firewall
    # can stay enabled with no gap and no need to disable it anywhere.
    _fixed_ray_ports = (
        "--node-manager-port=10002 "
        "--object-manager-port=10003 "
        "--runtime-env-agent-port=10004 "
        "--dashboard-agent-listen-port=10005 "
        "--dashboard-agent-grpc-port=10006 "
        "--metrics-export-port=10007 "
        "--min-worker-port=10008 --max-worker-port=19999"
    )

    if role == "head":
        service_name = "ray-head"
        exec_start = (
            f"{venv_bin}/ray start --head --port=6379 "
            f"--num-gpus={num_gpus} "
            "--dashboard-host=0.0.0.0 --include-dashboard=true "
            f"{_fixed_ray_ports} "
            "--block"
        )
    else:
        service_name = "ray-worker"
        exec_start = (
            f"{venv_bin}/ray start --address={head_ip}:6379 "
            f"--num-gpus={num_gpus} "
            f"{_fixed_ray_ports} "
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
        f"User={ssh_user}",
        f"Group={ssh_user}",
        f'Environment="PATH={venv_bin}:{cuda_path}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin"',
        f'Environment="NCCL_SOCKET_IFNAME={net_iface}"',
        # Gloo (the CPU-side control/coordination backend PyTorch uses
        # alongside NCCL) picks its own interface unless told otherwise --
        # without this it can select an IPv6 link-local address, which the
        # firewall's IPv4-only accept-rule doesn't cover, causing
        # "Gloo connectFullMesh failed ... Permission denied" between nodes.
        f'Environment="GLOO_SOCKET_IFNAME={net_iface}"',
        'Environment="VLLM_USE_V1=1"',
        'Environment="LD_LIBRARY_PATH=/usr/local/cuda/lib64"',
        # Ray's memory monitor fails to read this VM's cgroup correctly
        # ("Got negative used memory for cgroup -1"), and that malfunction
        # was observed stalling the raylet's own heartbeat cadence right
        # before GCS marks it dead ("mistakenly been marked as dead").
        # Disabling the monitor (refresh_ms=0) removes the broken component
        # instead of trying to fix cgroup detection inside the guest.
        'Environment="RAY_memory_monitor_refresh_ms=0"',
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
        "",
        # Verify service is active
        f"sudo systemctl is-active {service_name}.service || {{ sudo journalctl -u {service_name}.service --no-pager; exit 1; }}",
        # Poll Ray status until GCS is up (Type=simple means "started" != "ready").
        # Ray head can take 10-60s to bootstrap; retry up to 2 min before failing.
        (
            f"for i in $(seq 1 24); do "
            f"  {venv_bin}/ray status" + ("" if role == "head" else f" --address={head_ip}:6379") + " 2>/dev/null && exit 0; "
            f"  sudo systemctl is-active --quiet {service_name}.service || {{ echo 'ERROR: {service_name} died'; sudo journalctl -u {service_name}.service --no-pager | tail -50; exit 1; }}; "
            f"  echo \"Waiting for ray... $i/24\"; sleep 5; "
            f"done; "
            f"echo 'ERROR: Ray not ready after 2 min'; "
            f"sudo journalctl -u {service_name}.service --no-pager | tail -50; "
            f"exit 1"
        ),
    ]
    run_commands(ip, ssh_user, ssh_pass, cmds, timeout=240)
    logger.info(f"Ray {role} started on {ip} via {service_name}.service")
    return {"ip_address": ip, "role": role, "step": "ray_configured"}

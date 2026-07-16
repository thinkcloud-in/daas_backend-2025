import base64
import io
import logging
import os
import tarfile
import time
from typing import Callable, Iterator, Optional

from kubernetes import client, config
from kubernetes.stream import stream

logger = logging.getLogger(__name__)

_NAMESPACE     = "thinkcloud"
_LABEL_SELECTOR = "name=library"


def _get_k8s_client() -> client.CoreV1Api:
    try:
        config.load_incluster_config()       # backend pod ke andar chal raha ho
    except Exception:
        config.load_kube_config()            # local dev ke liye fallback
    return client.CoreV1Api()


def _find_pod(v1: client.CoreV1Api) -> str:
    """Label selector se current running library pod ka naam dhundho."""
    pods = v1.list_namespaced_pod(
        namespace=_NAMESPACE,
        label_selector=_LABEL_SELECTOR,
    )
    running = [p for p in pods.items if p.status.phase == "Running"]
    if not running:
        raise RuntimeError(f"No running pod found with label '{_LABEL_SELECTOR}' in '{_NAMESPACE}'")
    return running[0].metadata.name


def _exec(v1: client.CoreV1Api, pod_name: str, command: list[str],
          stdin_data: bytes | None = None) -> str:
    """
    Pod mein command exec karo.
    stdin_data diya to stdin mein write karo (file upload ke liye).
    Returns: stdout string
    """
    resp = stream(
        v1.connect_get_namespaced_pod_exec,
        pod_name,
        _NAMESPACE,
        command=command,
        stdin=stdin_data is not None,
        stdout=True,
        stderr=True,
        tty=False,
        _preload_content=False,
    )
    stdout_buf = []
    stderr_buf = []

    if stdin_data is not None:
        # Chunked write to avoid websocket frame size limits
        chunk_size = 512 * 1024  # 512 KB
        for i in range(0, len(stdin_data), chunk_size):
            resp.write_stdin(stdin_data[i : i + chunk_size])

    while resp.is_open():
        resp.update(timeout=5)
        if resp.peek_stdout():
            stdout_buf.append(resp.read_stdout())
        if resp.peek_stderr():
            stderr_buf.append(resp.read_stderr())

    resp.close()
    if stderr_buf:
        logger.debug(f"[K8s exec] stderr: {''.join(stderr_buf)}")
    return "".join(stdout_buf)


# ── Public API ────────────────────────────────────────────────────────────────

def stream_to_pod(
    filename: str,
    chunks: Iterator[bytes],
    file_size: int,
    remote_dir: str,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> int:
    """
    HTTP request bytes ko seedha pod mein stream karo — koi temp file nahi.
    Tar header on-the-fly banata hai, phir chunks stream karta hai pod exec stdin mein.
    progress_cb(bytes_sent, total) — DB/heartbeat update ke liye.
    Returns: total bytes sent.
    """
    v1       = _get_k8s_client()
    pod_name = _find_pod(v1)

    _exec(v1, pod_name, command=["mkdir", "-p", remote_dir])

    # Tar header banao (512 bytes) — puri file memory mein nahi chahiye
    info       = tarfile.TarInfo(name=filename)
    info.size  = file_size
    info.mode  = 0o644
    info.mtime = int(time.time())
    header     = info.tobuf(format=tarfile.GNU_FORMAT)

    # File data ke baad padding (tar blocks 512-byte aligned hote hain)
    remainder = file_size % 512
    padding   = b"\0" * (512 - remainder) if remainder else b""

    logger.info(f"[K8s] Streaming {filename} → pod={pod_name} dir={remote_dir} size={file_size:,}")

    resp = stream(
        v1.connect_get_namespaced_pod_exec,
        pod_name, _NAMESPACE,
        command=["tar", "xf", "-", "-C", remote_dir],
        stdin=True, stdout=True, stderr=True, tty=False,
        _preload_content=False,
    )
    try:
        resp.write_stdin(header)

        bytes_sent = 0
        for chunk in chunks:
            resp.write_stdin(chunk)
            bytes_sent += len(chunk)
            if progress_cb:
                progress_cb(bytes_sent, file_size)

        # Padding + end-of-archive (2 x 512 zero blocks)
        resp.write_stdin(padding + b"\0" * 1024)
    finally:
        resp.close()

    logger.info(f"[K8s] Stream complete: {remote_dir}/{filename} ({bytes_sent:,} bytes)")
    return bytes_sent


def upload_file_to_pod(local_path: str, remote_dir: str) -> None:
    """
    local_path ki file ko library pod ke remote_dir mein copy karo.
    tar stdin pipe use karta hai — SFTP nahi chahiye.
    Directory exist nahi kare to create kar deta hai.
    """
    v1       = _get_k8s_client()
    pod_name = _find_pod(v1)
    file_name = os.path.basename(local_path)

    # Directory ensure karo
    _exec(v1, pod_name, command=["mkdir", "-p", remote_dir])

    # File ko in-memory tar mein pack karo
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        tar.add(local_path, arcname=file_name)
    tar_bytes = buf.getvalue()

    logger.info(f"[K8s] Uploading {file_name} → pod={pod_name} dir={remote_dir}")
    _exec(v1, pod_name,
          command=["tar", "xf", "-", "-C", remote_dir],
          stdin_data=tar_bytes)
    logger.info(f"[K8s] Upload complete: {remote_dir}/{file_name}")


def download_file_from_pod(remote_path: str, local_path: str) -> None:
    """
    Library pod se remote_path ki file ko local_path pe save karo.
    base64 encode karke transfer karta hai (binary safe).
    """
    v1       = _get_k8s_client()
    pod_name = _find_pod(v1)

    logger.info(f"[K8s] Downloading pod={pod_name} {remote_path} → {local_path}")
    b64 = _exec(v1, pod_name,
                command=["sh", "-c", f'base64 -w 0 "{remote_path}"'])
    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    with open(local_path, "wb") as f:
        f.write(base64.b64decode(b64.strip()))
    logger.info(f"[K8s] Download complete: {local_path}")


def delete_file_from_pod(remote_path: str) -> None:
    """Library pod se file delete karo."""
    v1       = _get_k8s_client()
    pod_name = _find_pod(v1)
    logger.info(f"[K8s] Deleting pod={pod_name} {remote_path}")
    _exec(v1, pod_name, command=["rm", "-f", remote_path])
    logger.info(f"[K8s] Deleted: {remote_path}")


def list_files_in_pod(remote_dir: str) -> list[dict]:
    """
    Library pod ke remote_dir mein files list karo.
    Returns: [{"name": str, "size": int, "path": str}]
    """
    v1       = _get_k8s_client()
    pod_name = _find_pod(v1)
    # -1 = one per line, -s = block size, --block-size=1 = bytes
    out = _exec(v1, pod_name,
                command=["sh", "-c",
                         f'find "{remote_dir}" -maxdepth 1 -type f '
                         f'-printf "%f\\t%s\\n" 2>/dev/null'])
    files = []
    for line in out.strip().splitlines():
        parts = line.split("\t", 1)
        if len(parts) == 2:
            name, size = parts
            files.append({
                "name": name,
                "size": int(size) if size.isdigit() else 0,
                "path": f"{remote_dir}/{name}",
            })
    return files


def file_exists_in_pod(remote_path: str) -> bool:
    """Check karo ki file pod mein exist karti hai ya nahi."""
    v1       = _get_k8s_client()
    pod_name = _find_pod(v1)
    out = _exec(v1, pod_name,
                command=["sh", "-c",
                         f'[ -f "{remote_path}" ] && echo yes || echo no'])
    return out.strip() == "yes"

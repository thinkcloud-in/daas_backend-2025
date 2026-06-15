import logging
import time
import paramiko

logger = logging.getLogger(__name__)

_CONNECT_RETRIES = 20
_RETRY_INTERVAL = 15  # seconds — OS boot / reboot can take 2-4 min


def _get_client(host: str, username: str, password: str) -> paramiko.SSHClient:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    for attempt in range(1, _CONNECT_RETRIES + 1):
        try:
            client.connect(host, username=username, password=password, timeout=10)
            logger.info(f"SSH connected to {host} on attempt {attempt}")
            return client
        except Exception as e:
            logger.warning(f"SSH attempt {attempt}/{_CONNECT_RETRIES} to {host} failed: {e}")
            if attempt < _CONNECT_RETRIES:
                time.sleep(_RETRY_INTERVAL)
    raise RuntimeError(f"Could not SSH into {host} after {_CONNECT_RETRIES} attempts")


def run_commands(host: str, username: str, password: str, commands: list[str], timeout: int = 600) -> list[dict]:
    """
    Connects via SSH and runs each command sequentially.
    Raises RuntimeError if any command exits non-zero.
    Returns list of {cmd, stdout, stderr, exit_code}.
    """
    client = _get_client(host, username, password)
    results = []
    try:
        for cmd in commands:
            logger.info(f"[SSH {host}] Running: {cmd[:120]}")
            _, stdout, stderr = client.exec_command(cmd, timeout=timeout)
            exit_code = stdout.channel.recv_exit_status()
            out = stdout.read().decode(errors="replace").strip()
            err = stderr.read().decode(errors="replace").strip()
            results.append({"cmd": cmd, "stdout": out, "stderr": err, "exit_code": exit_code})
            if exit_code != 0:
                raise RuntimeError(
                    f"Command failed (exit {exit_code}) on {host}:\n"
                    f"  CMD   : {cmd}\n"
                    f"  STDERR: {err}"
                )
    finally:
        client.close()
    return results


def reboot_and_wait(host: str, username: str, password: str, wait_before_retry: int = 60):
    """
    Issues a reboot command then waits for the host to come back online.
    wait_before_retry: seconds to wait before starting reconnect attempts
    (give the OS time to actually go down before we start polling).
    """
    try:
        client = _get_client(host, username, password)
        client.exec_command("sudo reboot")
        client.close()
    except Exception:
        pass  # connection drop on reboot is expected

    logger.info(f"Reboot issued to {host}, waiting {wait_before_retry}s before reconnect attempts...")
    time.sleep(wait_before_retry)

    # Now poll until SSH is back
    for attempt in range(1, _CONNECT_RETRIES + 1):
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(host, username=username, password=password, timeout=10)
            client.close()
            logger.info(f"{host} is back online after reboot (attempt {attempt})")
            return
        except Exception as e:
            logger.warning(f"Waiting for {host} to come back... attempt {attempt}/{_CONNECT_RETRIES}: {e}")
            time.sleep(_RETRY_INTERVAL)

    raise RuntimeError(f"{host} did not come back online after reboot within expected time")

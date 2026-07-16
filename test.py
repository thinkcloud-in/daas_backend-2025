# # import httpx
# # url = "http://172.16.1.31:8765/v1/hyper-v/get_node_status_from_cluster"
# # response = httpx.get(url)
# # data = response.json()
# data = {
#   "status": "OK",
#   "code": 200,
#   "msg": "Node Status from Cluster",
#   "data": [
#     {
#       "Node_name": "hyperv-sr-1",
#       "Status": "Up",
#       "IP": "172.16.1.31",
#       "VMCount": 1
#     },
#     {
#       "Node_name": "hyperv-sr-2",
#       "Status": "Up",
#       "IP": "172.16.1.32",
#       "VMCount": 0
#     }
#   ]
# }
# node_name = ''
# node_ip = ''
# vm_count = 1
# raw_data = data.get('data')

# for i in range(len(raw_data)):
#     if raw_data[i].get('Status')=='Up' and raw_data[i].get('VMCount') <= vm_count:
#         vm_count = raw_data[i].get('VMCount')
#         node_ip = raw_data[i].get('IP')
#         node_name = raw_data[i].get('Node_name')

# print(node_ip,vm_count,node_name)


# url = "http://172.16.1.31:8765"

# print(url.split('//')[1].split(':')[1])



import sys, time, paramiko

IP, SSH_USER, SSH_PASS = "172.16.0.168", "vllm", "Teamw0rk@1"

HOME_DIR = "/home/vllm"
VLLM_BIN = f"{HOME_DIR}/vllm-ray-env/bin/python3 -m vllm.entrypoints.openai.api_server"
VLLM_ARGS = (
    " --max-model-len 4096"
    " --gpu-memory-utilization 0.90"
    " --enable-chunked-prefill"
    " --trust-remote-code"
    " --host 0.0.0.0 --port 8000"
)

# Exact replica of vllm_launch command from activities_llm_inference_v2.py
launch_cmd = (
    "source /etc/profile || true; "
    "source ~/.bash_profile || true; "
    "source ~/.bashrc || true; "
    "while IFS='=' read -r _k _v; do "
    "  case \"$_k\" in '#'*|'') continue;; esac; "
    "  export \"$_k=$_v\"; "
    "done < /etc/environment; "
    f"source {HOME_DIR}/vllm-ray-env/bin/activate; "
    "export CUDA_HOME=/usr/local/cuda; "
    "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/cuda/lib64:/usr/lib64:/usr/lib/x86_64-linux-gnu; "
    "export PATH=$PATH:/usr/local/cuda/bin; "
    "export VLLM_DEVICE=cuda; "
    "export CUDA_VISIBLE_DEVICES=0; "
    "RESOLVED_MODEL=''; "
    "if [ -n \"${LLM_MODEL_PATH:-}\" ] && [ -n \"${LLM_MODEL_NAME:-}\" ]; then "
    "  _combined=\"${LLM_MODEL_PATH}/${LLM_MODEL_NAME}\"; "
    "  if [ -f \"${_combined}/config.json\" ]; then "
    "    RESOLVED_MODEL=\"$_combined\"; "
    "    echo \"[vLLM] Model resolved: $RESOLVED_MODEL\"; "
    "  else "
    "    echo \"[vLLM] WARNING: ${_combined}/config.json not found\"; "
    "  fi; "
    "fi; "
    "if [ -z \"$RESOLVED_MODEL\" ]; then "
    "  for _cfg in /vllm_data/hf_cache/*/config.json; do "
    "    [ -f \"$_cfg\" ] && { RESOLVED_MODEL=$(dirname \"$_cfg\"); "
    "      echo \"[vLLM] Found model: $RESOLVED_MODEL\"; break; }; "
    "  done; "
    "fi; "
    "if [ -z \"$RESOLVED_MODEL\" ]; then "
    "  echo 'VLLM_SKIP: no model found'; "
    "  echo \"LLM_MODEL_PATH=${LLM_MODEL_PATH:-<unset>}\"; "
    "  echo \"LLM_MODEL_NAME=${LLM_MODEL_NAME:-<unset>}\"; "
    "  exit 1; "
    "fi; "
    "echo '[step1] before pkill'; "
    "pgrep -f 'vllm.entrypoints.openai.api_server' | grep -v $$ | xargs -r kill 2>/dev/null || true; "
    "echo '[step2] after pkill'; "
    f"echo '===== vLLM launch =====' > {HOME_DIR}/vllm_server.log; "
    "echo '[step3] log created'; "
    f"echo '[vLLM-env] VLLM_DEVICE=cuda' >> {HOME_DIR}/vllm_server.log; "
    f"echo '[vLLM-env] /dev/nvidia*' >> {HOME_DIR}/vllm_server.log; "
    "echo '[step4] env logged'; "
    f"nohup env VLLM_DEVICE=cuda CUDA_VISIBLE_DEVICES=0 CUDA_HOME=/usr/local/cuda {VLLM_BIN}"
    f"  --model \"$RESOLVED_MODEL\""
    f"  --served-model-name \"$RESOLVED_MODEL\""
    f"  {VLLM_ARGS}"
    f"  >> {HOME_DIR}/vllm_server.log 2>&1 &"
    "echo '[step5] nohup fired'; "
    "echo '[vLLM] Process launched in background'"
)

def ssh_run(client, cmd, timeout=120):
    _, stdout, stderr = client.exec_command(cmd)
    stdout.channel.settimeout(timeout)
    out = stdout.read().decode(errors="replace").strip()
    err = stderr.read().decode(errors="replace").strip()
    rc = stdout.channel.recv_exit_status()
    return rc, out, err

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(IP, username=SSH_USER, password=SSH_PASS, timeout=15)
client.get_transport().set_keepalive(15)

# Step 1: fire the launch command
print("=== FIRING LAUNCH COMMAND ===")
rc, out, err = ssh_run(client, launch_cmd)
print(f"exit_code: {rc}")
print(f"stdout:\n{out}")
if err:
    print(f"stderr:\n{err}")

if rc not in (0, -1):  # -1 = channel closed without exit status (normal for nohup+disown)
    print("\n[LAUNCH FAILED — not waiting for log]")
    client.close()
    raise SystemExit(1)

# Step 2: immediate diagnostics — no wait needed
print("\n=== IMMEDIATE DIAGNOSTICS ===")
rc2, diag, _ = ssh_run(client, (
    f"echo '--- ls -la {HOME_DIR} ---'; ls -la {HOME_DIR}/; "
    f"echo '--- whoami ---'; whoami; "
    f"echo '--- touch test ---'; touch {HOME_DIR}/test_write.tmp && echo 'write OK' || echo 'write FAILED'; "
    f"echo '--- log file ---'; ls -la {HOME_DIR}/vllm_server.log 2>/dev/null || echo 'log NOT found'; "
    f"echo '--- vllm process ---'; pgrep -fa 'vllm.entrypoints' || echo 'vllm NOT running'; "
    f"echo '--- nohup.out ---'; cat {HOME_DIR}/nohup.out 2>/dev/null || echo 'no nohup.out'; "
))
print(diag)

client.close()
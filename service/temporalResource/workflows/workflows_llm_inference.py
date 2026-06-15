import logging
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.workflow import ParentClosePolicy

from service.temporalResource.activity import activities_llm_inference

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
    maximum_attempts=3,
)


@workflow.defn(sandboxed=False)
class CreateLLMInferenceWorkflow:
    """
    Orchestrates the full LLM inference VM provisioning pipeline:
      1. Create VM with GPU passthrough
      2. Add HA affinity rule
      3. Configure cloud-init + boot OS
      4. Install devraq agent
      5. Install ray + vllm
      6. Configure ray head
      7. Launch vllm model
      8. Persist final state to DB
    """

    @workflow.run
    async def run(self, payload: dict) -> dict:
        inference_id = payload["inference_id"]
        ssh_creds = {
            "ssh_user": payload.get("ssh_user"),
            "ssh_pass": payload.get("ssh_pass"),
        }
        pve_creds = {
            "pve_ssh_user": payload.get("pve_ssh_user"),
            "pve_ssh_pass": payload.get("pve_ssh_pass"),
        }

        # ── Step 1: Create VM ─────────────────────────────────────────────────
        vm_result = await workflow.execute_activity(
            activities_llm_inference.create_vm_with_gpu_activity,
            args=[{**payload, **pve_creds}],
            retry_policy=_RETRY,
            start_to_close_timeout=timedelta(minutes=15),
        )
        vmid = vm_result["vmid"]
        node = vm_result["node"]
        # ip/gateway/dns/hostname already set on VM during creation
        ip_address = vm_result["ip_address"]

        await workflow.execute_activity(
            activities_llm_inference.update_llm_inference_status_activity,
            args=[{"inference_id": inference_id, "vmid": vmid, "ip_address": ip_address, "status": "vm_created"}],
            retry_policy=_RETRY,
            start_to_close_timeout=timedelta(minutes=2),
        )

        # ── Step 2: Affinity rule ─────────────────────────────────────────────
        await workflow.execute_activity(
            activities_llm_inference.add_affinity_rule_activity,
            args=[{"cluster_id": payload["cluster_id"], "vmid": vmid, "node": node}],
            retry_policy=_RETRY,
            start_to_close_timeout=timedelta(minutes=2),
        )

        # ── Step 3: Boot VM (cloud-init config was set during VM creation) ────
        await workflow.execute_activity(
            activities_llm_inference.configure_cloudinit_activity,
            args=[{
                "cluster_id": payload["cluster_id"],
                "vmid": vmid,
                "node": node,
                "reserved_ip": payload["reserved_ip"],
            }],
            retry_policy=_RETRY,
            start_to_close_timeout=timedelta(minutes=10),
        )

        ssh_base = {"ip_address": ip_address, **ssh_creds}

        # ── Step 4: devraq agent ──────────────────────────────────────────────
        await workflow.execute_activity(
            activities_llm_inference.install_devraq_agent_activity,
            args=[{**ssh_base, "devraq_rpm_url": payload.get("devraq_rpm_url", "")}],
            retry_policy=_RETRY,
            start_to_close_timeout=timedelta(minutes=10),
        )

        # ── Step 5: ray + vllm ────────────────────────────────────────────────
        await workflow.execute_activity(
            activities_llm_inference.install_ray_vllm_activity,
            args=[{
                **ssh_base,
                "name": payload["name"],
                "subnet": payload.get("subnet", "192.168.100.0/24"),
                "net_iface": payload.get("net_iface", "ens18"),
            }],
            retry_policy=_RETRY,
            start_to_close_timeout=timedelta(minutes=60),
        )

        # ── Step 6: configure ray ─────────────────────────────────────────────
        await workflow.execute_activity(
            activities_llm_inference.configure_ray_activity,
            args=[{**ssh_base, "role": "head"}],
            retry_policy=_RETRY,
            start_to_close_timeout=timedelta(minutes=5),
        )

        # ── Step 7: launch vllm ───────────────────────────────────────────────
        launch_result = await workflow.execute_activity(
            activities_llm_inference.launch_vllm_model_activity,
            args=[{
                **ssh_base,
                "model": payload["model"],
                "hf_token": payload.get("hf_token", ""),
                "tensor_parallel_size": payload.get("tensor_parallel_size", 1),
                "pipeline_parallel_size": payload.get("pipeline_parallel_size", 1),
                "quantization": payload.get("quantization", ""),
                "max_model_len": payload.get("max_model_len", 32768),
                "gpu_memory_utilization": payload.get("gpu_memory_utilization", 0.90),
                "served_model_name": payload.get("served_model_name", ""),
            }],
            retry_policy=_RETRY,
            start_to_close_timeout=timedelta(minutes=10),
        )
        endpoint_url = launch_result["endpoint_url"]

        # ── Step 8: final DB update ───────────────────────────────────────────
        await workflow.execute_activity(
            activities_llm_inference.update_llm_inference_status_activity,
            args=[{
                "inference_id": inference_id,
                "endpoint_url": endpoint_url,
                "status": "running",
            }],
            retry_policy=_RETRY,
            start_to_close_timeout=timedelta(minutes=2),
        )

        logger.info(f"LLM Inference {inference_id} fully provisioned. Endpoint: {endpoint_url}")
        return {"inference_id": inference_id, "vmid": vmid, "endpoint_url": endpoint_url, "status": "running"}

import asyncio
import logging
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from service.temporalResource.activity import activities_llm_inference_v2
from service.temporalResource.activity import activities_llm_inference  # reuse ray activities

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
    maximum_attempts=3,
)


@workflow.defn(sandboxed=False)
class CreateMultiNodeLLMWorkflow:
    """
    Multi-node LLM inference provisioning:
      Phase 1 — Clone + configure one VM per node (parallel)
      Phase 2 — Update DB with VM info
      Phase 3 — Affinity rules
      Phase 4 — Ray head on nodes[0]
      Phase 5 — Ray workers on nodes[1..] (parallel)
      Phase 6 — Launch vLLM on head node
      Phase 7 — Persist final state
    """

    @workflow.run
    async def run(self, payload: dict) -> dict:
        job_id    = payload["job_id"]
        nodes     = payload["nodes"]          # [{"node": str, "gpu": [str]}]
        ips       = payload["reserved_ips"]   # [{"ip": str, "pool_id": int}]
        ssh_creds = {"ssh_user": payload.get("ssh_user"), "ssh_pass": payload.get("ssh_pass")}

        # ── Phase 1: Clone + configure all VMs in parallel ───────────────────
        clone_tasks = []
        for i, (node_cfg, reserved) in enumerate(zip(nodes, ips)):
            clone_tasks.append(
                workflow.execute_activity(
                    activities_llm_inference_v2.clone_and_configure_vm_activity,
                    args=[{
                        "cluster_id":  payload["cluster_id"],
                        "node":        node_cfg["node"],
                        "gpus":        node_cfg["gpu"],
                        "reserved_ip": reserved["ip"],
                        "ip_pool_id":  reserved["pool_id"],
                        "template":    payload["template"],
                        "storage":     payload.get("storage", "local-lvm"),
                        "pool_name":   payload["name"],
                        **ssh_creds,
                    }],
                    retry_policy=RetryPolicy(maximum_attempts=1),
                    start_to_close_timeout=timedelta(minutes=90),
                )
            )

        vm_results = list(await asyncio.gather(*clone_tasks))

        for i, r in enumerate(vm_results):
            if "error" in r:
                raise Exception(f"VM clone failed for node[{i}] ({nodes[i]['node']}): {r['error']}")

        vmids    = [r["vmid"]       for r in vm_results]
        ip_addrs = [r["ip_address"] for r in vm_results]
        head_ip  = ip_addrs[0]

        # ── Phase 2: Update DB with VM info ───────────────────────────────────
        await workflow.execute_activity(
            activities_llm_inference_v2.update_llm_inference_job_activity,
            args=[{"job_id": job_id, "vmids": vmids, "ip_addresses": ip_addrs,
                   "head_ip": head_ip, "status": "vms_ready"}],
            retry_policy=_RETRY,
            start_to_close_timeout=timedelta(minutes=2),
        )

        # ── Phase 3: Affinity rules ───────────────────────────────────────────
        for vmid in vmids:
            await workflow.execute_activity(
                activities_llm_inference.add_affinity_rule_activity,
                args=[{"cluster_id": payload["cluster_id"], "vmid": vmid, "node": nodes[0]["node"]}],
                retry_policy=_RETRY,
                start_to_close_timeout=timedelta(minutes=2),
            )

        # ── Phase 4: Ray head on first VM ─────────────────────────────────────
        await workflow.execute_activity(
            activities_llm_inference.configure_ray_activity,
            args=[{"ip_address": head_ip, "role": "head", **ssh_creds}],
            retry_policy=_RETRY,
            start_to_close_timeout=timedelta(minutes=10),
        )

        await workflow.execute_activity(
            activities_llm_inference_v2.update_llm_inference_job_activity,
            args=[{"job_id": job_id, "status": "ray_head_ready"}],
            retry_policy=_RETRY,
            start_to_close_timeout=timedelta(minutes=2),
        )

        # ── Phase 5: Ray workers on remaining VMs (parallel) ──────────────────
        worker_tasks = [
            workflow.execute_activity(
                activities_llm_inference.configure_ray_activity,
                args=[{"ip_address": ip, "role": "worker", "head_ip": head_ip, **ssh_creds}],
                retry_policy=_RETRY,
                start_to_close_timeout=timedelta(minutes=10),
            )
            for ip in ip_addrs[1:]
        ]
        if worker_tasks:
            await asyncio.gather(*worker_tasks)

        # ── Phase 6: Launch vLLM on head node ────────────────────────────────
        launch_result = await workflow.execute_activity(
            activities_llm_inference_v2.launch_vllm_from_template_activity,
            args=[{
                "ip_address":             head_ip,
                "tensor_parallel_size":   payload.get("tensor_parallel_size", 1),
                "pipeline_parallel_size": payload.get("pipeline_parallel_size", 1),
                **ssh_creds,
            }],
            retry_policy=RetryPolicy(maximum_attempts=1),
            start_to_close_timeout=timedelta(minutes=20),
        )
        endpoint_url = launch_result.get("endpoint_url", f"http://{head_ip}:8000/v1")

        # ── Phase 7: Final DB update ──────────────────────────────────────────
        await workflow.execute_activity(
            activities_llm_inference_v2.update_llm_inference_job_activity,
            args=[{"job_id": job_id, "endpoint_url": endpoint_url, "status": "running"}],
            retry_policy=_RETRY,
            start_to_close_timeout=timedelta(minutes=2),
        )

        logger.info(f"LLMInferenceJob {job_id} fully provisioned. Endpoint: {endpoint_url}")
        return {"job_id": job_id, "vmids": vmids, "endpoint_url": endpoint_url, "status": "running"}

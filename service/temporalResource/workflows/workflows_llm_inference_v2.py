import asyncio
import logging
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from service.temporalResource.activity import activities_llm_inference_v2
from service.temporalResource.activity import activities_llm_inference  # reuse ray activities

_RETRY_ONCE = RetryPolicy(maximum_attempts=1)

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

        # Compute parallelism sizes from topology
        n_nodes          = len(nodes)                        # one stage per node
        n_gpus_per_node  = len(nodes[0]["gpu"]) if nodes else 1  # GPUs within each node
        net_iface        = payload.get("net_iface", "ens18")

        # ── Phase 2: Update DB with VM info ───────────────────────────────────
        await workflow.execute_activity(
            activities_llm_inference_v2.update_llm_inference_job_activity,
            args=[{"job_id": job_id, "vmids": vmids, "ip_addresses": ip_addrs,
                   "head_ip": head_ip, "status": "vms_ready"}],
            retry_policy=_RETRY,
            start_to_close_timeout=timedelta(minutes=2),
        )

        # ── Phase 2.5: Install NVIDIA + CUDA + Ray + vLLM on all VMs (parallel) ─
        install_tasks = [
            workflow.execute_activity(
                activities_llm_inference.install_ray_vllm_activity,
                args=[{
                    "ip_address": ip,
                    "name":       f"{payload['name']}-{i}",
                    "subnet":     payload.get("subnet", "192.168.100.0/24"),
                    "net_iface":  payload.get("net_iface", "ens18"),
                    "model":      payload.get("model", ""),
                    "model_path": payload.get("model_path", "/vllm_data/hf_cache"),
                    **ssh_creds,
                }],
                retry_policy=RetryPolicy(maximum_attempts=1),
                start_to_close_timeout=timedelta(minutes=90),
            )
            for i, ip in enumerate(ip_addrs)
        ]
        await asyncio.gather(*install_tasks)

        await workflow.execute_activity(
            activities_llm_inference_v2.update_llm_inference_job_activity,
            args=[{"job_id": job_id, "status": "ray_vllm_installed"}],
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
            args=[{
                "ip_address": head_ip,
                "role":       "head",
                "num_gpus":   n_gpus_per_node,
                "net_iface":  net_iface,
                **ssh_creds,
            }],
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
                args=[{
                    "ip_address": ip,
                    "role":       "worker",
                    "head_ip":    head_ip,
                    "num_gpus":   n_gpus_per_node,
                    "net_iface":  net_iface,
                    **ssh_creds,
                }],
                retry_policy=_RETRY,
                start_to_close_timeout=timedelta(minutes=10),
            )
            for ip in ip_addrs[1:]
        ]
        if worker_tasks:
            await asyncio.gather(*worker_tasks)

        # ── Phase 6: Launch vLLM on head node ────────────────────────────────
        # tensor_parallel_size = number of nodes (pipeline stages)
        # pipeline_parallel_size = GPUs per node (within-node parallelism)
        launch_result = await workflow.execute_activity(
            activities_llm_inference_v2.launch_vllm_from_template_activity,
            args=[{
                "ip_address":             head_ip,
                "tensor_parallel_size":   n_nodes,
                "pipeline_parallel_size": n_gpus_per_node,
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


# ── DB status per action ──────────────────────────────────────────────────────
_POOL_ACTION_STATUS = {
    "start":    "vms_ready",
    "restart":  "vms_ready",
    "stop":     "stopped",
    "shutdown": "stopped",
}


@workflow.defn(sandboxed=False)
class PoolVMActionWorkflow:
    """
    Performs start / stop / shutdown / restart on every VM in an LLM pool
    in parallel, waits for each to reach the target state, then:
      - For start/restart: waits for Ray services, relaunches vLLM on head node.
      - For stop/shutdown: updates DB status only.
    """

    @workflow.run
    async def run(self, payload: dict) -> dict:
        job_id     = payload["job_id"]
        action     = payload["action"]
        vmids      = payload["vmids"]
        nodes      = payload["nodes"]
        cluster_id = payload["cluster_id"]
        head_ip    = payload["head_ip"]
        ssh_creds  = {"ssh_user": payload.get("ssh_user"), "ssh_pass": payload.get("ssh_pass")}
        tp_size    = payload.get("tensor_parallel_size", 1)
        pp_size    = payload.get("pipeline_parallel_size", 1)
        ip_addrs   = payload.get("ip_addresses", [])

        # ── Phase 1: VM power action on all nodes in parallel ─────────────────
        action_tasks = [
            workflow.execute_activity(
                activities_llm_inference_v2.vm_power_action_activity,
                args=[{
                    "cluster_id": cluster_id,
                    "vmid":       vmid,
                    "node":       nodes[i]["node"] if i < len(nodes) else nodes[0]["node"],
                    "action":     action,
                }],
                retry_policy=RetryPolicy(maximum_attempts=2),
                start_to_close_timeout=timedelta(minutes=10),
            )
            for i, vmid in enumerate(vmids)
        ]
        results = list(await asyncio.gather(*action_tasks))

        # ── Phase 2: Restore services (only for start / restart) ──────────────
        if action in ("start", "restart"):
            # Workers first (parallel) — Ray workers connect to head
            worker_restore = [
                workflow.execute_activity(
                    activities_llm_inference_v2.restore_llm_services_activity,
                    args=[{
                        "ip_address": ip_addrs[i],
                        "role":       "worker",
                        **ssh_creds,
                    }],
                    retry_policy=RetryPolicy(maximum_attempts=2),
                    start_to_close_timeout=timedelta(minutes=10),
                )
                for i in range(1, len(ip_addrs))   # nodes[1..] are workers
            ]
            if worker_restore:
                await asyncio.gather(*worker_restore)

            # Head node last — waits for full Ray cluster, then relaunches vLLM
            await workflow.execute_activity(
                activities_llm_inference_v2.restore_llm_services_activity,
                args=[{
                    "ip_address":           head_ip,
                    "role":                 "head",
                    "tensor_parallel_size": tp_size,
                    "pipeline_parallel_size": pp_size,
                    **ssh_creds,
                }],
                retry_policy=RetryPolicy(maximum_attempts=1),
                start_to_close_timeout=timedelta(minutes=30),
            )

        # ── Phase 3: Update DB ────────────────────────────────────────────────
        final_status = _POOL_ACTION_STATUS[action]
        await workflow.execute_activity(
            activities_llm_inference_v2.update_llm_inference_job_activity,
            args=[{"job_id": job_id, "status": final_status}],
            retry_policy=_RETRY,
            start_to_close_timeout=timedelta(minutes=2),
        )

        logger.info(f"PoolVMActionWorkflow: job {job_id} action='{action}' → '{final_status}'")
        return {"job_id": job_id, "action": action, "results": results, "status": final_status}


@workflow.defn(sandboxed=False)
class DeleteLLMPoolWorkflow:
    """
    Deletes all VMs, releases IPs, and removes the DB record for an LLM job.
    Single activity so the full deletion is visible as one step in /workflows.
    """

    @workflow.run
    async def run(self, payload: dict) -> dict:
        result = await workflow.execute_activity(
            activities_llm_inference_v2.delete_llm_pool_activity,
            args=[payload],
            retry_policy=_RETRY_ONCE,
            start_to_close_timeout=timedelta(minutes=30),
        )
        logger.info(f"DeleteLLMPoolWorkflow: job {payload.get('job_id')} deleted")
        return result

import asyncio
import logging
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from service.temporalResource.activity import activities_llm_inference_v2
from service.temporalResource.activity import activities_llm_inference  # reuse ray activities

_RETRY_ONCE = RetryPolicy(maximum_attempts=1)

# Logging is configured centrally in utils/logging_config.py (called from
# main.py at startup) — do not reconfigure it per-module.
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
        job_id = payload["job_id"]
        try:
            return await self._provision(payload)
        except Exception as e:
            # Any failure (bad GPU, clone error, vLLM launch, etc.) must flip the
            # DB job to 'failed' so the UI stops showing "provisioning" forever.
            try:
                await workflow.execute_activity(
                    activities_llm_inference_v2.update_llm_inference_job_activity,
                    args=[{"job_id": job_id, "status": "failed"}],
                    retry_policy=_RETRY,
                    start_to_close_timeout=timedelta(minutes=2),
                )
            except Exception as upd_err:
                logger.error(f"Failed to mark job {job_id} as failed: {upd_err}")
            raise

    async def _provision(self, payload: dict) -> dict:
        job_id    = payload["job_id"]
        nodes     = payload["nodes"]          # [{"node": str, "gpu": [str]}]
        ips       = payload["reserved_ips"]   # [{"ip": str, "pool_id": int}]
        ssh_creds = {"ssh_user": payload.get("ssh_user"), "ssh_pass": payload.get("ssh_pass")}

        if not payload.get("subnet"):
            raise ValueError(
                "payload['subnet'] is required (the real cluster subnet in CIDR "
                "form, e.g. '172.16.4.0/24'). It is used to open the inter-node "
                "firewall accept-rule between all cluster VMs. A missing/wrong "
                "subnet here leaves node-to-node traffic (e.g. the PyTorch/NCCL "
                "rendezvous port) unprotected, causing 'No route to host' errors "
                "between GPU workers during multi-node vLLM launch."
            )

        # ── Phase 0.5: Reserve one VMID + one name per node UP FRONT, sequentially ─
        # Must happen before the parallel clone tasks below -- if each node's
        # clone activity independently called /cluster/nextid or computed its
        # own "next free name", two nodes cloning at the same moment could
        # both be handed the same id (Proxmox's nextid has no locking) or the
        # same name (e.g. both "lucky-001"), since neither sees the other's
        # not-yet-created VM.
        reservation = await workflow.execute_activity(
            activities_llm_inference_v2.reserve_vmids_activity,
            args=[{
                "cluster_id":    payload["cluster_id"],
                "count":         len(nodes),
                "pool_name":     payload["name"],
                "name_template": payload.get("name_template"),
            }],
            retry_policy=RetryPolicy(maximum_attempts=1),
            start_to_close_timeout=timedelta(minutes=2),
        )
        reserved_vmids = reservation["vmids"]
        reserved_names = reservation["vm_names"]

        # ── Phase 1: Clone + configure all VMs in parallel ───────────────────
        clone_tasks = []
        for i, (node_cfg, reserved) in enumerate(zip(nodes, ips)):
            clone_tasks.append(
                workflow.execute_activity(
                    activities_llm_inference_v2.clone_and_configure_vm_activity,
                    args=[{
                        "cluster_id":    payload["cluster_id"],
                        "node":          node_cfg["node"],
                        "gpus":          node_cfg["gpu"],
                        "reserved_ip":   reserved["ip"],
                        "ip_pool_id":    reserved["pool_id"],
                        "template":      payload["template"],
                        "storage":       payload.get("storage", "local-lvm"),
                        "pool_name":     payload["name"],
                        "name_template": payload.get("name_template"),
                        "vmid":          reserved_vmids[i],
                        "vm_name":       reserved_names[i],
                        **ssh_creds,
                    }],
                    retry_policy=RetryPolicy(maximum_attempts=1),
                    start_to_close_timeout=timedelta(minutes=210),
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
        n_nodes          = len(nodes)                             # pipeline stages (cross-node)
        n_gpus_per_node  = len(nodes[0]["gpu"]) if nodes else 1  # tensor parallelism (within-node)
        net_iface        = payload.get("net_iface")  # None -> activity auto-detects the VM's real interface

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
                    "subnet":     payload["subnet"],
                    "net_iface":  payload.get("net_iface"),  # None -> activity auto-detects the VM's real interface
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

        # ── Phase 2.6: Verify GPU health on EVERY node, with auto-reboot-retry ──
        # A broken driver (kernel module / userspace library version mismatch --
        # usually from an update that hasn't been rebooted into) previously went
        # completely undetected on worker nodes, since the old GPU/NVML wait
        # only ever ran on the head node inside launch_vllm_from_template_activity.
        # That let vLLM fail deep inside Ray actor init with a confusing,
        # multi-layer error instead of failing fast here with a clear one.
        gpu_health_tasks = [
            workflow.execute_activity(
                activities_llm_inference_v2.verify_gpu_health_activity,
                args=[{"ip_address": ip, **ssh_creds}],
                retry_policy=RetryPolicy(maximum_attempts=1),
                start_to_close_timeout=timedelta(minutes=5),
            )
            for ip in ip_addrs
        ]
        await asyncio.gather(*gpu_health_tasks)

        # ── Phase 3: Affinity rules ───────────────────────────────────────────
        # Each VM must be pinned to the node IT was actually cloned onto --
        # using nodes[0] for every vmid here previously pinned every VM to the
        # first node, so Proxmox's HA agent would try to migrate later VMs
        # (e.g. VM 2 on node B) onto node A, colliding with VM 1's GPU there
        # ("PCI device already in use").
        for i, vmid in enumerate(vmids):
            await workflow.execute_activity(
                activities_llm_inference.add_affinity_rule_activity,
                args=[{"cluster_id": payload["cluster_id"], "vmid": vmid, "node": nodes[i]["node"]}],
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

        # ── Phase 5.5: Confirm Ray cluster actually sees all expected GPUs ────
        # Catches the "Pending Demands stuck forever" scenario immediately,
        # before wasting a 900s health-poll timeout on a launch that could
        # never succeed.
        await workflow.execute_activity(
            activities_llm_inference_v2.verify_ray_cluster_gpu_activity,
            args=[{
                "ip_address":    head_ip,
                "expected_gpus": n_gpus_per_node * n_nodes,
                **ssh_creds,
            }],
            retry_policy=RetryPolicy(maximum_attempts=1),
            start_to_close_timeout=timedelta(minutes=2),
        )

        # ── Phase 6: Launch vLLM on head node ────────────────────────────────
        # tensor_parallel_size  = GPUs per node (within-node, NVLink/PCIe)
        # pipeline_parallel_size = number of nodes (cross-node pipeline stages)
        launch_result = await workflow.execute_activity(
            activities_llm_inference_v2.launch_vllm_from_template_activity,
            args=[{
                "ip_address":             head_ip,
                "tensor_parallel_size":   n_gpus_per_node,
                "pipeline_parallel_size": n_nodes,
                **ssh_creds,
            }],
            retry_policy=RetryPolicy(maximum_attempts=1),
            start_to_close_timeout=timedelta(minutes=30),
        )
        endpoint_url = launch_result.get("endpoint_url")  # None if vLLM was skipped (no model)

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

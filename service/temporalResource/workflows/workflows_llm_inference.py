import asyncio
from datetime import timedelta
from urllib.parse import urlsplit
from temporalio import workflow
from temporalio.common import RetryPolicy
import os
from dotenv import load_dotenv
load_dotenv()
from service.temporalResource.activity import activities_llm_inference

_RETRY_ONCE = RetryPolicy(maximum_attempts=1)

# For the Harbor template pull specifically: safe to retry because failure
# cleanup (in the activity itself) removes both the partial VM and the
# uploaded backup archive before the exception propagates, and every retry
# gets a fresh vmid from Proxmox -- so a retry never collides with the failed
# attempt's leftovers. Kept modest (not 3+) since each attempt can itself
# take a long time given the transfer size involved.
_RETRY_TEMPLATE_PULL = RetryPolicy(
    initial_interval=timedelta(seconds=10),
    backoff_coefficient=2.0,
    maximum_attempts=2,
)

# The push (PV -> Proxmox + VM create) is split from the pull above
# specifically so it can retry more freely -- it never re-touches Harbor,
# so a higher attempt count here doesn't cost a 40-50GB+ re-download each
# time, unlike the pull.
_RETRY_TEMPLATE_PUSH = RetryPolicy(
    initial_interval=timedelta(seconds=10),
    backoff_coefficient=2.0,
    maximum_attempts=2,
)

# Logging is configured centrally in utils/logging_config.py (called from
# main.py at startup) — do not reconfigure it per-module.
logger = workflow.logger
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
                    activities_llm_inference.update_llm_inference_job_activity,
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

        # ── Phase 0.5: Harbor-sourced OS disk -> import, templatize ───────────
        # Runs once for the whole pool, not per node -- every node below clones
        # from this one resulting template exactly like a manually-picked one.
        #
        # Deliberately BEFORE the VMID reservation below: this activity picks
        # its own VMID via /cluster/nextid, independent of that reservation's
        # snapshot of "what's in use". Reserving first and templatizing second
        # let both calls see the same not-yet-created state and hand out the
        # same id to both -- the reservation would then never know 111 (say)
        # was about to become the template, and a later clone attempting to
        # create its own VM 111 would collide with the template that already
        # exists there. Templatizing first makes the template a real VM before
        # the reservation's own live cluster query runs, so its existing
        # exclusion logic (used_ids from the current VM list) naturally skips
        # it -- no special-casing needed.
        template_vmid = payload["template"]
        harbor = payload.get("harbor")
        vm_config = payload.get("vm_config") or {}
        if payload.get("template_source") == "harbor" and harbor:
            template_conn = harbor["template_conn"]
            template_storage = payload.get("template_storage")
            # Split into pull (Harbor -> PV) then push (PV -> Proxmox + VM
            # create) as two separate activities. A push-side failure
            # (chunked-encoding quirks, storage space, VMID collisions --
            # all seen in testing) used to force a full re-pull of a
            # 40-50GB+ file on every retry since it was one combined
            # activity; now retrying the push alone never touches Harbor.
            pull_result = await workflow.execute_activity(
                activities_llm_inference.pull_harbor_template_to_pv_activity,
                args=[{
                    "cluster_id":  payload["cluster_id"],
                    "harbor_url":  template_conn["harbor_url"],
                    "harbor_user": template_conn["harbor_user"],
                    "harbor_pass": template_conn["harbor_pass"],
                    "project":     template_conn["project"],
                    "repository":  template_conn["repository"],
                    "tag":         template_conn["tag"],
                    "template_storage": template_storage,
                }],
                retry_policy=_RETRY_TEMPLATE_PULL,
                start_to_close_timeout=timedelta(hours=3),
                # Without this, a worker that dies mid-transfer (crash, killed
                # process, lost connection) leaves the activity frozen at
                # "Started" forever -- nothing ever notices or retries it,
                # since Temporal has no independent way to check the worker
                # is still alive. The activity heartbeats every ~20s while
                # waiting on the pod's pull call (see _post_with_heartbeat);
                # if none arrive within this window, Temporal marks it failed
                # and retries automatically instead of hanging indefinitely.
                heartbeat_timeout=timedelta(seconds=60),
            )
            push_result = await workflow.execute_activity(
                activities_llm_inference.push_template_to_proxmox_activity,
                args=[{
                    "cluster_id":  payload["cluster_id"],
                    "template_storage": template_storage,
                    "staged_path": pull_result["staged_path"],
                    "filename":    pull_result["filename"],
                }],
                retry_policy=_RETRY_TEMPLATE_PUSH,
                start_to_close_timeout=timedelta(hours=3),
                heartbeat_timeout=timedelta(seconds=60),
            )
            create_result = await workflow.execute_activity(
                activities_llm_inference.create_vm_from_import_activity,
                args=[{
                    "cluster_id":  payload["cluster_id"],
                    "storage":     payload.get("storage", "local-lvm"),
                    # A qcow2 carries no VM config -- these come from the form.
                    "cores":       vm_config.get("cores"),
                    "memory":      vm_config.get("memory"),
                    "network":     vm_config.get("network"),
                    "template_storage": template_storage,
                    "new_vmid":    pull_result["new_vmid"],
                    "import_volid": push_result["import_volid"],
                }],
                retry_policy=_RETRY_TEMPLATE_PUSH,
                start_to_close_timeout=timedelta(hours=3),
                heartbeat_timeout=timedelta(seconds=60),
            )
            restore_result = await workflow.execute_activity(
                activities_llm_inference.convert_to_template_activity,
                args=[{
                    "cluster_id":  payload["cluster_id"],
                    "template_storage": template_storage,
                    "vmid":        create_result["vmid"],
                    "import_volid": push_result["import_volid"],
                }],
                retry_policy=_RETRY,
                start_to_close_timeout=timedelta(minutes=10),
            )
            template_vmid = str(restore_result["template_vmid"])
            await workflow.execute_activity(
                activities_llm_inference.update_llm_inference_job_activity,
                args=[{"job_id": job_id, "template": template_vmid}],
                retry_policy=_RETRY,
                start_to_close_timeout=timedelta(minutes=2),
            )

        # ── Phase 0.6: Reserve one VMID + one name per node UP FRONT, sequentially ─
        # Must happen before the parallel clone tasks below -- if each node's
        # clone activity independently called /cluster/nextid or computed its
        # own "next free name", two nodes cloning at the same moment could
        # both be handed the same id (Proxmox's nextid has no locking) or the
        # same name (e.g. both "lucky-001"), since neither sees the other's
        # not-yet-created VM. Runs AFTER the template above so its live query
        # of existing VMs already sees the template and excludes its VMID.
        reservation = await workflow.execute_activity(
            activities_llm_inference.reserve_vmids_activity,
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
                    activities_llm_inference.clone_and_configure_vm_activity,
                    args=[{
                        "cluster_id":    payload["cluster_id"],
                        "node":          node_cfg["node"],
                        "gpus":          node_cfg["gpu"],
                        "reserved_ip":   reserved["ip"],
                        "ip_pool_id":    reserved["pool_id"],
                        "template":      template_vmid,
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

        # ── Phase 1.5: Harbor-sourced template -> now safe to delete ──────────
        # Clones are full (independent-disk), confirmed above -- nothing
        # depends on the template past this point. Best-effort: a failure
        # here shouldn't fail the whole pool creation, delete_llm_pool_activity
        # carries a safety-net retry of this same cleanup.
        if payload.get("template_source") == "harbor" and harbor:
            try:
                await workflow.execute_activity(
                    activities_llm_inference.delete_harbor_template_activity,
                    args=[{"cluster_id": payload["cluster_id"], "template_vmid": template_vmid}],
                    retry_policy=_RETRY,
                    start_to_close_timeout=timedelta(minutes=5),
                )
            except Exception as cleanup_err:
                logger.warning(f"Harbor template {template_vmid} cleanup failed (non-fatal): {cleanup_err}")

        vmids    = [r["vmid"]       for r in vm_results]
        ip_addrs = [r["ip_address"] for r in vm_results]
        head_ip  = ip_addrs[0]

        # Compute parallelism sizes from topology
        n_nodes          = len(nodes)                             # pipeline stages (cross-node)
        n_gpus_per_node  = len(nodes[0]["gpu"]) if nodes else 1  # tensor parallelism (within-node)
        net_iface        = payload.get("net_iface")  # None -> activity auto-detects the VM's real interface

        # ── Phase 2: Update DB with VM info ───────────────────────────────────
        # Status intentionally stays "provisioning" here (and through the rest
        # of this workflow) -- only "running" (success) or "failed" (the outer
        # except in run()) are meaningful end states for the pool-level status.
        # Granular sub-steps are still visible per-activity in Temporal's own
        # workflow history, so they don't need a matching DB status value.
        # reserved_names (Phase 0.5) is already in the same order as vmids/ip_addrs
        # here -- asyncio.gather preserves clone_tasks order, and clone_tasks was
        # built by zipping nodes/ips in that same order with reserved_vmids[i]/
        # reserved_names[i], so no re-sorting is needed.
        await workflow.execute_activity(
            activities_llm_inference.update_llm_inference_job_activity,
            args=[{"job_id": job_id, "vmids": vmids, "ip_addresses": ip_addrs,
                   "head_ip": head_ip, "machines_name": reserved_names}],
            retry_policy=_RETRY,
            start_to_close_timeout=timedelta(minutes=2),
        )

        # ── Phase 2.5: Install NVIDIA + CUDA + Ray + vLLM on all VMs (parallel) ─
        install_tasks = [
            workflow.execute_activity(
                activities_llm_inference.configure_llm_node_activity,
                args=[{
                    "ip_address": ip,
                    # The VM's actual reserved name (e.g. "lucky-001") --
                    # must match machines_name / what's shown as "Machine
                    # Name" in the UI, since this becomes the OS hostname,
                    # and Telegraf's "host" tag (what the Grafana dashboard's
                    # host variable filters on) is the OS hostname. Was
                    # previously "<pool_name>-<index>", which never matched.
                    "name":       reserved_names[i],
                    "subnet":     payload["subnet"],
                    "net_iface":  payload.get("net_iface"),  # None -> activity auto-detects the VM's real interface
                    "model":      payload.get("model", ""),
                    "model_path": payload.get("model_path", "/vllm_data/hf_cache"),
                    "role":       "head" if i == 0 else "worker",
                    **ssh_creds,
                }],
                retry_policy=RetryPolicy(maximum_attempts=1),
                start_to_close_timeout=timedelta(minutes=90),
            )
            for i, ip in enumerate(ip_addrs)
        ]
        await asyncio.gather(*install_tasks)

        # ── Phase 2.6: Verify GPU health on EVERY node, with auto-reboot-retry ──
        # A broken driver (kernel module / userspace library version mismatch --
        # usually from an update that hasn't been rebooted into) previously went
        # completely undetected on worker nodes, since the old GPU/NVML wait
        # only ever ran on the head node inside launch_vllm_from_template_activity.
        # That let vLLM fail deep inside Ray actor init with a confusing,
        # multi-layer error instead of failing fast here with a clear one.
        gpu_health_tasks = [
            workflow.execute_activity(
                activities_llm_inference.verify_gpu_health_activity,
                args=[{"ip_address": ip, **ssh_creds}],
                retry_policy=RetryPolicy(maximum_attempts=1),
                start_to_close_timeout=timedelta(minutes=5),
            )
            for ip in ip_addrs
        ]
        await asyncio.gather(*gpu_health_tasks)

        # ── Phase 2.65: Harbor-sourced model -> pull once, share if multi-node ─
        # Pulled onto the head node only, ever -- on a multi-node pool it's
        # shared out over NFS so workers read it from there instead of each
        # pulling their own copy. configure_llm_node_activity is re-run per node
        # afterward purely to (re)write LLM_MODEL_PATH/LLM_MODEL_NAME -- its
        # driver/venv install steps are already-satisfied no-ops at that point.
        if payload.get("template_source") == "harbor" and harbor and harbor.get("model_repository"):
            # Base dir only -- pull_harbor_model_to_vm_activity creates the
            # model's own subfolder inside this (named from whatever Harbor
            # actually returns, e.g. "qwen2.5-0.5b-instruct"), matching the
            # dedicated volume's own mount_path below exactly so the model
            # folder lands directly on that volume, not nested under an
            # extra fixed "harbor_model" layer.
            model_dir = "/vllm_data/hf_cache"

            # Dedicated volume for the weights, mounted at the path the pull
            # below writes into. The activity sizes it from the artifact's own
            # metadata. Attached to the head node only -- workers reach the
            # same files over NFS. Keeps the model off the OS disk, which
            # arrives fixed-size from Harbor and cannot grow.
            await workflow.execute_activity(
                activities_llm_inference.provision_model_volume_activity,
                args=[{
                    "cluster_id":  payload["cluster_id"],
                    "node":        nodes[0]["node"],
                    "vmid":        vmids[0],
                    "ip_address":  head_ip,
                    "storage":     payload.get("storage", "local-lvm"),
                    "slot":        "scsi1",
                    "mount_path":  "/vllm_data/hf_cache",
                    "harbor_url":  harbor["harbor_url"],
                    "harbor_user": harbor["harbor_user"],
                    "harbor_pass": harbor["harbor_pass"],
                    "project":     harbor["project"],
                    "repository":  harbor["model_repository"],
                    "tag":         harbor["model_tag"],
                    **ssh_creds,
                }],
                retry_policy=_RETRY_ONCE,
                start_to_close_timeout=timedelta(minutes=30),
            )

            pull_result = await workflow.execute_activity(
                activities_llm_inference.pull_harbor_model_to_vm_activity,
                args=[{
                    "ip_address":  head_ip,
                    "harbor_url":  harbor["harbor_url"],
                    "harbor_user": harbor["harbor_user"],
                    "harbor_pass": harbor["harbor_pass"],
                    "project":     harbor["project"],
                    "repository":  harbor["model_repository"],
                    "tag":         harbor["model_tag"],
                    "model_dir":   model_dir,
                    **ssh_creds,
                }],
                retry_policy=_RETRY_ONCE,
                start_to_close_timeout=timedelta(hours=2),
            )
            model_file = pull_result["model_file"]

            if len(ip_addrs) > 1:
                await workflow.execute_activity(
                    activities_llm_inference.share_model_nfs_activity,
                    args=[{"ip_address": head_ip, "model_dir": model_dir, "subnet": payload["subnet"], **ssh_creds}],
                    retry_policy=_RETRY,
                    start_to_close_timeout=timedelta(minutes=10),
                )
                mount_tasks = [
                    workflow.execute_activity(
                        activities_llm_inference.mount_model_nfs_activity,
                        args=[{"ip_address": ip, "head_ip": head_ip, "model_dir": model_dir, **ssh_creds}],
                        retry_policy=_RETRY,
                        start_to_close_timeout=timedelta(minutes=10),
                    )
                    for ip in ip_addrs[1:]
                ]
                await asyncio.gather(*mount_tasks)

            env_tasks = [
                workflow.execute_activity(
                    activities_llm_inference.configure_llm_node_activity,
                    args=[{
                        "ip_address": ip,
                        "name":       reserved_names[i],
                        "subnet":     payload["subnet"],
                        "net_iface":  payload.get("net_iface"),
                        "model":      model_file,
                        "model_path": model_dir,
                        "role":       "head" if i == 0 else "worker",
                        **ssh_creds,
                    }],
                    retry_policy=RetryPolicy(maximum_attempts=1),
                    start_to_close_timeout=timedelta(minutes=10),
                )
                for i, ip in enumerate(ip_addrs)
            ]
            await asyncio.gather(*env_tasks)

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
            activities_llm_inference.verify_ray_cluster_gpu_activity,
            args=[{
                "ip_address":    head_ip,
                "expected_gpus": n_gpus_per_node * n_nodes,
                **ssh_creds,
            }],
            retry_policy=RetryPolicy(maximum_attempts=1),
            start_to_close_timeout=timedelta(minutes=2),
        )
        # ── Phase 6: Configure Telegraf -> InfluxDB on every VM (parallel) ─────

        # Production sets URL and PORT as two separate env vars (INFLUXDB_URL
        # has no port baked in) -- combine them here into the one full URL
        # Telegraf's config actually needs. If INFLUXDB_URL already has an
        # explicit port (someone sets it combined), that's left alone rather
        # than double-appending.
        _influxdb_url_raw  = os.getenv("INFLUXDB_URL")
        _influxdb_port     = os.getenv("INFLUXDB_PORT")
        if _influxdb_url_raw and _influxdb_port and urlsplit(_influxdb_url_raw).port is None:
            influxdb_url = f"{_influxdb_url_raw.rstrip('/')}:{_influxdb_port}"
        else:
            influxdb_url = _influxdb_url_raw

        influxdb_token  = os.getenv("INFLUXDB_TOKEN")
        influxdb_org    = os.getenv("INFLUXDB_ORG")
        # Deliberately NOT the same as INFLUXDB_BUCKET -- that one is used
        # for Proxmox host-level metrics (a separate, pre-existing feature).
        # LLM pool VMs need their own bucket, added to the deployment env
        # specifically for this purpose.
        influxdb_bucket = os.getenv("INFLUXDB_LINUX_METRICS_BUCKET")

        influx_tasks = [
            workflow.execute_activity(
                activities_llm_inference.lunch_configure_influxdb_activity,
                args=[{
                    "ip_address":      ip,
                    "influxdb_url":    influxdb_url,
                    "influxdb_token":  influxdb_token,
                    "influxdb_org":    influxdb_org,
                    "influxdb_bucket": influxdb_bucket,
                    **ssh_creds,
                }],
                retry_policy=RetryPolicy(maximum_attempts=2),
                start_to_close_timeout=timedelta(minutes=5),
            )
            for ip in ip_addrs
        ]
        await asyncio.gather(*influx_tasks)

        # ── Phase 7: Launch vLLM on head node ────────────────────────────────
        # tensor_parallel_size  = GPUs per node (within-node, NVLink/PCIe)
        # pipeline_parallel_size = number of nodes (cross-node pipeline stages)
        launch_result = await workflow.execute_activity(
            activities_llm_inference.launch_vllm_from_template_activity,
            args=[{
                "ip_address":             head_ip,
                "tensor_parallel_size":   n_gpus_per_node,
                "pipeline_parallel_size": n_nodes,
                "model_type":             payload.get("model_type"),
                "max_images_per_request": payload.get("max_images_per_request"),
                "vllm_extra_params":      payload.get("vllm_extra_params"),
                "api_key":                payload.get("api_key"),
                **ssh_creds,
            }],
            retry_policy=RetryPolicy(maximum_attempts=1),
            start_to_close_timeout=timedelta(minutes=30),
        )
        endpoint_url = launch_result.get("endpoint_url")  # None if vLLM was skipped (no model)

        # ── Phase 8: Final DB update ──────────────────────────────────────────
        await workflow.execute_activity(
            activities_llm_inference.update_llm_inference_job_activity,
            args=[{"job_id": job_id, "endpoint_url": endpoint_url, "status": "running"}],
            retry_policy=_RETRY,
            start_to_close_timeout=timedelta(minutes=2),
        )

        logger.info(f"LLMInferenceJob {job_id} fully provisioned. Endpoint: {endpoint_url}")
        return {"job_id": job_id, "vmids": vmids, "endpoint_url": endpoint_url, "status": "running"}


# ── DB status per action ──────────────────────────────────────────────────────
_POOL_ACTION_STATUS = {
    "start":    "running",
    "restart":  "running",
    "stop":     "stopped",
    "shutdown": "stopped",
}

# Per-action failure status -- kept distinct from creation's "failed" since an
# action dying doesn't mean the pool was never usable (it may have been
# running fine before this action was attempted), and distinct per action so
# the UI/DB can tell which operation broke.
_POOL_ACTION_FAILED_STATUS = {
    "start":    "start_failed",
    "restart":  "restart_failed",
    "stop":     "stop_failed",
    "shutdown": "stop_failed",
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
        job_id = payload["job_id"]
        action = payload.get("action")
        try:
            return await self._execute(payload)
        except Exception as e:
            # Without this, a failure partway through (e.g. a bad vLLM launch
            # config) leaves the DB status stuck on the pending value
            # ("restarting", "starting", ...) forever, since nothing else
            # ever writes to it -- the UI shows an action in progress
            # indefinitely even though the workflow has already died.
            failed_status = _POOL_ACTION_FAILED_STATUS.get(action, "failed")
            try:
                await workflow.execute_activity(
                    activities_llm_inference.update_llm_inference_job_activity,
                    args=[{"job_id": job_id, "status": failed_status}],
                    retry_policy=_RETRY,
                    start_to_close_timeout=timedelta(minutes=2),
                )
            except Exception as upd_err:
                logger.error(f"Failed to mark job {job_id} as {failed_status} after pool action error: {upd_err}")
            raise

    async def _execute(self, payload: dict) -> dict:
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
                activities_llm_inference.vm_power_action_activity,
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
                    activities_llm_inference.restore_llm_services_activity,
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
                activities_llm_inference.restore_llm_services_activity,
                args=[{
                    "ip_address":           head_ip,
                    "role":                 "head",
                    "tensor_parallel_size": tp_size,
                    "pipeline_parallel_size": pp_size,
                    "model_type":             payload.get("model_type"),
                    "max_images_per_request": payload.get("max_images_per_request"),
                    "vllm_extra_params":      payload.get("vllm_extra_params"),
                    "api_key":                payload.get("api_key"),
                    **ssh_creds,
                }],
                retry_policy=RetryPolicy(maximum_attempts=1),
                start_to_close_timeout=timedelta(minutes=30),
            )

        # ── Phase 3: Update DB ────────────────────────────────────────────────
        final_status = _POOL_ACTION_STATUS[action]
        await workflow.execute_activity(
            activities_llm_inference.update_llm_inference_job_activity,
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
            activities_llm_inference.delete_llm_pool_activity,
            args=[payload],
            retry_policy=_RETRY_ONCE,
            start_to_close_timeout=timedelta(minutes=30),
        )
        logger.info(f"DeleteLLMPoolWorkflow: job {payload.get('job_id')} deleted")
        return result

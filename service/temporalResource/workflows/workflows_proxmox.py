import asyncio
from temporalio import workflow
from datetime import timedelta
from service.temporalResource.activity import activities_proxmox
import logging
from temporalio.common import RetryPolicy
from temporalio.workflow import ParentClosePolicy

# NOTE: activities_pool is intentionally NOT imported at module level here —
# activities_pool -> proxmoxService -> workflows_proxmox would be a circular
# import. finalize_cloned_machine_activity / configure_domain_join_activity
# (defined in activities_pool.py) are referenced below by their registered
# string name instead, which Temporal resolves against the worker's activity
# registry at run time.

# Logging is configured centrally in utils/logging_config.py (called from
# main.py at startup) — do not reconfigure it per-module.
logger = logging.getLogger("create_machine_workflow")
 
@workflow.defn(sandboxed=False)
class WaitAndAssignIPsWorkflow:
    @workflow.run
    async def run(self, vms, ip_list, pool_id):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=90),
            maximum_attempts=5,
        )
    
        for (name, vmid, node, upid, _), ip in zip(vms, ip_list):
            await workflow.execute_activity(
                activities_proxmox.update_machine_provisioning_status_activity,
                args=[{"vmid": vmid, "step": "waiting_for_boot"}],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=30),
            )
            await workflow.execute_activity(
                activities_proxmox.wait_for_vm_ready_activity,
                args=[{
                    "pool_id": pool_id,
                    "node": node,
                    "upid": upid,
                    "vmid": vmid,
                    "timeout": 300,
                }],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(minutes=5),
            )
            await workflow.execute_activity(
                activities_proxmox.update_machine_provisioning_status_activity,
                args=[{"vmid": vmid, "step": "assigning_ip"}],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=30),
            )
            await workflow.execute_activity(
                activities_proxmox.assign_ip_to_vm_activity,
                args=[{
                    "vmid": vmid,
                    "pool_id": pool_id,
                    "ip_address": ip,
                }],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(minutes=5),
            )
            await workflow.execute_activity(
                activities_proxmox.update_machine_provisioning_status_activity,
                args=[{"vmid": vmid, "step": "powering_on"}],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=30),
            )
            # Auto-start the VM after IP assignment
            await workflow.execute_activity(
                activities_proxmox.start_vm_proxmox_activity,
                args=[str(vmid), str(pool_id)],
                task_queue="vmpower-task-queue",
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=150),
            )
            await workflow.execute_activity(
                activities_proxmox.update_machine_provisioning_status_activity,
                args=[{"vmid": vmid, "step": "completed"}],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=30),
            )
 
 
def batch_items(items, batch_size):
    for i in range(0, len(items), batch_size):
        yield items[i:i+batch_size]
 
@workflow.defn(sandboxed=False)
class CloneVMWorkflow:
    @workflow.run
    async def run(self, clone_payload: dict):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=90),
            maximum_attempts=5,
        )
        short_retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=3,
        )

        pool_id = clone_payload["pool_id"]
        join_ad = bool(clone_payload.get("join_ad"))
        email = clone_payload.get("email")

        total_count = clone_payload["count"]
        ip_list = clone_payload["ip_list"]

        BATCH_SIZE = 10
        results = []
        machines_out = []
        for batch_start in range(0, total_count, BATCH_SIZE):
            batch_count = min(BATCH_SIZE, total_count - batch_start)
            batch_ip_list = ip_list[batch_start:batch_start + batch_count]
            batch_payload = clone_payload.copy()
            batch_payload["count"] = batch_count
            batch_payload["ip_list"] = batch_ip_list

            logger.info(
                f"Processing batch {batch_start // BATCH_SIZE + 1}: "
                f"VMs {batch_start} to {batch_start + batch_count - 1} "
                f"with IPs {batch_ip_list}"
            )


            result = await workflow.execute_activity(
                activities_proxmox.clone_vm_activity,
                args=[batch_payload],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(minutes=10),
            )
            if isinstance(result, dict) and "error" in result:

                return result

            vms = result.get("vms", [])

            # ── Step 1: create the Machine DB row for every VM in the batch,
            # so later steps (which look the row up by vmid) can find it. ──
            ready_vms = []  # vms that got a DB row successfully
            for (name, vmid, node, upid, clone_workflow_id), ip in zip(vms, batch_ip_list):
                create_result = await workflow.execute_activity(
                    "finalize_cloned_machine_activity",
                    args=[{
                        "pool_id": pool_id,
                        "vmid": vmid,
                        "name": name,
                        "ip": ip,
                        "email": email,
                        "workflow_ids": [clone_workflow_id, workflow.info().workflow_id],
                    }],
                    retry_policy=short_retry_policy,
                    start_to_close_timeout=timedelta(seconds=60),
                )
                vm_record = {
                    "name": name, "vmid": vmid, "node": node, "upid": upid,
                    "clone_workflow_id": clone_workflow_id,
                    "wait_assign_workflow_id": workflow.info().workflow_id,
                    "ip": ip,
                }
                if create_result.get("status") != "ok":
                    logger.error(f"Failed to create machine row for vmid={vmid}: {create_result}")
                    vm_record["error"] = create_result.get("error")
                    results.append(vm_record)
                    continue
                machines_out.append(create_result.get("machine"))
                ready_vms.append(vm_record)

            # ── Step 2: wait for each VM's clone task to actually finish
            # (disk copy done) before touching its config or booting it ──
            for vm_record in ready_vms:
                vmid = vm_record["vmid"]
                await workflow.execute_activity(
                    activities_proxmox.update_machine_provisioning_status_activity,
                    args=[{"vmid": vmid, "step": "waiting_for_boot"}],
                    retry_policy=short_retry_policy,
                    start_to_close_timeout=timedelta(seconds=30),
                )
                await workflow.execute_activity(
                    activities_proxmox.wait_for_vm_ready_activity,
                    args=[{
                        "pool_id": pool_id,
                        "node": vm_record["node"],
                        "upid": vm_record["upid"],
                        "vmid": vmid,
                        "timeout": 300,
                    }],
                    retry_policy=retry_policy,
                    start_to_close_timeout=timedelta(minutes=5),
                )

            # ── Step 3: assign IP to every VM that has a DB row ──
            for vm_record in ready_vms:
                vmid = vm_record["vmid"]
                await workflow.execute_activity(
                    activities_proxmox.update_machine_provisioning_status_activity,
                    args=[{"vmid": vmid, "step": "assigning_ip"}],
                    retry_policy=short_retry_policy,
                    start_to_close_timeout=timedelta(seconds=30),
                )
                await workflow.execute_activity(
                    activities_proxmox.assign_ip_to_vm_activity,
                    args=[{
                        "vmid": vmid,
                        "pool_id": pool_id,
                        "ip_address": vm_record["ip"],
                    }],
                    retry_policy=retry_policy,
                    start_to_close_timeout=timedelta(minutes=5),
                )

            # ── Step 4: attach the AD-join cloud-init snippet BEFORE power-on,
            # so it's picked up on first boot (only if the pool requested it). ──
            if join_ad and ready_vms:
                for vm_record in ready_vms:
                    await workflow.execute_activity(
                        activities_proxmox.update_machine_provisioning_status_activity,
                        args=[{"vmid": vm_record["vmid"], "step": "configuring_domain_join"}],
                        retry_policy=short_retry_policy,
                        start_to_close_timeout=timedelta(seconds=30),
                    )
                await workflow.execute_activity(
                    "configure_domain_join_activity",
                    args=[{
                        "pool_id": pool_id,
                        "pool_ad_domain": clone_payload.get("domain"),
                        "pool_ad_password": clone_payload.get("domain_password"),
                        "pool_ad_username": clone_payload.get("username"),
                        "pool_ad_path": clone_payload.get("ou"),
                    }],
                    retry_policy=short_retry_policy,
                    start_to_close_timeout=timedelta(minutes=2),
                )

            # ── Step 5: power on every VM that made it this far ──
            for vm_record in ready_vms:
                vmid = vm_record["vmid"]
                await workflow.execute_activity(
                    activities_proxmox.update_machine_provisioning_status_activity,
                    args=[{"vmid": vmid, "step": "powering_on"}],
                    retry_policy=short_retry_policy,
                    start_to_close_timeout=timedelta(seconds=30),
                )
                await workflow.execute_activity(
                    activities_proxmox.start_vm_proxmox_activity,
                    args=[str(vmid), str(pool_id)],
                    task_queue="vmpower-task-queue",
                    retry_policy=retry_policy,
                    start_to_close_timeout=timedelta(seconds=150),
                )
                await workflow.execute_activity(
                    activities_proxmox.update_machine_provisioning_status_activity,
                    args=[{"vmid": vmid, "step": "completed"}],
                    retry_policy=short_retry_policy,
                    start_to_close_timeout=timedelta(seconds=30),
                )

            logger.info(
                f"Batch {batch_start // BATCH_SIZE + 1} cloned VMs: {[v['name'] for v in ready_vms]}"
            )
            results.extend(ready_vms)

        logger.info("All batches processed.")
        failed_vms = [v for v in results if v.get("error")]
        response = {"vms": results, "machines": machines_out}
        if failed_vms:
            # These VMs were actually cloned in Proxmox but couldn't be
            # registered (e.g. a stale Guacamole connection squatting the
            # generated name) — surface this loudly instead of burying it
            # inside individual vm dicts, since real orphaned VMs need
            # someone to go clean them up.
            response["partial_failure"] = True
            response["failed_vms"] = failed_vms
            response["msg"] = (
                f"{len(failed_vms)} of {len(results)} VM(s) were cloned in Proxmox but could not be "
                f"registered and are now orphaned — manual cleanup required. See failed_vms for details."
            )
            logger.error(response["msg"])
        return response
   
 

@workflow.defn(sandboxed=False)
class LiveMigrateWorkflow:
    @workflow.run
    async def run(self, migration_payload: dict) -> str:
        poll_interval = migration_payload.get("poll_interval", 60)
        batch_size = migration_payload.get("batch_size", 5000)
    
        last_time = migration_payload.get("start_time")
        if not last_time:
        
            import datetime
            last_time = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        

        while True:
            migration_payload["time_range_start"] = last_time
            migration_payload["batch_size"] = batch_size
            result = await workflow.execute_activity(
                activities_proxmox.migrate_bucket_new_data_activity,
                args=[migration_payload],
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=2),
                    backoff_coefficient=2.0,
                    maximum_interval=timedelta(seconds=30),
                    maximum_attempts=3,
                ),  
                start_to_close_timeout=timedelta(seconds=120),
            )
            new_time = result.get("max_time", last_time)
            if new_time > last_time:
                
                last_time = new_time
            else:
                await asyncio.sleep(poll_interval)




@workflow.defn(sandboxed=False)
class StartVMProxmoxWorkflow:
    @workflow.run
    async def run(self, vmid: str, pool_id: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=3,
        )
        result = await workflow.execute_activity(
            activities_proxmox.start_vm_proxmox_activity,
            args=[vmid,pool_id],
            start_to_close_timeout=timedelta(seconds=120),
            retry_policy=retry_policy
        )
        return result
    

@workflow.defn(sandboxed=False)
class StopVMProxmoxWorkflow:
    @workflow.run
    async def run(self, vmid: str, pool_id: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=3,
        )
        result = await workflow.execute_activity(
            activities_proxmox.stop_vm_proxmox_activity,
            args=[vmid,pool_id],
            start_to_close_timeout=timedelta(seconds=120),
            retry_policy=retry_policy
        )
        return result
    

@workflow.defn(sandboxed=False)
class RebootVMProxmoxWorkflow:
    @workflow.run
    async def run(self, vmid: str, pool_id: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=3,
        )
        result = await workflow.execute_activity(
            activities_proxmox.reboot_vm_proxmox_activity,
            args=[vmid,pool_id],
            start_to_close_timeout=timedelta(seconds=120),
            retry_policy=retry_policy
        )
        return result
    


@workflow.defn(sandboxed=False)
class ShutdownVMProxmoxWorkflow:
    @workflow.run
    async def run(self, vmid: str, pool_id: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        result = await workflow.execute_activity(
            activities_proxmox.shutdown_vm_proxmox_activity,
            args=[vmid,pool_id],
            start_to_close_timeout=timedelta(seconds=120),
            retry_policy=retry_policy
        )
        return result
    


@workflow.defn(sandboxed=False)
class VmRebuildWorkflow:
    @workflow.run
    async def run(self, vmid: int, pool_id: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        short_retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=3,
        )

        result = await workflow.execute_activity(
            activities_proxmox.vm_rebuild_activity,
            args=[vmid, pool_id],
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(seconds=120),
        )
        if result.get("status") != "success":
            return result

        upid = result["upid"]
        node = result["node"]
        ip_address = result["ip_address"]

        # Inline wait-ready -> assign-IP -> power-on, awaited in this same
        # workflow (not an abandoned child) so failures actually propagate
        # back to the caller instead of vanishing, matching CloneVMWorkflow.
        await workflow.execute_activity(
            activities_proxmox.update_machine_provisioning_status_activity,
            args=[{"vmid": vmid, "step": "waiting_for_boot"}],
            retry_policy=short_retry_policy,
            start_to_close_timeout=timedelta(seconds=30),
        )
        await workflow.execute_activity(
            activities_proxmox.wait_for_vm_ready_activity,
            args=[{
                "pool_id": pool_id,
                "node": node,
                "upid": upid,
                "vmid": vmid,
                "timeout": 300,
            }],
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(minutes=5),
        )

        await workflow.execute_activity(
            activities_proxmox.update_machine_provisioning_status_activity,
            args=[{"vmid": vmid, "step": "assigning_ip"}],
            retry_policy=short_retry_policy,
            start_to_close_timeout=timedelta(seconds=30),
        )
        await workflow.execute_activity(
            activities_proxmox.assign_ip_to_vm_activity,
            args=[{
                "vmid": vmid,
                "pool_id": pool_id,
                "ip_address": ip_address,
            }],
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(minutes=5),
        )

        await workflow.execute_activity(
            activities_proxmox.update_machine_provisioning_status_activity,
            args=[{"vmid": vmid, "step": "powering_on"}],
            retry_policy=short_retry_policy,
            start_to_close_timeout=timedelta(seconds=30),
        )
        await workflow.execute_activity(
            activities_proxmox.start_vm_proxmox_activity,
            args=[str(vmid), str(pool_id)],
            task_queue="vmpower-task-queue",
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(seconds=150),
        )
        await workflow.execute_activity(
            activities_proxmox.update_machine_provisioning_status_activity,
            args=[{"vmid": vmid, "step": "completed"}],
            retry_policy=short_retry_policy,
            start_to_close_timeout=timedelta(seconds=30),
        )

        return {
            "status": "success",
            "rebuild_result": result,
        }
from typing import Union
from typing import Optional
from temporalio import workflow
from temporalio.common import RetryPolicy
from datetime import timedelta
from service.temporalResource.activity import activities_hyper_v
from sqlalchemy.orm import Session

logger = workflow.logger
@workflow.defn(sandboxed=False)
class CloneVMHyperVWorkflow:
    @workflow.run
    async def run(self, request):
        logger.info("CloneVMHyperVWorkflow started for request keys: %s", list(request.keys()) if isinstance(request, dict) else str(type(request)))
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

        wf_id = workflow.info().workflow_id
        wf_run_id = workflow.info().run_id
        logger.info("This workflow id: %s run id: %s", wf_id, wf_run_id)

        pool_id = request.get("pool_id")
        email = request.get("email")

        # Step 1: clone via the agent — one call already does clone + IP +
        # domain-join (if requested) internally, so there's nothing to split
        # or track in between; this is the one real checkpoint available.
        result = await workflow.execute_activity(
            activities_hyper_v.clone_vm_hyper_v_activity,
            args=[request],
            start_to_close_timeout=timedelta(seconds=120),
            retry_policy=retry_policy
        )

        try:
            vms = result.get("vms", []) if isinstance(result, dict) else []
        except Exception:
            vms = []

        machines_out = []
        vms_out = []
        for vm in vms:
            if not isinstance(vm, dict):
                continue
            vm.setdefault("clone_workflow_id", wf_id)
            vmid = vm.get("vmid")
            name = vm.get("name")
            ip = vm.get("ip")

            if not vmid:
                vm["error"] = "Agent did not return a vmid for this VM"
                vms_out.append(vm)
                continue

            # Step 2: register the machine (DB row + Guacamole connection),
            # via the raw create_machine_activity — same as Proxmox — so
            # CreateMachineWorkflow's auto-power-on doesn't fire early; this
            # workflow powers the VM on itself once registration is confirmed.
            create_result = await workflow.execute_activity(
                "finalize_cloned_machine_activity",
                args=[{
                    "pool_id": pool_id,
                    "vmid": vmid,
                    "name": name,
                    "ip": ip,
                    "email": email,
                    "workflow_ids": [wf_id],
                }],
                retry_policy=short_retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            if create_result.get("status") != "ok":
                logger.error(f"Failed to register Hyper-V machine vmid={vmid}: {create_result}")
                vm["error"] = create_result.get("error")
                vms_out.append(vm)
                continue

            machines_out.append(create_result.get("machine"))

            # Step 3: power on — reuses the same start_vm_proxmox_activity
            # Proxmox uses, which already branches internally on cluster.type
            # to call the Hyper-V agent's handle_action when needed. Keeping
            # "how do we power on a VM" in one shared place rather than a
            # second, parallel implementation here.
            await workflow.execute_activity(
                "update_machine_provisioning_status_activity",
                args=[{"vmid": vmid, "step": "powering_on"}],
                retry_policy=short_retry_policy,
                start_to_close_timeout=timedelta(seconds=30),
            )
            await workflow.execute_activity(
                "start_vm_proxmox_activity",
                args=[str(vmid), str(pool_id)],
                task_queue="vmpower-task-queue",
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=150),
            )
            await workflow.execute_activity(
                "update_machine_provisioning_status_activity",
                args=[{"vmid": vmid, "step": "completed"}],
                retry_policy=short_retry_policy,
                start_to_close_timeout=timedelta(seconds=30),
            )

            vms_out.append(vm)

        logger.info("CloneVMHyperVWorkflow completed. workflow_id=%s", wf_id)
        response = {"vms": vms_out, "machines": machines_out, "clone_workflow_id": wf_id}
        failed_vms = [v for v in vms_out if v.get("error")]
        if failed_vms:
            response["partial_failure"] = True
            response["failed_vms"] = failed_vms
            response["msg"] = (
                f"{len(failed_vms)} of {len(vms_out)} VM(s) were cloned but could not be "
                f"registered/started — see failed_vms for details."
            )
            logger.error(response["msg"])
        return response


@workflow.defn(sandboxed=False)
class DeleteVMHyperVWorkflow:
    @workflow.run
    async def run(self, request: dict):
        logger.info("DeleteVMHyperVWorkflow started, vm_id=%s", request.get("vm_id"))
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=20),
            maximum_attempts=5,
        )
        wf_id = workflow.info().workflow_id
        result = await workflow.execute_activity(
            activities_hyper_v.delete_vm_hyper_v_activity,
            args=[request],
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=retry_policy,
        )
        if isinstance(result, dict):
            result["delete_workflow_id"] = wf_id
        logger.info("DeleteVMHyperVWorkflow completed, workflow_id=%s", wf_id)
        return result


@workflow.defn(sandboxed=False)
class HandleActionHyperVWorkflow:
    @workflow.run
    async def run(self, request: dict):
        logger.info(
            "HandleActionHyperVWorkflow started, action=%s, vm_id=%s",
            request.get("action"),
            request.get("vm_id")
        )
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=20),
            maximum_attempts=5,
        )
        wf_id = workflow.info().workflow_id
        result = await workflow.execute_activity(
            activities_hyper_v.handle_action_activity,
            args=[request],
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=retry_policy,
        )
        if isinstance(result, dict):
            result["action_workflow_id"] = wf_id
        logger.info("HandleActionHyperVWorkflow completed, workflow_id=%s", wf_id)
        return result


@workflow.defn(sandboxed=False)
class DeleteHyperVDiskWorkflow:
    @workflow.run
    async def run(self, request: dict):
        logger.info("DeleteHyperVDiskWorkflow started, disk_path=%s", request.get("disk_path"))
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=20),
            maximum_attempts=5,
        )
        wf_id = workflow.info().workflow_id
        result = await workflow.execute_activity(
            activities_hyper_v.delete_hyperv_disk_activity,
            args=[request],
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=retry_policy,
        )
        if isinstance(result, dict):
            result["delete_disk_workflow_id"] = wf_id
        logger.info("DeleteHyperVDiskWorkflow completed, workflow_id=%s", wf_id)
        return result


@workflow.defn(sandboxed=False)
class VmRebuildHyperVWorkflow:
    @workflow.run
    async def run(self, request: dict):
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
            activities_hyper_v.vm_rebuild_hyper_v_activity,
            args=[request],
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(minutes=5),
        )
        if result.get("status") != "success":
            return result

        # vm_rebuild_hyper_v_activity re-clones the VM but leaves it powered
        # off (cloning doesn't auto-start it) — power it back on explicitly,
        # tracked, same as the clone flow. Reuses start_vm_proxmox_activity,
        # which already branches internally for Hyper-V.
        vm_id = result.get("vm_id")
        pool_id = result.get("pool_id")
        if vm_id and pool_id:
            await workflow.execute_activity(
                "update_machine_provisioning_status_activity",
                args=[{"vmid": vm_id, "step": "powering_on"}],
                retry_policy=short_retry_policy,
                start_to_close_timeout=timedelta(seconds=30),
            )
            await workflow.execute_activity(
                "start_vm_proxmox_activity",
                args=[str(vm_id), str(pool_id)],
                task_queue="vmpower-task-queue",
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=150),
            )
            await workflow.execute_activity(
                "update_machine_provisioning_status_activity",
                args=[{"vmid": vm_id, "step": "completed"}],
                retry_policy=short_retry_policy,
                start_to_close_timeout=timedelta(seconds=30),
            )
        else:
            logger.warning(f"VmRebuildHyperVWorkflow: missing vm_id/pool_id in result, skipping power-on: {result}")

        return result


@workflow.defn(sandboxed=False)
class HyperVPoolRebuildWorkflow:
    @workflow.run
    async def run(self, request: dict):
        logger.info("HyperVPoolRebuildWorkflow started for pool_id=%s", request.get("pool_id"))
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=5),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=60),
            maximum_attempts=3,
        )

        # 1. Gather data (machines in pool, template info)
        pool_data = await workflow.execute_activity(
            activities_hyper_v.get_pool_rebuild_data_activity,
            args=[request],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=retry_policy,
        )
        if pool_data.get("status") == "error":
            return pool_data
        # # 2. Duplicate Parent/Template VM
        # parent_result = await workflow.execute_activity(
        #     activities_hyper_v.duplicate_parent_vm_activity,
        #     args=[pool_data],
        #     start_to_close_timeout=timedelta(minutes=15),
        #     retry_policy=retry_policy,
        # )
        # if parent_result.get("status") == "error":
        #     return parent_result

        # # Update pool_data with the new template info for child cloning
        # pool_data["new_template"] = parent_result

        short_retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=3,
        )

        # 3. Rebuild each machine in sequence
        machines = pool_data.get("machines", [])
        machine_results = []
        for machine in machines:
            res = await workflow.execute_activity(
                activities_hyper_v.rebuild_machine_in_pool_activity,
                args=[{"vhdPath": request.get("vhdPath"), "machine": machine, "pool_id": pool_data["pool_id"], "new_template": pool_data["template_data"]}],
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=retry_policy,
            )
            machine_results.append(res)

            # rebuild_machine_in_pool_activity re-clones but leaves the VM
            # powered off — power it back on explicitly, same as the single-VM
            # rebuild path and the clone flow.
            if res.get("status") == "success" and res.get("new_vm_id") and res.get("pool_id"):
                vm_id = res["new_vm_id"]
                machine_pool_id = res["pool_id"]
                await workflow.execute_activity(
                    "update_machine_provisioning_status_activity",
                    args=[{"vmid": vm_id, "step": "powering_on"}],
                    retry_policy=short_retry_policy,
                    start_to_close_timeout=timedelta(seconds=30),
                )
                await workflow.execute_activity(
                    "start_vm_proxmox_activity",
                    args=[str(vm_id), str(machine_pool_id)],
                    task_queue="vmpower-task-queue",
                    retry_policy=retry_policy,
                    start_to_close_timeout=timedelta(seconds=150),
                )
                await workflow.execute_activity(
                    "update_machine_provisioning_status_activity",
                    args=[{"vmid": vm_id, "step": "completed"}],
                    retry_policy=short_retry_policy,
                    start_to_close_timeout=timedelta(seconds=30),
                )

        return {
            "status": "success",
            "msg": f"Pool rebuild completed for {len(machines)} machines.",
            # "new_parent": parent_result,
            "machine_results": machine_results
        }

@workflow.defn(sandboxed=False)
class PingAgentWorkflow:
    @workflow.run
    async def run(self, cluster_id: Optional[int], ip: str, port: Union[int, str]):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        result = await workflow.execute_activity(
            activities_hyper_v.ping_agent_activity,
            args=[cluster_id, ip, port],
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(minutes=5),
        )
        return result


@workflow.defn(sandboxed=False)
class FetchClusterNodesWorkflow:
    @workflow.run
    async def run(self, request:dict):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=20),
            maximum_attempts=3,
        )
        result = await workflow.execute_activity(
            activities_hyper_v.fetch_cluster_nodes_activity,
            args=[request],
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=retry_policy,
        )
        return result

@workflow.defn(sandboxed=False)
class VerifyHyperVWorkflow:
    @workflow.run
    async def run(self, request: dict, cluster_id: Optional[int] = None):
        logger.info("VerifyHyperVWorkflow started for ip=%s", request.get("ip"))
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=20),
            maximum_attempts=5,
        )
        result = await workflow.execute_activity(
            activities_hyper_v.verify_hyper_v_activity,
            args=[request, cluster_id],
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=retry_policy,
        )
        return result
from typing import Union
from typing import Optional
from temporalio import workflow
from temporalio.common import RetryPolicy
from datetime import timedelta
from service.temporalResource.activity import activities_hyper_v
import logging
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

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

        wf_id = workflow.info().workflow_id
        wf_run_id = workflow.info().run_id
        logger.info("This workflow id: %s run id: %s", wf_id, wf_run_id)

        result = await workflow.execute_activity(
            activities_hyper_v.clone_vm_hyper_v_activity,
            args=[request],
            start_to_close_timeout=timedelta(seconds=120),
            retry_policy=retry_policy
        )

        try:
            vms = result.get("vms", []) if isinstance(result, dict) else []
            for vm in vms:
                if isinstance(vm, dict):
                    vm.setdefault("clone_workflow_id", wf_id)
            if isinstance(result, dict):
                result["clone_workflow_id"] = wf_id
        except Exception:
            result = {"vms": [], "clone_workflow_id": wf_id}

        logger.info("CloneVMHyperVWorkflow completed. workflow_id=%s", wf_id)
        return result


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
        result = await workflow.execute_activity(
            activities_hyper_v.vm_rebuild_hyper_v_activity,
            args=[request],
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(minutes=5),
        )
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
    async def run(self, request: dict):
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
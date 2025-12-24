from temporalio import workflow
from temporalio.common import RetryPolicy
from datetime import timedelta
from service.temporalResource.activity import activities_hyper_v
import logging

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

        # get this workflow's id (and run id if you need it)
        wf_id = workflow.info().workflow_id
        wf_run_id = workflow.info().run_id
        logger.info("This workflow id: %s run id: %s", wf_id, wf_run_id)

        # Pass ONLY serializable primitives to the activity (request is a dict)
        result = await workflow.execute_activity(
            activities_hyper_v.clone_vm_single_node_activity,
            args=[request],   # removed db from args
            start_to_close_timeout=timedelta(seconds=120),
            retry_policy=retry_policy
        )

        # Ensure result is the expected shape and attach this workflow id
        # so callers can persist it as clone_workflow_id. If `result` contains a 'vms' list:
        try:
            vms = result.get("vms", []) if isinstance(result, dict) else []
            for vm in vms:
                # attach clone_workflow_id for each VM dict returned
                if isinstance(vm, dict):
                    vm.setdefault("clone_workflow_id", wf_id)
                # if vm is a tuple/list, convert to dict or handle accordingly
            # Return the modified structure
            if isinstance(result, dict):
                result["clone_workflow_id"] = wf_id  # optional top-level field
        except Exception:
            # if result is not the expected dict -> just wrap minimal info
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
            activities_hyper_v.delete_vm_single_node_activity,
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

        logger.info(
            "HandleActionHyperVWorkflow completed, workflow_id=%s",
            wf_id
        )
        return result

@workflow.defn(sandboxed=False)
class DeleteHyperVDiskWorkflow:
    @workflow.run
    async def run(self, request: dict):
        logger.info(
            "DeleteHyperVDiskWorkflow started, disk_path=%s",
            request.get("disk_path")
        )

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

        logger.info(
            "DeleteHyperVDiskWorkflow completed, workflow_id=%s",
            wf_id
        )
        return result
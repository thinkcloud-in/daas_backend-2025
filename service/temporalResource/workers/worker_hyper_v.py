from utils.temporal_client import TemporalClientManager
from temporalio.worker import Worker
from service.temporalResource.workflows import workflows_hyper_v
from service.temporalResource.activity import activities_hyper_v

import logging

async def hyperv_worker():
    client = await TemporalClientManager.get_temporal_client()
    if client is None:
        return

    logging.info("Hyper-V Worker started...")

    worker = Worker(
        client,
        task_queue="hyperv-task-queue",
        workflows=[
            # workflows_hyper_v.CloneVMHyperVWorkflow,
            # workflows_hyper_v.DeleteVMHyperVWorkflow,
            workflows_hyper_v.HandleActionHyperVWorkflow,
            # workflows_hyper_v.DeleteHyperVDiskWorkflow,
            workflows_hyper_v.VmRebuildHyperVWorkflow,
            workflows_hyper_v.HyperVPoolRebuildWorkflow,
            workflows_hyper_v.PingAgentWorkflow,
            workflows_hyper_v.VerifyStandaloneHyperVWorkflow,
        ],
        activities=[
            # activities_hyper_v.clone_vm_hyper_v_activity,
            # activities_hyper_v.delete_vm_hyper_v_activity,
            activities_hyper_v.handle_action_activity,
            # activities_hyper_v.delete_hyperv_disk_activity,
            activities_hyper_v.vm_rebuild_hyper_v_activity,
            activities_hyper_v.rebuild_machine_in_pool_activity,
            activities_hyper_v.get_pool_rebuild_data_activity,
            activities_hyper_v.ping_agent_activity,
            activities_hyper_v.verify_standalone_hyper_v_activity,
            # activities_hyper_v.duplicate_parent_vm_activity,
        ],
    )

    try:
        await worker.run()
    except Exception as e:
        raise Exception(f"Error in Hyper-V worker: {e}")

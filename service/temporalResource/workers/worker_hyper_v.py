from service.gucamoleService import connectionWithClient
from temporalio.worker import Worker
from service.temporalResource.workflows import workflows_hyper_v
from service.temporalResource.activity import activities_hyper_v

import logging

async def hyperv_worker():
    client = await connectionWithClient()
    if client is None:
        return

    logging.info("Hyper-V Worker started...")

    worker = Worker(
        client,
        task_queue="hyperv-task-queue",

        workflows=[
            workflows_hyper_v.CloneVMHyperVWorkflow,
            workflows_hyper_v.DeleteVMHyperVWorkflow,
            workflows_hyper_v.HandleActionHyperVWorkflow,
            workflows_hyper_v.DeleteHyperVDiskWorkflow,
        ],

        activities=[
            activities_hyper_v.clone_vm_single_node_activity,
            activities_hyper_v.delete_vm_single_node_activity,
            activities_hyper_v.handle_action_activity,
            activities_hyper_v.delete_hyperv_disk_activity,
        ],
    )

    try:
        await worker.run()
    except Exception as e:
        raise Exception(f"Error in Hyper-V worker: {e}")

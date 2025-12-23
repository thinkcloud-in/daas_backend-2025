from service.gucamoleService import connectionWithClient
from temporalio.client import Client
from temporalio.worker import Worker
from service.temporalResource.workflows import workflows_hyper_v
from service.temporalResource.activity import activities_hyper_v
from service.temporalResource.workflows import workflows_proxmox

import logging
from service.temporalResource.activity import activities_proxmox

async def cloneVm_SingleNode_HyperV_worker():
    client = await connectionWithClient()
    if client is None:
        return

    logging.info("Clone VM Single Node HyperV Worker started...")
    worker = Worker(
        client,
        task_queue="clonevmhyperv-task-queue",
        workflows=[workflows_hyper_v.CloneVMHyperVWorkflow], 
        activities=[activities_hyper_v.clone_vm_single_node_activity],
    )   
    try:
        await worker.run()
        logging.info("Clone VM Single Node HyperV Worker stopped.")

    except Exception as e:
        raise Exception(f"Error in clone VM worker: {e}")
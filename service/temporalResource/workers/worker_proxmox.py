from temporalio.client import Client
from temporalio.worker import Worker
from service.gucamoleService import connectionWithClient
from service.temporalResource.workflows import workflows_proxmox
from service.temporalResource.activity.activities_proxmox import (
    clone_vm_activity,
    wait_for_vm_ready_activity,
    assign_ip_to_vm_activity,
    migrate_bucket_new_data_activity,
    start_vm_proxmox_activity,
    stop_vm_proxmox_activity,
    reboot_vm_proxmox_activity,
    shutdown_vm_proxmox_activity,
    vm_rebuild_activity

)

    
async def clone_vm_worker():
    client = await connectionWithClient()
    if client is None:
        
        return 
    worker = Worker(
        client,
        task_queue="clonevm-task-queue",
        workflows=[workflows_proxmox.CloneVMWorkflow,
            workflows_proxmox.WaitAndAssignIPsWorkflow, 
         ], 
        activities=[
            clone_vm_activity,
            wait_for_vm_ready_activity,
            assign_ip_to_vm_activity,
        ],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise Exception(f"Error in clone VM worker: {e}")
        

async def migrate_worker():
    client = await connectionWithClient()
    if client is None:
        
        return 
    worker = Worker(
        client,
        task_queue="migration-task-queue",
        workflows=[workflows_proxmox.LiveMigrateWorkflow],
        activities=[migrate_bucket_new_data_activity],
    )
    
    await worker.run()

async def vm_power_worker():
    client = await connectionWithClient()
    if client is None:
        
        return 
    worker = Worker(
        client,
        task_queue="vmpower-task-queue",
        workflows=[
            workflows_proxmox.StartVMProxmoxWorkflow,
            workflows_proxmox.StopVMProxmoxWorkflow,
            workflows_proxmox.RebootVMProxmoxWorkflow,
            workflows_proxmox.ShutdownVMProxmoxWorkflow,
        ],
        activities=[
            start_vm_proxmox_activity,
            stop_vm_proxmox_activity,
            reboot_vm_proxmox_activity,
            shutdown_vm_proxmox_activity,
        ],
    )
    await worker.run()


async def vm_rebuild_worker():
    client = await connectionWithClient()
    worker = Worker(
        client,
        task_queue="vm-rebuild-task-queue",
        workflows=[
            workflows_proxmox.VmRebuildWorkflow,
            workflows_proxmox.WaitAndAssignIPsWorkflow,
        ],
        activities=[
            vm_rebuild_activity,
            wait_for_vm_ready_activity,
            assign_ip_to_vm_activity,   
        ],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise Exception(f"Error in VM rebuild worker: {e}")

async def start_all_proxmox_workers():
    import asyncio
    await asyncio.gather(
        clone_vm_worker(),
        migrate_worker(),
        vm_power_worker(),
        vm_rebuild_worker()
    )
        
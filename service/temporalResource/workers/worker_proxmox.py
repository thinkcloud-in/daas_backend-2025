from temporalio.client import Client
from temporalio.worker import Worker
from utils.temporal_client import TemporalClientManager
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

import logging

    
async def clone_vm_worker():
    client = await TemporalClientManager.get_temporal_client()
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
    client = await TemporalClientManager.get_temporal_client()
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
    client = await TemporalClientManager.get_temporal_client()
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
    client = await TemporalClientManager.get_temporal_client()
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


async def run_all_proxmox_workers():
    """Starts all Proxmox-related workers concurrently in the same event loop."""
    import asyncio
    
    tasks = [
        clone_vm_worker(),
        migrate_worker(),
        vm_power_worker(),
        vm_rebuild_worker()
    ]
    
    print("Starting all Proxmox workers...")
    await asyncio.gather(*tasks)
        
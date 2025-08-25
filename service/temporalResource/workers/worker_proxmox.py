import asyncio
from temporalio.client import Client
from temporalio.worker import Worker
from service.gucamoleService import connectionWithClient
from service.temporalResource.workflows import workflows_proxmox
from service.temporalResource.activity.activities_proxmox import (
    clone_vm_activity,
    wait_for_vm_ready_activity,
    assign_ip_to_vm_activity,
    # delete_proxmox_vm_activity,
    migrate_bucket_new_data_activity,
    start_vm_proxmox_activity,
    stop_vm_proxmox_activity,
    reboot_vm_proxmox_activity,
    shutdown_vm_proxmox_activity,
    vm_rebuild_activity

)
import os

async def connectionWithTemporal():
    print('Connecting to Temporal server...')
    try:
        client = await Client.connect(os.getenv('TEMPORAL_SERVER'))  
        print('Connected to Temporal server.')
        return client
    except Exception as e:
        print(f"Connection Refused to Temporal server: {e}")
        return None
    
async def clone_vm_worker():
    client = await connectionWithClient()
    if client is None:
        print("Could not connect to Temporal server, terminating.")
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
    print("Clone VM worker starting...")
    try:
        await worker.run()
        print("Temporal worker started. Waiting for tasks...")
    except Exception as e:
        print(f"Error in Temporal worker: {e}")
        print('Worker stopped...')


# async def delete_proxmox_vm_worker():
#     client = await connectionWithClient()
#     if client is None:
#         print("Could not connect to Temporal server, terminating.")
#         return 
#     worker = Worker(
#         client,
#         task_queue="proxmox-task-queue",
#         workflows=[workflows_proxmox.DeleteProxmoxVmWorkflow],
#         activities=[delete_proxmox_vm_activity],
#     )
#     await worker.run()


async def migrate_worker():
    client = await connectionWithClient()
    if client is None:
        print("Could not connect to Temporal server, terminating.")
        return 
    worker = Worker(
        client,
        task_queue="migration-task-queue",
        workflows=[workflows_proxmox.LiveMigrateWorkflow],
        activities=[migrate_bucket_new_data_activity],
    )
    print("Worker started")
    await worker.run()




# async def start_vm_proxmox_worker():
#     client = await connectionWithClient()
#     if client is None:
#         print("Could not connect to Temporal server, terminating.")
#         return 
#     print("Starting worker for start VM Proxmox Initially...")
#     worker = Worker(
#         client,
#         task_queue="startvm-task-queue",
#         workflows=[workflows_proxmox.StartVMProxmoxWorkflow],
#         activities=[start_vm_proxmox_activity],
#     )
#     print("Worker starting for start VM Proxmox...")
#     await worker.run()


# async def stop_vm_proxmox_worker():
#     client = await connectionWithClient()
#     if client is None:
#         print("Could not connect to Temporal server, terminating.")
#         return 
#     worker = Worker(
#         client,
#         task_queue="stoptvm-task-queue",
#         workflows=[workflows_proxmox.StopVMProxmoxWorkflow],
#         activities=[stop_vm_proxmox_activity],
#     )
#     await worker.run()


# async def reboot_vm_proxmox_worker():
#     client = await connectionWithClient()
#     if client is None:
#         print("Could not connect to Temporal server, terminating.")
#         return 
#     worker = Worker(
#         client,
#         task_queue="rebootvm-task-queue",
#         workflows=[workflows_proxmox.RebootVMProxmoxWorkflow],
#         activities=[reboot_vm_proxmox_activity],
#     )
#     await worker.run()



# async def shutdown_vm_proxmox_worker():
#     client = await connectionWithClient()
#     if client is None:
#         print("Could not connect to Temporal server, terminating.")
#         return 
#     worker = Worker(
#         client,
#         task_queue="shutdownvm-task-queue",
#         workflows=[workflows_proxmox.ShutdownVMProxmoxWorkflow],
#         activities=[shutdown_vm_proxmox_activity],
#     )
#     await worker.run()


async def vm_power_worker():
    client = await connectionWithClient()
    if client is None:
        print("Could not connect to Temporal server, terminating.")
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
            workflows_proxmox.WaitAndAssignIPsWorkflow,  # <-- needed for child workflow!
        ],
        activities=[
            vm_rebuild_activity,
            wait_for_vm_ready_activity,
            assign_ip_to_vm_activity,   
        ],
    )
    print("VM Rebuild worker starting...")
    try:
        await worker.run()
        print("VM Rebuild worker started. Waiting for tasks...")
    except Exception as e:
        print(f"Error in VM Rebuild worker: {e}")
        print('Worker stopped...')
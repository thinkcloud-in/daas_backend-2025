import os
import asyncio
from temporalio.worker import Worker
from service.temporalResource.activity import activities_ipmi
from service.temporalResource.workflows import workflows_ipmi
from utils.temporal_client import TemporalClientManager

async def start_all_ipmi_workers():
    client = await TemporalClientManager.get_temporal_client()
    
    queues = [
        ("Createipmi-task-queue", [workflows_ipmi.IpmiCreationWorkflow], [activities_ipmi.ipmi_activity]),
        ("Updateipmi-task-queue", [workflows_ipmi.IpmiUpdateWorkflow], [activities_ipmi.update_ipmi_activity]),
        ("Deleteipmi-task-queue", [workflows_ipmi.IpmiDeletionWorkflow], [activities_ipmi.delete_ipmi_activity]),
    ]
    
    workers = []
    for queue_name, wfs, acts in queues:
        workers.append(Worker(client, task_queue=queue_name, workflows=wfs, activities=acts))
    
    await asyncio.gather(*[w.run() for w in workers])
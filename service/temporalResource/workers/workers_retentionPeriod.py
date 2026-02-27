import os
import asyncio
from temporalio.worker import Worker
from service.temporalResource.workflows import workflows_retentionPeriod
from service.temporalResource.activity import activities_retentionPeriod
from utils.temporal_client import TemporalClientManager

async def start_all_retention_workers():
    client = await TemporalClientManager.get_temporal_client()
    
    queues = [
        ("namespace-tasks", [workflows_retentionPeriod.GetNamespacesWorkflow], [activities_retentionPeriod.list_namespaces_activity]),
        ("update-Retention-tasks", [workflows_retentionPeriod.UpdateRetentionWorkflow], [activities_retentionPeriod.update_retention_activity]),
    ]
    
    workers = []
    for queue_name, wfs, acts in queues:
        workers.append(Worker(client, task_queue=queue_name, workflows=wfs, activities=acts))
    
    await asyncio.gather(*[w.run() for w in workers])

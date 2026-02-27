from temporalio.worker import Worker
from service.temporalResource.workflows import workflows_cluster 
from service.temporalResource.activity import activities_cluster 
from utils.temporal_client import TemporalClientManager
import os

async def connectionWithTemporal():
    return await TemporalClientManager.get_temporal_client()
async def combined_worker():
    client = await connectionWithTemporal()
    if client is None:
        
        return 

    worker = Worker(
        client,
        task_queue="cluster-task-queue",
        workflows=[
            workflows_cluster.CreateClusterWorkflow,
            workflows_cluster.DeleteClusterWorkflow,
            workflows_cluster.AssignRoleToUserWorkflow,
            workflows_cluster.CreateUserWorkflow,
            workflows_cluster.UpdateClusterWorkflow,
        ],
        activities=[
            activities_cluster.create_cluster_activity,
            activities_cluster.delete_cluster_activity,
            activities_cluster.create_user_activity,
            activities_cluster.Assign_role_to_user_activity,
            activities_cluster.update_cluster_activity,
        ],
    )
    
    await worker.run()
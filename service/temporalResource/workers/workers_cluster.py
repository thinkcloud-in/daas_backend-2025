import asyncio
from temporalio.worker import Worker

# Import your workflow and activities
from service.temporalResource.workflows import workflows_cluster 
from service.temporalResource.activity import activities_cluster 
from temporalio.client import Client
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
async def combined_worker():
    client = await connectionWithTemporal()
    if client is None:
        print("Could not connect to Temporal server, terminating.")
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
    print("Combined worker started. Listening for tasks...")
    await worker.run()



# async def create_user_worker():
#     client = await Client.connect("172.16.0.108:7233")
#     worker = Worker(
#         client,
#         task_queue="createUser-task-queue",
#         workflows=[workflows_cluster.CreateUserWorkflow],
#         activities=[activities_cluster.create_user_activity],
#     )
#     print("Worker started. Listening for tasks...")
#     await worker.run()

# async def assign_role_to_user_worker():
#     client = await Client.connect("172.16.0.108:7233")
#     worker = Worker(
#         client,
#         task_queue="assignRoleToUser-task-queue",
#         workflows=[workflows_cluster.AssignRoleToUserWorkflow],
#         activities=[activities_cluster.Assign_role_to_user_activity],
#     )
#     print("Worker started. Listening for tasks...")
#     await worker.run()
    
# async def create_cluster_worker():
  
#     client = await Client.connect("172.16.0.108:7233")
#     worker = Worker(
#         client,
#         task_queue="cluster-task-queue",
#         workflows=[workflows_cluster.CreateClusterWorkflow],
#         activities=[activities_cluster.create_cluster_activity],
#     )
#     print("Worker started. Listening for tasks...")
#     await worker.run()

# async def delete_cluster_worker():
  
#     client = await Client.connect("172.16.0.108:7233")
#     worker = Worker(
#         client,
#         task_queue="cluster-task-queue",
#         workflows=[workflows_cluster.DeleteClusterWorkflow],
#         activities=[activities_cluster.delete_cluster_activity],
#     )
#     print("Worker started. Listening for tasks...")
#     await worker.run()

# async def update_cluster_worker():
  
#     client = await Client.connect("172.16.0.108:7233")
#     worker = Worker(
#         client,
#         task_queue="cluster-task-queue",
#         workflows=[workflows_cluster.UpdateClusterWorkflow],
#         activities=[activities_cluster.update_cluster_activity],
#     )
#     print("Worker started. Listening for tasks...")
#     await worker.run()



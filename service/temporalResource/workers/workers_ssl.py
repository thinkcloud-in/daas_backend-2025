import asyncio
from temporalio.client import Client
from temporalio.worker import Worker
from utils.temporal_client import TemporalClientManager
from service.temporalResource.activity import activities_ssl
from service.temporalResource.workflows import workflows_ssl

async def ssl_workers():
    client = await TemporalClientManager.get_temporal_client()
    
    worker = Worker(
        client,
        task_queue="ssl-task-queue",
        workflows=[workflows_ssl.SSLUploadWorkflow, workflows_ssl.SSLRenewWorkflow, workflows_ssl.SSLDeleteWorkflow, workflows_ssl.SSLGetStatusWorkflow],
        activities=[
            activities_ssl.upload_certificate_activity,
            activities_ssl.renew_certificate_activity,
            activities_ssl.delete_certificate_activity,
            activities_ssl.get_certificate_status_activity
        ],
    )
    print("🚀 Temporal SSL Worker is running and waiting for tasks...")
    await worker.run()

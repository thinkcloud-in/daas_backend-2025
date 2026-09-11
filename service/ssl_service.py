import uuid
from typing import Optional
from temporalio.client import Client
from utils.temporal_client import TemporalClientManager
from service.temporalResource.workflows import workflows_ssl

class SSLService:
    def __init__(self):
        self.temporal_client: Optional[Client] = None
        self.TASK_QUEUE = "ssl-task-queue"

    async def get_client(self) -> Client:
        if not self.temporal_client:
            # Set up the connection to the Temporal local/production server
            self.temporal_client = await TemporalClientManager.get_temporal_client()
        return self.temporal_client

    async def upload_certificate(self, cert_bytes: bytes, key_bytes: bytes) -> dict:
        client = await self.get_client()
        
        payload = {
            "cert_content": cert_bytes.decode("utf-8"),
            "key_content": key_bytes.decode("utf-8")
        }
        
        handle = await client.start_workflow(
            workflows_ssl.SSLUploadWorkflow.run,
            payload,
            id=f"ssl-upload-{uuid.uuid4()}",
            task_queue=self.TASK_QUEUE
        )
        result = await handle.result()
        return result

    async def renew_certificate(self, payload_cn: Optional[str], request_host: Optional[str]) -> dict:
        client = await self.get_client()
        
        payload = {
            "payload_cn": payload_cn,
            "request_host": request_host
        }
        
        handle = await client.start_workflow(
            workflows_ssl.SSLRenewWorkflow.run,
            payload,
            id=f"ssl-renew-{uuid.uuid4()}",
            task_queue=self.TASK_QUEUE
        )
        result = await handle.result()
        return result

    async def delete_certificate(self) -> dict:
        client = await self.get_client()
        
        handle = await client.start_workflow(
            workflows_ssl.SSLDeleteWorkflow.run,
            id=f"ssl-delete-{uuid.uuid4()}",
            task_queue=self.TASK_QUEUE
        )
        result = await handle.result()
        return result

    async def get_certificate_status(self) -> dict:
        client = await self.get_client()
        
        handle = await client.start_workflow(
            workflows_ssl.SSLGetStatusWorkflow.run,
            id=f"ssl-status-{uuid.uuid4()}",
            task_queue=self.TASK_QUEUE
        )
        result = await handle.result()
        return result

# Singleton instance, accessed through the routers
ssl_service = SSLService()
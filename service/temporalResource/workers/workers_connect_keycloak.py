import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.worker import Worker

from service.temporalResource.activity.activities_connect_keycloak import (
    connect_keycloak_activity,
    disconnect_keycloak_activity,
)
from service.temporalResource.workflows.workflows_connect_keycloak import (
    ConnectKeycloakWorkflow,
    DisconnectKeycloakWorkflow,
)
from utils.temporal_client import TemporalClientManager

logger = logging.getLogger(__name__)
TASK_QUEUE = "connect-keycloak-queue"


async def connect_keycloak_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue                = TASK_QUEUE,
        workflows                 = [ConnectKeycloakWorkflow, DisconnectKeycloakWorkflow],
        activities                = [connect_keycloak_activity, disconnect_keycloak_activity],
        activity_executor         = ThreadPoolExecutor(max_workers=3),
        max_concurrent_activities = 3,
    )
    logger.info(f"[ConnectKeycloakWorker] Listening on '{TASK_QUEUE}'")
    await worker.run()


async def run_connect_keycloak_worker():
    await asyncio.gather(connect_keycloak_worker())

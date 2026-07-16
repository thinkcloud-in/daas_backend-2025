import os
from temporalio.client import Client
from dotenv import load_dotenv
import logging

load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

class TemporalClientManager:
    _client = None

    @classmethod
    async def get_temporal_client(cls) -> Client:
        if cls._client is None:
            try:
                temporal_server = os.getenv('TEMPORAL_SERVER', 'localhost:7233')
                # Remove protocol if present
                if "://" in temporal_server:
                    temporal_server = temporal_server.split("://")[1]
                
                logger.info(f"Connecting to Temporal server at {temporal_server}...")
                namespace = os.getenv('TEMPORAL_NAMESPACE', 'default')
                cls._client = await Client.connect(temporal_server, namespace=namespace)
                logger.info("Successfully connected to Temporal server.")
            except Exception as e:
                logger.error(f"Failed to connect to Temporal server: {e}", exc_info=True)
                raise
        return cls._client

    @classmethod
    async def close(cls):
        if cls._client:
            cls._client = None
            logger.info("Temporal client connection reset.")

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
                logger.info(f"Connecting to Temporal server at {temporal_server}...")
                cls._client = await Client.connect(temporal_server)
                logger.info("Successfully connected to Temporal server.")
            except Exception as e:
                logger.error(f"Failed to connect to Temporal server: {e}", exc_info=True)
                raise
        return cls._client

    @classmethod
    async def close(cls):
        if cls._client:
            # Temporal client doesn't strictly require a close method in some versions, 
            # but it's good practice if the library supports it or for cleanup logic.
            # Currently, the python sdk client doesn't expose a close awaitable in the same way 
            # as some other clients, but we can set it to None.
            # If the underlying connection needs closing, it depends on the framework.
            # For now, we just reset the singleton.
            cls._client = None
            logger.info("Temporal client connection reset.")

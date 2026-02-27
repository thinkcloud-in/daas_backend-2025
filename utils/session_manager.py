import requests
import httpx
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

logger = logging.getLogger(__name__)

class SessionManager:
    _session = None
    _async_client = None

    @classmethod
    def get_session(cls) -> requests.Session:
        if cls._session is None:
            logger.info("Initializing shared requests Session...")
            cls._session = requests.Session()
            
            # Configure retries and connection pooling
            retries = Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[500, 502, 503, 504]
            )
            adapter = HTTPAdapter(
                pool_connections=100, 
                pool_maxsize=100, 
                max_retries=retries
            )
            cls._session.mount("http://", adapter)
            cls._session.mount("https://", adapter)
            
            logger.info("Shared requests Session initialized with pooling.")
        return cls._session

    @classmethod
    def get_async_client(cls) -> httpx.AsyncClient:
        if cls._async_client is None:
            logger.info("Initializing shared httpx AsyncClient...")
            limits = httpx.Limits(max_connections=100, max_keepalive_connections=50)
            cls._async_client = httpx.AsyncClient(limits=limits, timeout=30.0)
            logger.info("Shared httpx AsyncClient initialized.")
        return cls._async_client

    @classmethod
    async def close_all(cls):
        if cls._session:
            cls._session.close()
            cls._session = None
            logger.info("Shared requests Session closed.")
        
        if cls._async_client:
            await cls._async_client.aclose()
            cls._async_client = None
            logger.info("Shared httpx AsyncClient closed.")

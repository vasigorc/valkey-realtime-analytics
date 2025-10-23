from typing import Optional
from glide import GlideClient, GlideClientConfiguration, NodeAddress
import logging


logger = logging.getLogger(__name__)


class ValkeyConnectionPool:
    """
    Singleton connection pool for Valkey.

    Usage:
        pool = ValkeyConnectionPool()
        client = await pool.get_client()
        await client.set("key", "value")
    """

    _instance: Optional["ValkeyConnectionPool"] = None
    _client: Optional[GlideClient] = None

    def __new__(cls):
        "Singleton object: single pool per app."
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    async def initialize(
        self,
        host: str = "localhost",
        port: int = 6379,
        request_timeout: int = 5000,
    ) -> None:
        """
        Initialize the connection pool.

        Args:
            host: Valkey server hostname
            port: Valkey server port
            request_timeout: Max time to wait for server response (in ms)
        """
        if self._client is not None:
            logger.info("Client already initialized, reusing existing connection")
            return

        try:
            config = GlideClientConfiguration(
                addresses=[NodeAddress(host, port)], request_timeout=request_timeout
            )
            self._client = await GlideClient.create(config)

            # Verify connection
            pong = (await self._client.ping()).decode("utf-8")
            logger.info(
                f"✅ Connected to Valkey server running at {host}:{port} - {pong}"
            )
        except Exception as e:
            logger.error(f"❌ Failed to connect to Valkey server: {e}")
            raise ConnectionError(
                f"Cannot connect to Valkey server at {host}:{port}"
            ) from e

    async def get_client(self) -> GlideClient:
        if self._client is None:
            raise RuntimeError(
                "Connection pool not initialized. Call initialize() first."
            )
        return self._client

    async def ping(self) -> str:
        """Health check: verify Valkey is responding."""
        client = await self.get_client()
        return (await client.ping()).decode("utf-8")

    async def close(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None
            logger.info("✅ Valkey connection pool closed")


# Global instance
valkey_pool = ValkeyConnectionPool()


class ValkeyConnection:
    """
    Async context manager for Vralkey opations.

    Usage:
        async with ValkeyConnection() as client:
            await client.set("key", "value")
    """

    def __init__(self, pool: ValkeyConnectionPool = valkey_pool) -> None:
        self.pool = pool
        self.client: Optional[GlideClient] = None

    async def __aenter__(self) -> GlideClient:
        self.client = await self.pool.get_client()
        return self.client

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            logger.error(f"Error is Valkey operation: {exc_val}")
        return False


async def with_valkey(func):
    """
    Decorator for functions that need Valkey access.

    Example:
        @with_valkey
        async def get_user(client: GlideClient, user_id: int):
            return await client.get(f"user:{user_id}")
    """

    async def wrapper(*args, **kwargs):
        async with ValkeyConnection() as client:
            return await func(client, *args, **kwargs)

    return wrapper

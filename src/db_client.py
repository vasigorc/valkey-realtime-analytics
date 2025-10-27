import asyncpg
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class DatabaseClient:
    """
    PostgreSQL connection pool manager.
    """

    def __init__(self) -> None:
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(
        self, host: str, port: int, database: str, user: str, password: str
    ) -> None:
        """
        Create connection pool.
        """
        if self._pool is not None:
            logger.info("PG client already connected, reusing existing pool")
            return
        try:
            dsn = f"postgres://{user}:{password}@{host}:{port}/{database}"
            self._pool = await asyncpg.create_pool(dsn=dsn, min_size=2)
        except Exception as e:
            logger.error(
                f"❌ Failed to connect to PostgreSQL server at {host}:{port}: {e}"
            )
            raise ConnectionError(
                f"Cannot connect to PostgreSQL server at {host}:{port}"
            ) from e

    async def disconnect(self) -> None:
        if self._pool is None:
            logger.warning("PG client already disconnected")
            return

        try:
            await self._pool.close()
            self._pool = None
        except Exception as e:
            logger.error("❌ Failed to close PostgreSQL connection", e)
            raise RuntimeError("Failed to close PostgreSQL connection") from e

    def _ensure_connected(self) -> None:
        if not self._pool:
            raise RuntimeError("PostgreSQL is not connected. Call connect() first.")

    async def execute_script(self, sql: str) -> None:
        self._ensure_connected()

        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(sql)

    async def fetch_one(self, query: str, *args) -> asyncpg.Record | None:
        self._ensure_connected()

        assert self._pool is not None
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetch_all(self, query: str, *args) -> list[asyncpg.Record]:
        self._ensure_connected()

        assert self._pool is not None
        async with self._pool.acquire() as conn:
            return await conn.fetch(query, *args)


db_client = DatabaseClient()

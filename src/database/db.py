"""
database.py
------------
Async database connection management using SQLAlchemy.

Provides:
- Engine and session setup (lazy creation)
- Safe async context managers
- Startup/shutdown hooks for FastAPI
- Health check (ping)
- Optional safe table creation with retry/backoff (for dev/test)
- Careful logging (DB URL masked) and custom exceptions
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Mapping, Optional

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import declarative_base

from src.utils.logger import get_logger

logger = get_logger("database_service_logger")

# Public SQLAlchemy base to be used by models
Base = declarative_base()


# ----- Custom Exceptions -----
class DatabaseConfigError(Exception):
    """Raised when DB configuration is invalid or missing."""


class DatabaseConnectionError(Exception):
    """Raised when the DB engine cannot connect."""


class DatabaseSessionError(Exception):
    """Raised for errors inside DB sessions."""


# ----- Helpers -----
def _mask_database_url(url: str) -> str:
    """
    Mask the credentials portion of a SQLAlchemy style URL for safe logging.

    Example:
        postgres://user:pass@host:5432/db  -> postgres://<hidden>:<hidden>@host:5432/db
    """
    try:
        parsed = make_url(url)
        # Replace username/password with '<hidden>' if present
        if parsed.username or parsed.password:
            safe_url = parsed._replace(
                username="<hidden>", password="<hidden>"
            ).render_as_string(hide_password=False)
        else:
            safe_url = str(parsed)
        return safe_url
    except Exception:
        # Fallback: don't reveal raw URL
        return "<unparsable-database-url>"


# ----- Database Session Manager -----
class DatabaseSessionManager:
    """
    Manages an async SQLAlchemy engine and session factory.

    Usage:
        manager = create_session_manager(database_url, **engine_kwargs)
        await manager.init_models(create_tables=True)  # optional for dev
        async with manager.session() as db:
            await db.execute(...)

    Public methods:
        - init_models(create_tables=False, retries=3, retry_backoff_seconds=1)
        - close()
        - ping()
        - session()  (async context manager)
    """

    def __init__(
        self,
        database_url: str,
        *,
        echo: bool = False,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: int = 30,
        pool_recycle: int = 1800,
        connect_args: Optional[Mapping[str, Any]] = None,
        **engine_kwargs: Any,
    ):
        """
        Construct the manager. This does not connect to the DB yet.

        Args:
            database_url: full SQLAlchemy URL string (required).
            echo: SQLAlchemy echo flag.
            pool_size, max_overflow, pool_timeout, pool_recycle: pool tuning defaults.
            connect_args: driver-specific connect args (optional).
            engine_kwargs: forwarded to create_async_engine.
        Raises:
            DatabaseConfigError: if database_url is missing.
        """
        if not database_url:
            raise DatabaseConfigError(
                "DATABASE_URL must be provided to DatabaseSessionManager"
            )

        self._database_url = database_url
        self._masked_url = _mask_database_url(database_url)
        self._echo = echo
        self._pool_size = pool_size
        self._max_overflow = max_overflow
        self._pool_timeout = pool_timeout
        self._pool_recycle = pool_recycle
        self._connect_args = connect_args or {}
        self._engine_kwargs = dict(engine_kwargs)

        self._engine: Optional[AsyncEngine] = None
        self._sessionmaker: Optional[async_sessionmaker] = None

        logger.info("DatabaseSessionManager constructed for %s", self._masked_url)

    def _create_engine(self) -> AsyncEngine:
        """
        Lazily create an AsyncEngine with sensible defaults merged with overrides.
        """
        if self._engine is None:
            # Compose engine kwargs safely; don't include DB credentials in logs
            engine_opts = {
                "echo": self._echo,
                # Note: For async drivers, some pool options may be driver-specific.
                "pool_pre_ping": True,
                "pool_recycle": self._pool_recycle,
                **self._engine_kwargs,
            }

            # connect_args is driver-specific (e.g. for asyncpg, you might pass server_settings)
            if self._connect_args:
                engine_opts["connect_args"] = dict(self._connect_args)

            # create engine
            try:
                engine = create_async_engine(self._database_url, **engine_opts)
                self._engine = engine
                # sessionmaker: keep autocommit/autoflush explicit
                self._sessionmaker = async_sessionmaker(
                    bind=self._engine, autocommit=False, autoflush=False
                )
                logger.info("Async engine created for %s", self._masked_url)
            except Exception as exc:
                logger.exception(
                    "Failed to create async engine for %s: %s", self._masked_url, exc
                )
                raise DatabaseConnectionError(
                    f"Failed to create DB engine: {exc}"
                ) from exc

        return self._engine

    async def init_models(
        self,
        create_tables: bool = False,
        retries: int = 0,
        retry_backoff_seconds: float = 1.0,
    ) -> None:
        """
        Optionally create tables using Base.metadata.create_all. This is intended for
        development/test usage only. For production prefer Alembic migrations.

        Args:
            create_tables: if True, run metadata.create_all (requires elevated permissions).
            retries: number of retries to attempt on failure (for transient DB unavailability).
            retry_backoff_seconds: base backoff (exponential).

        Raises:
            DatabaseConnectionError on repeated failure.
        """
        # Ensure engine exists (but do not connect yet)
        engine = self._create_engine()

        if not create_tables:
            logger.info(
                "init_models: create_tables=False; skipping auto-create. Use Alembic for production."
            )
            return

        attempt = 0
        while True:
            try:
                logger.info(
                    "init_models: creating tables (attempt %d) for %s",
                    attempt + 1,
                    self._masked_url,
                )
                async with engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                logger.info(
                    "Database tables initialized successfully for %s", self._masked_url
                )
                return
            except Exception as exc:
                attempt += 1
                logger.exception(
                    "init_models: attempt %d failed for %s: %s",
                    attempt,
                    self._masked_url,
                    exc,
                )
                if attempt > retries:
                    raise DatabaseConnectionError(
                        f"Failed to initialize DB models after {attempt} attempts: {exc}"
                    ) from exc
                sleep_for = retry_backoff_seconds * (2 ** (attempt - 1))
                logger.info("init_models: retrying after %.1fs", sleep_for)
                await asyncio.sleep(sleep_for)

    async def close(self) -> None:
        """
        Dispose engine and free resources. Safe to call multiple times.
        """
        if self._engine:
            try:
                await self._engine.dispose()
                logger.info("Database engine disposed for %s", self._masked_url)
            except Exception as exc:
                logger.exception(
                    "Error while disposing engine for %s: %s", self._masked_url, exc
                )
                # swallow to allow graceful shutdown

            self._engine = None
            self._sessionmaker = None
        else:
            logger.debug(
                "close() called but engine was not initialized for %s", self._masked_url
            )

    async def ping(self) -> bool:
        """
        Simple DB connectivity check. Returns True if a simple query succeeds.
        """
        try:
            engine = self._create_engine()
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            logger.debug("Database ping succeeded for %s", self._masked_url)
            return True
        except Exception as exc:
            logger.exception("Database ping failed for %s: %s", self._masked_url, exc)
            return False

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """
        Async context manager that yields an AsyncSession.

        Usage:
            async with manager.session() as db:
                await db.execute(...)

        Behavior:
            - Rolls back on any Exception
            - Logs exceptions with context
            - Does NOT auto-commit (call await db.commit() explicitly)
            - Does NOT call await db.close(); the session context manager handles cleanup.
        Raises:
            DatabaseSessionError if an unexpected error occurs while managing the session.
        """
        try:
            _ = self._create_engine()

            # sessionmaker guaranteed to be set by _create_engine()
            assert self._sessionmaker is not None
            async with self._sessionmaker() as db:
                try:
                    yield db
                except Exception as e:
                    # Rollback on any exception to avoid partial transactions
                    logger.exception("DB session error for %s: %s", self._masked_url, e)
                    try:
                        await db.rollback()
                    except Exception as rollback_error:
                        logger.exception(
                            "Failed to rollback DB session for %s: %s",
                            self._masked_url,
                            rollback_error,
                        )
                    raise
                # context manager closes session automatically; do NOT await db.close()
        except Exception as exc:
            # Any exception in acquiring engine/session factory
            logger.exception(
                "Failed to create DB session context for %s: %s",
                getattr(self, "_masked_url", "<unknown>"),
                exc,
            )
            raise DatabaseSessionError(f"Failed to create DB session: {exc}") from exc


# ----- Module-level factory & accessor (lazy) -----
_session_manager: Optional[DatabaseSessionManager] = None


def create_session_manager(
    database_url: Optional[str] = None, **engine_kwargs: Any
) -> DatabaseSessionManager:
    """
    Factory for a singleton DatabaseSessionManager instance.

    Prefer calling this during application startup with explicit database_url and desired engine kwargs.
    """
    global _session_manager
    if _session_manager is None:
        # Allow explicit parameter or fallback to environment
        db_url = database_url or os.getenv("DATABASE_URL")
        if not db_url:
            raise DatabaseConfigError(
                "DATABASE_URL must be set when creating the session manager"
            )
        _session_manager = DatabaseSessionManager(db_url, **engine_kwargs)
    return _session_manager


def get_session_manager() -> DatabaseSessionManager:
    """
    Accessor for the module-level session manager. Raises if not initialized.
    """
    if _session_manager is None:
        raise DatabaseConfigError(
            "Session manager not initialized. Call create_session_manager(...) during startup."
        )
    return _session_manager


# ----- FastAPI lifecycle convenience functions -----
async def startup(
    db_url: Optional[str] = None,
    *,
    auto_create_tables: bool = False,
    create_table_retries: int = 3,
    **engine_kwargs: Any,
) -> None:
    """
    Should be called from FastAPI startup event.

    Args:
        db_url: optional override for DATABASE_URL
        auto_create_tables: only set True for dev/test; prefer migrations in prod
        create_table_retries: number of retries for table creation
        engine_kwargs: forwarded to create_session_manager
    """
    # initialize manager
    manager = create_session_manager(db_url, **engine_kwargs)
    await manager.init_models(
        create_tables=auto_create_tables, retries=create_table_retries
    )


async def shutdown() -> None:
    """
    Should be called from FastAPI shutdown event.
    """
    if _session_manager:
        try:
            await _session_manager.close()
        except Exception:
            logger.exception("Error occurred during DB shutdown.")
    else:
        logger.debug("shutdown() called but session manager was not initialized.")

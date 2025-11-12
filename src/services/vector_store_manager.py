from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional

try:
    import chromadb
    from chromadb import DEFAULT_TENANT
    from chromadb.api import ClientAPI
    from chromadb.api.models.Collection import Collection
    from chromadb.config import Settings
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("chromadb is required for vector store management") from exc

from llama_index.vector_stores.chroma import ChromaVectorStore
from sqlalchemy import select

from src.config.settings import VectorStoreConfig
from src.database.db import get_session_manager
from src.database.models import Tenant
from src.providers.vector_stores import VectorStoreFactory
from src.utils.logger import get_logger

logger = get_logger("services.vector_store.manager")


@dataclass(frozen=True)
class TenantVectorSettings:
    tenant: str
    database: str
    collection: str


@dataclass
class TenantVectorResources:
    vector_store: ChromaVectorStore
    collection: Collection
    client: ClientAPI


class TenantVectorStoreManager:
    """
    Manages tenant-scoped ChromaDB resources.

    Responsibilities:
    - Lazily load tenant vector store configuration from the database.
    - Provision Chroma databases per tenant via AdminClient.
    - Cache Chroma HttpClient/Collection/VectorStore handles per tenant.
    """

    def __init__(self, base_config: Optional[VectorStoreConfig] = None):
        self._base_config = base_config or VectorStoreConfig()
        if not self._base_config.use_remote:
            raise ValueError(
                "TenantVectorStoreManager requires remote Chroma configuration. "
                "Set VECTOR_DB_USE_REMOTE=true and provide host/port."
            )

        if not self._base_config.remote_host or not self._base_config.remote_port:
            raise ValueError(
                "Remote Chroma host/port must be configured for multi-tenant operation."
            )

        self._admin_settings = Settings(
            chroma_api_impl=self._base_config.api_impl,
            chroma_server_host=self._base_config.remote_host,
            chroma_server_http_port=self._base_config.remote_port,
        )
        self._admin_client = chromadb.AdminClient(self._admin_settings)

        self._tenant_settings_cache: Dict[str, TenantVectorSettings] = {}
        self._resource_cache: Dict[str, TenantVectorResources] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    # --------------------------------------------------------------------- #
    # Public API
    # --------------------------------------------------------------------- #

    def generate_database_name(self, tenant_identifier: str) -> str:
        """Generate a deterministic database name for a tenant."""
        slug = re.sub(r"[^a-zA-Z0-9_-]", "-", tenant_identifier).lower()
        return f"{self._base_config.database_prefix}-{slug}"

    def ensure_database(
        self, *, database: str, tenant: str, create: bool = False
    ) -> None:
        """Ensure the tenant database exists in Chroma."""
        try:
            self._admin_client.get_database(database, tenant)
            logger.debug(
                "Chroma database found: tenant=%s database=%s", tenant, database
            )
        except Exception as e:  # pylint: disable=broad-except

            if create:
                self._admin_client.create_database(database, tenant)
                logger.info(
                    f"Chroma database created for tenant={tenant} database={database}"
                )
            else:
                logger.error(f"Database: {database} not found for tenant={tenant}: {e}")
                raise e

    async def get_resources(self, tenant_id: str) -> TenantVectorResources:
        """Get or initialize Chroma resources for a tenant."""
        if tenant_id in self._resource_cache:
            logger.info(
                f"=======> Cache hit for vector store resources for tenant_id={tenant_id}"
            )
            return self._resource_cache[tenant_id]

        logger.info(f"=======> Locking for tenant_id={tenant_id}")
        lock = self._locks.setdefault(tenant_id, asyncio.Lock())
        async with lock:
            if tenant_id in self._resource_cache:
                logger.info(
                    f"=======> Cache hit for vector store resources for tenant_id={tenant_id} after locking"
                )
                return self._resource_cache[tenant_id]

            logger.info(
                f"=======> Fetching vector store resources for tenant_id={tenant_id} after locking"
            )
            settings = await self._get_tenant_settings(tenant_id)
            self.ensure_database(database=settings.database, tenant=settings.tenant)
            resources = self._create_resources(settings)
            self._resource_cache[tenant_id] = resources
            return resources

    def register_tenant_settings(
        self,
        tenant_id: str,
        *,
        database: str,
        collection: str,
        tenant: Optional[str] = None,
    ) -> None:
        """Register tenant settings in cache (used after onboarding)."""
        self._tenant_settings_cache[tenant_id] = TenantVectorSettings(
            tenant=tenant or self._base_config.admin_tenant,
            database=database,
            collection=collection,
        )

    # --------------------------------------------------------------------- #
    # Internal helpers
    # --------------------------------------------------------------------- #

    async def _get_tenant_settings(self, tenant_id: str) -> TenantVectorSettings:
        """Load tenant vector configuration from cache or database."""
        if tenant_id in self._tenant_settings_cache:
            return self._tenant_settings_cache[tenant_id]

        manager = get_session_manager()
        async with manager.session() as db:
            result = await db.execute(
                select(Tenant).where(Tenant.tenant_id == tenant_id)
            )
            tenant = result.scalar_one_or_none()

        if tenant is None:
            raise ValueError(f"Tenant not found for tenant_id={tenant_id}")

        vector_config: Dict[str, Any] = (tenant.config or {}).get("vector_store", {})
        database = vector_config.get("database")
        if not database:
            raise ValueError(
                f"Tenant {tenant_id} missing Chroma database configuration. "
                "Re-run onboarding after enabling multi-tenancy."
            )

        collection = vector_config.get(
            "collection_name", self._base_config.collection_name
        )
        tenant_scope = vector_config.get(
            "tenant", self._base_config.admin_tenant or DEFAULT_TENANT
        )

        settings = TenantVectorSettings(
            tenant=tenant_scope,
            database=database,
            collection=collection,
        )

        self._tenant_settings_cache[tenant_id] = settings
        return settings

    def _create_resources(
        self, settings: TenantVectorSettings
    ) -> TenantVectorResources:
        """Instantiate vector store resources for the tenant."""
        vector_store, collection, client = VectorStoreFactory.create(
            self._base_config,
            tenant=settings.tenant,
            database=settings.database,
            collection_name=settings.collection,
        )
        return TenantVectorResources(
            vector_store=vector_store,
            collection=collection,
            client=client,
        )


_tenant_vector_store_manager: Optional[TenantVectorStoreManager] = None
_manager_lock: Optional[asyncio.Lock] = None


async def get_tenant_vector_store_manager() -> TenantVectorStoreManager:
    """Get singleton tenant vector store manager."""
    global _tenant_vector_store_manager
    global _manager_lock

    if _tenant_vector_store_manager is not None:
        return _tenant_vector_store_manager

    if _manager_lock is None:
        _manager_lock = asyncio.Lock()

    async with _manager_lock:
        if _tenant_vector_store_manager is None:
            _tenant_vector_store_manager = TenantVectorStoreManager()
        return _tenant_vector_store_manager


def get_tenant_vector_store_manager_sync() -> TenantVectorStoreManager:
    """
    Synchronous accessor for contexts where event loop is not readily available
    (e.g., FastAPI startup hooks).
    """
    global _tenant_vector_store_manager
    if _tenant_vector_store_manager is None:
        _tenant_vector_store_manager = TenantVectorStoreManager()
    return _tenant_vector_store_manager

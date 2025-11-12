from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Dict, Optional

from sqlalchemy import select

from src.database.db import get_session_manager
from src.database.models import Tenant
from src.services.security.encryption_service import EncryptionService
from src.utils.logger import get_logger

logger = get_logger("services.security.tenant_secret")


@dataclass(frozen=True)
class TenantSecrets:
    primary: str
    secondary: str


class TenantSecretManager:
    """Caches decrypted tenant secret keys for HMAC validation."""

    def __init__(self, encryption_service: Optional[EncryptionService] = None):
        self._encryption_service = encryption_service or EncryptionService()
        self._cache: Dict[str, TenantSecrets] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    async def get_secrets(self, tenant_id: str) -> TenantSecrets:
        """Fetch and cache decrypted primary/secondary secrets for a tenant."""
        if tenant_id in self._cache:
            return self._cache[tenant_id]

        lock = self._locks.setdefault(tenant_id, asyncio.Lock())
        async with lock:
            if tenant_id in self._cache:
                return self._cache[tenant_id]

            manager = get_session_manager()
            async with manager.session() as db:
                result = await db.execute(
                    select(
                        Tenant.secret_key_primary,
                        Tenant.secret_key_secondary,
                    ).where(Tenant.tenant_id == tenant_id)
                )
                row = result.first()

            logger.info(f"Tenant secrets found for tenant_id={tenant_id} : {row}")

            if not row:
                raise ValueError(f"Tenant secrets not found for tenant_id={tenant_id}")

            primary_encrypted, secondary_encrypted = row
            primary_secret = self._encryption_service.decrypt(primary_encrypted)
            secondary_secret = self._encryption_service.decrypt(secondary_encrypted)

            secrets = TenantSecrets(primary=primary_secret, secondary=secondary_secret)
            self._cache[tenant_id] = secrets
            return secrets

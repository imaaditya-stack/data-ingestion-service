"""
Tenant Onboarding Service
Handles tenant onboarding: secret generation, encryption, and tenant record creation.
"""

from typing import Any, Dict, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.config.settings import VectorStoreConfig
from src.database.models import OnboardingStatus, Tenant
from src.services.security.encryption_service import EncryptionService
from src.services.vector_store_manager import get_tenant_vector_store_manager_sync
from src.utils.logger import get_logger
from src.utils.secret_generator import generate_secret_key

logger = get_logger("services.tenant.onboarding")


class TenantOnboardingService:
    """
    Service for onboarding new tenants.

    Responsibilities:
    - Generate cryptographically secure secret keys
    - Encrypt secret keys before storage
    - Create tenant records with default configurations
    - Handle tenant validation and error cases
    """

    def __init__(self, encryption_service: Optional[EncryptionService] = None):
        """
        Initialize tenant onboarding service.

        Args:
            encryption_service: EncryptionService instance (creates new if not provided)
        """
        self.encryption_service = encryption_service or EncryptionService()
        logger.info("TenantOnboardingService initialized")

    def _generate_default_config(self, tenant_identifier: str) -> Dict[str, Any]:
        """
        Generate default configuration for tenant.

        For now, uses default VectorStoreConfig settings.
        Multi-tenancy for ChromaDB will be handled later.

        Returns:
            Dictionary with default configuration
        """
        vector_config = VectorStoreConfig()
        manager = get_tenant_vector_store_manager_sync()
        database_name = manager.generate_database_name(tenant_identifier)
        collection_name = vector_config.collection_name

        manager.ensure_database(
            database=database_name,
            tenant=vector_config.admin_tenant,
            create=True,
        )
        manager.register_tenant_settings(
            tenant_identifier,
            database=database_name,
            collection=collection_name,
            tenant=vector_config.admin_tenant,
        )

        return {
            "vector_store": {
                "collection_name": collection_name,
                "use_remote": vector_config.use_remote,
                "remote_host": vector_config.remote_host,
                "remote_port": vector_config.remote_port,
                "tenant": vector_config.admin_tenant,
                "database": database_name,
            }
        }

    async def onboard_tenant(
        self,
        db: AsyncSession,
        tenant_id: str,
        tenant_name: str,
    ) -> Tuple[Tenant, str, str]:
        """
        Onboard a new tenant.

        Generates secret keys, encrypts them, and creates tenant record.

        Args:
            db: Database session
            tenant_id: External tenant ID from Django source app
            tenant_name: Human-readable tenant name

        Returns:
            Tuple of (Tenant object, primary_secret_key, secondary_secret_key)
            Note: Secret keys are returned in plaintext only at creation time.
                  They are encrypted in the database and cannot be retrieved later.

        Raises:
            ValueError: If tenant_id or tenant_name already exists
            Exception: If encryption or database operations fail
        """
        logger.info(
            f"Onboarding tenant: tenant_id={tenant_id}, tenant_name={tenant_name}"
        )

        # Check if tenant already exists
        existing = await self._check_tenant_exists(db, tenant_id, tenant_name)
        if existing:
            raise ValueError(
                f"Tenant already ets: tenant_id={tenant_id} or tenant_name={tenant_name}"
            )

        # Generate secret keys
        primary_secret = generate_secret_key()
        secondary_secret = generate_secret_key()

        logger.debug("Generated secret keys for tenant")

        # Encrypt secret keys
        encrypted_primary = self.encryption_service.encrypt(primary_secret)
        encrypted_secondary = self.encryption_service.encrypt(secondary_secret)

        logger.debug("Encrypted secret keys")

        # Generate default configuration
        default_config = self._generate_default_config(tenant_id)

        # Create tenant record
        tenant = Tenant(
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            secret_key_primary=encrypted_primary,
            secret_key_secondary=encrypted_secondary,
            config=default_config,
            onboarding_status=OnboardingStatus.PENDING,
        )

        try:
            db.add(tenant)

            await db.commit()
            await db.refresh(tenant)

            logger.info(
                f"Tenant created successfully: id={tenant.id}, tenant_id={tenant_id}"
            )

            # Return tenant and plaintext secret keys (only time they're available)
            return tenant, primary_secret, secondary_secret
        except IntegrityError as e:
            logger.error(f"Failed to create tenant due to integrity error: {e}")
            raise ValueError(
                f"Tenant already exists: tenant_id={tenant_id} or tenant_name={tenant_name}"
            ) from e
        except Exception as e:
            logger.error(f"Failed to create tenant: {e}", exc_info=True)
            raise

    async def _check_tenant_exists(
        self, db: AsyncSession, tenant_id: str, tenant_name: str
    ) -> Optional[Tenant]:
        """
        Check if a tenant with given tenant_id or tenant_name already exists.

        Args:
            db: Database session
            tenant_id: External tenant ID
            tenant_name: Tenant name

        Returns:
            Tenant object if exists, None otherwise
        """
        result = await db.execute(
            select(Tenant).where(
                (Tenant.tenant_id == tenant_id) | (Tenant.tenant_name == tenant_name)
            )
        )
        return result.scalar_one_or_none()

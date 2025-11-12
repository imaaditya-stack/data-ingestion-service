"""
Simple validation script for security services.
Tests encryption/decryption and HMAC signature generation/verification.
"""

import asyncio
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from datetime import datetime, timezone

from src.services.kafka.models import EventMetadata, KafkaEvent, OperationType
from src.services.security.encryption_service import EncryptionService
from src.services.security.hmac_service import HMACService
from src.services.security.tenant_secret_manager import TenantSecretManager
from src.utils.logger import get_logger
from src.utils.secret_generator import generate_secret_key

logger = get_logger("validate_security")


def test_encryption_service():
    """Test encryption and decryption"""
    logger.info("Testing Encryption Service...")

    encryption = EncryptionService()
    plaintext = "test-secret-key-12345"

    encrypted = encryption.encrypt(plaintext)
    decrypted = encryption.decrypt(encrypted)

    assert decrypted == plaintext, "Decryption failed"
    logger.info("✓ Encryption/Decryption: PASSED")
    return True


event = KafkaEvent(
    event_id="evt-test-123",
    tenant_id="tenant_abc",
    timestamp=datetime.now(timezone.utc),
    data={"id": "123", "product_name": "Test Product", "category": "Electronics"},
    metadata=EventMetadata(operation=OperationType.CREATE, source="test-script"),
    schema_version=1,
)


def test_hmac_service():
    """Test HMAC signature generation and verification with Kafka event"""
    logger.info("Testing HMAC Service...")

    # Generate secret key
    secret_key = generate_secret_key()

    # Convert event to dict for HMAC
    event_dict = event.model_dump(mode="json")

    # Generate signature
    signature = HMACService.generate_signature(secret_key, event_dict)
    print(f"Signature: {signature}")
    # Verify signature
    is_valid = HMACService.verify_signature(secret_key, event_dict, signature)
    print(f"Is valid: {is_valid}")
    assert is_valid, "Signature verification failed"

    logger.info("✓ HMAC Generation/Verification: PASSED")
    return True


def test_hmac_with_wrong_key():
    """Test HMAC verification with wrong key"""
    logger.info("Testing HMAC Service with Wrong Key...")
    secret_key = generate_secret_key()
    event_dict = event.model_dump(mode="json")
    signature = HMACService.generate_signature(secret_key, event_dict)

    wrong_secret_key = generate_secret_key()
    is_invalid = HMACService.verify_signature(wrong_secret_key, event_dict, signature)
    print(f"Is invalid: {is_invalid}")
    assert not is_invalid, "Signature should be invalid with wrong key"
    logger.info("✓ HMAC Verification with Wrong Key: PASSED")
    return False


def test_tenant_hmac_flow():
    """Validate HMAC generation/verification using stored tenant secrets."""
    tenant_id = os.getenv("TEST_TENANT_ID")
    if not tenant_id:
        logger.info(
            "Skipping tenant HMAC verification (set TEST_TENANT_ID to enable this test)"
        )
        return True

    async def _run():
        manager = TenantSecretManager()
        secrets = await manager.get_secrets(tenant_id)
        payload = event.model_dump(mode="json")
        signature = HMACService.generate_signature(secrets.primary, payload)
        is_valid, key_used = HMACService.verify_signature_with_fallback(
            primary_key=secrets.primary,
            secondary_key=secrets.secondary,
            payload=payload,
            provided_signature=signature,
        )
        assert is_valid, "Tenant HMAC verification failed"
        logger.info(
            "✓ Tenant HMAC verification succeeded using %s key", key_used.upper()
        )

    asyncio.run(_run())
    return True


def main():
    """Run all validation tests"""
    logger.info("=" * 50)
    logger.info("Security Services Validation")
    logger.info("=" * 50)

    try:
        # test_encryption_service()
        # test_hmac_service()
        test_hmac_with_wrong_key()
        test_tenant_hmac_flow()

        return 0
    except AssertionError as e:
        logger.error(f"✗ Test FAILED: {e}")
        return 1
    except Exception as e:
        logger.error(f"✗ Error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

"""
HMAC Service
Handles HMAC signature generation and verification for Kafka event authentication.
Uses Python's standard library hmac module.
"""

import hmac
import json
from typing import Any, Dict, Tuple, Union

from src.utils.logger import get_logger

logger = get_logger("services.security.hmac")


class HMACService:
    """
    Service for generating and verifying HMAC signatures.

    Uses HMAC-SHA256 for message authentication. The source app will generate
    signatures for Kafka event payloads, and this service will verify them.
    """

    DEFAULT_ALGORITHM = "sha256"

    @staticmethod
    def generate_signature(
        secret_key: str,
        payload: Union[str, Dict[str, Any], bytes],
        algorithm: str = DEFAULT_ALGORITHM,
    ) -> str:
        """
        Generate HMAC signature for a payload.

        Args:
            secret_key: Secret key (plaintext, will be converted to bytes)
            payload: Payload to sign. Can be:
                - String (will be encoded to UTF-8)
                - Dictionary (will be JSON-serialized and encoded)
                - Bytes (used as-is)
            algorithm: Hash algorithm (default: sha256)

        Returns:
            Hex-encoded HMAC signature string

        Example:
            >>> service = HMACService()
            >>> payload = {"event_id": "123", "data": {"key": "value"}}
            >>> signature = service.generate_signature("my-secret-key", payload)
            >>> len(signature)  # 64 characters for SHA256
            64
        """
        # Convert payload to bytes
        if isinstance(payload, dict):
            # JSON-serialize dictionary payloads
            payload_bytes = json.dumps(payload, sort_keys=True).encode("utf-8")
        elif isinstance(payload, str):
            payload_bytes = payload.encode("utf-8")
        elif isinstance(payload, bytes):
            payload_bytes = payload
        else:
            raise ValueError(
                f"Payload must be str, dict, or bytes, got {type(payload)}"
            )

        # Convert secret key to bytes
        if isinstance(secret_key, str):
            secret_key_bytes = secret_key.encode("utf-8")
        elif isinstance(secret_key, bytes):
            secret_key_bytes = secret_key
        else:
            raise ValueError(f"Secret key must be str or bytes, got {type(secret_key)}")

        # Generate HMAC signature
        try:
            mac = hmac.new(secret_key_bytes, payload_bytes, digestmod=algorithm)
            signature = mac.hexdigest()
            logger.debug(f"Generated HMAC signature using {algorithm}")
            return signature
        except Exception as e:
            logger.error(f"Failed to generate HMAC signature: {e}")
            raise ValueError(f"HMAC signature generation failed: {e}") from e

    @staticmethod
    def verify_signature(
        secret_key: str,
        payload: Union[str, Dict[str, Any], bytes],
        provided_signature: str,
        algorithm: str = DEFAULT_ALGORITHM,
    ) -> bool:
        """
        Verify HMAC signature against a payload.

        Uses hmac.compare_digest() for constant-time comparison to prevent
        timing attacks.

        Args:
            secret_key: Secret key (plaintext, will be converted to bytes)
            payload: Payload that was signed. Can be:
                - String (will be encoded to UTF-8)
                - Dictionary (will be JSON-serialized and encoded)
                - Bytes (used as-is)
            provided_signature: Signature to verify (hex-encoded string)
            algorithm: Hash algorithm (default: sha256)

        Returns:
            True if signature is valid, False otherwise

        Example:
            >>> service = HMACService()
            >>> payload = {"event_id": "123", "data": {"key": "value"}}
            >>> signature = service.generate_signature("my-secret-key", payload)
            >>> service.verify_signature("my-secret-key", payload, signature)
            True
            >>> service.verify_signature("wrong-key", payload, signature)
            False
        """
        if not provided_signature:
            logger.warning("Empty signature provided for verification")
            return False

        try:
            # Generate expected signature
            expected_signature = HMACService.generate_signature(
                secret_key, payload, algorithm
            )

            # Use constant-time comparison to prevent timing attacks
            # Reference: https://docs.python.org/3/library/hmac.html#hmac.compare_digest
            is_valid = hmac.compare_digest(
                provided_signature.encode("utf-8"),
                expected_signature.encode("utf-8"),
            )

            if not is_valid:
                logger.warning(
                    "HMAC signature verification failed: signatures do not match"
                )
            else:
                logger.debug("HMAC signature verification successful")

            return is_valid
        except Exception as e:
            logger.error(f"HMAC signature verification error: {e}")
            return False

    @staticmethod
    def verify_signature_with_fallback(
        primary_key: str,
        secondary_key: str,
        payload: Union[str, Dict[str, Any], bytes],
        provided_signature: str,
        algorithm: str = DEFAULT_ALGORITHM,
    ) -> Tuple[bool, str]:
        """
        Verify HMAC signature using primary key, with fallback to secondary key.

        Useful for key rotation scenarios where both keys should be checked.

        Args:
            primary_key: Primary secret key
            secondary_key: Secondary secret key (for key rotation)
            payload: Payload that was signed
            provided_signature: Signature to verify
            algorithm: Hash algorithm (default: sha256)

        Returns:
            Tuple of (is_valid: bool, key_used: str)
            key_used will be "primary", "secondary", or "none"

        Example:
            >>> service = HMACService()
            >>> payload = {"event_id": "123"}
            >>> signature = service.generate_signature("primary-key", payload)
            >>> is_valid, key_used = service.verify_signature_with_fallback(
            ...     "primary-key", "secondary-key", payload, signature
            ... )
            >>> is_valid, key_used
            (True, 'primary')
        """
        # Try primary key first
        if primary_key and HMACService.verify_signature(
            primary_key, payload, provided_signature, algorithm
        ):
            logger.debug("Signature verified with primary key")
            return True, "primary"

        # Fallback to secondary key
        if secondary_key and HMACService.verify_signature(
            secondary_key, payload, provided_signature, algorithm
        ):
            logger.debug("Signature verified with secondary key")
            return True, "secondary"

        logger.warning(
            "Signature verification failed with both primary and secondary keys"
        )
        return False, "none"

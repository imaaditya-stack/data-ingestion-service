"""
Encryption Service
Handles encryption and decryption of sensitive data (e.g., secret keys) using Fernet.
"""

import os
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken
from dotenv import load_dotenv

from src.utils.logger import get_logger

load_dotenv()

logger = get_logger("services.security.encryption")


class EncryptionService:
    """
    Service for encrypting and decrypting sensitive data.

    Uses Fernet (symmetric encryption) from the cryptography library.
    Encryption key is loaded from ENCRYPTION_KEY environment variable.
    """

    def __init__(self, encryption_key: Optional[str] = None):
        """
        Initialize encryption service.

        Args:
            encryption_key: Fernet encryption key (base64-encoded).
                If not provided, loads from ENCRYPTION_KEY environment variable.
                If not found, generates a new key (for development only).

        Raises:
            ValueError: If encryption key is invalid or missing in production.
        """
        key = encryption_key or os.getenv("ENCRYPTION_KEY")

        if not key:
            # In production, this should raise an error
            # For development, we can generate a key (but warn)
            if os.getenv("ENVIRONMENT") == "production":
                raise ValueError(
                    "ENCRYPTION_KEY environment variable is required in production"
                )
            logger.warning(
                "ENCRYPTION_KEY not found. Generating a new key for development. "
                "This key will not persist across restarts!"
            )
            key = Fernet.generate_key().decode("utf-8")
            logger.warning(f"Generated encryption key: {key}")

        try:
            # Ensure key is bytes
            if isinstance(key, str):
                key = key.encode("utf-8")
            self._fernet = Fernet(key)
            logger.info("EncryptionService initialized successfully")
        except Exception as e:
            raise ValueError(f"Invalid encryption key: {e}") from e

    def encrypt(self, plaintext: str) -> str:
        """
        Encrypt a plaintext string.

        Args:
            plaintext: String to encrypt

        Returns:
            Encrypted string (base64-encoded)
        """
        if not plaintext:
            raise ValueError("Plaintext cannot be empty")

        try:
            encrypted_bytes = self._fernet.encrypt(plaintext.encode("utf-8"))
            return encrypted_bytes.decode("utf-8")
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            raise ValueError(f"Encryption failed: {e}") from e

    def decrypt(self, ciphertext: str) -> str:
        """
        Decrypt a ciphertext string.

        Args:
            ciphertext: Encrypted string (base64-encoded)

        Returns:
            Decrypted plaintext string

        Raises:
            ValueError: If decryption fails (invalid token, wrong key, etc.)
        """
        if not ciphertext:
            raise ValueError("Ciphertext cannot be empty")

        try:
            decrypted_bytes = self._fernet.decrypt(ciphertext.encode("utf-8"))
            return decrypted_bytes.decode("utf-8")
        except InvalidToken as e:
            logger.error(f"Decryption failed: Invalid token or wrong key")
            raise ValueError("Decryption failed: Invalid token or wrong key") from e
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            raise ValueError(f"Decryption failed: {e}") from e

    @staticmethod
    def generate_key() -> str:
        """
        Generate a new Fernet encryption key.

        Returns:
            Base64-encoded encryption key string
        """
        return Fernet.generate_key().decode("utf-8")

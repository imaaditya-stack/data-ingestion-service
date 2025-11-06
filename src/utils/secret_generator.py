"""
Secret Generator Utility
Generates cryptographically secure random secrets for HMAC signing.
"""

import secrets
from typing import Optional


def generate_secret_key(length: int = 32) -> str:
    """
    Generate a cryptographically secure random secret key.

    Args:
        length: Length of the secret in bytes (default: 32 bytes = 256 bits)

    Returns:
        Hex-encoded secret key string

    Example:
        >>> key = generate_secret_key()
        >>> len(key)  # 64 characters (32 bytes * 2 hex chars)
        64
    """
    if length < 16:
        raise ValueError("Secret key length must be at least 16 bytes (128 bits)")
    if length > 128:
        raise ValueError("Secret key length must be at most 128 bytes (1024 bits)")

    # Generate random bytes and convert to hex string
    random_bytes = secrets.token_bytes(length)
    return random_bytes.hex()


def generate_secret_key_base64(length: int = 32) -> str:
    """
    Generate a cryptographically secure random secret key in base64 encoding.

    Args:
        length: Length of the secret in bytes (default: 32 bytes = 256 bits)

    Returns:
        Base64-encoded secret key string

    Example:
        >>> key = generate_secret_key_base64()
        >>> len(key)  # 44 characters (base64 encoding of 32 bytes)
        44
    """
    if length < 16:
        raise ValueError("Secret key length must be at least 16 bytes (128 bits)")
    if length > 128:
        raise ValueError("Secret key length must be at most 128 bytes (1024 bits)")

    import base64

    random_bytes = secrets.token_bytes(length)
    return base64.urlsafe_b64encode(random_bytes).decode("utf-8")

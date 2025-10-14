"""
Logging utility with namespace support
"""

import logging
import sys


def get_logger(namespace: str, level: int = logging.INFO) -> logging.Logger:
    """
    Get a logger with a specific namespace

    Args:
        namespace: The namespace/module name for the logger
        level: Logging level (default: INFO)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(f"data_ingestion.{namespace}")

    if not logger.handlers:
        # Create console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)

        # Create formatter
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)

        # Add handler to logger
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False

    return logger

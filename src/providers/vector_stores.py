"""
Vector store factory
Supports ChromaDB (can be extended to other vector stores)
"""

from typing import Any, Tuple

import chromadb
from llama_index.vector_stores.chroma import ChromaVectorStore

from src.core.models import VectorStoreConfig
from src.utils.logger import get_logger

logger = get_logger("providers.vector_stores")


class VectorStoreFactory:
    """Factory for creating vector store instances"""

    @staticmethod
    def create_chroma(config: VectorStoreConfig) -> Tuple[ChromaVectorStore, Any]:
        """
        Create a ChromaDB vector store

        Args:
            config: Vector store configuration

        Returns:
            Tuple of (vector_store, collection)
        """
        logger.info(f"Creating ChromaDB vector store: {config.collection_name}")

        # Initialize Chroma client
        client = chromadb.PersistentClient(path=config.persist_path)

        # Get or create collection
        collection = client.get_or_create_collection(name=config.collection_name)

        # Create vector store wrapper
        vector_store = ChromaVectorStore(chroma_collection=collection)

        logger.info(f"ChromaDB initialized at: {config.persist_path}")
        return vector_store, collection

    @staticmethod
    def create(config: VectorStoreConfig) -> Tuple[ChromaVectorStore, Any]:
        """
        Create a vector store based on configuration
        Currently only supports ChromaDB, but can be extended

        Args:
            config: Vector store configuration

        Returns:
            Tuple of (vector_store, collection)
        """
        # Currently only Chroma is supported, but this can be extended
        return VectorStoreFactory.create_chroma(config)

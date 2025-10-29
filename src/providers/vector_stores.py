# """
# Vector store factory
# Supports ChromaDB (can be extended to other vector stores)
# """

# from typing import Any, Tuple

# import chromadb
# from llama_index.vector_stores.chroma import ChromaVectorStore

# from src.config.settings import VectorStoreConfig
# from src.utils.logger import get_logger

# logger = get_logger("providers.vector_stores")


# class VectorStoreFactory:
#     """Factory for creating vector store instances"""

#     @staticmethod
#     def create_chroma(config: VectorStoreConfig) -> Tuple[ChromaVectorStore, Any]:
#         """
#         Create a ChromaDB vector store

#         Args:
#             config: Vector store configuration

#         Returns:
#             Tuple of (vector_store, collection)
#         """
#         logger.info(f"Creating ChromaDB vector store: {config.collection_name}")

#         # Initialize Chroma client
#         client = chromadb.PersistentClient(path=config.persist_path)

#         # Get or create collection
#         collection = client.get_or_create_collection(name=config.collection_name)

#         # Create vector store wrapper
#         vector_store = ChromaVectorStore(chroma_collection=collection)

#         logger.info(f"ChromaDB initialized at: {config.persist_path}")
#         return vector_store, collection

#     @staticmethod
#     def create(
#         config: VectorStoreConfig,
#     ) -> Tuple[ChromaVectorStore, chromadb.Collection]:
#         """
#         Create a vector store based on configuration
#         Currently only supports ChromaDB, but can be extended

#         Args:
#             config: Vector store configuration

#         Returns:
#             Tuple of (vector_store, collection)
#         """
#         # Currently only Chroma is supported, but this can be extended
#         return VectorStoreFactory.create_chroma(config)

from typing import Tuple, Any
from llama_index.vector_stores.chroma import ChromaVectorStore

import chromadb

from src.config.settings import VectorStoreConfig
from src.utils.logger import get_logger


logger = get_logger("providers.vector_stores")


class VectorStoreFactory:
    @staticmethod
    def create_chroma(config: VectorStoreConfig) -> Tuple[ChromaVectorStore, Any]:
        logger.info(f"Creating ChromaDB vector store: {config.collection_name}")
        if config.use_remote:
            # Remote HTTP client mode
            logger.info(
                f"Connecting to remote Chroma server at {config.remote_host}:{config.remote_port}"
            )
            client = chromadb.HttpClient(
                host=config.remote_host, port=config.remote_port
            )
        else:
            # Local persistent mode
            assert (
                config.persist_path is not None
            ), "persist_path must be set for local mode"
            logger.info(
                f"Initializing local Chroma persistent at path: {config.persist_path}"
            )
            client = chromadb.PersistentClient(path=config.persist_path)

        collection = client.get_or_create_collection(name=config.collection_name)
        vector_store = ChromaVectorStore(chroma_collection=collection)

        logger.info("ChromaDB initialized")
        return vector_store, collection

    @staticmethod
    def create(
        config: VectorStoreConfig,
    ) -> Tuple[ChromaVectorStore, chromadb.Collection]:
        return VectorStoreFactory.create_chroma(config)

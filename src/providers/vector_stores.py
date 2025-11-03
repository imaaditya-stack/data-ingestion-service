from typing import Tuple

import chromadb
from chromadb.api import ClientAPI
from llama_index.vector_stores.chroma import ChromaVectorStore

from src.config.settings import VectorStoreConfig
from src.utils.logger import get_logger

logger = get_logger("providers.vector_stores")


class VectorStoreFactory:
    @staticmethod
    def create_chroma(
        config: VectorStoreConfig,
    ) -> Tuple[ChromaVectorStore, chromadb.Collection, ClientAPI]:
        logger.info(f"Creating ChromaDB vector store: {config.collection_name}")

        if not config.use_remote:
            # Local persistent mode is deprecated and no longer supported
            raise ValueError(
                "Local persistent mode is deprecated and no longer supported. "
                "Please use remote ChromaDB mode by setting use_remote=True. "
            )

        # Remote HTTP client mode (only supported mode)
        logger.info(
            f"Connecting to remote Chroma server at {config.remote_host}:{config.remote_port}"
        )
        client = chromadb.HttpClient(
            host=config.remote_host,
            port=config.remote_port,
        )

        collection = client.get_or_create_collection(name=config.collection_name)
        vector_store = ChromaVectorStore(chroma_collection=collection)

        logger.info("ChromaDB initialized")
        return vector_store, collection, client

    @staticmethod
    def create(
        config: VectorStoreConfig,
    ) -> Tuple[ChromaVectorStore, chromadb.Collection, ClientAPI]:
        return VectorStoreFactory.create_chroma(config)

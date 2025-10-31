from typing import Tuple, Any
from llama_index.vector_stores.chroma import ChromaVectorStore

import chromadb
from chromadb.api import ClientAPI

from src.config.settings import VectorStoreConfig
from src.utils.logger import get_logger


logger = get_logger("providers.vector_stores")


class VectorStoreFactory:
    @staticmethod
    def create_chroma(
        config: VectorStoreConfig,
    ) -> Tuple[ChromaVectorStore, chromadb.Collection, ClientAPI]:
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
        return vector_store, collection, client

    @staticmethod
    def create(
        config: VectorStoreConfig,
    ) -> Tuple[ChromaVectorStore, chromadb.Collection, ClientAPI]:
        return VectorStoreFactory.create_chroma(config)

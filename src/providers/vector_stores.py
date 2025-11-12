from typing import Any, Dict, Optional, Tuple

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
        *,
        tenant: Optional[str] = None,
        database: Optional[str] = None,
        collection_name: Optional[str] = None,
    ) -> Tuple[ChromaVectorStore, chromadb.Collection, ClientAPI]:
        logger.info(
            "Creating ChromaDB vector store: %s",
            collection_name or config.collection_name,
        )

        if not config.use_remote:
            # Local persistent mode is deprecated and no longer supported
            raise ValueError(
                "Local persistent mode is deprecated and no longer supported. "
                "Please use remote ChromaDB mode by setting use_remote=True. "
            )

        collection_name = collection_name or config.collection_name

        # Remote HTTP client mode (only supported mode)
        client_kwargs: Dict[str, Any] = {
            "host": config.remote_host,
        }

        if config.remote_port:
            client_kwargs["port"] = config.remote_port

        if tenant or config.admin_tenant:
            client_kwargs["tenant"] = tenant or config.admin_tenant

        if database:
            client_kwargs["database"] = database

        logger.info(
            "Connecting to remote Chroma server at %s:%s (tenant=%s, database=%s)",
            config.remote_host,
            config.remote_port,
            client_kwargs.get("tenant"),
            database or "<default>",
        )

        client = chromadb.HttpClient(**client_kwargs)

        collection = client.get_or_create_collection(name=collection_name)
        vector_store = ChromaVectorStore(chroma_collection=collection)

        logger.info("ChromaDB initialized")
        return vector_store, collection, client

    @staticmethod
    def create(
        config: VectorStoreConfig,
        **kwargs: Any,
    ) -> Tuple[ChromaVectorStore, chromadb.Collection, ClientAPI]:
        return VectorStoreFactory.create_chroma(config, **kwargs)

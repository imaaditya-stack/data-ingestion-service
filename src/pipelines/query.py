from typing import Optional

from llama_index.core.base.embeddings.base import BaseEmbedding
from llama_index.core.base.llms.base import BaseLLM
from llama_index.vector_stores.chroma import ChromaVectorStore

from src.config.settings import QueryConfig
from src.providers.embeddings import EmbeddingFactory
from src.providers.llms import LLMFactory
from src.providers.vector_stores import VectorStoreFactory
from src.utils.logger import get_logger

logger = get_logger("pipelines.query")


class QueryPipeline:
    """
    Pipeline for data querying
    Provides query infrastructure and components
    """

    def __init__(self, config: Optional[QueryConfig] = None):
        """
        Initialize query pipeline

        Args:
            config: Query configuration (defaults to QueryConfig.create_default())
        """
        self.config = config or QueryConfig.create_default()
        logger.info("Initializing Query Pipeline")

        # Initialize providers
        self._embed_model = EmbeddingFactory.create(self.config.embedding)
        self._llm = LLMFactory.create(self.config.llm)
        self._vector_store, self._collection = VectorStoreFactory.create(
            self.config.vector_store
        )

        logger.info("Query Pipeline initialized successfully")

    @property
    def chroma_collection(self):
        """Get the ChromaDB collection for direct operations"""
        return self._collection

    @property
    def embed_model(self) -> BaseEmbedding:
        """Get the embedding model"""
        return self._embed_model

    @property
    def llm(self) -> BaseLLM:
        """Get the LLM"""
        return self._llm

    @property
    def vector_store(self) -> ChromaVectorStore:
        """Get the vector store"""
        return self._vector_store

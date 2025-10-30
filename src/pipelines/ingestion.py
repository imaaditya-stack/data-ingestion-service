from typing import Optional

from llama_index.core.ingestion import IngestionPipeline
from llama_index.core.node_parser import SimpleNodeParser

from src.config.settings import IngestionConfig
from src.providers.embeddings import EmbeddingFactory
from src.providers.vector_stores import VectorStoreFactory
from src.utils.logger import get_logger

logger = get_logger("pipelines.ingestion")


class DataIngestionPipeline:
    """
    Pipeline for data ingestion
    Ingests data into vector database
    """

    def __init__(self, config: Optional[IngestionConfig] = None):
        """
        Initialize ingestion pipeline

        Args:
            config: Ingestion configuration (defaults to IngestionConfig.create_default())
        """
        self.config = config or IngestionConfig.create_default()
        logger.info("Initializing Ingestion Pipeline")

        # Initialize providers (only what's needed for ingestion)
        self._embed_model = EmbeddingFactory.create(self.config.embedding)
        self._vector_store, self._collection, _ = VectorStoreFactory.create(
            self.config.vector_store
        )

        # Create the LlamaIndex ingestion pipeline
        self._pipeline = self._create_ingestion_pipeline()

        logger.info("Ingestion Pipeline initialized successfully")

    def _create_ingestion_pipeline(self) -> IngestionPipeline:
        """Create the ingestion pipeline with transformations"""
        return IngestionPipeline(
            transformations=[
                SimpleNodeParser(),
                self._embed_model,
            ],
            vector_store=self._vector_store,
        )

    @property
    def chroma_collection(self):
        """Get the ChromaDB collection for direct operations"""
        return self._collection

    @property
    def embed_model(self):
        """Get the embedding model"""
        return self._embed_model

    @property
    def vector_store(self):
        """Get the vector store"""
        return self._vector_store

    @property
    def pipeline(self):
        """Get the LlamaIndex ingestion pipeline"""
        return self._pipeline

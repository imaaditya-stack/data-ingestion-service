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

    def __init__(
        self,
        embed_model,
        vector_store,
        collection,
        config: Optional[IngestionConfig] = None,
    ):
        """
        Initialize ingestion pipeline

        Args:
            embed_model: Embedding model
            vector_store: Vector store instance
            collection: ChromaDB collection instance
            config: Ingestion configuration (defaults to IngestionConfig.create_default())
        """
        self.config = config or IngestionConfig.create_default()
        logger.info("Initializing Ingestion Pipeline")

        # Initialize providers (only what's needed for ingestion)
        self._embed_model = embed_model
        self._collection = collection
        self._vector_store = vector_store

        # Create the LlamaIndex ingestion pipeline
        self._pipeline = self._create_ingestion_pipeline()

        logger.info("Ingestion Pipeline initialized successfully")

    @classmethod
    def create(cls, config: Optional[IngestionConfig] = None):
        """
        Async factory method to create DataIngestionPipeline

        Args:
            config: Ingestion configuration (defaults to IngestionConfig.create_default())

        Returns:
            DataIngestionPipeline instance
        """
        config = config or IngestionConfig.create_default()
        logger.info("Initializing Ingestion Pipeline")

        # Initialize providers (only what's needed for ingestion)
        embed_model = EmbeddingFactory.create(config.embedding)
        vector_store, collection, _ = VectorStoreFactory.create(config.vector_store)

        return cls(
            embed_model=embed_model,
            vector_store=vector_store,
            collection=collection,
            config=config,
        )

    def _create_ingestion_pipeline(self) -> IngestionPipeline:
        """Create the ingestion pipeline with transformations"""
        return IngestionPipeline(
            transformations=[
                # LLAMA INDEX Node Parser generates UUID for vector store id,
                # Hence we need to override the behavior by passing id_func.
                SimpleNodeParser(id_func=lambda idx, doc: doc.id_),
                self._embed_model,
            ],
            vector_store=self._vector_store,
            disable_cache=True,
            docstore=None,
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

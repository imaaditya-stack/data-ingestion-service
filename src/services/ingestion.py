"""
Ingestion service
Handles document chunking, embedding, and vector store ingestion
"""

from typing import List

from llama_index.core import Document
from llama_index.core.base.embeddings.base import BaseEmbedding
from llama_index.core.ingestion import IngestionPipeline
from llama_index.core.node_parser import SimpleNodeParser
from llama_index.vector_stores.chroma import ChromaVectorStore

from src.core.models import ChunkingConfig
from src.utils.logger import get_logger


logger = get_logger("services.ingestion")


class IngestionService:
    """Service for ingesting documents into vector store"""

    def __init__(
        self,
        embed_model: BaseEmbedding,
        vector_store: ChromaVectorStore,
        chunking_config: ChunkingConfig,
    ):
        """
        Initialize ingestion service

        Args:
            embed_model: Embedding model instance
            vector_store: Vector store instance
            chunking_config: Configuration for text chunking
        """
        self.embed_model = embed_model
        self.vector_store = vector_store
        self.chunking_config = chunking_config
        self.pipeline = self._create_pipeline()

    def _create_pipeline(self) -> IngestionPipeline:
        """Create the ingestion pipeline with transformations"""
        logger.info(
            f"Creating ingestion pipeline with chunk_size={self.chunking_config.chunk_size}, "
            f"chunk_overlap={self.chunking_config.chunk_overlap}"
        )

        return IngestionPipeline(
            transformations=[
                SimpleNodeParser(),
                self.embed_model,
            ],
            vector_store=self.vector_store,
        )

    async def ingest(self, documents: List[Document]) -> List:
        """
        Asynchronously ingest documents into vector database

        Args:
            documents: List of Document objects to ingest

        Returns:
            List of created nodes
        """
        logger.info(f"Starting ingestion of {len(documents)} documents")

        # Run pipeline asynchronously
        nodes = await self.pipeline.arun(documents=documents)

        logger.info(f"Successfully ingested {len(nodes)} nodes into vector store")
        return nodes

    def ingest_sync(self, documents: List[Document]) -> List:
        """
        Synchronously ingest documents into vector database

        Args:
            documents: List of Document objects to ingest

        Returns:
            List of created nodes
        """
        logger.info(f"Starting sync ingestion of {len(documents)} documents")

        # Run pipeline synchronously
        nodes = self.pipeline.run(documents=documents)

        logger.info(f"Successfully ingested {len(nodes)} nodes into vector store")
        return nodes

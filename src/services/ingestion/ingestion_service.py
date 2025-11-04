"""
Ingestion Service
Business logic orchestrator for document operations: add, update, delete
"""

import asyncio
from typing import Any, Dict, List, Optional

from chromadb import IDs, Where, WhereDocument
from chromadb.api.models.AsyncCollection import AsyncCollection
from llama_index.core import Document

from src.config.settings import IngestionConfig
from src.pipelines.ingestion import DataIngestionPipeline
from src.utils.logger import get_logger

logger = get_logger("services.ingestion")


class IngestionService:
    """
    Business logic orchestrator for document operations.

    Responsibilities:
    - Add new documents to vector database
    - Update existing documents (delete old + add new)
    - Delete documents from vector database
    - Batch operations
    - Error handling and retries
    """

    def __init__(self, pipeline_factory: DataIngestionPipeline):
        """
        Initialize ingestion service

        Args:
            pipeline_factory: DataIngestionPipeline instance
        """
        # Initialize the pipeline with config
        self.pipeline_factory: DataIngestionPipeline = pipeline_factory

        # Get components from pipeline
        self.embed_model = self.pipeline_factory.embed_model
        self.vector_store = self.pipeline_factory.vector_store
        self.chroma_collection: AsyncCollection = (
            self.pipeline_factory.chroma_collection
        )
        self.pipeline = self.pipeline_factory.pipeline
        self.chunking_config = self.pipeline_factory.config.chunking

        logger.info("IngestionService initialized")

    @classmethod
    def create(cls, config: Optional[IngestionConfig] = None):
        """
        Async factory method to create IngestionService

        Args:
            config: Ingestion configuration (defaults to IngestionConfig.create_default())

        Returns:
            IngestionService instance
        """
        pipeline_factory = DataIngestionPipeline.create(config)
        return cls(pipeline_factory=pipeline_factory)

    def _compute_documents_to_ingest(self, documents: List[Document]) -> List[Document]:
        """
        Compute which documents need to be ingested by filtering out existing ones.

        Args:
            documents: List of Document objects to check

        Returns:
            List of Document objects that need to be ingested
        """
        # print(documents)
        documents_to_ingest = []
        existing_documents = self.chroma_collection.get(
            ids=[document.id_ for document in documents],
            # where={"document_id": {"$in": [document.id_ for document in documents]}},
            include=["metadatas"],
        )
        existing_document_ids = existing_documents["ids"]
        if not existing_document_ids:
            logger.info("No existing documents found")
            documents_to_ingest = documents
        else:
            documents_to_ingest = [
                document
                for document in documents
                if document.id_ not in existing_document_ids
            ]

        return documents_to_ingest

    async def add_documents(
        self, documents: List[Document], max_retries: int = 3
    ) -> List:
        """
        Add new documents to vector database

        Args:
            documents: List of Document objects to add
            max_retries: Maximum number of retry attempts

        Returns:
            List of created nodes
        """
        logger.info(f"Received {len(documents)} new documents to ingest")

        for attempt in range(max_retries + 1):
            try:
                documents_to_ingest = self._compute_documents_to_ingest(documents)

                if not documents_to_ingest:
                    logger.info("No new documents to ingest")
                    return []

                logger.info(
                    f"Ingesting {len(documents_to_ingest)} new documents out of {len(documents)} total documents"
                )

                nodes = await self.pipeline.arun(
                    documents=documents_to_ingest, num_workers=None
                )
                logger.info(f"Successfully added {len(nodes)} nodes")
                return nodes
            except Exception as e:
                if attempt < max_retries:
                    wait_time = 2**attempt
                    logger.warning(
                        f"Add attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s..."
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        f"Failed to add documents after {max_retries + 1} attempts: {e}"
                    )
                    raise

    async def delete_documents(
        self,
        max_retries: int = 3,
        *,
        ids: IDs | None = None,
        where: Where | None = None,
        where_document: WhereDocument | None = None,
    ) -> bool:
        """
        Delete documents by their IDs.

        Args:
            document_ids: List of document IDs to delete
            filter_metadata: Metadata filter to identify documents to delete
            max_retries: Maximum number of retry attempts

        Returns:
            Number of deleted documents
        """

        for attempt in range(max_retries + 1):
            try:
                self.chroma_collection.delete(
                    ids=ids, where=where, where_document=where_document
                )
                logger.info(f"Successfully deleted documents")
                return True
            except Exception as e:
                if attempt < max_retries:
                    wait_time = 2**attempt
                    logger.warning(
                        f"Delete attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s..."
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        f"Failed to delete documents after {max_retries + 1} attempts: {e}"
                    )
                    raise

        return False

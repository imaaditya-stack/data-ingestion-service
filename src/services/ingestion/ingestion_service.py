"""
Ingestion Service
Business logic orchestrator for document operations: add, update, delete
"""

import asyncio
from typing import Any, Dict, List, Optional

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

    def __init__(self, config: Optional[IngestionConfig] = None):
        """
        Initialize ingestion service

        Args:
            config: Ingestion configuration (defaults to IngestionConfig.create_default())
        """
        # Initialize the pipeline with config
        self.pipeline_factory = DataIngestionPipeline(config)

        # Get components from pipeline
        self.embed_model = self.pipeline_factory.embed_model
        self.vector_store = self.pipeline_factory.vector_store
        self.chroma_collection = self.pipeline_factory.chroma_collection
        self.pipeline = self.pipeline_factory.pipeline
        self.chunking_config = self.pipeline_factory.config.chunking

        logger.info("IngestionService initialized")

    # ==================== ADD OPERATIONS ====================

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
        logger.info(f"Adding {len(documents)} new documents")

        for attempt in range(max_retries + 1):
            try:
                nodes = await self.pipeline.arun(documents=documents, num_workers=None)
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

    async def add_documents_in_batches(
        self, documents: List[Document], batch_size: int = 10
    ) -> List:
        """
        Add documents in smaller batches to reduce load on embedding service

        Args:
            documents: List of Document objects to add
            batch_size: Number of documents per batch

        Returns:
            List of all created nodes
        """
        logger.info(f"Adding {len(documents)} documents in batches of {batch_size}")

        all_nodes = []
        total_batches = (len(documents) + batch_size - 1) // batch_size

        for i in range(0, len(documents), batch_size):
            batch = documents[i : i + batch_size]
            batch_num = i // batch_size + 1

            logger.info(
                f"Processing batch {batch_num}/{total_batches} ({len(batch)} documents)"
            )

            try:
                batch_nodes = await self.add_documents(batch)
                all_nodes.extend(batch_nodes)

                # Delay between batches
                if i + batch_size < len(documents):
                    await asyncio.sleep(2.0)
            except Exception as e:
                logger.error(f"Failed to process batch {batch_num}: {e}")
                # Handle Ollama-specific errors
                if "EOF" in str(e) or "no longer running" in str(e):
                    logger.warning("Ollama crash detected. Waiting 10s before retry...")
                    await asyncio.sleep(10.0)
                    batch_nodes = await self.add_documents(batch)
                    all_nodes.extend(batch_nodes)
                else:
                    raise

        logger.info(f"Successfully added {len(all_nodes)} total nodes")
        return all_nodes

    # ==================== UPDATE OPERATIONS ====================

    async def update_documents(
        self,
        documents: List[Document],
        filter_metadata: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
    ) -> int:
        """
        Update documents in vector database.

        Process:
        1. Delete old documents by filter
        2. Add new documents

        Args:
            documents: List of new Document objects
            filter_metadata: Metadata filter to identify documents to update
                           (e.g., {"entity_id": "123", "entity_type": "company"})
            max_retries: Maximum number of retry attempts

        Returns:
            Number of updated documents
        """
        logger.info(f"Updating documents based on filter: {filter_metadata}")

        # Step 1: Delete old documents
        if filter_metadata:
            deleted_count = await self.delete_documents_by_metadata(filter_metadata)
            logger.info(f"Deleted {deleted_count} old documents")
        else:
            logger.warning("No filter provided for update operation")
            deleted_count = 0

        # Step 2: Add new documents
        try:
            nodes = await self.add_documents(documents, max_retries=max_retries)
            logger.info(f"Updated {len(nodes)} documents")
            return len(nodes)
        except Exception as e:
            logger.error(f"Failed to update documents: {e}")
            raise

    async def update_documents_in_batches(
        self,
        documents: List[Document],
        filter_metadata: Optional[Dict[str, Any]] = None,
        batch_size: int = 10,
    ) -> int:
        """
        Update documents in batches.

        Args:
            documents: List of new Document objects
            filter_metadata: Metadata filter to identify documents to update
            batch_size: Number of documents per batch

        Returns:
            Total number of updated documents
        """
        logger.info(f"Updating {len(documents)} documents in batches")

        # Delete old documents first
        if filter_metadata:
            deleted_count = await self.delete_documents_by_metadata(filter_metadata)
            logger.info(f"Deleted {deleted_count} old documents")

        # Add new documents in batches
        nodes = await self.add_documents_in_batches(documents, batch_size=batch_size)
        return len(nodes)

    # ==================== DELETE OPERATIONS ====================

    async def delete_documents(
        self, document_ids: List[str], max_retries: int = 3
    ) -> int:
        """
        Delete documents by their IDs.

        Args:
            document_ids: List of document IDs to delete
            max_retries: Maximum number of retry attempts

        Returns:
            Number of deleted documents
        """
        logger.info(f"Deleting {len(document_ids)} documents by ID")

        for attempt in range(max_retries + 1):
            try:
                # Use ChromaDB collection to delete by IDs
                self.chroma_collection.delete(ids=document_ids)
                logger.info(f"Successfully deleted {len(document_ids)} documents")
                return len(document_ids)
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

        return 0

    async def delete_documents_by_metadata(
        self, filter_metadata: Dict[str, Any], max_retries: int = 3
    ) -> int:
        """
        Delete documents matching metadata filter.

        Args:
            filter_metadata: Metadata filter (e.g., {"entity_type": "company", "entity_id": "123"})
            max_retries: Maximum number of retry attempts

        Returns:
            Number of deleted documents
        """
        logger.info(f"Deleting documents by metadata filter: {filter_metadata}")

        for attempt in range(max_retries + 1):
            try:
                # Build where clause for ChromaDB
                where = filter_metadata

                # Query to get matching document IDs
                results = self.chroma_collection.get(where=where, include=["metadatas"])

                if results and results["ids"]:
                    ids_to_delete = results["ids"]
                    logger.info(f"Found {len(ids_to_delete)} documents to delete")

                    # Delete by IDs
                    self.chroma_collection.delete(ids=ids_to_delete)
                    logger.info(f"Successfully deleted {len(ids_to_delete)} documents")
                    return len(ids_to_delete)
                else:
                    logger.info("No matching documents found for deletion")
                    return 0
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

        return 0

    async def delete_documents_by_metadata_in_batches(
        self, filter_metadata: Dict[str, Any], batch_size: int = 100
    ) -> int:
        """
        Delete documents by metadata in batches (for large datasets).

        Args:
            filter_metadata: Metadata filter
            batch_size: Number of documents to delete per batch

        Returns:
            Total number of deleted documents
        """
        logger.info(f"Deleting documents by metadata in batches: {filter_metadata}")

        total_deleted = 0

        while True:
            try:
                # Get batch of matching IDs
                results = self.chroma_collection.get(
                    where=filter_metadata, limit=batch_size, include=["metadatas"]
                )

                if results and results["ids"]:
                    ids_to_delete = results["ids"]

                    # Delete batch
                    self.chroma_collection.delete(ids=ids_to_delete)
                    total_deleted += len(ids_to_delete)

                    logger.info(
                        f"Deleted batch: {len(ids_to_delete)} documents (total: {total_deleted})"
                    )

                    # If we got fewer than batch_size, we're done
                    if len(ids_to_delete) < batch_size:
                        break

                    # Small delay between batches
                    await asyncio.sleep(0.5)
                else:
                    break
            except Exception as e:
                logger.error(f"Error during batch deletion: {e}")
                raise

        logger.info(f"Successfully deleted {total_deleted} total documents")
        return total_deleted

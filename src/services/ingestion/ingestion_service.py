"""
Ingestion Service
Business logic orchestrator for document operations: add, update, delete
"""

import asyncio
from typing import Dict, List, Optional

from chromadb import IDs, Where, WhereDocument
from chromadb.api.models.AsyncCollection import AsyncCollection
from llama_index.core import Document

from src.config.settings import IngestionConfig
from src.pipelines.ingestion import DataIngestionPipeline
from src.services.vector_store_manager import get_tenant_vector_store_manager_sync
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

    _DEFAULT_PIPELINE_KEY = "__default__"

    def __init__(self, config: Optional[IngestionConfig] = None):
        """
        Initialize ingestion service

        Args:
            pipeline_factory: DataIngestionPipeline instance
        """
        self.config = config or IngestionConfig.create_default()
        self._default_pipeline = DataIngestionPipeline.create(self.config)
        self.embed_model = self._default_pipeline.embed_model
        self.chunking_config = self.config.chunking
        self._tenant_manager = get_tenant_vector_store_manager_sync()
        self._pipelines: Dict[str, DataIngestionPipeline] = {
            self._DEFAULT_PIPELINE_KEY: self._default_pipeline
        }
        self._pipeline_locks: Dict[str, asyncio.Lock] = {}

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
        return cls(config=config)

    def _compute_documents_to_ingest(
        self, collection: AsyncCollection, documents: List[Document]
    ) -> List[Document]:
        """
        Compute which documents need to be ingested by filtering out existing ones.

        Args:
            documents: List of Document objects to check

        Returns:
            List of Document objects that need to be ingested
        """
        # print(documents)
        documents_to_ingest = []
        existing_documents = collection.get(
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
        self,
        documents: List[Document],
        tenant_id: Optional[str] = None,
        max_retries: int = 3,
    ) -> List:
        """
        Add new documents to vector database

        Args:
            documents: List of Document objects to add
            max_retries: Maximum number of retry attempts

        Returns:
            List of created nodes
        """
        resolved_tenant = self._resolve_tenant_id(tenant_id, documents)
        pipeline = await self._get_pipeline(resolved_tenant)
        collection = pipeline.chroma_collection

        logger.info(f"Received {len(documents)} new documents to ingest")

        for attempt in range(max_retries + 1):
            try:
                documents_to_ingest = self._compute_documents_to_ingest(
                    collection, documents
                )

                if not documents_to_ingest:
                    logger.info("No new documents to ingest")
                    return []

                logger.info(
                    f"Ingesting {len(documents_to_ingest)} new documents out of {len(documents)} total documents"
                )

                nodes = await pipeline.pipeline.arun(
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
        tenant_id: Optional[str] = None,
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
        resolved_tenant = self._resolve_tenant_id(tenant_id, [])
        pipeline = await self._get_pipeline(resolved_tenant)
        collection = pipeline.chroma_collection

        for attempt in range(max_retries + 1):
            try:
                collection.delete(ids=ids, where=where, where_document=where_document)
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

    async def _get_pipeline(self, tenant_id: Optional[str]) -> DataIngestionPipeline:
        if not tenant_id:
            return self._pipelines[self._DEFAULT_PIPELINE_KEY]

        if tenant_id in self._pipelines:
            logger.info(f"=======> Cache hit for pipeline for tenant_id={tenant_id}")
            return self._pipelines[tenant_id]

        lock = self._pipeline_locks.setdefault(tenant_id, asyncio.Lock())
        async with lock:
            if tenant_id in self._pipelines:
                logger.info(
                    f"=======> Cache hit for pipeline for tenant_id={tenant_id} after locking"
                )
                return self._pipelines[tenant_id]

            logger.info(
                f"=======> Fetching tenant resources for tenant_id={tenant_id} after locking"
            )
            resources = await self._tenant_manager.get_resources(tenant_id)
            pipeline = DataIngestionPipeline(
                embed_model=self.embed_model,
                vector_store=resources.vector_store,
                collection=resources.collection,
                config=self.config,
            )
            logger.info(f"=======> Pipeline created for tenant_id={tenant_id}")
            self._pipelines[tenant_id] = pipeline
            return pipeline

    def _resolve_tenant_id(
        self, tenant_id: Optional[str], documents: List[Document]
    ) -> Optional[str]:
        if tenant_id:
            return tenant_id

        tenant_ids = {
            str(doc.metadata.get("tenant_id"))
            for doc in documents
            if doc.metadata.get("tenant_id") is not None
        }

        if not tenant_ids:
            return None

        if len(tenant_ids) > 1:
            raise ValueError(
                "Documents contain multiple tenant IDs. "
                "Provide tenant_id explicitly when calling add_documents."
            )

        return tenant_ids.pop()

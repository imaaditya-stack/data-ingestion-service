"""
Event Processor
Orchestrates event processing and ingestion
"""

import asyncio
from typing import Any, Dict, List, Optional

from llama_index.core import Document

from src.services.ingestion.ingestion_service import IngestionService
from src.services.kafka.event_parser import EventParser
from src.services.kafka.models import OperationType
from src.utils.logger import get_logger

logger = get_logger("services.kafka.event_processor")


class EventProcessor:
    """Orchestrates event processing and ingestion"""

    def __init__(
        self,
        ingestion_service: IngestionService,
        event_parser: EventParser,
    ):
        """
        Initialize event processor

        Args:
            ingestion_service: IngestionService for document operations
            event_parser: EventParser for Kafka message parsing
        """
        self.ingestion_service = ingestion_service
        self.event_parser = event_parser

    async def batch_events_processor(
        self, messages: List[tuple[str, Dict[str, Any], Optional[str]]]
    ) -> None:
        """
        Process multiple messages in a batch with operation-based grouping

        Args:
            messages: List of (topic, message_payload, key) tuples
        """
        if not messages:
            return

        # Parse all documents
        documents: List[Document] = []
        for topic, message, _key in messages:
            try:
                document = self.event_parser.parse(topic, message)
                documents.append(document)
            except Exception as e:
                logger.error(
                    f"Error building document for topic={topic}, "
                    f"tenant={message.get('tenant_id')}: {e}",
                    exc_info=True,
                )
                # Continue processing other messages in batch

        if not documents:
            logger.warning("No valid documents to process in batch")
            return

        # Group documents by operation
        create_docs = [
            document
            for document in documents
            if document.metadata.get("operation") == OperationType.CREATE.value
        ]
        update_docs = [
            document
            for document in documents
            if document.metadata.get("operation") == OperationType.UPDATE.value
        ]
        delete_docs = [
            document
            for document in documents
            if document.metadata.get("operation") == OperationType.DELETE.value
        ]

        logger.info(
            "\n----------------- Batch Processing ----------------"
            "\nTotal Documents: %s"
            "\nCREATE: %s"
            "\nUPDATE: %s"
            "\nDELETE: %s"
            "\n---------------------------------------------------",
            len(documents),
            len(create_docs),
            len(update_docs),
            len(delete_docs),
        )

        # Execute operations
        try:

            # CREATE: Batch add all documents
            if create_docs:
                await self.ingestion_service.add_documents(create_docs)

            # DELETE: Batch delete by document IDs
            if delete_docs:
                await self.ingestion_service.delete_documents(
                    ids=[document.id_ for document in delete_docs]
                )

            # UPDATE: Sequential delete + add (must be sequential to avoid race conditions)
            if update_docs:
                # Delete old versions by ID
                try:
                    await self.ingestion_service.delete_documents(
                        ids=[document.id_ for document in update_docs]
                    )
                    await asyncio.sleep(
                        0.3
                    )  # Added a delay to let operation complete properly before we add
                    # Add new documents
                    await self.ingestion_service.add_documents(update_docs)
                except Exception as e:
                    logger.error(f"UPDATE operation failed: {e}", exc_info=True)
                    raise

            logger.info(
                "\n----------------- Batch Processed Successfully ----------------"
                "\nTotal Documents: %s"
                "\n---------------------------------------------------------------",
                len(documents),
            )
        except Exception as e:
            logger.error(f"Error ingesting batch: {e}", exc_info=True)
            raise

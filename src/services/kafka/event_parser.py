"""
Event Parser
Parses Kafka events and converts them to LlamaIndex Documents
"""

from typing import Any, Dict, Optional

from llama_index.core import Document

from src.services.kafka.entity_router import EntityRouter
from src.services.kafka.models import KafkaEvent, OperationType
from src.utils.logger import get_logger

logger = get_logger("services.kafka.event_parser")


class EventParser:
    """Parses Kafka events and converts them to LlamaIndex Documents"""

    def __init__(self, router: EntityRouter, dry_run_mode: bool = False):
        """
        Initialize event parser

        Args:
            router: EntityRouter for topic-to-entity mapping
            dry_run_mode: If True, adds operation prefix to text for dry run testing
        """
        self.router = router
        self.dry_run_mode = dry_run_mode
        if self.dry_run_mode:
            logger.info("Event Parser running in dry run mode")
        else:
            logger.info("Event Parser running in production mode")

    def parse(self, topic: str, message: Dict[str, Any]) -> Document:
        """
        Parse Kafka message and build Document

        Args:
            topic: Kafka topic name
            message: Raw message payload

        Returns:
            Document object ready for ingestion

        Raises:
            ValidationError: If event or data payload validation fails
            ValueError: If topic routing fails
        """
        # Validate and parse Kafka event structure
        event = KafkaEvent(**message)
        route = self.router.route(topic)

        # Validate payload against entity schema
        validated = route.data_model_cls(**event.data)
        data = validated.model_dump()

        # Build text and metadata using processors
        text = route.text_processor.process_row(data, list(data.keys()))
        custom_metadata = route.metadata_processor.process_metadata(
            data, list(data.keys())
        )

        # Add operation prefix for UPDATE in dry run mode for verification
        if (
            self.dry_run_mode
            and event.metadata.operation.value == OperationType.UPDATE.value
        ):
            op_prefix = f"[{event.metadata.operation.value}] "
            text = op_prefix + text

        # Merge processor metadata with Kafka event metadata
        doc_metadata: Dict[str, Any] = {
            **custom_metadata,
            "tenant_id": event.tenant_id,
            "operation": event.metadata.operation.value,
            "event_id": event.event_id,
            "source": event.metadata.source,
            "schema_version": event.schema_version,
        }

        # Add entity_id if available
        if event.entity_id is not None:
            doc_metadata["entity_id"] = event.entity_id

        # Generate deterministic document ID
        doc_id = event.build_document_id(entity_type=route.entity_type)
        return Document(text=text, metadata=doc_metadata, id_=doc_id)

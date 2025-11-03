"""
Entity Router
Routes Kafka topics to entity-specific processors and data models
"""

from typing import Type

from pydantic import BaseModel

from src.core.protocols import MetadataProcessor, TextProcessor
from src.data_processors.company_processors import (
    CompanyDataProcessor,
    CompanyMetadataProcessor,
)
from src.data_processors.product_processors import (
    ProductDataProcessor,
    ProductMetadataProcessor,
)
from src.services.kafka.models import CompanyData, KafkaTopic, ProductData
from src.utils.logger import get_logger

logger = get_logger("services.kafka.entity_router")


class EntityRoute:
    """Represents a route configuration for an entity type"""

    def __init__(
        self,
        entity_type: str,
        text_processor: TextProcessor,
        metadata_processor: MetadataProcessor,
        data_model_cls: Type[BaseModel],
    ):
        self.entity_type = entity_type
        self.text_processor = text_processor
        self.metadata_processor = metadata_processor
        self.data_model_cls = data_model_cls


class EntityRouter:
    """Routes Kafka topics to entity-specific processors"""

    def route(self, topic: str) -> EntityRoute:
        """
        Route a topic to its corresponding entity configuration

        Args:
            topic: Kafka topic name (e.g., "product-events", "company-events")

        Returns:
            EntityRoute with processors and data model

        Raises:
            ValueError: If topic doesn't match any known entity type
        """
        topic_lower = topic.lower()

        if KafkaTopic.COMPANY_EVENTS.value in topic_lower:
            return EntityRoute(
                entity_type="company",
                text_processor=CompanyDataProcessor(),
                metadata_processor=CompanyMetadataProcessor(),
                data_model_cls=CompanyData,
            )

        if KafkaTopic.PRODUCT_EVENTS.value in topic_lower:
            return EntityRoute(
                entity_type="product",
                text_processor=ProductDataProcessor(),
                metadata_processor=ProductMetadataProcessor(),
                data_model_cls=ProductData,
            )

        logger.warning(
            f"Unknown topic for event routing: topic={topic}. Skipping document creation."
        )
        raise ValueError(f"Unknown topic: {topic}")

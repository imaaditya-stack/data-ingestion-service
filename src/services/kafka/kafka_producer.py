"""
Kafka Producer Service (Async)
Handles publishing events to Kafka topics using aiokafka
"""

import json
from typing import Any, Dict, Optional

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from src.config.settings import KafkaConfig
from src.utils.logger import get_logger

logger = get_logger("services.kafka_producer")


class KafkaProducerService:
    """Service for publishing events to Kafka"""

    def __init__(self, config: KafkaConfig):
        """
        Initialize Kafka producer

        Args:
            config: Kafka configuration
        """
        self.config = config
        self.producer: Optional[AIOKafkaProducer] = None

    async def start(self):
        """Start the producer"""
        logger.info(
            f"Creating Kafka producer for bootstrap_servers={self.config.bootstrap_servers}"
        )

        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.config.bootstrap_servers,
            client_id=self.config.client_id,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks=self.config.acks,
            enable_idempotence=self.config.enable_idempotence,
            retry_backoff_ms=100,
            request_timeout_ms=40000,  # Default timeout for requests
        )

        await self.producer.start()
        logger.info("Kafka producer started successfully")

    async def publish_event(
        self,
        topic: str,
        event_data: Dict[str, Any],
        key: Optional[str] = None,
    ) -> bool:
        """
        Publish event to Kafka topic

        Args:
            topic: Kafka topic name
            event_data: Event payload
            key: Optional key for partitioning (tenant_id:entity_id format)

        Returns:
            True if successful, False otherwise
        """
        if not self.producer:
            logger.error("Producer not started. Call start() first.")
            return False

        try:
            future = await self.producer.send(topic, value=event_data, key=key)
            record_metadata = await future

            logger.info(
                f"Event published successfully to topic={topic}, "
                f"partition={record_metadata.partition}, "
                f"offset={record_metadata.offset}"
            )
            return True

        except KafkaError as e:
            logger.error(f"Failed to publish event to topic={topic}: {e}")
            return False

    async def flush(self):
        """Flush all pending messages"""
        if self.producer:
            await self.producer.flush()

    async def close(self):
        """Close the producer"""
        logger.info("Closing Kafka producer")
        if self.producer:
            await self.producer.stop()

    async def __aenter__(self):
        """Async context manager entry"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()

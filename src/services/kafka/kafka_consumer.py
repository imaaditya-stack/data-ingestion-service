"""
Kafka Consumer Service (Async)
Handles consuming events from Kafka topics using aiokafka
"""

import asyncio
from typing import Any, Awaitable, Callable, Dict, List

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from src.config.settings import KafkaConfig
from src.utils.logger import get_logger

logger = get_logger("services.kafka_consumer")


class KafkaConsumerService:
    """Service for consuming events from Kafka"""

    def __init__(
        self,
        config: KafkaConfig,
        topics: List[str],
        message_handler: Callable[[Dict[str, Any]], Awaitable[None]],
    ):
        """
        Initialize Kafka consumer

        Args:
            config: Kafka configuration
            topics: List of topics to subscribe to
            message_handler: Async function to handle consumed messages
        """
        self.config = config
        self.topics = topics
        self.message_handler = message_handler
        self.consumer: AIOKafkaConsumer = None
        self.running = False

    async def start(self, max_retries: int = 5, retry_delay: float = 5.0):
        """
        Start consuming messages with retry logic for Kafka connection

        Args:
            max_retries: Maximum number of connection retry attempts
            retry_delay: Initial delay between retries in seconds
        """
        logger.info(
            f"Creating Kafka consumer for bootstrap_servers={self.config.bootstrap_servers}"
        )

        self.consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=self.config.bootstrap_servers,
            group_id=self.config.consumer_group_id,
            value_deserializer=lambda m: m.decode("utf-8"),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            enable_auto_commit=False,  # Manual commit for reliability
            max_poll_interval_ms=self.config.max_poll_interval_ms,
        )

        # Retry connecting to Kafka
        for attempt in range(max_retries):
            try:
                await self.consumer.start()
                logger.info(
                    f"Kafka consumer started successfully for topics={self.topics}"
                )
                self.running = True
                # Consume messages in a loop
                await self._consume_loop()
                break
            except (KafkaError, ConnectionError, Exception) as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2**attempt)  # Exponential backoff
                    logger.warning(
                        f"Failed to connect to Kafka (attempt {attempt + 1}/{max_retries}): {e}. "
                        f"Retrying in {wait_time}s..."
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        f"Failed to connect to Kafka after {max_retries} attempts: {e}"
                    )
                    raise

    async def _consume_loop(self):
        """Main consumption loop"""
        logger.info("Starting Kafka consumer loop")

        try:
            async for message in self.consumer:
                if not self.running:
                    break

                try:
                    import json

                    # Deserialize JSON value
                    message_value = json.loads(message.value)

                    logger.info(
                        f"Received message from topic={message.topic}, partition={message.partition}, offset={message.offset}"
                    )

                    # Call the message handler
                    await self.message_handler(message_value)

                    # Commit offset after successful processing
                    await self.consumer.commit()
                    logger.info(
                        f"Committed offset for topic={message.topic}, partition={message.partition}"
                    )

                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    # In production, you'd send to DLQ here

        except asyncio.CancelledError:
            logger.info("Consumer loop cancelled")
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}")
            raise

    async def stop(self):
        """Stop consuming messages"""
        logger.info("Stopping Kafka consumer")
        self.running = False
        if self.consumer:
            await self.consumer.stop()

    async def __aenter__(self):
        """Async context manager entry"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.stop()

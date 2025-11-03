"""
Kafka Consumer Service (Async)
Handles consuming events from Kafka topics using aiokafka
"""

import asyncio
from typing import Any, Dict, List, Optional

from aiokafka import AIOKafkaConsumer, TopicPartition
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
        message_handler: Any,
    ):
        """
        Initialize Kafka consumer

        Args:
            config: Kafka configuration
            topics: List of topics to subscribe to
            message_handler: Either:
                - EventProcessor instance with batch_events_processor() method (for batching), or
                - Callable with signature (topic: str, message: Dict, key: Optional[str]) -> Awaitable[None]
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
            "Creating Kafka consumer for bootstrap_servers=%s",
            self.config.bootstrap_servers,
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
                    "Kafka consumer started successfully for topics=%s", self.topics
                )
                self.running = True
                # Consume messages in a loop
                await self._consume_loop()
                break
            except (KafkaError, ConnectionError, Exception) as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2**attempt)  # Exponential backoff
                    logger.warning(
                        "Failed to connect to Kafka (attempt %d/%d): %s. Retrying in %.1fs...",
                        attempt + 1,
                        max_retries,
                        e,
                        wait_time,
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        "Failed to connect to Kafka after %d attempts: %s",
                        max_retries,
                        e,
                    )
                    raise

    async def _consume_loop(
        self,
        timeout_ms: int = 10000,
        max_records: int = 100,
        batch_size: int = 100,
        flush_interval: float = 10.0,
    ):
        """
        Main consumption loop with message batching for efficient embedding API calls.

        The loop accumulates messages in a buffer and flushes when either:
        - batch_size messages are collected (size-based flush), OR
        - flush_interval seconds have passed since last flush (time-based flush)

        This ensures we batch efficiently for bulk processing while avoiding
        indefinite delays when message arrival is slow.

        Args:
            timeout_ms: Timeout for getmany() calls (how long to wait for messages)
            max_records: Max records per getmany() call (per fetch limit)
            batch_size: Number of messages to accumulate before processing (size-based flush)
            flush_interval: Seconds to wait before flushing batch (time-based flush)
        """
        logger.info(
            "Starting Kafka consumer loop with batching (batch_size=%d, flush_interval=%.1fs)",
            batch_size,
            flush_interval,
        )

        import json

        # Accumulate messages across multiple getmany() calls
        batch_buffer: List[
            tuple[str, Dict[str, Any], Optional[str], TopicPartition, int]
        ] = []
        last_flush_time = asyncio.get_event_loop().time()

        try:
            while self.running:
                # Fetch messages from Kafka
                results = await self.consumer.getmany(
                    timeout_ms=timeout_ms, max_records=max_records
                )

                # Add messages to batch buffer
                for tp, messages in results.items():
                    for msg in messages:
                        try:
                            message_value = json.loads(msg.value)
                            batch_buffer.append(
                                (msg.topic, message_value, msg.key, tp, msg.offset)
                            )
                        except Exception as e:
                            logger.error(
                                "Failed to decode message from topic=%s, partition=%d, offset=%d: %s",
                                msg.topic,
                                tp.partition,
                                msg.offset,
                                e,
                            )

                # Check if we should flush the batch
                now = asyncio.get_event_loop().time()
                should_flush = batch_buffer and (
                    len(batch_buffer) >= batch_size
                    or (now - last_flush_time) >= flush_interval
                )

                if should_flush:
                    batch_count = len(batch_buffer)
                    if batch_count >= batch_size:
                        flush_reason = "batch_size"
                    else:
                        time_elapsed = now - last_flush_time
                        flush_reason = "flush_interval (%.1fs)" % time_elapsed
                    # logger.info(
                    #     "Flushing batch: %d messages (reason: %s)",
                    #     batch_count,
                    #     flush_reason,
                    # )
                    logger.info(
                        "\n----------------- New Events Captured ----------------"
                        "\nTotal Events: %s"
                        "\nFlush Reason: %s"
                        "\n------------------------------------------------------",
                        batch_count,
                        flush_reason,
                    )

                    await self._process_batch(batch_buffer)
                    batch_buffer.clear()
                    last_flush_time = now

        except asyncio.CancelledError:
            logger.info("Consumer loop cancelled")
            # Process any remaining messages before shutdown
            if batch_buffer:
                logger.info(
                    "Processing %d remaining messages before shutdown",
                    len(batch_buffer),
                )
                await self._process_batch(batch_buffer)
        except Exception as e:
            logger.error("Error in consumer loop: %s", e, exc_info=True)
            raise

    async def _process_batch(
        self,
        batch: List[tuple[str, Dict[str, Any], Optional[str], TopicPartition, int]],
    ):
        """
        Process a batch of messages and commit offsets per partition.

        Args:
            batch: List of (topic, message, key, topic_partition, offset) tuples
        """
        if not batch:
            return

        # Extract messages for handler (topic, message, key)
        messages_for_handler = [(topic, msg, key) for topic, msg, key, _, _ in batch]

        try:
            # Try to use batch handler if available
            if hasattr(self.message_handler, "batch_events_processor"):
                await self.message_handler.batch_events_processor(messages_for_handler)
            else:
                raise ValueError(
                    "Message handler does not have a batch_events_processor method"
                )

            # Build commit offsets per partition (max offset + 1 for each partition)
            max_offsets: Dict[TopicPartition, int] = {}
            for topic, _, _, tp, offset in batch:
                if tp not in max_offsets or offset > max_offsets[tp]:
                    max_offsets[tp] = offset

            # Commit all successful partitions (commit offset = max_offset + 1)
            # Build commit dict explicitly to ensure TopicPartition objects are keys
            commit_dict: Dict[TopicPartition, int] = {}
            for tp, max_offset in max_offsets.items():
                commit_dict[tp] = max_offset + 1

            await self.consumer.commit(commit_dict)
            logger.info(
                "Committed offsets: %s",
                ", ".join(
                    "%s:%d->%d" % (tp.topic, tp.partition, offset)
                    for tp, offset in commit_dict.items()
                ),
            )

        except Exception as e:
            logger.error("Error processing batch: %s", e, exc_info=True)
            # Don't commit - messages will be retried on next fetch

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

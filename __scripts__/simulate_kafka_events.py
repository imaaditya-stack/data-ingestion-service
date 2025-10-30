"""
Simple test producer to send sample events to Kafka (async version)
This simulates what will come from Django later
"""

import asyncio
import time
from typing import Any, Dict

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust depth if needed
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.settings import KafkaConfig
from src.services.kafka.kafka_producer import KafkaProducerService
from src.utils.logger import get_logger

logger = get_logger("kafka_test_producer")


def create_sample_product_event(
    product_id: int, tenant_id: str = "tenant_abc"
) -> Dict[str, Any]:
    """Create a sample product event"""
    return {
        "event_name": "product.updated.v1",
        "event_id": f"evt-{int(time.time())}-{product_id}",
        "tenant_id": tenant_id,
        "timestamp": "2025-01-27T12:34:56Z",
        "source": "test-producer",
        "data": {
            "id": product_id,
            "name": f"Product {product_id}",
            "description": f"Description for product {product_id}",
            "price": 99.99,
        },
        "metadata": {
            "operation": "UPDATE",
            "entity_type": "product",
            "initiated_by": "test_user",
        },
        "schema_version": 1,
    }


async def main():
    """Run the test producer"""
    logger.info("Starting test producer")

    config = KafkaConfig()
    topic = "product-events"

    async with KafkaProducerService(config) as producer:
        # Send a few sample events
        for i in range(1, 6):
            event_data = create_sample_product_event(i)
            key = f"tenant_abc:{i}"  # tenant_id:entity_id format

            logger.info(f"Sending event {i}")
            success = await producer.publish_event(topic, event_data, key)

            if success:
                logger.info(f"✓ Event {i} sent successfully")
            else:
                logger.error(f"✗ Failed to send event {i}")

            await asyncio.sleep(0.5)  # Small delay between sends

        # Flush before closing
        await producer.flush()
        logger.info("All events sent. Closing producer.")


if __name__ == "__main__":
    asyncio.run(main())

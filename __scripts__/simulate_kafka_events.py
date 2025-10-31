import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust depth if needed
sys.path.insert(0, str(PROJECT_ROOT))

from datetime import datetime, timezone

from src.config.settings import KafkaConfig
from src.data_loaders.csv_loader import CSVLoader
from src.services.kafka.kafka_producer import KafkaProducerService
from src.services.kafka.models import CompanyData, ProductData
from src.utils.logger import get_logger

logger = get_logger("kafka_test_producer")


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_event_from_row(
    entity: str, row: Dict[str, Any], tenant_id: str, operation: str = "UPDATE"
) -> Dict[str, Any]:
    """Build Kafka event using strict entity data models; no CSV normalization."""
    if entity == "product":
        data_model = ProductData(**row)
    elif entity == "company":
        data_model = CompanyData(**row)
    else:
        raise ValueError(f"Unsupported entity: {entity}")

    entity_id = data_model.id

    event = {
        "event_id": f"evt-{int(time.time()*1000)}-{entity}-{entity_id}",
        "tenant_id": tenant_id,
        "timestamp": now_iso(),
        "data": data_model.model_dump(),
        "metadata": {
            "operation": operation,
            "source": "test-producer",
        },
        "schema_version": 1,
    }
    return event


async def main():
    """Run the test producer from CSV data with sampling."""
    parser = argparse.ArgumentParser(description="Simulate Kafka events from CSV")
    parser.add_argument(
        "--entity",
        choices=["product", "company"],
        required=True,
        help="Entity type to simulate",
    )
    parser.add_argument(
        "--csv",
        required=True,
        help="Path to CSV file containing records",
    )
    parser.add_argument(
        "--tenant-id", default="tenant_abc", help="Tenant id to include in events"
    )
    parser.add_argument(
        "--count", type=int, default=10, help="Number of records to send"
    )
    parser.add_argument(
        "--operation",
        default="UPDATE",
        choices=["CREATE", "UPDATE", "DELETE"],
        help="Operation type",
    )
    args = parser.parse_args()

    logger.info(
        f"Starting test producer: entity={args.entity}, csv={args.csv}, count={args.count}"
    )

    loader = CSVLoader()
    records = loader.get_records_from_csv(args.csv)
    if not records:
        logger.error("No records loaded from CSV")
        return

    # Sample records (deterministically slice or random via simple step)
    import random

    if len(records) > args.count:
        records = random.sample(records, args.count)
    else:
        records = records[: args.count]

    topic = "product-events" if args.entity == "product" else "company-events"

    config = KafkaConfig()
    async with KafkaProducerService(config) as producer:
        for idx, row in enumerate(records, start=1):
            try:
                event_data = build_event_from_row(
                    entity=args.entity,
                    row=row,
                    tenant_id=args.tenant_id,
                    operation=args.operation,
                )
                key = f"{args.tenant_id}:{event_data['data'].get('id')}"
                logger.info(f"Sending {args.entity} event {idx}/{len(records)}")
                with open(f"__data__/{args.entity}_event_{idx}.json", "w") as f:
                    json.dump(event_data, f)
                success = await producer.publish_event(topic, event_data, key)
                if success:
                    logger.info("✓ Event sent successfully")
                else:
                    logger.error("✗ Failed to send event")
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Error building/sending event: {e}", exc_info=True)

        await producer.flush()
        logger.info("All events sent. Closing producer.")


if __name__ == "__main__":
    asyncio.run(main())


# Run examples:
# Products:
# python __scripts__/simulate_kafka_events.py --entity product --csv __data__/__products_data.csv --count 10 --tenant-id tenant_abc --operation CREATE
# Companies:
# python __scripts__/simulate_kafka_events.py --entity company --csv __data__/__companies_data.csv --count 10 --tenant-id tenant_abc --operation CREATE
# python __scripts__/simulate_kafka_events.py --entity company --csv __data__/__companies_cleaned.csv --count 10 --tenant-id tenant_abc --operation CREATE

"""
Realistic Kafka Event Simulator
Configuration-based simulator that mimics real-world event distributions and patterns

Example usage:
    # Send events to Kafka
    python __scripts__/kafka_events_simulator.py --scenario normal_traffic

    # Dry run: Log events to JSON file without sending
    python __scripts__/kafka_events_simulator.py --scenario normal_traffic --dry-run

The normal_traffic scenario sends 100 events with 60% CREATE, 30% UPDATE, and 10% DELETE operations.
"""

import argparse
import asyncio
import json
import random
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from datetime import datetime, timezone

from src.config.settings import KafkaConfig
from src.data_loaders.csv_loader import CSVLoader
from src.services.kafka.kafka_producer import KafkaProducerService
from src.services.kafka.models import (
    CompanyData,
    KafkaTopic,
    OperationType,
    ProductData,
)
from src.utils.logger import get_logger

logger = get_logger("realistic_kafka_simulator")


@dataclass
class DistributionStrategy:
    """Configuration for operation distribution"""

    create_pct: float = 0.5  # 50% CREATE
    update_pct: float = 0.3  # 30% UPDATE
    delete_pct: float = 0.2  # 20% DELETE

    def __post_init__(self):
        total = self.create_pct + self.update_pct + self.delete_pct
        if abs(total - 1.0) > 0.01:
            raise ValueError(f"Distribution percentages must sum to 1.0, got {total}")

    def get_operation(self) -> OperationType:
        """Get random operation based on distribution"""
        rand = random.random()
        if rand < self.create_pct:
            return OperationType.CREATE
        elif rand < self.create_pct + self.update_pct:
            return OperationType.UPDATE
        else:
            return OperationType.DELETE


@dataclass
class LoadProfile:
    """Configuration for event load and timing"""

    total_events: int = 100
    batch_size: int = 10  # Events per batch
    inter_batch_delay: float = 1.0  # Seconds between batches
    inter_event_delay: float = 0.1  # Seconds between events within batch
    burst_probability: float = 0.1  # 10% chance of burst (rapid events)


@dataclass
class ScenarioConfig:
    """Complete scenario configuration"""

    name: str
    entities: List[Dict[str, str]] = field(
        default_factory=list
    )  # [{"type": "company", "csv": "...", "topic": "..."}]
    distribution: DistributionStrategy = field(default_factory=DistributionStrategy)
    load_profile: LoadProfile = field(default_factory=LoadProfile)
    tenants: List[str] = field(default_factory=lambda: ["tenant_abc"])
    dry_run: bool = False


def now_iso() -> str:
    """Get current ISO timestamp"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_event_from_row(
    entity: str,
    row: Dict[str, Any],
    tenant_id: str,
    operation: OperationType,
    source: str = "realistic-simulator",
) -> Dict[str, Any]:
    """Build Kafka event using strict entity data models"""
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
            "operation": operation.value,
            "source": source,
        },
        "schema_version": 1,
    }
    return event


class RealisticEventSimulator:
    """Simulates realistic Kafka events based on configuration"""

    def __init__(self, config: ScenarioConfig):
        self.config = config
        self.producer: Optional[KafkaProducerService] = None
        self.events_log: List[Dict[str, Any]] = []  # For dry-run mode

    async def __aenter__(self):
        """Async context manager entry"""
        if not self.config.dry_run:
            kafka_config = KafkaConfig()
            self.producer = KafkaProducerService(kafka_config)
            await self.producer.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.producer:
            await self.producer.flush()
            await self.producer.__aexit__(exc_type, exc_val, exc_tb)

    async def send_event(
        self, topic: str, event_data: Dict[str, Any], key: str
    ) -> bool:
        """Send a single event or log it in dry-run mode"""
        if self.config.dry_run:
            self.events_log.append(
                {
                    "topic": topic,
                    "key": key,
                    "event": event_data,
                }
            )
            return True

        if self.producer is None:
            raise RuntimeError("Producer not initialized")
        return await self.producer.publish_event(topic, event_data, key)

    async def simulate_entity(self, entity_type: str, csv_path: str, topic: str) -> int:
        """Simulate events for a single entity type"""
        logger.info(f"Loading data for {entity_type} from {csv_path}")

        # Load records
        loader = CSVLoader()
        records = loader.get_records_from_csv(csv_path)
        if not records:
            logger.error(f"No records loaded from {csv_path}")
            return 0

        # Sample records to match load profile
        if len(records) > self.config.load_profile.total_events:
            # records = random.sample(records, self.config.load_profile.total_events)
            records = records[: self.config.load_profile.total_events]
        else:
            records = records[: self.config.load_profile.total_events]

        logger.info(f"Processing {len(records)} events for {entity_type}")

        # Simulate with bursts and delays
        sent_count = 0
        for i, row in enumerate(records, start=1):
            # Determine operation based on distribution
            operation = self.config.distribution.get_operation()

            # Select random tenant
            tenant_id = random.choice(self.config.tenants)

            # Build event
            try:
                event_data = build_event_from_row(
                    entity=entity_type,
                    row=row,
                    tenant_id=tenant_id,
                    operation=operation,
                )
                key = f"{tenant_id}:{event_data['data'].get('id')}"

                # Send event
                success = await self.send_event(topic, event_data, key)
                if success:
                    sent_count += 1
                    if i % 10 == 0:
                        logger.debug(f"Sent {i}/{len(records)} events")
                else:
                    logger.error(f"Failed to send event {i}")

                # Determine delay
                if random.random() < self.config.load_profile.burst_probability:
                    # Burst mode: minimal delay
                    await asyncio.sleep(0.01)
                else:
                    # Normal delay
                    await asyncio.sleep(self.config.load_profile.inter_event_delay)

            except Exception as e:
                logger.error(f"Error processing event {i}: {e}", exc_info=True)

        logger.info(
            f"✓ Completed {entity_type}: {sent_count}/{len(records)} events sent"
        )
        return sent_count

    def save_events_to_file(self, output_file: str) -> None:
        """Save logged events to a JSON file"""
        output_path = Path(PROJECT_ROOT) / output_file
        with open(output_path, "w") as f:
            json.dump(self.events_log, f, indent=2)
        logger.info(f"💾 Saved {len(self.events_log)} events to {output_path}")

    async def simulate(self) -> Dict[str, int]:
        """Run the complete simulation"""
        logger.info(f"🚀 Starting scenario: {self.config.name}")
        logger.info(
            f"Distribution: CREATE={self.config.distribution.create_pct*100:.0f}%, "
            f"UPDATE={self.config.distribution.update_pct*100:.0f}%, "
            f"DELETE={self.config.distribution.delete_pct*100:.0f}%"
        )

        results = {}
        for entity_config in self.config.entities:
            entity_type = entity_config["type"]
            csv_path = entity_config["csv"]
            topic = entity_config.get("topic", f"{entity_type}-events")

            count = await self.simulate_entity(entity_type, csv_path, topic)
            results[entity_type] = count

        logger.info(f"✅ Scenario complete: {self.config.name}")
        logger.info(f"Results: {results}")
        return results


def get_scenario_config(scenario_name: str) -> ScenarioConfig:
    """Get predefined scenario configuration"""
    scenarios = {
        "initial_load": ScenarioConfig(
            name="Initial Load",
            entities=[
                # {
                #     "type": "company",
                #     "csv": "__data__/__companies_cleaned.csv",
                #     "topic": KafkaTopic.COMPANY_EVENTS.value,
                # },
                {
                    "type": "product",
                    "csv": "__data__/__products_data.csv",
                    "topic": KafkaTopic.PRODUCT_EVENTS.value,
                },
            ],
            distribution=DistributionStrategy(
                create_pct=1.0, update_pct=0.0, delete_pct=0.0
            ),
            load_profile=LoadProfile(total_events=2, inter_batch_delay=0.0),
        ),
        "normal_traffic": ScenarioConfig(
            name="Normal Traffic",
            entities=[
                # {
                #     "type": "company",
                #     "csv": "__data__/__companies_cleaned.csv",
                #     "topic": KafkaTopic.COMPANY_EVENTS.value,
                # },
                {
                    "type": "product",
                    "csv": "__data__/__products_data.csv",
                    "topic": KafkaTopic.PRODUCT_EVENTS.value,
                },
            ],
            distribution=DistributionStrategy(
                create_pct=0.6, update_pct=0.3, delete_pct=0.1
            ),
            load_profile=LoadProfile(total_events=50, inter_event_delay=0.15),
        ),
        "update_heavy": ScenarioConfig(
            name="Update Heavy",
            entities=[
                # {"type": "company", "csv": "__data__/__companies_cleaned.csv"},
                {"type": "product", "csv": "__data__/__products_data.csv"},
            ],
            distribution=DistributionStrategy(
                create_pct=0.0, update_pct=1.0, delete_pct=0.0
            ),
            load_profile=LoadProfile(total_events=2, inter_event_delay=0.1),
        ),
        "delete_cleanup": ScenarioConfig(
            name="Delete Cleanup",
            entities=[
                {"type": "product", "csv": "__data__/__products_data.csv"},
            ],
            distribution=DistributionStrategy(
                create_pct=0.0, update_pct=0.0, delete_pct=1.0
            ),
            load_profile=LoadProfile(total_events=2, inter_event_delay=0.0),
        ),
        "burst_traffic": ScenarioConfig(
            name="Burst Traffic",
            entities=[
                {"type": "company", "csv": "__data__/__companies_cleaned.csv"},
                {"type": "product", "csv": "__data__/__products_data.csv"},
            ],
            distribution=DistributionStrategy(
                create_pct=0.4, update_pct=0.4, delete_pct=0.2
            ),
            load_profile=LoadProfile(
                total_events=200,
                burst_probability=0.5,  # 50% bursts
                inter_event_delay=0.05,
            ),
        ),
        "mixed_load": ScenarioConfig(
            name="Mixed Load",
            entities=[
                {
                    "type": "company",
                    "csv": "__data__/__companies_cleaned.csv",
                    "topic": KafkaTopic.COMPANY_EVENTS.value,
                },
                {
                    "type": "product",
                    "csv": "__data__/__products_data.csv",
                    "topic": KafkaTopic.PRODUCT_EVENTS.value,
                },
            ],
            distribution=DistributionStrategy(
                create_pct=0.5, update_pct=0.3, delete_pct=0.2
            ),
            load_profile=LoadProfile(
                total_events=150, inter_batch_delay=0.5, inter_event_delay=0.1
            ),
            tenants=["tenant_abc", "tenant_xyz"],
        ),
    }

    return scenarios.get(scenario_name)


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Realistic Kafka event simulator with configuration"
    )
    parser.add_argument(
        "--scenario",
        choices=[
            "initial_load",
            "normal_traffic",
            "update_heavy",
            "delete_cleanup",
            "burst_traffic",
            "mixed_load",
        ],
        default="normal_traffic",
        help="Predefined scenario to run",
    )
    parser.add_argument(
        "--custom-config",
        type=str,
        help="Path to custom JSON configuration file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode (log events to JSON file without sending to Kafka)",
    )

    args = parser.parse_args()

    # Get configuration
    if args.custom_config:
        # TODO: Load from JSON if needed
        logger.error("Custom config loading not yet implemented")
        return
    else:
        config = get_scenario_config(args.scenario)
        if not config:
            logger.error(f"Unknown scenario: {args.scenario}")
            return

    config.dry_run = args.dry_run

    logger.info("=" * 60)
    logger.info(f"Scenario: {config.name}")
    logger.info(f"Entities: {[e['type'] for e in config.entities]}")
    logger.info(f"Tenants: {config.tenants}")
    logger.info("=" * 60)

    # Run simulation
    async with RealisticEventSimulator(config) as simulator:
        results = await simulator.simulate()

        # Save events to file if in dry-run mode
        if config.dry_run:
            output_file = f"__data__/scenario_{args.scenario}_events.json"
            simulator.save_events_to_file(output_file)

    # Summary
    logger.info("=" * 60)
    logger.info("📊 Simulation Summary")
    logger.info("=" * 60)
    for entity, count in results.items():
        logger.info(f"  {entity}: {count} events")
    total = sum(results.values())
    logger.info(f"  Total: {total} events")
    if config.dry_run:
        logger.info(
            f"  📁 Events logged to: __data__/scenario_{args.scenario}_events.json"
        )
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

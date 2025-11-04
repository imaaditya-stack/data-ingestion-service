import asyncio

from dotenv import load_dotenv

load_dotenv()

from src.config.settings import IngestionConfig, KafkaConfig
from src.services.ingestion.ingestion_service import IngestionService
from src.services.kafka.entity_router import EntityRouter
from src.services.kafka.event_parser import EventParser
from src.services.kafka.event_processor import EventProcessor
from src.services.kafka.kafka_consumer import KafkaConsumerService
from src.services.kafka.models import KafkaTopic


ingestion_config = IngestionConfig.create_default()
ingestion_service = IngestionService.create(config=ingestion_config)

event_parser = EventParser(
    router=EntityRouter(), dry_run_mode=ingestion_config.dry_run_mode
)
event_processor = EventProcessor(
    ingestion_service=ingestion_service, event_parser=event_parser
)


kafka_config = KafkaConfig()

topics = [
    KafkaTopic.PRODUCT_EVENTS.value,
    KafkaTopic.COMPANY_EVENTS.value,
]

consumer_service = KafkaConsumerService(
    config=kafka_config,
    topics=topics,
    message_handler=event_processor,
)

if __name__ == "__main__":
    asyncio.run(consumer_service.start())

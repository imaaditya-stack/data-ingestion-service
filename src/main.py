import asyncio
from contextlib import asynccontextmanager
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI

load_dotenv()

from src.config.settings import IngestionConfig, KafkaConfig
from src.services.ingestion.ingestion_service import IngestionService
from src.services.kafka.entity_router import EntityRouter
from src.services.kafka.event_parser import EventParser
from src.services.kafka.event_processor import EventProcessor
from src.services.kafka.kafka_consumer import KafkaConsumerService
from src.services.kafka.models import KafkaTopic
from src.utils.logger import get_logger

logger = get_logger("api.main")


class AppContext:
    """Manages application lifecycle and service initialization"""

    def __init__(self):
        self.ingestion_service: Optional[IngestionService] = None
        self.consumer_service: Optional[KafkaConsumerService] = None
        self.event_processor: Optional[EventProcessor] = None
        self._consumer_task: Optional[asyncio.Task] = None
        self.router = EntityRouter()

    async def startup(self):
        """Startup application services"""
        logger.info("Starting FastAPI Application")

        # Initialize services with dependency injection
        ingestion_config = IngestionConfig.create_default()
        self.ingestion_service = IngestionService.create(config=ingestion_config)

        # Pass dry_run_mode to EventParser for operation-based prefixing
        event_parser = EventParser(
            router=self.router, dry_run_mode=ingestion_config.dry_run_mode
        )
        self.event_processor = EventProcessor(
            ingestion_service=self.ingestion_service, event_parser=event_parser
        )

        logger.info("Data Ingestion Service Initialized")

        # Initialize Kafka consumer
        kafka_config = KafkaConfig()
        topics = [
            KafkaTopic.PRODUCT_EVENTS.value,
            KafkaTopic.COMPANY_EVENTS.value,
        ]

        self.consumer_service = KafkaConsumerService(
            config=kafka_config,
            topics=topics,
            message_handler=self.event_processor,
        )
        logger.info("Starting Kafka Consumer")

        self._consumer_task = asyncio.create_task(self.consumer_service.start())

    async def shutdown(self):
        """Shutdown application services"""
        logger.info("Shutting down FastAPI Application")

        if self._consumer_task is not None:
            self._consumer_task.cancel()

            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass

        if self.consumer_service:
            await self.consumer_service.stop()

        logger.info("Shutdown Complete")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan manager for FastAPI app"""
    app_context = AppContext()
    await app_context.startup()
    try:
        yield
    finally:
        await app_context.shutdown()


app = FastAPI(
    title="Data Ingestion API",
    description="Kafka consumer for multi-tenant reindexing",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "Data Ingestion API is running"}


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "data-ingestion-api"}

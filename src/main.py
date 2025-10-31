import asyncio
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Type

from fastapi import FastAPI
from llama_index.core import Document
from pydantic import BaseModel

from src.config.settings import IngestionConfig, KafkaConfig
from src.core.protocols import MetadataProcessor, TextProcessor
from src.data_processors.company_processors import (
    CompanyDataProcessor,
    CompanyMetadataProcessor,
)
from src.data_processors.product_processors import (
    ProductDataProcessor,
    ProductMetadataProcessor,
)
from src.services.ingestion.ingestion_service import IngestionService
from src.services.kafka.kafka_consumer import KafkaConsumerService
from src.services.kafka.models import CompanyData, KafkaEvent, ProductData
from src.utils.logger import get_logger

logger = get_logger("api.main")
# logger.disabled = True


class EntityRoute:
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


class KafkaEventRouter:
    def route(self, topic: str) -> EntityRoute:
        topic_lower = topic.lower()
        if "company-events" in topic_lower:
            return EntityRoute(
                entity_type="company",
                text_processor=CompanyDataProcessor(),
                metadata_processor=CompanyMetadataProcessor(),
                data_model_cls=CompanyData,
            )
        if "product-events" in topic_lower:
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


class EventProcessor:
    def __init__(self, ingestion_service: IngestionService, router: KafkaEventRouter):
        self.ingestion_service = ingestion_service
        self.router = router

    def _build_document(self, topic: str, event: KafkaEvent) -> Document:
        route = self.router.route(topic)
        # Validate payload
        validated = route.data_model_cls(**event.data)
        data = validated.model_dump()
        # Build text and metadata
        text = route.text_processor.process_row(data, list(data.keys()))
        custom_metadata = route.metadata_processor.process_metadata(
            data, list(data.keys())
        )

        doc_metadata: Dict[str, Any] = {
            **custom_metadata,
            "tenant_id": event.tenant_id,
            "operation": event.metadata.operation,
            "event_id": event.event_id,
            "source": event.metadata.source,
            "schema_version": event.schema_version,
        }
        if event.entity_id is not None:
            doc_metadata["entity_id"] = event.entity_id

        doc_id = event.build_document_id(entity_type=route.entity_type)
        return Document(text=text, metadata=doc_metadata, id_=doc_id)

    async def process(self, topic: str, message: Dict[str, Any], key: Optional[str]):
        logger.info(
            f"Received message on topic={topic} for tenant={message.get('tenant_id')}"
        )
        try:
            event = KafkaEvent(**message)
            document = self._build_document(topic, event)

            logger.info(
                f"Prepared document for entity_type={document.metadata.get('entity')} entity_id={document.metadata.get('entity_id')}"
            )
            logger.info(f"Document: {document}")

            # Call the ingestion service to add the document to the vector store
            await self.ingestion_service.add_documents([document])
        except Exception as e:
            logger.error(f"Error handling message: {e}", exc_info=True)


class AppContext:
    def __init__(self):
        self.ingestion_service: Optional[IngestionService] = None
        self.consumer_service: Optional[KafkaConsumerService] = None
        self.event_processor: Optional[EventProcessor] = None
        self._consumer_task: Optional[asyncio.Task] = None
        self.router = KafkaEventRouter()

    async def startup(self):
        logger.info("Starting FastAPI Application")
        self.ingestion_service = IngestionService()
        self.event_processor = EventProcessor(self.ingestion_service, self.router)

        logger.info("Data Ingestion Service Initialized")

        kafka_config = KafkaConfig()
        topics = ["product-events", "company-events", "seller-events"]

        self.consumer_service = KafkaConsumerService(
            config=kafka_config,
            topics=topics,
            message_handler=self.event_processor.process,
        )
        logger.info("Starting Kafka Consumer")

        self._consumer_task = asyncio.create_task(self.consumer_service.start())

    async def shutdown(self):
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

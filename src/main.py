import asyncio
from contextlib import asynccontextmanager
from typing import Any, Dict, List

from fastapi import FastAPI
from llama_index.core import Document

from src.config.settings import IngestionConfig, KafkaConfig
from src.pipelines.ingestion import DataIngestionPipeline
from src.services.kafka.kafka_consumer import KafkaConsumerService
from src.utils.logger import get_logger
from src.data_processors import (
    CompanyDataProcessor,
    CompanyMetadataProcessor,
)

logger = get_logger("api.main")

# Global variables for services
ingestion_pipeline = None
consumer_service = None


def kafka_event_to_document(event: Dict[str, Any]) -> Document:
    """
    Convert Kafka event to LlamaIndex Document

    Args:
        event: Kafka event payload

    Returns:
        Document object for ingestion
    """
    data = event.get("data", {})
    metadata = event.get("metadata", {})

    text_processor = CompanyDataProcessor()
    metadata_processor = CompanyMetadataProcessor()

    text = text_processor.process_row(data, list(data.keys()))
    custom_metadata = metadata_processor.process_metadata(data, list(data.keys()))

    # Create document metadata (combine custom + Kafka metadata)
    doc_metadata = {
        **custom_metadata,  # From processor
        "operation": metadata.get("operation", "update"),
        "source": event.get("source", "kafka"),
    }

    # Create document ID
    if "id" in data:
        doc_metadata["entity_id"] = data["id"]
        doc_id = f"{event.get('tenant_id')}_{metadata.get('entity_type')}_{data['id']}"
    else:
        doc_id = f"{event.get('tenant_id')}_{event.get('event_id')}"

    return Document(text=text, metadata=doc_metadata, id_=doc_id)


async def handle_kafka_message(message: Dict[str, Any]):
    """
    Handle incoming Kafka message

    Args:
        message: Kafka message payload
    """
    logger.info(
        f"Received message: {message.get('event_name')} for tenant={message.get('tenant_id')}"
    )

    logger.info(f"Message: {message}")

    try:
        # Convert event to document
        document = kafka_event_to_document(message)

        # Ingest into vector store
        logger.info(
            f"Ingesting document for entity_id={message.get('data', {}).get('id')}"
        )

        logger.info(f"Document: {document}")

        # nodes = await ingestion_pipeline.ingest([document])
        # logger.info(f"Successfully ingested {len(nodes)} nodes")

    except Exception as e:
        logger.error(f"Error handling message: {e}", exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan manager for FastAPI app"""
    global ingestion_pipeline, consumer_service

    logger.info("Starting FastAPI application")

    # Initialize ingestion pipeline
    ingestion_pipeline = DataIngestionPipeline()
    logger.info("Ingestion pipeline initialized")

    # Start Kafka consumer in background
    kafka_config = KafkaConfig()
    topics = ["product-events", "company-events", "seller-events"]

    consumer_service = KafkaConsumerService(
        config=kafka_config,
        topics=topics,
        message_handler=handle_kafka_message,
    )

    logger.info("Starting Kafka consumer")

    # Start consumer in background task
    consumer_task = asyncio.create_task(consumer_service.start())

    yield

    # Cleanup
    logger.info("Shutting down FastAPI application")
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    if consumer_service:
        await consumer_service.stop()
    logger.info("Shutdown complete")


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


# @app.get("/health")
# async def health_check():
#     """Health check endpoint"""
#     return {"status": "healthy", "service": "data-ingestion-api"}

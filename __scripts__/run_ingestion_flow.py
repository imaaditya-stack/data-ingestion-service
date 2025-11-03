import asyncio
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust depth if needed
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from llama_index.core import Document

from src.core.protocols import MetadataProcessor, TextProcessor
from src.data_loaders.csv_loader import CSVLoader
from src.data_processors import (
    CompanyDataProcessor,
    CompanyMetadataProcessor,
    ProductDataProcessor,
    ProductMetadataProcessor,
)
from src.services.ingestion import IngestionService
from src.services.kafka.models import KafkaEvent


def load_documents_from_csv(
    file_path: str,
    text_processor: TextProcessor,
    metadata_processor: MetadataProcessor,
    **kwargs,
) -> List[Document]:
    """Load documents from CSV"""
    loader = CSVLoader()
    return loader.load_csv(
        csv_path=file_path,
        text_processor=text_processor,
        metadata_processor=metadata_processor,
        **kwargs,
    )


async def demo_add_documents_for_companies():
    """Demo: Add new documents"""
    print("\n📥 Step 1: Adding Documents")

    file_path = os.path.join(
        os.path.dirname(__file__), "../", "__data__", "__companies_data.csv"
    )
    text_processor = CompanyDataProcessor()
    metadata_processor = CompanyMetadataProcessor()

    # Load documents
    documents = load_documents_from_csv(file_path, text_processor, metadata_processor)

    # Initialize service
    service = IngestionService.create()

    # Add documents
    nodes = service.add_documents(documents)
    print(f"✅ Added {len(nodes)} documents")

    return documents


async def demo_add_documents_for_products():
    """Demo: Add new documents"""
    print("\n📥 Step 1: Adding Documents")

    file_path = os.path.join(
        os.path.dirname(__file__), "../", "__data__", "__products_data.csv"
    )
    text_processor = ProductDataProcessor()
    metadata_processor = ProductMetadataProcessor()

    # Load documents
    documents = load_documents_from_csv(file_path, text_processor, metadata_processor)

    # Initialize service
    service = IngestionService.create()

    # Add documents
    nodes = service.add_documents(documents)
    print(f"✅ Added {len(nodes)} documents")

    return documents


async def demo_update_documents_for_companies(kafka_records: List[Dict[str, Any]]):
    """Demo: Update existing documents from Kafka-like events"""
    print("\n🔄 Step 2: Updating Documents from Kafka Events")

    service = IngestionService.create()

    def to_document(topic: str, record: Dict[str, Any]) -> Document:
        event = KafkaEvent(**record)

        topic_lower = topic.lower()
        if "company-events" in topic_lower:
            text_processor = CompanyDataProcessor()
            metadata_processor = CompanyMetadataProcessor()
            entity_type = "company"
        elif "product-events" in topic_lower:
            text_processor = ProductDataProcessor()
            metadata_processor = ProductMetadataProcessor()
            entity_type = "product"
        else:
            # fallback to metadata
            if event.entity_type == "company":
                text_processor = CompanyDataProcessor()
                metadata_processor = CompanyMetadataProcessor()
            else:
                text_processor = ProductDataProcessor()
                metadata_processor = ProductMetadataProcessor()
            entity_type = event.entity_type

        data = event.data
        text = text_processor.process_row(data, list(data.keys()))
        custom_metadata = metadata_processor.process_metadata(data, list(data.keys()))
        metadata: Dict[str, Any] = {
            **custom_metadata,
            "operation": event.metadata.operation,
            "source": event.source,
            "tenant_id": event.tenant_id,
            "entity_type": entity_type,
            "schema_version": event.schema_version,
            "event_id": event.event_id,
        }
        if event.entity_id is not None:
            metadata["entity_id"] = event.entity_id
        doc_id = event.build_document_id()
        return Document(text=text, metadata=metadata, id_=doc_id)

    # Convert Kafka records to documents using topic-based routing
    updated_documents: List[Document] = []
    for record in kafka_records:
        # For demo, allow caller to provide topic; default to company-events
        topic = record.get("__topic__", "company-events")
        doc = to_document(topic, record)
        updated_documents.append(doc)

    # Update: Delete old documents first, then add new ones
    if updated_documents:
        # Filter by document_id to delete old versions
        filter_metadata = {
            "document_id": {
                "$in": [doc.id_ for doc in updated_documents],
            },
        }
        # Delete old versions
        try:
            service.delete_documents(filter_metadata=filter_metadata)
            # Add new documents
            nodes = service.add_documents(updated_documents)
            print(
                f"✅ Updated {len(nodes)} documents from {len(kafka_records)} Kafka events"
            )
        except Exception as e:
            print(f"❌ Error updating documents: {e}")
            raise


async def demo_delete_documents_for_companies(kafka_records: List[Dict[str, Any]]):
    """Demo: Delete documents based on Kafka delete events"""
    print("\n🗑️  Step 3: Deleting Documents from Kafka Events")

    service = IngestionService.create()

    # Delete using metadata from Kafka records
    # Get metadata from first record (assuming all have same entity_type)

    if kafka_records:
        # Convert Kafka records to documents to get document IDs
        def to_document(record: Dict[str, Any]) -> Document:
            event = KafkaEvent(**record)

            if event.entity_type == "company":
                text_processor = CompanyDataProcessor()
                metadata_processor = CompanyMetadataProcessor()
            else:
                text_processor = ProductDataProcessor()
                metadata_processor = ProductMetadataProcessor()

            data = event.data
            text = text_processor.process_row(data, list(data.keys()))
            custom_metadata = metadata_processor.process_metadata(
                data, list(data.keys())
            )
            metadata: Dict[str, Any] = {
                **custom_metadata,
                "operation": event.metadata.operation,
                "source": event.source,
                "tenant_id": event.tenant_id,
                "entity_type": event.entity_type,
                "schema_version": event.schema_version,
                "event_id": event.event_id,
            }
            if event.entity_id is not None:
                metadata["entity_id"] = event.entity_id
            doc_id = event.build_document_id()
            return Document(text=text, metadata=metadata, id_=doc_id)

        # Get document IDs from records
        delete_documents = [to_document(record) for record in kafka_records]

        filter_metadata = {
            "document_id": {
                "$in": [doc.id_ for doc in delete_documents],
            },
        }
        service.delete_documents(filter_metadata=filter_metadata)
        print(
            f"✅ Deleted {len(delete_documents)} documents from {len(kafka_records)} Kafka events"
        )


async def main():
    """Run all demos"""
    print("🚀 Ingestion Service Demo\n")

    # Note: Run only one at a time

    # Step 1: Add documents from CSV
    # demo_add_documents_for_companies()

    demo_add_documents_for_products()

    # Step 2: Update documents from Kafka-like events
    # Mock Kafka records for update operation
    kafka_update_records = [
        {
            "event_id": "evt_001",
            "tenant_id": "tenant_1",
            "data": {
                "id": "3",
                "COMPANY_NAME": "Soltherm Company",
                "COMPANY_ADDRESS": "205, 2nd Floor Palika Bazar, Lucknow - 226024, Uttar Pradesh, India",
                "COMPANY_DESCRIPTION": "we are a company that sells products",
                "COMPANY_REPRESENTATIVE_DESIGNATION": "CEO",
            },
            "metadata": {"entity_type": "company", "operation": "update"},
            "source": "kafka",
        },
    ]

    # demo_update_documents_for_companies(kafka_update_records)

    # Step 3: Delete documents from Kafka-like events
    # Uncomment to test delete operation
    kafka_delete_records = [
        {
            "event_id": "evt_del_001",
            "tenant_id": "tenant_1",
            "data": {"id": "3"},
            "metadata": {"entity_type": "company", "operation": "delete"},
            "source": "kafka",
        }
    ]
    # demo_delete_documents_for_companies(kafka_delete_records)

    print("\n✅ All demos completed!")


if __name__ == "__main__":
    asyncio.run(main())

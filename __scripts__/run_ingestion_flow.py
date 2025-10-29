import asyncio
import os
from typing import Any, Dict, List
import sys
from pathlib import Path

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
        os.path.dirname(__file__), "../", "__data__", "__companies.csv"
    )
    text_processor = CompanyDataProcessor()
    metadata_processor = CompanyMetadataProcessor()

    # Load documents
    documents = load_documents_from_csv(file_path, text_processor, metadata_processor)

    # Initialize service
    service = IngestionService()

    # Add documents
    nodes = await service.add_documents(documents)
    print(f"✅ Added {len(nodes)} documents")

    return documents


async def demo_update_documents_for_companies(kafka_records: List[Dict[str, Any]]):
    """Demo: Update existing documents from Kafka-like events"""
    print("\n🔄 Step 2: Updating Documents from Kafka Events")

    service = IngestionService()

    # Use the same processors as in CSV loading
    text_processor = CompanyDataProcessor()
    metadata_processor = CompanyMetadataProcessor()

    # Convert Kafka records to documents using the same processors
    updated_documents: List[Document] = []
    for record in kafka_records:
        data = record.get("data", {})
        metadata = record.get("metadata", {})

        # Use the same processors (now they accept dict)
        text = text_processor.process_row(data, list(data.keys()))
        custom_metadata = metadata_processor.process_metadata(data, list(data.keys()))

        # Create document metadata (combine custom + Kafka metadata)
        doc_metadata = {
            **custom_metadata,  # From processor
            "operation": metadata.get("operation", "update"),
            "source": record.get("source", "kafka"),
        }

        # Create document ID
        if "id" in data:
            doc_metadata["entity_id"] = data["id"]
            doc_id = (
                f"{record.get('tenant_id')}_{metadata.get('entity_type')}_{data['id']}"
            )
        else:
            doc_id = f"{record.get('tenant_id')}_{record.get('event_id')}"

        # Create document
        doc = Document(text=text, metadata=doc_metadata, id_=doc_id)
        updated_documents.append(doc)

    # Update using filter metadata
    if updated_documents:
        # Filter by entity_type and entity_id
        filter_metadata = {
            "id": {
                "$in": [doc.metadata.get("id") for doc in updated_documents],
            },
        }
        count = await service.update_documents(updated_documents, filter_metadata)
        print(f"✅ Updated {count} documents from {len(kafka_records)} Kafka events")


async def demo_delete_documents_for_companies(kafka_records: List[Dict[str, Any]]):
    """Demo: Delete documents based on Kafka delete events"""
    print("\n🗑️  Step 3: Deleting Documents from Kafka Events")

    service = IngestionService()

    # Delete using metadata from Kafka records
    # Get metadata from first record (assuming all have same entity_type)

    if kafka_records:

        filter_metadata = {
            "id": {
                "$in": [doc.get("data", {}).get("id") for doc in kafka_records],
            },
        }
        deleted_count = await service.delete_documents_by_metadata(filter_metadata)
        print(
            f"✅ Deleted {deleted_count} documents from {len(kafka_records)} Kafka events"
        )


async def main():
    """Run all demos"""
    print("🚀 Ingestion Service Demo\n")

    # Note: Run only one at a time

    # Step 1: Add documents from CSV
    await demo_add_documents_for_companies()

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

    # await demo_update_documents_for_companies(kafka_update_records)

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
    # await demo_delete_documents_for_companies(kafka_delete_records)

    print("\n✅ All demos completed!")


if __name__ == "__main__":
    asyncio.run(main())

"""
Example: Using separate pipelines for ingestion and querying
"""

import asyncio
import os
from typing import Any, Dict, List

import pandas as pd
from llama_index.core import VectorStoreIndex
from llama_index.core.indices.vector_store.retrievers import retriever

from src.core.text_processors import MetadataProcessor, TextProcessor
from src.pipelines.ingestion import IngestionPipeline


class CompanyMetadataProcessor:
    """
    Metadata processor for company data with essential fields
    """

    def process_metadata(self, row: pd.Series, columns: List[str]) -> Dict[str, Any]:
        """
        Process company row metadata with essential fields only
        """
        metadata = {"entity": "company"}

        # Essential fields for company data
        essential_fields = [
            "id",
            "COMPANY_ADDRESS",
            "CREATED_AT",
            "UPDATED_AT",
            "SELLER_id",
        ]

        for field in essential_fields:
            if field in row.index and pd.notna(row[field]):
                # Truncate long values to prevent metadata bloat
                value = str(row[field])
                if len(value) > 100:
                    value = value[:100] + "..."
                metadata[field] = value

        return metadata


class ProductMetadataProcessor:
    """
    Metadata processor for product data with essential fields
    """

    def process_metadata(self, row: pd.Series, columns: List[str]) -> Dict[str, Any]:
        """
        Process product row metadata with essential fields only
        """
        metadata = {"entity": "product"}

        # Essential fields for product data
        essential_fields = [
            "id",
            "CREATED_AT",
            "UPDATED_AT",
            "SELLER_id",
            "PRODUCT_CATEGORY_id",
        ]

        for field in essential_fields:
            if field in row.index and pd.notna(row[field]):
                # Truncate long values to prevent metadata bloat
                value = str(row[field])
                if len(value) > 100:
                    value = value[:100] + "..."
                metadata[field] = value

        return metadata


def safe_get_text(value, max_length: int = 1000) -> str:
    """Safely extract text from any value type"""
    if pd.isna(value) or value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() in ["nan", "none", "null"]:
        return ""
    return text[:max_length] if len(text) > max_length else text


class CompanyDataProcessor:
    """Processor for company data with specific formatting"""

    def process_row(self, row: pd.Series, columns: list) -> str:
        """Process a row of company data with clean formatting"""

        text_parts = []

        # Company name
        name = safe_get_text(row.get("COMPANY_NAME"))
        if name:
            text_parts.append(f"Name: {name}")

        # Company description
        description = safe_get_text(row.get("COMPANY_DESCRIPTION"), 1000)
        if description:
            text_parts.append(f"Description: {description}")

        # Nature of business
        business = safe_get_text(row.get("NATURE_OF_BUSINESS"))
        if business:
            text_parts.append(f"Nature of Business: {business}")

        # Company address
        address = safe_get_text(row.get("COMPANY_ADDRESS"), 200)
        if address:
            text_parts.append(f"Address: {address}")

        if text_parts:
            return f"Type: Company | {' | '.join(text_parts)}"
        else:
            return "Type: Company | No data available"


class ProductDataProcessor:
    """Processor for product data with specific formatting"""

    def process_row(self, row: pd.Series, columns: list) -> str:
        """Process a row of product data with clean formatting"""

        text_parts = []

        # Product name
        name = safe_get_text(row.get("PRODUCT_NAME"))
        if name:
            text_parts.append(f"Name: {name}")

        # Product description
        description = safe_get_text(row.get("PRODUCT_DESCRIPTION"), 1000)
        if description:
            text_parts.append(f"Description: {description}")

        # Product category
        category_name = safe_get_text(row.get("CATEGORY"))
        if category_name:
            text_parts.append(f"Category: {category_name}")

        # Product price (if available)
        price = safe_get_text(row.get("PRODUCT_PRICE"), 50)
        if price:
            text_parts.append(f"Price: {price}")

        if text_parts:
            return f"Type: Product | {' | '.join(text_parts)}"
        else:
            return "Type: Product | No data available"


async def run_ingestion(
    file_path: str,
    text_processor: TextProcessor,
    metadata_processor: MetadataProcessor,
    **kwargs,
):
    """Ingest data - run this first"""
    print("\n🚀 Step 1: Ingestion\n")

    pipeline = IngestionPipeline()

    # Use batch ingestion to be gentle on Ollama
    nodes = await pipeline.ingest_from_csv(
        csv_path=file_path,
        text_processor=text_processor,
        metadata_processor=metadata_processor,
        **kwargs,
    )

    print(f"✅ Ingested {len(nodes)} nodes\n")


if __name__ == "__main__":

    def run_ingestion_for_companies():
        file_path = os.path.join(
            os.path.dirname(__file__), "__data__", "__companies.csv"
        )
        text_processor = CompanyDataProcessor()
        metadata_processor = CompanyMetadataProcessor()
        return run_ingestion(file_path, text_processor, metadata_processor)

    def run_ingestion_for_products():
        file_path = os.path.join(
            os.path.dirname(__file__), "__data__", "__products.csv"
        )
        text_processor = ProductDataProcessor()
        metadata_processor = ProductMetadataProcessor()
        return run_ingestion(file_path, text_processor, metadata_processor)

    asyncio.run(run_ingestion_for_companies())
    asyncio.run(run_ingestion_for_products())

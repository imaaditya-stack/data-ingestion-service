"""
Product data processors
Converts product data to text and metadata
"""

from typing import Any, Dict


def safe_get_text(value: Any, max_length: int = 1000) -> str:
    """Safely extract text from any value type"""
    if value is None:
        return ""

    text = str(value).strip()
    if not text or text.lower() in ["nan", "none", "null"]:
        return ""

    return text[:max_length] if len(text) > max_length else text


class ProductDataProcessor:
    """Processor for product data with specific formatting"""

    def process_row(self, row: Dict[str, Any], columns: list) -> str:
        """Process a row of product data with clean formatting"""

        text_parts = []

        # Product name
        name = safe_get_text(row.get("product_name"))
        if name:
            text_parts.append(f"Name: {name}")

        # Product description
        description = safe_get_text(row.get("product_description"), 1000)
        if description:
            text_parts.append(f"Description: {description}")

        # Product category
        category_name = safe_get_text(row.get("category"))
        if category_name:
            text_parts.append(f"Category: {category_name}")

        if text_parts:
            return f"Type: Product | {' | '.join(text_parts)}"

        return "Type: Product | No data available"


class ProductMetadataProcessor:
    """Metadata processor for product data with essential fields"""

    def process_metadata(self, row: Dict[str, Any], columns: list) -> Dict[str, Any]:
        """
        Process product row metadata with essential fields only

        Args:
            row: Dictionary representing a single row
            columns: List of column names (not used, kept for compatibility)

        Returns:
            Dictionary of metadata key-value pairs
        """
        metadata = {"entity": "product"}

        # Essential fields for product data
        essential_fields = [
            "id",
            "category",
            "category_id",
            "created_at",
            "updated_at",
            "seller_id",
            "category_id",
        ]

        for field in essential_fields:
            if field in row and row[field] is not None:
                # Truncate long values to prevent metadata bloat
                value = str(row[field])
                if len(value) > 100:
                    value = value[:100] + "..."
                metadata[field] = value

        return metadata

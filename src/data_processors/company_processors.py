"""
Company data processors
Converts company data to text and metadata
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


class CompanyDataProcessor:
    """Processor for company data with specific formatting"""

    def process_row(self, row: Dict[str, Any], columns: list) -> str:
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


class CompanyMetadataProcessor:
    """Metadata processor for company data with essential fields"""

    def process_metadata(self, row: Dict[str, Any], columns: list) -> Dict[str, Any]:
        """
        Process company row metadata with essential fields only

        Args:
            row: Dictionary representing a single row
            columns: List of column names (not used, kept for compatibility)

        Returns:
            Dictionary of metadata key-value pairs
        """
        metadata = {"entity": "company"}

        # Essential fields for company data
        essential_fields = [
            "id",
            "COMPANY_ADDRESS",
            "COMPANY_CREATED_AT",
            "COMPANY_UPDATED_AT",
            "SELLER_id",
            "BUSINESS_TYPE_NAME",
            "BUSINESS_TYPE_ID",
        ]

        for field in essential_fields:
            if field in row and row[field] is not None:
                # Truncate long values to prevent metadata bloat
                value = str(row[field])
                if len(value) > 100:
                    value = value[:100] + "..."
                metadata[field] = value

        return metadata

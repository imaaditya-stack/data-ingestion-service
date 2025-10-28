"""
Text processing protocols and implementations for flexible text concatenation
"""

from typing import Any, Dict, List, Protocol

import pandas as pd


class TextProcessor(Protocol):
    """
    Protocol for text processors that convert DataFrame rows to text for embeddings
    """

    def process_row(self, row: pd.Series, columns: List[str]) -> str:
        """
        Process a DataFrame row into text for embeddings

        Args:
            row: Pandas Series representing a single row
            columns: List of column names to include in processing

        Returns:
            Formatted text string
        """
        pass


class MetadataProcessor(Protocol):
    """
    Protocol for metadata processors that extract metadata from DataFrame rows
    """

    def process_metadata(self, row: pd.Series, columns: List[str]) -> Dict[str, Any]:
        """
        Process a DataFrame row into metadata dictionary

        Args:
            row: Pandas Series representing a single row
            columns: List of column names to include in processing

        Returns:
            Dictionary of metadata key-value pairs
        """
        pass


class DefaultTextProcessor:
    """
    Default text processor that joins columns with " | " separator
    """

    def process_row(self, row: pd.Series, columns: List[str]) -> str:
        """
        Process row using default format: "column: value | column: value"
        """
        text_parts = []
        for col in columns:
            value = row[col]
            if pd.notna(value):  # Skip NaN values
                text_parts.append(f"{col}: {value}")

        return " | ".join(text_parts)


class DefaultMetadataProcessor:
    """
    Default metadata processor that includes no additional fields
    """

    def process_metadata(self, row: pd.Series, columns: List[str]) -> Dict[str, Any]:
        """
        Process row metadata - returns empty dict by default
        """
        return {}

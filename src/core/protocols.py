from typing import Any, Dict, List, Protocol

from llama_index.core.base.embeddings.base import BaseEmbedding
from llama_index.core.base.llms.base import BaseLLM


class EmbeddingProvider(Protocol):
    """Protocol for embedding model providers"""

    def get_embedding_model(self) -> BaseEmbedding:
        """Get the embedding model instance"""


class LLMProvider(Protocol):
    """Protocol for LLM providers"""

    def get_llm(self) -> BaseLLM:
        """Get the LLM instance"""


class VectorStoreProvider(Protocol):
    """Protocol for vector store providers"""

    def get_vector_store(self) -> Any:
        """Get the vector store instance"""

    def get_collection(self) -> Any:
        """Get the collection instance"""


class TextProcessor(Protocol):
    """
    Protocol for text processors that convert data rows to text for embeddings
    """

    def process_row(self, row: Dict[str, Any], columns: List[str]) -> str:
        """
        Process a data row into text for embeddings

        Args:
            row: Dictionary representing a single row
            columns: List of column names to include in processing

        Returns:
            Formatted text string
        """
        pass


class MetadataProcessor(Protocol):
    """
    Protocol for metadata processors that extract metadata from data rows
    """

    def process_metadata(
        self, row: Dict[str, Any], columns: List[str]
    ) -> Dict[str, Any]:
        """
        Process a data row into metadata dictionary

        Args:
            row: Dictionary representing a single row
            columns: List of column names to include in processing

        Returns:
            Dictionary of metadata key-value pairs
        """
        pass


class DefaultTextProcessor:
    """
    Default text processor that joins columns with " | " separator
    """

    def process_row(self, row: Dict[str, Any], columns: List[str]) -> str:
        """
        Process row using default format: "column: value | column: value"
        """
        text_parts = []
        for col in columns:
            value = row.get(col)
            if value is not None:  # Skip None values
                text_parts.append(f"{col}: {value}")

        return " | ".join(text_parts)


class DefaultMetadataProcessor:
    """
    Default metadata processor that includes no additional fields
    """

    def process_metadata(
        self, row: Dict[str, Any], columns: List[str]
    ) -> Dict[str, Any]:
        """
        Process row metadata - returns empty dict by default
        """
        return {}

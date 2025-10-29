"""
CSV Loader
Loads data from CSV files into LlamaIndex Documents
"""

from pathlib import Path
from typing import List, Optional

import pandas as pd
from llama_index.core import Document

from src.core.protocols import MetadataProcessor, TextProcessor
from src.data_loaders.base_loader import BaseLoader
from src.utils.logger import get_logger

logger = get_logger("data_loaders.csv_loader")


class CSVLoader(BaseLoader):
    """Loader for CSV files"""

    def load_csv(
        self,
        csv_path: str,
        text_columns: Optional[List[str]] = None,
        text_processor: Optional[TextProcessor] = None,
        metadata_processor: Optional[MetadataProcessor] = None,
        encoding: str = "utf-8",
        delimiter: str = ",",
        **kwargs,
    ) -> List[Document]:
        """
        Load data from CSV file

        Args:
            csv_path: Path to CSV file
            text_columns: Specific columns to use for text (None for all)
            text_processor: Custom text processor for formatting text (None for default)
            encoding: File encoding (default: utf-8)
            delimiter: CSV delimiter (default: ,)
            **kwargs: Additional pandas read_csv parameters

        Returns:
            List of Document objects
        """
        logger.info(f"Loading CSV: {csv_path}")

        # Read CSV file
        df = pd.read_csv(csv_path, encoding=encoding, delimiter=delimiter, **kwargs)

        # Extract filename for metadata
        filename = Path(csv_path).stem

        documents = self._dataframe_to_documents(
            df, filename, text_columns, "csv", text_processor, metadata_processor
        )

        logger.info(f"Loaded {len(documents)} documents from CSV: {csv_path}")
        return documents

    def load_multiple_files(
        self,
        file_paths: List[str],
        auto_detect: bool = True,
        text_processor: Optional[TextProcessor] = None,
        metadata_processor: Optional[MetadataProcessor] = None,
        **kwargs,
    ) -> List[Document]:
        """
        Load data from multiple files (CSV only)

        Args:
            file_paths: List of file paths
            auto_detect: Auto-detect file type from extension
            text_processor: Custom text processor for formatting text (None for default)
            **kwargs: Additional parameters for load functions

        Returns:
            Combined list of Document objects
        """
        return self._load_multiple_files(
            file_paths=file_paths,
            supported_extensions=[".csv"],
            load_method=self.load_csv,
            auto_detect=auto_detect,
            text_processor=text_processor,
            metadata_processor=metadata_processor,
            **kwargs,
        )

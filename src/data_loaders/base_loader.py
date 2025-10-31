from pathlib import Path
from typing import Callable, List, Optional

import pandas as pd
from llama_index.core import Document

from src.core.protocols import (
    DefaultMetadataProcessor,
    DefaultTextProcessor,
    MetadataProcessor,
    TextProcessor,
)
from src.utils.logger import get_logger

logger = get_logger("data_loaders.base")


class BaseLoader:
    """Base class for data loaders with common functionality"""

    @staticmethod
    def _dataframe_to_documents(
        df: pd.DataFrame,
        source_name: str,
        text_columns: Optional[List[str]] = None,
        source_type: str = "table",
        text_processor: Optional[TextProcessor] = None,
        metadata_processor: Optional[MetadataProcessor] = None,
    ) -> List[Document]:
        """
        Convert DataFrame rows to LlamaIndex Documents

        Args:
            df: Pandas DataFrame
            source_name: Name of the source (sheet name or filename)
            text_columns: Specific columns to include in text (None for all)
            source_type: Type of source ("excel", "csv", "table")
            text_processor: Custom text processor for formatting text (None for default)
            metadata_processor: Custom metadata processor (None for default)

        Returns:
            List of Document objects
        """
        documents = []

        # Determine which columns to use for text
        if text_columns:
            columns_to_use = [col for col in text_columns if col in df.columns]
            if not columns_to_use:
                logger.warning(
                    f"None of the specified text_columns {text_columns} found in DataFrame. Using all columns."
                )
                columns_to_use = df.columns.tolist()
        else:
            columns_to_use = df.columns.tolist()

        # Use provided processors or defaults
        text_proc = text_processor or DefaultTextProcessor()
        metadata_proc = metadata_processor or DefaultMetadataProcessor()

        for idx, row in df.iterrows():
            # Convert pandas Series to dict for processors
            row_dict = row.to_dict()

            # Use the text processor to format the text
            text = text_proc.process_row(row_dict, columns_to_use)

            # Create base metadata
            metadata = {
                "source": source_type,
                "source_name": source_name,
                "row_index": int(idx),
            }

            # Add custom metadata using metadata processor
            custom_metadata = metadata_proc.process_metadata(row_dict, columns_to_use)
            metadata.update(custom_metadata)

            doc = Document(text=text, metadata=metadata, id_=f"{source_name}_row_{idx}")
            documents.append(doc)

        return documents

    @staticmethod
    def _load_multiple_files(
        file_paths: List[str],
        supported_extensions: List[str],
        load_method: Callable,
        auto_detect: bool = True,
        **kwargs,
    ) -> List[Document]:
        """
        Generic method to load data from multiple files

        Args:
            file_paths: List of file paths
            supported_extensions: List of supported file extensions (e.g., [".csv"])
            load_method: The load method to call (e.g., self.load_csv)
            auto_detect: Auto-detect file type from extension
            **kwargs: Additional parameters for load functions

        Returns:
            Combined list of Document objects
        """
        logger.info(f"Loading {len(file_paths)} files")
        all_documents = []

        for file_path in file_paths:
            # Auto-detect file type from extension
            if auto_detect:
                ext = Path(file_path).suffix.lower()
                if ext in supported_extensions:
                    docs = load_method(file_path, **kwargs)
                else:
                    logger.warning(f"Skipping unsupported file: {file_path}")
                    continue
            else:
                # Default to using the load method if auto-detect is disabled
                docs = load_method(file_path, **kwargs)

            all_documents.extend(docs)

        logger.info(
            f"Loaded {len(all_documents)} total documents from {len(file_paths)} files"
        )
        return all_documents

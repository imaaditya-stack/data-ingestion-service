"""
Data loading service
Handles loading data from CSV and Excel files into LlamaIndex Documents
"""

from pathlib import Path
from typing import List, Optional

import pandas as pd
from llama_index.core import Document

from src.core.text_processors import (
    DefaultMetadataProcessor,
    DefaultTextProcessor,
    MetadataProcessor,
    TextProcessor,
)
from src.utils.logger import get_logger

logger = get_logger("services.loader")


class DataLoaderService:
    """Service for loading structured data files"""

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
            # Use the text processor to format the text
            text = text_proc.process_row(row, columns_to_use)

            # Create base metadata
            metadata = {
                "source": source_type,
                "source_name": source_name,
                "row_index": int(idx),
            }

            # Add custom metadata using metadata processor
            custom_metadata = metadata_proc.process_metadata(row, columns_to_use)
            metadata.update(custom_metadata)

            doc = Document(text=text, metadata=metadata, id_=f"{source_name}_row_{idx}")
            documents.append(doc)

        return documents

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

    def load_excel(
        self,
        excel_path: str,
        sheet_name: Optional[str] = None,
        text_columns: Optional[List[str]] = None,
        text_processor: Optional[TextProcessor] = None,
        metadata_processor: Optional[MetadataProcessor] = None,
    ) -> List[Document]:
        """
        Load data from Excel file

        Args:
            excel_path: Path to Excel file
            sheet_name: Specific sheet name (None for all sheets)
            text_columns: Specific columns to use for text (None for all)
            text_processor: Custom text processor for formatting text (None for default)

        Returns:
            List of Document objects
        """
        logger.info(f"Loading Excel: {excel_path}")
        documents = []

        if sheet_name:
            # Load specific sheet
            df = pd.read_excel(excel_path, sheet_name=sheet_name)
            documents.extend(
                self._dataframe_to_documents(
                    df,
                    sheet_name,
                    text_columns,
                    "excel",
                    text_processor,
                    metadata_processor,
                )
            )
        else:
            # Load all sheets
            excel_file = pd.ExcelFile(excel_path)
            for sheet in excel_file.sheet_names:
                df = pd.read_excel(excel_path, sheet_name=sheet)
                documents.extend(
                    self._dataframe_to_documents(
                        df,
                        sheet,
                        text_columns,
                        "excel",
                        text_processor,
                        metadata_processor,
                    )
                )

        logger.info(f"Loaded {len(documents)} documents from Excel: {excel_path}")
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
        Load data from multiple files (CSV and/or Excel)

        Args:
            file_paths: List of file paths
            auto_detect: Auto-detect file type from extension
            text_processor: Custom text processor for formatting text (None for default)
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
                if ext in [".csv", ".txt"]:
                    docs = self.load_csv(
                        file_path,
                        text_processor=text_processor,
                        metadata_processor=metadata_processor,
                        **kwargs,
                    )
                elif ext in [".xlsx", ".xls"]:
                    docs = self.load_excel(
                        file_path,
                        text_processor=text_processor,
                        metadata_processor=metadata_processor,
                        **kwargs,
                    )
                else:
                    logger.warning(f"Skipping unsupported file: {file_path}")
                    continue
            else:
                # Default to CSV if auto-detect is disabled
                docs = self.load_csv(file_path, text_processor=text_processor, **kwargs)

            all_documents.extend(docs)

        logger.info(
            f"Loaded {len(all_documents)} total documents from {len(file_paths)} files"
        )
        return all_documents

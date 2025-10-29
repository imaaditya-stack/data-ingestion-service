"""
Excel Loader
Loads data from Excel files into LlamaIndex Documents
"""

from pathlib import Path
from typing import List, Optional

import pandas as pd
from llama_index.core import Document

from src.core.protocols import MetadataProcessor, TextProcessor
from src.data_loaders.base_loader import BaseLoader
from src.utils.logger import get_logger

logger = get_logger("data_loaders.excel_loader")


class ExcelLoader(BaseLoader):
    """Loader for Excel files"""

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
        Load data from multiple files (Excel only)

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
            supported_extensions=[".xlsx", ".xls"],
            load_method=self.load_excel,
            auto_detect=auto_detect,
            text_processor=text_processor,
            metadata_processor=metadata_processor,
            **kwargs,
        )

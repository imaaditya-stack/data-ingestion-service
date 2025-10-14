"""
Data loading service
Handles loading data from CSV and Excel files into LlamaIndex Documents
"""

from pathlib import Path
from typing import List, Optional

import pandas as pd
from llama_index.core import Document

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
    ) -> List[Document]:
        """
        Convert DataFrame rows to LlamaIndex Documents

        Args:
            df: Pandas DataFrame
            source_name: Name of the source (sheet name or filename)
            text_columns: Specific columns to include in text (None for all)
            source_type: Type of source ("excel", "csv", "table")

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

        for idx, row in df.iterrows():
            # Concatenate specified columns into text content
            text_parts = []
            for col in columns_to_use:
                value = row[col]
                if pd.notna(value):  # Skip NaN values
                    text_parts.append(f"{col}: {value}")

            text = " | ".join(text_parts)

            # Create metadata with all columns
            metadata = {
                "source": source_type,
                "source_name": source_name,
                "row_index": int(idx),
                **{str(col): str(row[col]) for col in df.columns if pd.notna(row[col])},
            }

            doc = Document(text=text, metadata=metadata, id_=f"{source_name}_row_{idx}")
            documents.append(doc)

        return documents

    async def load_csv(
        self,
        csv_path: str,
        text_columns: Optional[List[str]] = None,
        encoding: str = "utf-8",
        delimiter: str = ",",
        **kwargs,
    ) -> List[Document]:
        """
        Load data from CSV file

        Args:
            csv_path: Path to CSV file
            text_columns: Specific columns to use for text (None for all)
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

        documents = self._dataframe_to_documents(df, filename, text_columns, "csv")

        logger.info(f"Loaded {len(documents)} documents from CSV: {csv_path}")
        return documents

    async def load_excel(
        self,
        excel_path: str,
        sheet_name: Optional[str] = None,
        text_columns: Optional[List[str]] = None,
    ) -> List[Document]:
        """
        Load data from Excel file

        Args:
            excel_path: Path to Excel file
            sheet_name: Specific sheet name (None for all sheets)
            text_columns: Specific columns to use for text (None for all)

        Returns:
            List of Document objects
        """
        logger.info(f"Loading Excel: {excel_path}")
        documents = []

        if sheet_name:
            # Load specific sheet
            df = pd.read_excel(excel_path, sheet_name=sheet_name)
            documents.extend(
                self._dataframe_to_documents(df, sheet_name, text_columns, "excel")
            )
        else:
            # Load all sheets
            excel_file = pd.ExcelFile(excel_path)
            for sheet in excel_file.sheet_names:
                df = pd.read_excel(excel_path, sheet_name=sheet)
                documents.extend(
                    self._dataframe_to_documents(df, sheet, text_columns, "excel")
                )

        logger.info(f"Loaded {len(documents)} documents from Excel: {excel_path}")
        return documents

    async def load_multiple_files(
        self,
        file_paths: List[str],
        auto_detect: bool = True,
        **kwargs,
    ) -> List[Document]:
        """
        Load data from multiple files (CSV and/or Excel)

        Args:
            file_paths: List of file paths
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
                if ext in [".csv", ".txt"]:
                    docs = await self.load_csv(file_path, **kwargs)
                elif ext in [".xlsx", ".xls"]:
                    docs = await self.load_excel(file_path, **kwargs)
                else:
                    logger.warning(f"Skipping unsupported file: {file_path}")
                    continue
            else:
                # Default to CSV if auto-detect is disabled
                docs = await self.load_csv(file_path, **kwargs)

            all_documents.extend(docs)

        logger.info(
            f"Loaded {len(all_documents)} total documents from {len(file_paths)} files"
        )
        return all_documents

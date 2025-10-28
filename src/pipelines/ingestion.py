"""
Ingestion Pipeline
Handles data loading and ingestion into vector store
"""

from typing import List, Optional

from llama_index.core import Document
from llama_index.embeddings.ollama import OllamaEmbedding


from src.core.models import IngestionConfig
from src.providers.vector_stores import VectorStoreFactory
from src.services.ingestion import IngestionService
from src.services.data_loader import DataLoaderService
from src.utils.logger import get_logger

logger = get_logger("pipelines.ingestion")


class IngestionPipeline:
    """
    Pipeline for data ingestion
    Loads data from files and stores in vector database
    """

    def __init__(self, config: Optional[IngestionConfig] = None):
        """
        Initialize ingestion pipeline

        Args:
            config: Ingestion configuration (defaults to IngestionConfig.create_default())
        """
        self.config = config or IngestionConfig.create_default()
        logger.info("Initializing Ingestion Pipeline")

        self.embed_model = OllamaEmbedding(model_name="nomic-embed-text")
        self.vector_store, self.collection = VectorStoreFactory.create(
            self.config.vector_store
        )

        # Initialize services
        self.loader_service = DataLoaderService()
        self.ingestion_service = IngestionService(
            embed_model=self.embed_model,
            vector_store=self.vector_store,
            chunking_config=self.config.chunking,
        )

        logger.info("Ingestion Pipeline initialized successfully")

    async def load_csv(
        self,
        csv_path: str,
        text_columns: Optional[List[str]] = None,
        encoding: str = "utf-8",
        delimiter: str = ",",
        **kwargs,
    ) -> List[Document]:
        """Load data from CSV file"""
        return self.loader_service.load_csv(
            csv_path=csv_path,
            text_columns=text_columns,
            encoding=encoding,
            delimiter=delimiter,
            **kwargs,
        )

    async def load_excel(
        self,
        excel_path: str,
        sheet_name: Optional[str] = None,
        text_columns: Optional[List[str]] = None,
    ) -> List[Document]:
        """Load data from Excel file"""
        return await self.loader_service.load_excel(
            excel_path=excel_path,
            sheet_name=sheet_name,
            text_columns=text_columns,
        )

    async def load_multiple_files(
        self,
        file_paths: List[str],
        auto_detect: bool = True,
        **kwargs,
    ) -> List[Document]:
        """Load data from multiple files"""
        return await self.loader_service.load_multiple_files(
            file_paths=file_paths,
            auto_detect=auto_detect,
            **kwargs,
        )

    async def ingest(self, documents: List[Document]) -> List:
        """Ingest documents into vector store"""
        return await self.ingestion_service.ingest(documents)

    def ingest_sync(self, documents: List[Document]) -> List:
        """Ingest documents into vector store (sync)"""
        return self.ingestion_service.ingest_sync(documents)

    async def ingest_from_csv(
        self,
        csv_path: str,
        text_columns: Optional[List[str]] = None,
        **kwargs,
    ) -> List:
        """Load and ingest CSV in one step"""
        logger.info(f"Loading and ingesting CSV: {csv_path}")
        documents = await self.load_csv(csv_path, text_columns=text_columns, **kwargs)
        return await self.ingest(documents)

    async def ingest_from_excel(
        self,
        excel_path: str,
        sheet_name: Optional[str] = None,
        text_columns: Optional[List[str]] = None,
    ) -> List:
        """Load and ingest Excel in one step"""
        logger.info(f"Loading and ingesting Excel: {excel_path}")
        documents = await self.load_excel(
            excel_path, sheet_name=sheet_name, text_columns=text_columns
        )
        return await self.ingest(documents)

    async def ingest_from_files(self, file_paths: List[str], **kwargs) -> List:
        """Load and ingest multiple files in one step"""
        logger.info(f"Loading and ingesting {len(file_paths)} files")
        documents = await self.load_multiple_files(file_paths, **kwargs)
        return await self.ingest(documents)

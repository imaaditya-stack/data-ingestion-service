"""
Ingestion service
Handles document chunking, embedding, and vector store ingestion
"""

import asyncio
import re
from typing import List

from llama_index.core import Document
from llama_index.core.base.embeddings.base import BaseEmbedding
from llama_index.core.ingestion import IngestionPipeline
from llama_index.core.node_parser import SimpleNodeParser
from llama_index.core.schema import TransformComponent
from llama_index.vector_stores.chroma import ChromaVectorStore

from src.core.models import ChunkingConfig
from src.utils.logger import get_logger


class CustomTransformComponent(TransformComponent):
    """Custom transform component to clean whitespace"""

    def __call__(self, nodes, **kwargs):
        new_nodes = []
        for node in nodes:
            # Clean the text
            cleaned_text = re.sub(r"\s+", " ", node.text).strip()

            # Create a new node with cleaned text
            # Use node.copy() or create new node with updated text
            new_node = node.model_copy(update={"text": cleaned_text})

            new_nodes.append(new_node)

        return new_nodes


logger = get_logger("services.ingestion")


class IngestionService:
    """Service for ingesting documents into vector store"""

    def __init__(
        self,
        embed_model: BaseEmbedding,
        vector_store: ChromaVectorStore,
        chunking_config: ChunkingConfig,
    ):
        """
        Initialize ingestion service

        Args:
            embed_model: Embedding model instance
            vector_store: Vector store instance
            chunking_config: Configuration for text chunking
        """
        self.embed_model = embed_model
        self.vector_store = vector_store
        self.chunking_config = chunking_config
        self.pipeline = self._create_pipeline()

    def _create_pipeline(self) -> IngestionPipeline:
        """Create the ingestion pipeline with transformations"""
        logger.info(
            f"Creating ingestion pipeline with chunk_size={self.chunking_config.chunk_size}, "
            f"chunk_overlap={self.chunking_config.chunk_overlap}"
        )

        return IngestionPipeline(
            transformations=[
                SimpleNodeParser(),
                # SentenceSplitter(
                #     chunk_size=self.chunking_config.chunk_size,
                #     chunk_overlap=self.chunking_config.chunk_overlap,
                # ),
                # CustomTransformComponent(),
                self.embed_model,
            ],
            vector_store=self.vector_store,
        )

    async def ingest(self, documents: List[Document], max_retries: int = 3) -> List:
        """
        Asynchronously ingest documents into vector database with retry logic

        Args:
            documents: List of Document objects to ingest
            max_retries: Maximum number of retry attempts for failed embeddings

        Returns:
            List of created nodes
        """
        logger.info(f"Starting ingestion of {len(documents)} documents")

        for attempt in range(max_retries + 1):
            try:
                nodes = await self.pipeline.arun(documents=documents)

                logger.info(
                    f"Successfully ingested {len(nodes)} nodes into vector store"
                )
                return nodes

            except Exception as e:
                if attempt < max_retries:
                    wait_time = 2**attempt  # Exponential backoff
                    logger.warning(
                        f"Ingestion attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s..."
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"All ingestion attempts failed. Last error: {e}")
                    raise

    async def ingest_in_batches(
        self, documents: List[Document], batch_size: int = 10
    ) -> List:
        """
        Asynchronously ingest documents in smaller batches to reduce load on embedding service

        Args:
            documents: List of Document objects to ingest
            batch_size: Number of documents to process in each batch

        Returns:
            List of all created nodes
        """
        logger.info(
            f"Starting batch ingestion of {len(documents)} documents in batches of {batch_size}"
        )

        all_nodes = []

        # Process documents in batches
        for i in range(0, len(documents), batch_size):
            batch = documents[i : i + batch_size]
            logger.info(
                f"Processing batch {i//batch_size + 1}/{(len(documents) + batch_size - 1)//batch_size} ({len(batch)} documents)"
            )

            try:
                batch_nodes = await self.ingest(batch)
                all_nodes.extend(batch_nodes)

                # Longer delay between batches to let Ollama recover
                if i + batch_size < len(documents):
                    logger.info(
                        "Waiting 2 seconds before next batch to let Ollama recover..."
                    )
                    await asyncio.sleep(2.0)

            except Exception as e:
                logger.error(f"Failed to process batch {i//batch_size + 1}: {e}")
                # Check if it's an Ollama EOF error
                if "EOF" in str(e) or "llama runner process no longer running" in str(
                    e
                ):
                    logger.warning(
                        "Detected Ollama crash. Waiting 10 seconds before retrying..."
                    )
                    await asyncio.sleep(10.0)
                    # Retry the same batch
                    try:
                        batch_nodes = await self.ingest(batch)
                        all_nodes.extend(batch_nodes)
                        logger.info(f"Successfully retried batch {i//batch_size + 1}")
                    except Exception as retry_error:
                        logger.error(
                            f"Retry also failed for batch {i//batch_size + 1}: {retry_error}"
                        )
                        raise
                else:
                    raise

        logger.info(f"Successfully ingested {len(all_nodes)} total nodes in batches")
        return all_nodes

    def ingest_sync(self, documents: List[Document]) -> List:
        """
        Synchronously ingest documents into vector database

        Args:
            documents: List of Document objects to ingest

        Returns:
            List of created nodes
        """
        logger.info(f"Starting sync ingestion of {len(documents)} documents")

        # Run pipeline synchronously with limited workers to avoid overwhelming Ollama
        nodes = self.pipeline.run(documents=documents, num_workers=None)

        logger.info(f"Successfully ingested {len(nodes)} nodes into vector store")
        return nodes

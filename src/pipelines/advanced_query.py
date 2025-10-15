"""
Advanced Query Pipeline
Uses AdvancedQueryService for full control
"""

from typing import Callable, Optional

from llama_index.core.schema import NodeWithScore
from llama_index.core.vector_stores import MetadataFilters
from llama_index.llms.ollama import Ollama
from llama_index.embeddings.ollama import OllamaEmbedding

from src.core.models import QueryConfig
from src.providers.vector_stores import VectorStoreFactory
from src.services.advanced_query_engine.query_engine_service import AdvancedQueryService
from src.services.advanced_query_engine.configs import (
    FilterConfig,
    RerankConfig,
    RerankStrategy,
    ResponseMode,
    RetrievalConfig,
    SynthesisConfig,
)
from src.services.advanced_query_engine.models import QueryResult
from src.utils.logger import get_logger

logger = get_logger("pipelines.advanced_query")


class AdvancedQueryPipeline:
    """
    Advanced query pipeline with full granular control
    Uses AdvancedQueryService for detailed query operations
    """

    def __init__(
        self,
        config: Optional[QueryConfig] = None,
        retrieval_config: Optional[RetrievalConfig] = None,
        filter_config: Optional[FilterConfig] = None,
        rerank_config: Optional[RerankConfig] = None,
        synthesis_config: Optional[SynthesisConfig] = None,
    ):
        """
        Initialize advanced query pipeline

        Args:
            config: Main query configuration
            retrieval_config: Retrieval stage configuration
            filter_config: Filter stage configuration
            rerank_config: Rerank stage configuration
            synthesis_config: Synthesis stage configuration
        """
        self.config = config or QueryConfig.create_default()
        logger.info("Initializing Advanced Query Pipeline")

        # Initialize providers
        self.embed_model = OllamaEmbedding(model_name="nomic-embed-text")
        self.llm = Ollama(
            model="gemma3:latest",
            context_window=8000,
            base_url="http://localhost:11434",
            request_timeout=120,
        )
        self.vector_store, self.collection = VectorStoreFactory.create(
            self.config.vector_store
        )

        # Initialize advanced query service
        self.query_service = AdvancedQueryService(
            vector_store=self.vector_store,
            embed_model=self.embed_model,
            llm=self.llm,
            retrieval_config=retrieval_config,
            filter_config=filter_config,
            rerank_config=rerank_config,
            synthesis_config=synthesis_config,
        )

        logger.info("Advanced Query Pipeline initialized successfully")

    # Expose all methods from AdvancedQueryService
    def __getattr__(self, name):
        """Delegate all unknown methods to query_service"""
        return getattr(self.query_service, name)

    async def query(
        self,
        query: str,
        top_k: int = 10,
        score_threshold: float = 0.5,
        rerank: bool = False,
        rerank_strategy: Optional[RerankStrategy] = None,
        metadata_filters: Optional[MetadataFilters] = None,
        response_mode: Optional[ResponseMode] = None,
        custom_filter: Optional[Callable[[NodeWithScore], bool]] = None,
    ) -> QueryResult:
        """
        Query the advanced query pipeline
        """
        return await self.query_service.query(
            query,
            top_k,
            score_threshold,
            rerank,
            rerank_strategy,
            metadata_filters,
            response_mode,
            custom_filter,
        )

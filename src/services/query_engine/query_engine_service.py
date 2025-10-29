"""
Advanced Query Service with full granular control
"""

import time
from typing import Callable, Dict, List, Optional

from llama_index.core import VectorStoreIndex
from llama_index.core.base.embeddings.base import BaseEmbedding
from llama_index.core.base.llms.base import BaseLLM
from llama_index.core.postprocessor import LLMRerank, SimilarityPostprocessor
from llama_index.core.response_synthesizers import get_response_synthesizer
from llama_index.core.schema import NodeWithScore
from llama_index.core.vector_stores import MetadataFilters

from src.pipelines.query import QueryPipeline
from src.utils.logger import get_logger

from .configs import (
    FilterConfig,
    FilterResult,
    QueryResult,
    RerankConfig,
    RerankResult,
    RerankStrategy,
    ResponseMode,
    RetrievalConfig,
    RetrievalResult,
    SynthesisConfig,
    SynthesisResult,
    TokenUsage,
)

logger = get_logger("services.advanced_query")


class QueryEngineService:
    """
    Advanced query service - business logic orchestrator for querying.

    Responsibilities:
    - Query orchestration and result handling
    - Multi-stage pipeline coordination
    - Response synthesis
    - Error handling

    Does NOT handle:
    - LLM/embedding model configuration (delegated to pipeline)
    - Vector store setup (delegated to pipeline)
    """

    def __init__(
        self,
        pipeline_factory: Optional[QueryPipeline] = None,
        retrieval_config: Optional[RetrievalConfig] = None,
        filter_config: Optional[FilterConfig] = None,
        rerank_config: Optional[RerankConfig] = None,
        synthesis_config: Optional[SynthesisConfig] = None,
    ):
        """
        Initialize advanced query service

        Args:
            pipeline_factory: Query pipeline factory instance (creates if None)
            retrieval_config: Configuration for retrieval stage
            filter_config: Configuration for filtering stage
            rerank_config: Configuration for re-ranking stage
            synthesis_config: Configuration for synthesis stage
        """
        # Create pipeline if not provided
        if pipeline_factory is None:
            pipeline_factory = QueryPipeline()

        self.pipeline_factory = pipeline_factory

        # Get components from pipeline
        self.vector_store = pipeline_factory.vector_store
        self.embed_model = pipeline_factory.embed_model
        self.llm = pipeline_factory.llm

        # Configs
        self.retrieval_config = retrieval_config or RetrievalConfig()
        self.filter_config = filter_config or FilterConfig()
        self.rerank_config = rerank_config or RerankConfig()
        self.synthesis_config = synthesis_config or SynthesisConfig()

        # Create index
        self.index = VectorStoreIndex.from_vector_store(
            vector_store=self.vector_store,
            embed_model=self.embed_model,
        )

        # Custom post-processors at different stages
        self.post_processors: Dict[str, List[Callable]] = {
            "post_retrieval": [],
            "post_filter": [],
            "post_rerank": [],
        }

        logger.info("AdvancedQueryService initialized")

    # ==================== STAGE 1: RETRIEVAL ====================

    async def retrieve(
        self,
        query: str,
        top_k: Optional[int] = None,
        metadata_filters: Optional[MetadataFilters] = None,
    ) -> RetrievalResult:
        """
        Stage 1: Retrieve nodes from vector store

        Args:
            query: Query string
            top_k: Number of nodes to retrieve (defaults to config)
            metadata_filters: Metadata filters to apply

        Returns:
            RetrievalResult with nodes and metadata
        """
        start_time = time.time()

        if top_k is None:
            top_k = self.retrieval_config.default_top_k

        logger.info(f"Retrieving top {top_k} nodes for query: {query[:50]}...")

        # Create retriever
        retriever = self.index.as_retriever(
            similarity_top_k=top_k,
            filters=metadata_filters,
        )

        # Retrieve nodes
        nodes = await retriever.aretrieve(query)

        # Apply post-retrieval processors
        for processor in self.post_processors["post_retrieval"]:
            nodes = processor(nodes)

        retrieval_time = time.time() - start_time

        logger.info(f"Retrieved {len(nodes)} nodes in {retrieval_time:.2f}s")

        return RetrievalResult(
            nodes=nodes,
            total_retrieved=len(nodes),
            retrieval_time=retrieval_time,
        )

    # ==================== STAGE 2: FILTERING ====================

    def filter_nodes(
        self,
        nodes: List[NodeWithScore],
        score_threshold: Optional[float] = None,
        custom_filter: Optional[Callable[[NodeWithScore], bool]] = None,
    ) -> FilterResult:
        """
        Stage 2: Filter nodes by score and custom logic

        Args:
            nodes: Nodes to filter
            score_threshold: Minimum score threshold
            custom_filter: Custom filter function

        Returns:
            FilterResult with filtered and removed nodes
        """
        logger.info(f"Filtering {len(nodes)} nodes")

        if score_threshold is None:
            score_threshold = self.filter_config.default_score_threshold

        original_count = len(nodes)
        filtered_nodes = nodes.copy()
        filter_stats = {"score_threshold": 0, "custom_filter": 0}

        # Apply score threshold filter using SimilarityPostprocessor
        if score_threshold > 0:
            similarity_filter = SimilarityPostprocessor(
                similarity_cutoff=score_threshold
            )
            before_filter = len(filtered_nodes)
            filtered_nodes = similarity_filter.postprocess_nodes(filtered_nodes)
            filter_stats["score_threshold"] = before_filter - len(filtered_nodes)
            logger.info(
                f"Score threshold {score_threshold}: {before_filter} → {len(filtered_nodes)} nodes"
            )

        # Apply custom filter
        if custom_filter:
            before_custom = len(filtered_nodes)
            filtered_nodes = [node for node in filtered_nodes if custom_filter(node)]
            filter_stats["custom_filter"] = before_custom - len(filtered_nodes)
            logger.info(f"Custom filter: {before_custom} → {len(filtered_nodes)} nodes")

        # Apply post-filter processors
        for processor in self.post_processors["post_filter"]:
            filtered_nodes = processor(filtered_nodes)

        # Calculate removed nodes
        removed_count = original_count - len(filtered_nodes)
        removed_nodes = []  # We don't track individual removed nodes for efficiency

        logger.info(f"Filtered to {len(filtered_nodes)} nodes, removed {removed_count}")

        return FilterResult(
            filtered_nodes=filtered_nodes,
            removed_nodes=removed_nodes,
            filter_stats=filter_stats,
        )

    # ==================== STAGE 3: RE-RANKING ====================

    async def rerank_nodes(
        self,
        query: str,
        nodes: List[NodeWithScore],
        strategy: Optional[RerankStrategy] = None,
        top_n: Optional[int] = None,
    ) -> RerankResult:
        """
        Stage 3: Re-rank nodes using various strategies

        Args:
            query: Original query
            nodes: Nodes to rerank
            strategy: Reranking strategy
            top_n: Number of top nodes to keep after reranking

        Returns:
            RerankResult with reranked nodes
        """
        start_time = time.time()

        if strategy is None:
            strategy = self.rerank_config.default_strategy

        if top_n is None:
            top_n = self.rerank_config.default_top_n

        logger.info(f"Reranking {len(nodes)} nodes using strategy: {strategy}")

        # Store original scores
        original_scores = {node.node.node_id: node.score for node in nodes}

        reranked_nodes = nodes.copy()

        # Apply reranking strategy
        if strategy == "llm":
            reranker = LLMRerank(top_n=top_n, llm=self.llm)
            reranked_nodes = reranker.postprocess_nodes(nodes, query_str=query)
        elif strategy == "score_fusion":
            # Combine original similarity with custom scoring
            reranked_nodes = self._apply_score_fusion(nodes, query)
            reranked_nodes = reranked_nodes[:top_n]
        elif strategy == "none":
            reranked_nodes = nodes[:top_n]

        # Apply post-rerank processors
        for processor in self.post_processors["post_rerank"]:
            reranked_nodes = processor(reranked_nodes)

        # Calculate score changes
        new_scores = {node.node.node_id: node.score for node in reranked_nodes}
        score_changes = {
            node_id: new_scores.get(node_id, 0) - original_scores[node_id]
            for node_id in original_scores
            if node_id in new_scores
        }

        rerank_time = time.time() - start_time

        logger.info(f"Reranked to {len(reranked_nodes)} nodes in {rerank_time:.2f}s")

        return RerankResult(
            reranked_nodes=reranked_nodes,
            original_scores=original_scores,
            new_scores=new_scores,
            score_changes=score_changes,
            rerank_time=rerank_time,
            strategy_used=strategy,
        )

    # ==================== STAGE 4: SYNTHESIS ====================

    async def synthesize_response(
        self,
        query: str,
        nodes: List[NodeWithScore],
        response_mode: Optional[ResponseMode] = None,
    ) -> SynthesisResult:
        """
        Stage 4: Generate response using LLM

        Args:
            query: Original query
            nodes: Source nodes for response
            response_mode: Response synthesis mode

        Returns:
            SynthesisResult with response and usage information
        """
        start_time = time.time()

        if response_mode is None:
            response_mode = self.synthesis_config.default_response_mode

        logger.info(
            f"Synthesizing response with {len(nodes)} nodes using mode: {response_mode}"
        )

        # Create synthesizer
        synthesizer = get_response_synthesizer(
            response_mode=response_mode,
            use_async=True,
            llm=self.llm,
        )

        # Generate response
        response = await synthesizer.asynthesize(
            query=query,
            nodes=nodes,
        )

        # Extract token usage
        llm_usage = TokenUsage()
        if hasattr(response, "metadata") and response.metadata:
            llm_usage.prompt_tokens = response.metadata.get("prompt_llm_token_count", 0)
            llm_usage.completion_tokens = response.metadata.get(
                "completion_llm_token_count", 0
            )
            llm_usage.total_tokens = response.metadata.get("total_llm_token_count", 0)

        synthesis_time = time.time() - start_time

        logger.info(
            f"Synthesized response in {synthesis_time:.2f}s, tokens: {llm_usage.total_tokens}"
        )

        return SynthesisResult(
            response=str(response),
            source_nodes=nodes,
            llm_usage=llm_usage,
            synthesis_time=synthesis_time,
            response_metadata=(
                response.metadata if hasattr(response, "metadata") else {}
            ),
        )

    # ==================== FULL PIPELINE (Simple API) ====================

    async def query(
        self,
        query: str,
        top_k: Optional[int] = None,
        score_threshold: Optional[float] = None,
        rerank: bool = False,
        rerank_strategy: Optional[RerankStrategy] = None,
        metadata_filters: Optional[MetadataFilters] = None,
        response_mode: Optional[ResponseMode] = None,
        custom_filter: Optional[Callable[[NodeWithScore], bool]] = None,
    ) -> QueryResult:
        """
        Complete pipeline in one call (convenience method)

        Args:
            query: Query string
            top_k: Number of nodes to retrieve
            score_threshold: Minimum score threshold
            rerank: Whether to rerank nodes
            rerank_strategy: Reranking strategy
            metadata_filters: Metadata filters
            response_mode: Response synthesis mode
            custom_filter: Custom node filter function

        Returns:
            QueryResult with ALL intermediate results
        """
        start_time = time.time()

        logger.info(f"Processing query: {query[:100]}")

        # Stage 1: Retrieve
        retrieval_result = await self.retrieve(
            query=query,
            top_k=top_k,
            metadata_filters=metadata_filters,
        )

        # Stage 2: Filter
        filter_result = self.filter_nodes(
            nodes=retrieval_result.nodes,
            score_threshold=score_threshold,
            custom_filter=custom_filter,
        )

        # Stage 3: Rerank (optional)
        rerank_result = None
        nodes_for_synthesis = filter_result.filtered_nodes

        if rerank and len(filter_result.filtered_nodes) > 0:
            rerank_result = await self.rerank_nodes(
                query=query,
                nodes=filter_result.filtered_nodes,
                strategy=rerank_strategy,
            )
            nodes_for_synthesis = rerank_result.reranked_nodes

        # Stage 4: Synthesize
        synthesis_result = await self.synthesize_response(
            query=query,
            nodes=nodes_for_synthesis,
            response_mode=response_mode,
        )

        total_time = time.time() - start_time

        logger.info(f"Query completed in {total_time:.2f}s")

        return QueryResult(
            query=query,
            retrieval_result=retrieval_result,
            filter_result=filter_result,
            rerank_result=rerank_result,
            synthesis_result=synthesis_result,
            total_time=total_time,
        )

    # ==================== CUSTOM POST-PROCESSORS ====================

    def add_postprocessor(
        self,
        postprocessor: Callable[[List[NodeWithScore]], List[NodeWithScore]],
        stage: str = "post_filter",
    ):
        """
        Add custom post-processor at any stage

        Args:
            postprocessor: Function that takes and returns list of nodes
            stage: Stage to apply processor (post_retrieval, post_filter, post_rerank)
        """
        if stage not in self.post_processors:
            raise ValueError(f"Invalid stage: {stage}")

        self.post_processors[stage].append(postprocessor)
        logger.info(f"Added post-processor to {stage}")

    # ==================== PRIVATE HELPER METHODS ====================

    def _apply_score_fusion(
        self, nodes: List[NodeWithScore], query: str
    ) -> List[NodeWithScore]:
        """Apply score fusion (combine similarity with other signals)"""
        # Placeholder for score fusion logic
        # Could combine semantic similarity with metadata-based scoring
        return sorted(nodes, key=lambda x: x.score, reverse=True)

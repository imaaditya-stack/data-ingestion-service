"""
Configuration classes for AdvancedQueryService
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional

from llama_index.core.schema import NodeWithScore

RerankStrategy = Literal["llm", "score_fusion", "none"]
ResponseMode = Literal["compact", "refine", "tree_summarize", "no_text"]


@dataclass
class RetrievalConfig:
    """Configuration for retrieval stage"""

    default_top_k: int = 10
    cache_embeddings: bool = True


@dataclass
class FilterConfig:
    """Configuration for filtering stage"""

    default_score_threshold: float = 0.0
    max_nodes_per_metadata: Optional[Dict[str, int]] = field(default=None)


@dataclass
class RerankConfig:
    """Configuration for re-ranking stage"""

    default_strategy: RerankStrategy = "none"
    default_top_n: int = 5
    enable_score_fusion: bool = False
    fusion_weight: float = 0.5  # 0 = only rerank, 1 = only original


@dataclass
class SynthesisConfig:
    """Configuration for synthesis stage"""

    default_response_mode: ResponseMode = "compact"
    enable_streaming: bool = False
    track_token_usage: bool = True
    max_context_tokens: int = 8000


@dataclass
class TokenUsage:
    """Token usage information"""

    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


@dataclass
class RetrievalResult:
    """Results from retrieval stage"""

    nodes: List[NodeWithScore]
    total_retrieved: int
    query_embedding: Optional[List[float]] = None
    retrieval_time: float = 0.0


@dataclass
class FilterResult:
    """Results from filtering stage"""

    filtered_nodes: List[NodeWithScore]
    removed_nodes: List[NodeWithScore]
    filter_stats: Dict[str, int] = field(default_factory=dict)


@dataclass
class RerankResult:
    """Results from re-ranking stage"""

    reranked_nodes: List[NodeWithScore]
    original_scores: Dict[str, float] = field(default_factory=dict)
    new_scores: Dict[str, float] = field(default_factory=dict)
    score_changes: Dict[str, float] = field(default_factory=dict)
    rerank_time: float = 0.0
    strategy_used: str = "none"


@dataclass
class SynthesisResult:
    """Results from synthesis stage"""

    response: str
    source_nodes: List[NodeWithScore]
    llm_usage: TokenUsage = field(default_factory=TokenUsage)
    synthesis_time: float = 0.0
    response_metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryResult:
    """Complete query result with all stages"""

    query: str
    retrieval_result: RetrievalResult
    filter_result: FilterResult
    rerank_result: Optional[RerankResult]
    synthesis_result: SynthesisResult
    total_time: float = 0.0

    def get_answer(self) -> str:
        """Convenience: Get just the answer"""
        return self.synthesis_result.response

    def get_sources(self) -> List[NodeWithScore]:
        """Convenience: Get source nodes"""
        return self.synthesis_result.source_nodes

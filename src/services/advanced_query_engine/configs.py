"""
Configuration classes for AdvancedQueryService
"""

from dataclasses import dataclass, field
from typing import Dict, Literal, Optional

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

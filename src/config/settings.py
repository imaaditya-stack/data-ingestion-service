import os
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator

from dotenv import load_dotenv

load_dotenv()


class EmbeddingConfig(BaseModel):
    """Configuration for embedding models"""

    provider: Literal["ollama", "openai"] = Field(default="ollama")
    model: str = Field(default="nomic-embed-text:latest")
    base_url: Optional[str] = Field(default="http://localhost:11434")
    api_key: Optional[str] = Field(default=None)

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, v: Optional[str], info) -> Optional[str]:
        if info.data.get("provider") == "ollama" and not v:
            raise ValueError("base_url is required for ollama provider")
        return v


class LLMConfig(BaseModel):
    """Configuration for LLM models"""

    provider: Literal["ollama", "openai"] = Field(default="ollama")
    model: str = Field(default="gemma3:latest")
    base_url: Optional[str] = Field(default="http://localhost:11434")
    api_key: Optional[str] = Field(default=None)
    temperature: float = Field(default=0.1, ge=0.0, le=2.0)
    request_timeout: float = Field(
        default=120.0, description="Request timeout in seconds"
    )
    context_window: Optional[int] = Field(
        default=8000, description="Context window in tokens"
    )

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, v: Optional[str], info) -> Optional[str]:
        if info.data.get("provider") == "ollama" and not v:
            raise ValueError("base_url is required for ollama provider")
        return v


class VectorStoreConfig(BaseModel):
    """Configuration for vector store"""

    collection_name: str = Field(
        default_factory=lambda: os.getenv("COLLECTION_NAME", "networking-platform")
    )
    persist_path: str = Field(
        default_factory=lambda: os.getenv("VECTOR_DB_PATH", "./chroma_db")
    )
    use_remote: bool = Field(
        default_factory=lambda: os.getenv("VECTOR_DB_USE_REMOTE"),
        description="Whether to connect to Chroma via HTTP client",
    )
    remote_host: Optional[str] = Field(
        default_factory=lambda: os.getenv("VECTOR_DB_REMOTE_HOST")
    )
    remote_port: Optional[int] = Field(
        default_factory=lambda: os.getenv("VECTOR_DB_REMOTE_PORT")
    )


class KafkaConfig(BaseModel):
    """Configuration for Kafka"""

    bootstrap_servers: str = Field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    client_id: str = Field(default="data-ingestion-client")
    acks: str = Field(default="all")
    enable_idempotence: bool = Field(default=True)
    retries: int = Field(default=5)
    max_poll_interval_ms: int = Field(default=300000)
    consumer_group_id: str = Field(default="reindexer-group-v1")


class ChunkingConfig(BaseModel):
    """Configuration for text chunking"""

    chunk_size: int = Field(default=512, gt=0, le=8192)
    chunk_overlap: int = Field(default=50, ge=0)

    @field_validator("chunk_overlap")
    @classmethod
    def validate_overlap(cls, v: int, info) -> int:
        chunk_size = info.data.get("chunk_size", 512)
        if v >= chunk_size:
            raise ValueError("chunk_overlap must be less than chunk_size")
        return v


class IngestionConfig(BaseModel):
    """Configuration for ingestion pipeline"""

    embedding: EmbeddingConfig = Field(default_factory=EmbeddingConfig)
    vector_store: VectorStoreConfig = Field(default_factory=VectorStoreConfig)
    chunking: ChunkingConfig = Field(default_factory=ChunkingConfig)

    class Config:
        """Pydantic config"""

        validate_assignment = True

    @classmethod
    def create_default(cls) -> "IngestionConfig":
        """Create default ingestion configuration"""
        return cls()


class QueryConfig(BaseModel):
    """Configuration for query pipeline"""

    embedding: EmbeddingConfig = Field(default_factory=EmbeddingConfig)
    vector_store: VectorStoreConfig = Field(default_factory=VectorStoreConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)

    class Config:
        """Pydantic config"""

        validate_assignment = True

    @classmethod
    def create_default(cls) -> "QueryConfig":
        """Create default query configuration"""
        return cls()

    # @classmethod
    # def create_openai(
    #     cls,
    #     embedding_model: str = "text-embedding-3-small",
    #     llm_model: str = "gpt-4o-mini",
    # ) -> "QueryConfig":
    #     """Create OpenAI query configuration"""
    #     return cls(
    #         embedding=EmbeddingConfig(provider="openai", model=embedding_model),
    #         llm=LLMConfig(provider="openai", model=llm_model),
    #     )

"""
Embedding model factory
Supports multiple providers: Ollama, OpenAI
"""

from llama_index.core.base.embeddings.base import BaseEmbedding
from llama_index.embeddings.ollama import OllamaEmbedding

# from llama_index.embeddings.openai import OpenAIEmbedding

from src.config.settings import EmbeddingConfig
from src.utils.logger import get_logger

logger = get_logger("providers.embeddings")


class EmbeddingFactory:
    """Factory for creating embedding model instances"""

    @staticmethod
    def create(config: EmbeddingConfig) -> BaseEmbedding:
        """
        Create an embedding model based on configuration

        Args:
            config: Embedding configuration

        Returns:
            Embedding model instance

        Raises:
            ValueError: If provider is not supported
        """
        logger.info(
            f"Creating embedding model: {config.provider}/{config.model}/{config.base_url}"
        )

        if config.provider == "ollama":
            return OllamaEmbedding(
                model_name=config.model,
                base_url=config.base_url,
            )
        # elif config.provider == "openai":
        #     return OpenAIEmbedding(
        #         model=config.model,
        #         api_key=config.api_key,
        #     )
        raise ValueError(f"Unsupported embedding provider: {config.provider}")

    @staticmethod
    def create_ollama(model: str, base_url: str) -> BaseEmbedding:
        """Convenience method for creating Ollama embedding"""
        config = EmbeddingConfig(provider="ollama", model=model, base_url=base_url)
        return EmbeddingFactory.create(config)

    # @staticmethod
    # def create_openai(model: str, api_key: str) -> BaseEmbedding:
    #     """Convenience method for creating OpenAI embedding"""
    #     config = EmbeddingConfig(provider="openai", model=model, api_key=api_key)
    #     return EmbeddingFactory.create(config)

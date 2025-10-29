"""
LLM factory
Supports multiple providers: Ollama, OpenAI
"""

from llama_index.core.base.llms.base import BaseLLM
from llama_index.llms.ollama import Ollama

# from llama_index.llms.openai import OpenAI

from src.config.settings import LLMConfig
from src.utils.logger import get_logger

logger = get_logger("providers.llms")


class LLMFactory:
    """Factory for creating LLM instances"""

    @staticmethod
    def create(config: LLMConfig) -> BaseLLM:
        """
        Create an LLM based on configuration

        Args:
            config: LLM configuration

        Returns:
            LLM instance

        Raises:
            ValueError: If provider is not supported
        """
        logger.info(f"Creating LLM: {config.provider}/{config.model}")

        if config.provider == "ollama":
            return Ollama(
                model=config.model,
                base_url=config.base_url,
                temperature=config.temperature,
                request_timeout=config.request_timeout,
                context_window=config.context_window,
            )
        # elif config.provider == "openai":
        #     return OpenAI(
        #         model=config.model,
        #         temperature=config.temperature,
        #         api_key=config.api_key,
        #     )
        else:
            raise ValueError(f"Unsupported LLM provider: {config.provider}")

    @staticmethod
    def create_ollama(
        model: str,
        base_url: str,
        temperature: float = 0.1,
    ) -> BaseLLM:
        """Convenience method for creating Ollama LLM"""
        config = LLMConfig(
            provider="ollama", model=model, base_url=base_url, temperature=temperature
        )
        return LLMFactory.create(config)

    # @staticmethod
    # def create_openai(
    #     model: str,
    #     temperature: float,
    #     api_key: str,
    # ) -> BaseLLM:
    #     """Convenience method for creating OpenAI LLM"""
    #     config = LLMConfig(
    #         provider="openai", model=model, temperature=temperature, api_key=api_key
    #     )
    #     return LLMFactory.create(config)

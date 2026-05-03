from __future__ import annotations

import os
from collections.abc import Callable
from typing import Literal

from botocore.config import Config as BotocoreConfig
from langchain_aws import ChatBedrockConverse
from pydantic import BaseModel, ConfigDict, SecretStr

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.rate_limiters import InMemoryRateLimiter
from langchain_openai import AzureChatOpenAI, ChatOpenAI

from .credentials import get_aws_credentials


def _require_env(var: str) -> str:
    """Return the value of an environment variable or raise an error if it's not set."""
    value = os.environ.get(var)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{var}' is not set or empty."
        )
    return value


class AzureModelConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    model_name: str
    reasoning_effort: Literal["low", "medium", "high"]
    max_completion_tokens: int = 500000
    max_retries: int = 3
    streaming: bool = False
    rate_limiter: InMemoryRateLimiter | None = None


class BedrockModelConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    model_name: str
    reasoning_effort: Literal["low", "medium", "high"] | None = None
    temperature: float | None = None
    max_tokens: int | None = None
    connect_timeout: float = 999.0
    read_timeout: float = 1800.0
    max_retries: int = 3
    streaming: bool = False
    rate_limiter: InMemoryRateLimiter | None = None


class VLLMModelConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    model_name: str
    base_url: str = "http://localhost:8000/v1"
    max_completion_tokens: int = 500000
    max_retries: int = 3
    streaming: bool = False
    rate_limiter: InMemoryRateLimiter | None = None


def _build_azure(config: AzureModelConfig) -> BaseChatModel:
    return AzureChatOpenAI(
        azure_deployment=config.model_name,
        azure_endpoint=_require_env("AZURE_OPENAI_ENDPOINT"),
        api_version=_require_env("AZURE_OPENAI_API_VERSION"),
        api_key=SecretStr(_require_env("AZURE_OPENAI_API_KEY")),
        max_completion_tokens=config.max_completion_tokens,
        max_retries=config.max_retries,
        streaming=config.streaming,
        reasoning={"effort": config.reasoning_effort, "summary": "detailed"},
        rate_limiter=config.rate_limiter,
    )


def _build_bedrock(config: BedrockModelConfig) -> BaseChatModel:
    transport_config = BotocoreConfig(
        connect_timeout=config.connect_timeout,
        read_timeout=config.read_timeout,
        retries={"max_attempts": config.max_retries},
    )
    kwargs = get_aws_credentials(config=transport_config)
    if "haiku-4-5" in config.model_name:
        additional_fields = {
            "thinking": {"type": "enabled", "budget_tokens": 10000},
        }
    elif "anthropic" in config.model_name:
        additional_fields = {
            "thinking": {"type": "adaptive"},
            "output_config": {"effort": config.reasoning_effort or "medium"},
        }
    else:
        additional_fields = {}
    bedrock_kwargs = {
        "model": config.model_name,
        "disable_streaming": not config.streaming,
        "rate_limiter": config.rate_limiter,
        "additional_model_request_fields": additional_fields,
        **kwargs,
    }
    if config.temperature is not None:
        bedrock_kwargs["temperature"] = config.temperature
    if config.max_tokens is not None:
        bedrock_kwargs["max_tokens"] = config.max_tokens
    return ChatBedrockConverse(**bedrock_kwargs)


def _build_vllm(config: VLLMModelConfig) -> BaseChatModel:
    return ChatOpenAI(
        model=config.model_name,
        base_url=config.base_url,
        max_completion_tokens=config.max_completion_tokens,
        max_retries=config.max_retries,
        streaming=config.streaming,
        rate_limiter=config.rate_limiter,
    )


_BUILDERS: dict[type, Callable[..., BaseChatModel]] = {
    AzureModelConfig: _build_azure,
    BedrockModelConfig: _build_bedrock,
    VLLMModelConfig: _build_vllm,
}


def create_model(
    config: AzureModelConfig | BedrockModelConfig | VLLMModelConfig,
) -> BaseChatModel:
    builder = _BUILDERS.get(type(config))
    if builder is None:
        raise ValueError(f"Unsupported config type: {type(config).__name__}")
    return builder(config)


__all__ = [
    "create_model",
    "AzureModelConfig",
    "BedrockModelConfig",
    "VLLMModelConfig",
]

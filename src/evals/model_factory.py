from __future__ import annotations

from typing import Literal, cast

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.rate_limiters import InMemoryRateLimiter

from models.factory import AzureModelConfig, BedrockModelConfig, create_model


DEFAULT_MODEL_FACTORY_PATH = "evals.model_factory:create_llms"
_VALID_REASONING_EFFORTS = frozenset({"low", "medium", "high"})


def _is_azure_model(model_name: str) -> bool:
    """Return True when *model_name* should be routed to Azure OpenAI."""
    return "gpt" in model_name.lower()


def _build_rate_limiter() -> InMemoryRateLimiter:
    """Build the shared rate limiter used by eval model factories."""
    return InMemoryRateLimiter(requests_per_second=1, max_bucket_size=1)


def create_chat_model(
    *,
    model_name: str,
    reasoning_effort: str | None = None,
    rate_limiter: InMemoryRateLimiter | None = None,
) -> BaseChatModel:
    """Create one chat model for the eval pipeline.

    Args:
        model_name: Target model identifier.
        reasoning_effort: ``"low"``, ``"medium"``, ``"high"``, or ``None``.
        rate_limiter: Optional shared LangChain rate limiter.

    Returns:
        A configured chat model instance.
    """
    if (
        reasoning_effort is not None
        and reasoning_effort not in _VALID_REASONING_EFFORTS
    ):
        raise ValueError(
            "Unsupported reasoning_effort. Expected one of: low, medium, high, or None. "
            f"Received: {reasoning_effort!r}"
        )

    shared_rate_limiter = rate_limiter or _build_rate_limiter()
    effort = (
        cast(Literal["low", "medium", "high"], reasoning_effort)
        if reasoning_effort
        else None
    )

    if _is_azure_model(model_name):
        config = AzureModelConfig(
            model_name=model_name,
            reasoning_effort=effort or "low",
            rate_limiter=shared_rate_limiter,
        )
    else:
        config = BedrockModelConfig(
            model_name=model_name,
            reasoning_effort=effort,
            rate_limiter=shared_rate_limiter,
        )

    return create_model(config)


def create_llms(*configs: tuple[str, str | None]) -> tuple[BaseChatModel, ...]:
    """Create one or more chat models for eval runs.

    Args:
        *configs: Sequence of ``(model_name, reasoning_effort)`` pairs.

    Returns:
        Instantiated chat models in input order.
    """
    shared_rate_limiter = _build_rate_limiter()
    return tuple(
        create_chat_model(
            model_name=model_name,
            reasoning_effort=reasoning_effort,
            rate_limiter=shared_rate_limiter,
        )
        for model_name, reasoning_effort in configs
    )


__all__ = [
    "DEFAULT_MODEL_FACTORY_PATH",
    "create_chat_model",
    "create_llms",
]

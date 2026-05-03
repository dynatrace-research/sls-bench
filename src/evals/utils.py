from __future__ import annotations

import asyncio
import inspect
import importlib
import json
import traceback as tb
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Callable, Iterable

from pydantic import BaseModel

from evals.schemas import ErrorInfo, TokenUsage, TrajectoryEvent

_RETRYABLE_HTTP_STATUS_CODES = {408, 409, 425, 429, 500, 502, 503, 504, 529}
_RETRYABLE_ERROR_NAME_PARTS = (
    "ratelimit",
    "rate_limit",
    "timeout",
    "connection",
    "serviceunavailable",
    "internalserver",
    "servererror",
    "overloaded",
    "temporarilyunavailable",
    "transport",
)
_RETRYABLE_MESSAGE_SNIPPETS = (
    "rate limit",
    "rate-limit",
    "too many requests",
    "retry after",
    "try again in",
    "please try again",
    "service unavailable",
    "temporarily unavailable",
    "temporarily overloaded",
    "internal server error",
    "server error",
    "bad gateway",
    "gateway timeout",
    "connection aborted",
    "connection reset",
    "connection refused",
    "remote protocol error",
    "read timed out",
    "request timed out",
    "operation timed out",
    "timed out",
    "timeout",
)


def utc_now_iso() -> str:
    """Return the current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    """Read a JSONL file into a list of dictionaries.

    Args:
        path: Path to the JSONL file.

    Returns:
        Parsed JSON objects.
    """
    resolved_path = Path(path)
    rows: list[dict[str, Any]] = []
    with resolved_path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid JSON on line {line_number} of {resolved_path}: {exc}"
                ) from exc
            if not isinstance(payload, dict):
                raise ValueError(
                    f"Expected an object on line {line_number} of {resolved_path}."
                )
            rows.append(payload)
    return rows


def _json_default(value: Any) -> Any:
    """Convert common Python objects into JSON-serializable values.

    Args:
        value: Any object.

    Returns:
        A JSON-serializable representation.
    """
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, set):
        return sorted(value)
    if isinstance(value, Exception):
        return {"type": type(value).__name__, "message": str(value)}
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def write_json(path: str | Path, payload: BaseModel | dict[str, Any]) -> None:
    """Write JSON atomically.

    Args:
        path: Output file path.
        payload: Data to write.
    """
    resolved_path = Path(path)
    resolved_path.parent.mkdir(parents=True, exist_ok=True)

    if isinstance(payload, BaseModel):
        data: Any = payload.model_dump(mode="json")
    else:
        data = payload

    with NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=resolved_path.parent,
        delete=False,
        suffix=".tmp",
    ) as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False, default=_json_default)
        handle.write("\n")
        temp_path = Path(handle.name)

    temp_path.replace(resolved_path)


def import_from_string(spec: str) -> Any:
    """Import an object from a `module:object` string.

    Args:
        spec: Import specifier.

    Returns:
        Imported object.
    """
    if ":" not in spec:
        raise ValueError(
            f"Expected an import string like 'package.module:object', got: {spec}"
        )
    module_name, object_name = spec.split(":", maxsplit=1)
    module = importlib.import_module(module_name)
    return getattr(module, object_name)


def build_chat_model(
    model_builder: Callable[..., Any],
    model_name: str,
    reasoning_effort: str | None,
) -> Any:
    """Build a chat model from a flexible factory.

    The eval code currently needs to support two common builder styles:

    1. Generation-style factories such as ``create_llms(*configs)`` that accept
       one or more ``(model_name, reasoning_effort)`` tuples and return a tuple
       of models.
    2. Direct factories that accept ``model_name`` and ``reasoning_effort`` as
       separate positional or keyword arguments and return a single model.

    Args:
        model_builder: Factory callable.
        model_name: Requested model name.
        reasoning_effort: Requested reasoning effort, or ``None`` to use the factory default.

    Returns:
        A single chat model instance.
    """
    spec = (model_name, reasoning_effort)

    try:
        signature = inspect.signature(model_builder)
    except (TypeError, ValueError):
        signature = None

    result: Any
    if signature is None:
        result = model_builder(spec)
    else:
        parameters = list(signature.parameters.values())
        parameter_names = {parameter.name for parameter in parameters}
        has_varargs = any(
            parameter.kind is inspect.Parameter.VAR_POSITIONAL
            for parameter in parameters
        )

        if {"model_name", "reasoning_effort"}.issubset(parameter_names):
            result = model_builder(
                model_name=model_name,
                reasoning_effort=reasoning_effort,
            )
        elif has_varargs or len(parameters) == 1:
            result = model_builder(spec)
        else:
            result = model_builder(model_name, reasoning_effort)

    if isinstance(result, tuple):
        if not result:
            raise ValueError("Model builder returned an empty tuple.")
        return result[0]

    return result


def normalize_exact_match_text(text: str | None) -> str:
    """Normalize text for exact-match evaluation.

    Args:
        text: Input text.

    Returns:
        Trimmed text, or an empty string for missing values.
    """
    return (text or "").strip()


def extract_content_text(
    content: Any,
    *,
    include_non_text_blocks: bool = True,
) -> str | None:
    """Extract text from LangChain message content.

    Args:
        content: Raw message content.
        include_non_text_blocks: Whether to stringify non-text blocks.

    Returns:
        Flattened text when available.
    """
    if content is None:
        return None
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        blocks: list[str] = []
        for item in content:
            if isinstance(item, str):
                blocks.append(item)
                continue
            if isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str):
                    blocks.append(text)
                elif include_non_text_blocks:
                    blocks.append(str(item))
                continue
            if include_non_text_blocks:
                blocks.append(str(item))
        return "\n".join(blocks).strip() or None
    return str(content) if include_non_text_blocks else None


def serialize_message(message: Any) -> dict[str, Any]:
    """Serialize a LangChain message into a JSON-friendly dictionary.

    Args:
        message: LangChain message.

    Returns:
        Serialized message.
    """
    if hasattr(message, "model_dump"):
        try:
            return message.model_dump(mode="json", exclude_none=True)
        except TypeError:
            return message.model_dump(exclude_none=True)
    if hasattr(message, "dict"):
        return message.dict()

    return {
        "type": type(message).__name__,
        "content": extract_content_text(getattr(message, "content", None)),
    }


def message_kind(message: Any) -> str:
    """Map a LangChain message object to a normalized kind.

    Args:
        message: Raw LangChain message.

    Returns:
        Normalized message kind.
    """
    class_name = type(message).__name__.lower()
    if "system" in class_name:
        return "system"
    if "human" in class_name:
        return "human"
    if "ai" in class_name or "assistant" in class_name:
        return "assistant"
    if "tool" in class_name:
        return "tool"
    return "unknown"


def extract_final_answer_from_messages(messages: list[Any]) -> str | None:
    """Extract the last assistant text that is not a tool-call request.

    Args:
        messages: Raw messages.

    Returns:
        Final assistant text, if present.
    """
    for message in reversed(messages):
        if message_kind(message) != "assistant":
            continue
        if getattr(message, "tool_calls", None):
            continue
        content = extract_content_text(
            getattr(message, "content", None),
            include_non_text_blocks=False,
        )
        if content:
            return content
    return None


@dataclass(slots=True)
class MessageTraceSummary:
    """Normalized summary derived from a LangChain message list."""

    final_answer: str | None
    raw_messages: list[dict[str, Any]]
    trajectory_events: list[TrajectoryEvent]
    token_usage: TokenUsage


def summarize_messages(messages: list[Any]) -> MessageTraceSummary:
    """Summarize raw LangChain messages into evaluation artifacts.

    Args:
        messages: Raw message objects.

    Returns:
        Normalized message summary.
    """
    events: list[TrajectoryEvent] = []
    token_usage = TokenUsage()

    for index, message in enumerate(messages):
        usage_metadata = getattr(message, "usage_metadata", None)
        usage = None
        if usage_metadata:
            usage = TokenUsage()
            usage.add_usage_metadata(usage_metadata)
            token_usage.add_usage_metadata(usage_metadata)

        events.append(
            TrajectoryEvent(
                index=index,
                kind=message_kind(message),  # type: ignore[arg-type]
                content=extract_content_text(getattr(message, "content", None)),
                tool_name=getattr(message, "name", None),
                tool_call_id=getattr(message, "tool_call_id", None),
                tool_calls=list(getattr(message, "tool_calls", None) or []),
                usage=usage,
                metadata={
                    "message_class": type(message).__name__,
                    "response_metadata": getattr(message, "response_metadata", None),
                },
            )
        )

    return MessageTraceSummary(
        final_answer=extract_final_answer_from_messages(messages),
        raw_messages=[serialize_message(message) for message in messages],
        trajectory_events=events,
        token_usage=token_usage,
    )


def error_info_from_exception(exc: BaseException) -> ErrorInfo:
    """Create a serializable error payload from an exception.

    Args:
        exc: Exception instance.

    Returns:
        Error information.
    """
    return ErrorInfo(
        error_type=type(exc).__name__,
        message=str(exc),
        traceback="".join(tb.format_exception(type(exc), exc, exc.__traceback__)),
    )


def truncate_text(text: str, max_chars: int = 12000) -> str:
    """Truncate long text while preserving the start and the end.

    Args:
        text: Input text.
        max_chars: Maximum output length.

    Returns:
        Truncated text.
    """
    if len(text) <= max_chars:
        return text

    head = max_chars // 2
    tail = max_chars - head
    return (
        f"{text[:head]}\n\n... [truncated {len(text) - max_chars} chars] ...\n\n"
        f"{text[-tail:]}"
    )


def extract_final_text_answer(response: Any) -> str:
    content = response.content
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                return block.get("text", "")
    return ""


def summarize_exception(exc: BaseException | None) -> dict[str, Any] | None:
    """Create a small JSON-friendly summary for an exception.

    Args:
        exc: Exception instance or `None`.

    Returns:
        Small summary payload, or `None`.
    """
    if exc is None:
        return None
    summary: dict[str, Any] = {
        "type": type(exc).__name__,
        "message": str(exc),
    }
    status_code = get_exception_status_code(exc)
    if status_code is not None:
        summary["status_code"] = status_code
    return summary


def format_exception_summary(exc: BaseException | None) -> str:
    """Build a concise exception summary string for logs.

    Args:
        exc: Exception instance.

    Returns:
        Compact string summary.
    """
    summary = summarize_exception(exc)
    if summary is None:
        return "unknown error"
    if "status_code" in summary:
        return (
            f"{summary['type']}({summary['message']}, "
            f"status_code={summary['status_code']})"
        )
    return f"{summary['type']}({summary['message']})"


def iter_exception_chain(
    exc: BaseException,
    *,
    max_depth: int = 8,
) -> Iterable[BaseException]:
    """Iterate through an exception and its causal chain.

    Args:
        exc: Root exception.
        max_depth: Maximum chain depth.

    Yields:
        Exceptions from outermost to innermost.
    """
    current: BaseException | None = exc
    seen: set[int] = set()
    depth = 0
    while current is not None and depth < max_depth and id(current) not in seen:
        yield current
        seen.add(id(current))
        depth += 1
        if current.__cause__ is not None:
            current = current.__cause__
            continue
        if current.__context__ is not None and not getattr(
            current, "__suppress_context__", False
        ):
            current = current.__context__
            continue
        current = None


def get_exception_status_code(exc: BaseException) -> int | None:
    """Extract an HTTP-like status code when available.

    Args:
        exc: Exception instance.

    Returns:
        Parsed status code, or `None`.
    """
    candidates = [
        getattr(exc, "status_code", None),
        getattr(exc, "http_status", None),
        getattr(getattr(exc, "response", None), "status_code", None),
    ]
    for candidate in candidates:
        if candidate is None:
            continue
        try:
            return int(candidate)
        except (TypeError, ValueError):
            continue
    return None


def is_retryable_transient_model_error(exc: BaseException) -> bool:
    """Heuristically decide whether a model/provider error is transient.

    This is intentionally conservative for non-provider errors. It is used for
    retrying LLM calls and should cover common provider exceptions such as rate
    limits, transient timeouts, connection failures, and 5xx responses.

    Args:
        exc: Exception to inspect.

    Returns:
        `True` when the error looks transient and retryable.
    """
    for candidate in iter_exception_chain(exc):
        if isinstance(candidate, (asyncio.TimeoutError, TimeoutError, ConnectionError)):
            return True

        status_code = get_exception_status_code(candidate)
        if status_code in _RETRYABLE_HTTP_STATUS_CODES:
            return True

        lowered_name = type(candidate).__name__.lower()
        if any(part in lowered_name for part in _RETRYABLE_ERROR_NAME_PARTS):
            return True

        message = str(candidate).lower()
        if any(snippet in message for snippet in _RETRYABLE_MESSAGE_SNIPPETS):
            return True

    return False

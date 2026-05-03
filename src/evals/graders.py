from __future__ import annotations

import asyncio
import json
import re
import logging
from langchain_core.messages import HumanMessage, SystemMessage
from tenacity import AsyncRetrying, retry_if_exception, stop_after_attempt
from tenacity import wait_exponential, wait_random
from typing import Any

from evals.agents import ModelBuilder, resolve_model_builder
from evals.schemas import GraderResult, TaskSpec
from evals.utils import (
    build_chat_model,
    extract_final_text_answer,
    format_exception_summary,
    import_from_string,
    is_retryable_transient_model_error,
    normalize_exact_match_text,
)


logger = logging.getLogger(__name__)

_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*\n(.*?)\n\s*```", re.DOTALL)


def _parse_json_permissive(text: str) -> Any:
    """Parse JSON that may be wrapped in markdown fences or surrounded by extra text."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    m = _JSON_FENCE_RE.search(text)
    if m:
        return json.loads(m.group(1))
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end > start:
        return json.loads(text[start : end + 1])
    raise json.JSONDecodeError("No JSON object found", text, 0)


class BaseGrader:
    """Base class for graders."""

    name = "base"

    async def grade(self, task: TaskSpec, prediction: str | None) -> GraderResult:
        """Grade one prediction.

        Args:
            task: Task specification.
            prediction: Predicted answer.

        Returns:
            Grader result.
        """
        raise NotImplementedError


class ExactMatchGrader(BaseGrader):
    """Strict exact-match grader with leading/trailing whitespace trimming."""

    name = "exact_match"

    async def grade(self, task: TaskSpec, prediction: str | None) -> GraderResult:
        """Evaluate exact match.

        Args:
            task: Task specification.
            prediction: Predicted answer.

        Returns:
            Grader result.
        """
        expected = normalize_exact_match_text(task.ground_truth_answer)
        observed = normalize_exact_match_text(prediction)
        passed = observed == expected
        return GraderResult(
            name=self.name,
            score=1.0 if passed else 0.0,
            result={
                "passed": passed,
                "explanation": (
                    "Prediction matches the reference answer exactly after trimming."
                    if passed
                    else "Prediction does not exactly match the reference answer."
                ),
            },
        )


class RubricLLMGrader(BaseGrader):
    """LLM-based semantic grader for ``(question, ground_truth, prediction)`` tuples."""

    name = "llm_rubric"

    def __init__(
        self,
        *,
        model_builder: ModelBuilder,
        model_name: str,
        reasoning_effort: str | None,
        system_prompt: str,
        timeout_seconds: float = 90.0,
        max_retries: int = 3,
    ) -> None:
        """Initialize the rubric grader.

        Args:
            model_builder: Callable that builds a judge model.
            model_name: Judge model name.
            reasoning_effort: Judge reasoning effort, or ``None`` to use the factory default.
            system_prompt: Judge system prompt text.
            timeout_seconds: Timeout per judge call.
            max_retries: Retry budget per judged answer.
        """
        self.model_builder = model_builder
        self.model_name = model_name
        self.reasoning_effort = reasoning_effort
        self.system_prompt = system_prompt
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self._model: Any | None = None

    @property
    def model(self) -> Any:
        """Lazily build the judge model."""
        if self._model is None:
            self._model = build_chat_model(
                self.model_builder,
                self.model_name,
                self.reasoning_effort,
            )
        return self._model

    async def grade(self, task: TaskSpec, prediction: str | None) -> GraderResult:
        """Evaluate a prediction with an LLM rubric.

        Args:
            task: Task specification.
            prediction: Predicted answer.

        Returns:
            Grader result.
        """
        if not (prediction or "").strip():
            return GraderResult(
                name=self.name,
                score=0.0,
                result={
                    "passed": False,
                    "explanation": "Prediction is empty.",
                },
            )

        async def _judge_once() -> GraderResult:
            return await self._judge_once(task, prediction or "")

        def _log_before_sleep(retry_state: Any) -> None:
            error: BaseException | None = None
            if retry_state.outcome is not None and retry_state.outcome.failed:
                error = retry_state.outcome.exception()
            sleep_seconds = float(
                getattr(getattr(retry_state, "next_action", None), "sleep", 0.0) or 0.0
            )
            logger.warning(
                "[%s] Rubric judge attempt %d failed with %s; retrying in %.1fs",
                task.task_id,
                int(retry_state.attempt_number),
                format_exception_summary(error),
                sleep_seconds,
            )

        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self.max_retries + 1),
            wait=wait_exponential(multiplier=2, min=20, max=60) + wait_random(0, 10),
            retry=retry_if_exception(is_retryable_transient_model_error),
            before_sleep=_log_before_sleep,
            reraise=True,
        ):
            with attempt:
                return await asyncio.wait_for(
                    _judge_once(),
                    timeout=self.timeout_seconds,
                )

        raise AssertionError("Unreachable: tenacity should either return or raise.")

    async def _judge_once(self, task: TaskSpec, prediction: str) -> GraderResult:
        """Run a single judge call.

        Args:
            task: Task specification.
            prediction: Predicted answer.

        Returns:
            Parsed grader result.
        """
        user_prompt = (
            f"Question:\n{task.question}\n\n---\n\n"
            f"Ground truth answer:\n{task.ground_truth_answer}\n\n---\n\n"
            f"Predicted answer:\n{prediction}"
        )
        response = await self.model.ainvoke(
            [
                SystemMessage(content=self.system_prompt),
                HumanMessage(content=user_prompt),
            ]
        )
        text = extract_final_text_answer(response)
        parsed = _parse_json_permissive(text)

        if parsed is None or not isinstance(parsed, dict):
            logger.warning(
                "[%s] Rubric judge returned non-JSON response text=%r",
                task.task_id,
                text,
            )
            raise ValueError(f"Judge did not return valid JSON: {text}")

        return GraderResult(
            name=self.name,
            result=parsed,
            score=None,
            metadata={
                "judge_model_name": self.model_name,
                "judge_reasoning_effort": self.reasoning_effort,
            },
        )


BUILTIN_GRADERS: dict[str, type[BaseGrader]] = {
    ExactMatchGrader.name: ExactMatchGrader,
    RubricLLMGrader.name: RubricLLMGrader,
}


def resolve_grader_class(spec: str) -> type[BaseGrader]:
    """Resolve a grader class from a registry name or import path.

    Args:
        spec: Built-in name or ``module:object`` path.

    Returns:
        Grader class.
    """
    if spec in BUILTIN_GRADERS:
        return BUILTIN_GRADERS[spec]
    resolved = import_from_string(spec)
    if not isinstance(resolved, type):
        raise TypeError(f"Expected a grader class for {spec}, got {type(resolved)}")
    return resolved


def build_graders(
    grader_specs: list[str],
    *,
    model_factory_path: str | None,
    model_name: str | None,
    reasoning_effort: str | None,
    system_prompt: str | None,
    timeout_seconds: float,
    max_retries: int,
) -> list[BaseGrader]:
    """Instantiate graders for a grading run.

    Args:
        grader_specs: Requested graders.
        model_factory_path: Import path for the judge model builder.
        model_name: Judge model name.
        reasoning_effort: Judge reasoning effort.
        system_prompt: Judge system prompt text.
        timeout_seconds: Judge timeout.
        max_retries: Judge retry budget.

    Returns:
        Instantiated graders.
    """
    graders: list[BaseGrader] = []
    model_builder: ModelBuilder | None = None

    if any(spec == RubricLLMGrader.name for spec in grader_specs):
        if not model_factory_path or not model_name or not system_prompt:
            raise ValueError(
                "LLM rubric grading requires model_factory_path, model_name, and system_prompt."
            )
        model_builder = resolve_model_builder(model_factory_path)

    for spec in grader_specs:
        grader_class = resolve_grader_class(spec)
        if grader_class is ExactMatchGrader:
            graders.append(ExactMatchGrader())
        elif grader_class is RubricLLMGrader:
            assert model_builder is not None
            graders.append(
                RubricLLMGrader(
                    model_builder=model_builder,
                    model_name=model_name or "",
                    reasoning_effort=reasoning_effort,
                    system_prompt=system_prompt or "",
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                )
            )
        else:
            graders.append(grader_class())

    return graders

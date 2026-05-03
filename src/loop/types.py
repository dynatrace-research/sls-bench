from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class GenerationResult[TOutput]:
    output: TOutput
    metadata: dict[str, Any] | None = None


@dataclass
class VerificationResult:
    passed: bool
    feedback: str | None = None
    metadata: dict[str, Any] | None = None


@dataclass
class LoopResult[T]:
    output: T
    passed: bool
    history: list[dict[str, Any]] = field(default_factory=list)


class GeneratorFn[TInput, TOutput](Protocol):
    async def __call__(
        self, input: TInput, previous_output: TOutput | None, feedback: str | None
    ) -> GenerationResult[TOutput]: ...


class VerifierFn[TInput, TOutput](Protocol):
    async def __call__(self, input: TInput, output: TOutput) -> VerificationResult: ...

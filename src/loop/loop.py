from __future__ import annotations

import asyncio
import logging
from functools import partial
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from tenacity import AsyncRetrying, RetryError, stop_after_attempt, wait_exponential

from loop.types import LoopResult
from loop.types import GeneratorFn, GenerationResult, VerificationResult, VerifierFn


logger = logging.getLogger(__name__)


@dataclass
class LoopConfig:
    max_correction_rounds: int = 10
    max_reruns: int = 0
    verifier_passes_required: int = 1
    concurrency: int = 10
    generator_timeout: float | None = None
    verifier_timeout: float | None = None
    retry_backoff_max: float = 60.0
    retry_max_attempts: int = 10

    def __post_init__(self) -> None:
        if self.max_correction_rounds < 0:
            raise ValueError("max_correction_rounds must be >= 0")
        if self.max_reruns < 0:
            raise ValueError("max_reruns must be >= 0")
        if self.verifier_passes_required < 1:
            raise ValueError("verifier_passes_required must be >= 1")
        if self.concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        if self.retry_max_attempts < 1:
            raise ValueError("retry_max_attempts must be >= 1")


class Loop[TInput, TOutput]:
    def __init__(
        self,
        generator: GeneratorFn[TInput, TOutput],
        verifier: VerifierFn[TInput, TOutput],
        config: LoopConfig | None = None,
    ) -> None:
        self.generator = generator
        self.verifier = verifier
        self.config = config or LoopConfig()
        self._semaphore = asyncio.Semaphore(self.config.concurrency)

    async def run(
        self,
        input: TInput,
        input_id: str | None = None,
        post_run_hook: Callable[[LoopResult[TOutput]], None] | None = None,
    ) -> LoopResult[TOutput]:
        async with self._semaphore:
            input_id = input_id or "-"
            result = await self._run(input, input_id)
        if post_run_hook is not None:
            post_run_hook(result)
        return result

    async def _run(self, input: TInput, input_id: str) -> LoopResult[TOutput]:
        cfg = self.config
        history: list[dict[str, Any]] = []
        last_gen_result: GenerationResult[TOutput]
        total_reruns = cfg.max_reruns + 1
        total_attempts = cfg.max_correction_rounds + 1

        logger.info("[%s] Loop started", input_id)

        for rerun in range(total_reruns):
            rerun_no = rerun + 1
            previous_output: TOutput | None = None
            feedback: str | None = None

            for round_idx in range(total_attempts):
                attempt_no = round_idx + 1
                logger.info(
                    "[%s] Attempt %d/%d started (rerun=%d/%d)",
                    input_id,
                    attempt_no,
                    total_attempts,
                    rerun_no,
                    total_reruns,
                )
                logger.info("[%s] Generator started", input_id)
                try:
                    gen_result = await self._retry(
                        partial(self.generator, input, previous_output, feedback),
                        timeout=cfg.generator_timeout,
                        label=f"[{input_id}] Generator",
                    )
                except RetryError as exc:
                    logger.warning(
                        "[%s] Generator exhausted retries (%s)",
                        input_id,
                        exc.last_attempt.exception(),
                    )
                    history.append(
                        {
                            "rerun": rerun,
                            "round": round_idx,
                            "passed": False,
                            "error": str(exc.last_attempt.exception()),
                            "error_type": type(exc.last_attempt.exception()).__name__,
                            "stage": "generator",
                        }
                    )
                    continue
                last_gen_result = gen_result
                logger.info("[%s] Generator finished", input_id)

                try:
                    ver_result = await self._verify_passes(
                        input, gen_result.output, input_id
                    )
                except RetryError as exc:
                    logger.warning(
                        "[%s] Verifier exhausted retries (%s)",
                        input_id,
                        exc.last_attempt.exception(),
                    )
                    history.append(
                        {
                            "rerun": rerun,
                            "round": round_idx,
                            "passed": False,
                            "error": str(exc.last_attempt.exception()),
                            "error_type": type(exc.last_attempt.exception()).__name__,
                            "stage": "verifier",
                        }
                    )
                    feedback = (
                        feedback
                        + "\n\nVerifier Exception Feedback (MUST FIX IF POSSIBLE):"
                        + str(exc)
                        if feedback
                        else str(exc)
                    )
                    continue

                history.append(
                    {
                        "rerun": rerun,
                        "round": round_idx,
                        "passed": ver_result.passed,
                        **(gen_result.metadata or {}),
                        **(ver_result.metadata or {}),
                    }
                )

                if ver_result.passed:
                    logger.info(
                        "[%s] Loop successfully finished on attempt %d/%d (rerun=%d/%d)",
                        input_id,
                        attempt_no,
                        total_attempts,
                        rerun_no,
                        total_reruns,
                    )
                    return LoopResult(
                        output=gen_result.output,
                        passed=True,
                        history=history,
                    )

                previous_output = gen_result.output
                feedback = ver_result.feedback
                logger.info(
                    "[%s] Attempt %d/%d failed (rerun=%d/%d)",
                    input_id,
                    attempt_no,
                    total_attempts,
                    rerun_no,
                    total_reruns,
                )

            logger.info(
                "[%s] Rerun %d/%d failed (exhausted all attempts)",
                input_id,
                rerun_no,
                total_reruns,
            )

        logger.info(
            "[%s] Loop failed (exhausted all attempts and re-runs)",
            input_id,
        )
        return LoopResult(
            output=last_gen_result.output,  # type: ignore[possibly-undefined]
            passed=False,
            history=history,
        )

    async def _verify_passes(
        self, input: TInput, output: TOutput, input_id: str
    ) -> VerificationResult:
        cfg = self.config
        last: VerificationResult = VerificationResult(passed=False)
        for pass_idx in range(cfg.verifier_passes_required):
            logger.info(
                "[%s] Verifier pass %d/%d started",
                input_id,
                pass_idx + 1,
                cfg.verifier_passes_required,
            )
            last = await self._retry(
                partial(self.verifier, input, output),
                timeout=cfg.verifier_timeout,
                label=f"[{input_id}] Verifier[{pass_idx + 1}/{cfg.verifier_passes_required}]",
            )
            logger.info(
                "[%s] Verifier pass %d/%d finished (passed=%s)",
                input_id,
                pass_idx + 1,
                cfg.verifier_passes_required,
                last.passed,
            )
            if not last.passed:
                return last
        return last

    async def _retry[R](
        self,
        coro_factory: Callable[[], Awaitable[R]],
        timeout: float | None,
        label: str,
    ) -> R:
        def _before_sleep(retry_state: Any) -> None:
            exc = retry_state.outcome.exception()
            logger.warning(
                "%s call %d failed (%s), retrying in %.1fs",
                label,
                retry_state.attempt_number,
                exc,
                retry_state.next_action.sleep,
            )

        async for attempt in AsyncRetrying(
            wait=wait_exponential(
                multiplier=1, min=30, max=self.config.retry_backoff_max
            ),
            stop=stop_after_attempt(self.config.retry_max_attempts),
            before_sleep=_before_sleep,
        ):
            with attempt:
                coro = coro_factory()
                if timeout is not None:
                    return await asyncio.wait_for(coro, timeout=timeout)
                return await coro
        raise AssertionError(
            "unreachable: RetryError should be raised before this point"
        )

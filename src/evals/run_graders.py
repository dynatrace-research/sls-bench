from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential, wait_random

from evals.graders import build_graders
from evals.model_factory import DEFAULT_MODEL_FACTORY_PATH
from evals.schemas import GradingMetadata, TaskRunRecord
from evals.utils import format_exception_summary, utc_now_iso, write_json


logger = logging.getLogger(__name__)

_NOISY_DEPENDENCY_LOGGERS = (
    "httpx",
    "httpcore",
    "openai",
)


class GraderRunConfig(BaseModel):
    """Configuration for the grader runner."""

    run_dir: str
    graders: list[str] = Field(default_factory=lambda: ["exact_match"])
    concurrency: int = 10
    overwrite: bool = False
    judge_model_factory_path: str | None = None
    judge_model_name: str | None = None
    judge_reasoning_effort: str | None = None
    judge_system_prompt_path: str | None = None
    judge_timeout_seconds: float = 180.0
    judge_max_retries: int = 3
    grader_max_retries: int = 3
    log_level: str = "INFO"


def build_arg_parser() -> argparse.ArgumentParser:
    """Create the CLI parser."""
    parser = argparse.ArgumentParser(description="Run graders on saved agent results.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument(
        "--grader",
        dest="graders",
        action="append",
        required=True,
        help="Repeatable. Built-ins: exact_match, llm_rubric.",
    )
    parser.add_argument("--concurrency", type=int, default=9)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument(
        "--judge-model-factory",
        dest="judge_model_factory_path",
        default=None,
        help=(
            "Import path like 'my_project.models:create_llms'. "
            f"Defaults to {DEFAULT_MODEL_FACTORY_PATH} when llm_rubric is used."
        ),
    )
    parser.add_argument("--judge-model-name", default=None)
    parser.add_argument("--judge-reasoning-effort", default=None)
    parser.add_argument(
        "--judge-system-prompt-path",
        default=None,
        help="Path to the rubric judge system prompt file. Required when llm_rubric is used.",
    )
    parser.add_argument("--judge-timeout-seconds", type=float, default=180.0)
    parser.add_argument("--judge-max-retries", type=int, default=3)
    parser.add_argument("--grader-max-retries", type=int, default=3)
    parser.add_argument(
        "--log-level",
        type=str.upper,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
    )
    return parser


async def run_graders(config: GraderRunConfig) -> Path:
    """Run graders across all saved task files.

    Args:
        config: Grader configuration.

    Returns:
        The graded run directory.
    """
    run_dir = Path(config.run_dir)
    task_files = sorted(
        path
        for path in run_dir.glob("**/*.json")
        if path.name != "run_manifest.json" and path.name != "grading_manifest.json"
    )

    logger.info(
        "Starting grading run run_dir=%s task_files=%d graders=%s",
        run_dir,
        len(task_files),
        ",".join(config.graders),
    )

    judge_model_factory_path = _resolve_judge_model_factory_path(config)
    judge_system_prompt_path, judge_system_prompt = _load_judge_system_prompt(config)

    graders = build_graders(
        config.graders,
        model_factory_path=judge_model_factory_path,
        model_name=config.judge_model_name,
        reasoning_effort=config.judge_reasoning_effort,
        system_prompt=judge_system_prompt,
        timeout_seconds=config.judge_timeout_seconds,
        max_retries=config.judge_max_retries,
    )

    semaphore = asyncio.Semaphore(config.concurrency)

    async def _handle_file(path: Path) -> None:
        async with semaphore:
            logger.info("Grading task file path=%s", path)
            await _grade_task_file(
                path,
                graders,
                config,
                judge_model_factory_path,
                judge_system_prompt_path,
            )

    await asyncio.gather(*[_handle_file(path) for path in task_files])

    manifest = {
        "schema_version": "1.0",
        "graded_at_utc": utc_now_iso(),
        "graders": config.graders,
        "judge_model_factory_path": judge_model_factory_path,
        "judge_model_name": config.judge_model_name,
        "judge_reasoning_effort": config.judge_reasoning_effort,
        "judge_system_prompt_path": (
            str(judge_system_prompt_path)
            if judge_system_prompt_path is not None
            else None
        ),
        "judge_timeout_seconds": config.judge_timeout_seconds,
        "judge_max_retries": config.judge_max_retries,
        "task_file_count": len(task_files),
    }
    write_json(run_dir / "grading_manifest.json", manifest)
    logger.info("Completed grading run run_dir=%s", run_dir)
    return run_dir


async def _grade_with_retry(
    grader: Any,
    task: Any,
    prediction: str | None,
    max_retries: int,
) -> Any:
    """Run a grader with tenacity retry on any failure."""

    def _log_before_sleep(retry_state: Any) -> None:
        error: BaseException | None = None
        if retry_state.outcome is not None and retry_state.outcome.failed:
            error = retry_state.outcome.exception()
        sleep_seconds = float(
            getattr(getattr(retry_state, "next_action", None), "sleep", 0.0) or 0.0
        )
        logger.warning(
            "[%s] Grader %s attempt %d failed with %s; retrying in %.1fs",
            task.task_id,
            grader.name,
            int(retry_state.attempt_number),
            format_exception_summary(error),
            sleep_seconds,
        )

    async for attempt in AsyncRetrying(
        stop=stop_after_attempt(max_retries + 1),
        wait=wait_exponential(multiplier=2, min=5, max=60) + wait_random(0, 5),
        before_sleep=_log_before_sleep,
        reraise=True,
    ):
        with attempt:
            return await grader.grade(task, prediction)

    raise AssertionError("Unreachable: tenacity should either return or raise.")


async def _grade_task_file(
    path: Path,
    graders: list[Any],
    config: GraderRunConfig,
    judge_model_factory_path: str | None,
    judge_system_prompt_path: Path | None,
) -> None:
    """Grade one saved task file in place.

    Args:
        path: Task result file path.
        graders: Instantiated graders.
        config: Grader configuration.
        judge_system_prompt_path: Loaded rubric system prompt path.
    """
    record = TaskRunRecord.model_validate_json(path.read_text(encoding="utf-8"))

    for trial in record.trials:
        if trial.status != "completed":
            logger.debug(
                "[%s] Skipping trial %d with status=%s",
                record.task.task_id,
                trial.trial_index + 1,
                trial.status,
            )
            continue
        requested_names = {grader.name for grader in graders}
        if not _trial_needs_grading(
            trial.grader_results, requested_names, config.overwrite
        ):
            logger.debug(
                "[%s] Skipping trial %d because requested graders already exist and grader_results is non-empty",
                record.task.task_id,
                trial.trial_index + 1,
            )
            continue

        for grader in graders:
            if grader.name in trial.grader_results and not config.overwrite:
                logger.debug(
                    "[%s] Skipping grader=%s for trial %d because result already exists",
                    record.task.task_id,
                    grader.name,
                    trial.trial_index + 1,
                )
                continue
            logger.info(
                "[%s] Running grader=%s for trial %d",
                record.task.task_id,
                grader.name,
                trial.trial_index + 1,
            )
            result = await _grade_with_retry(
                grader,
                record.task,
                trial.final_answer,
                config.grader_max_retries,
            )
            trial.grader_results[grader.name] = result
            logger.info(
                "[%s] Completed grader=%s for trial %d",
                record.task.task_id,
                grader.name,
                trial.trial_index + 1,
            )

    record.grading = GradingMetadata(
        graders=[grader.name for grader in graders],
        graded_at_utc=utc_now_iso(),
        judge_model_factory_path=judge_model_factory_path,
        judge_model_name=config.judge_model_name,
        judge_reasoning_effort=config.judge_reasoning_effort,
        judge_system_prompt_path=(
            str(judge_system_prompt_path)
            if judge_system_prompt_path is not None
            else None
        ),
    )
    record.execution.updated_at_utc = utc_now_iso()
    write_json(path, record)


def _trial_needs_grading(
    grader_results: dict[str, Any],
    requested_names: set[str],
    overwrite: bool,
) -> bool:
    if overwrite:
        return True
    if not grader_results:
        return True
    existing_names = set(grader_results)
    return not requested_names.issubset(existing_names)


def _resolve_judge_model_factory_path(config: GraderRunConfig) -> str | None:
    """Resolve the judge model factory path for the current grading run."""
    if config.judge_model_factory_path:
        return config.judge_model_factory_path
    if "llm_rubric" not in config.graders:
        return None
    return DEFAULT_MODEL_FACTORY_PATH


def _load_judge_system_prompt(
    config: GraderRunConfig,
) -> tuple[Path | None, str | None]:
    """Load the rubric judge system prompt for the current grading run."""
    if "llm_rubric" not in config.graders:
        return None, None
    if not config.judge_system_prompt_path:
        raise ValueError(
            "LLM rubric grading requires --judge-system-prompt-path to be specified."
        )

    prompt_path = Path(config.judge_system_prompt_path).expanduser()
    if not prompt_path.is_file():
        raise FileNotFoundError(f"Judge system prompt file not found: {prompt_path}")

    prompt_text = prompt_path.read_text(encoding="utf-8").strip()
    if not prompt_text:
        raise ValueError(f"Judge system prompt file is empty: {prompt_path}")

    return prompt_path, prompt_text


def _configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        force=True,
    )

    for logger_name in _NOISY_DEPENDENCY_LOGGERS:
        logging.getLogger(logger_name).setLevel(logging.WARNING)


def main() -> None:
    """CLI entrypoint."""
    parser = build_arg_parser()
    args = parser.parse_args()
    config = GraderRunConfig(**vars(args))

    _configure_logging(config.log_level)

    logger.info("=== Grader evaluation run started ===")
    graded_dir = asyncio.run(run_graders(config))
    logger.info("=== Grader evaluation run finished ===")
    print(graded_dir)


if __name__ == "__main__":
    main()

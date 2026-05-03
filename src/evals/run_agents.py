from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from tqdm.asyncio import tqdm

from evals.agents import (
    BaseTaskAgent,
    execute_task_agent,
    resolve_agent_class,
    resolve_model_builder,
)
from evals.model_factory import DEFAULT_MODEL_FACTORY_PATH
from evals.schemas import (
    AgentMetadata,
    ExecutionMetadata,
    TaskRunRecord,
    TaskSummary,
    TaskSpec,
    TrialRecord,
)
from evals.tasks import load_tasks
from evals.utils import (
    error_info_from_exception,
    format_exception_summary,
    is_retryable_transient_model_error,
    utc_now_iso,
    write_json,
)


logger = logging.getLogger(__name__)

_NOISY_DEPENDENCY_LOGGERS = ("httpx", "httpcore", "openai", "langchain_aws")


class AgentRunConfig(BaseModel):
    """Configuration for the agent runner."""

    problems_path: str
    logs_dir: str
    output_dir: str
    agent: str = "simple_csv"
    model_factory_path: str = DEFAULT_MODEL_FACTORY_PATH
    model_name: str
    reasoning_effort: str | None = None
    n_trials: int = 1
    concurrency: int = 10
    trial_timeout_seconds: float = 20.0 * 60.0
    max_trial_retries: int = 1
    model_call_timeout_seconds: float | None = 90.0
    max_model_call_retries: int = 5
    log_level: str = "INFO"
    overwrite: bool = False
    run_id: str | None = None
    agent_kwargs: dict[str, Any] = Field(default_factory=dict)


def build_arg_parser() -> argparse.ArgumentParser:
    """Create the CLI parser."""
    parser = argparse.ArgumentParser(
        description="Run LangChain agents on log-processing tasks."
    )
    parser.add_argument("--problems-path", required=True)
    parser.add_argument("--logs-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--agent", default="simple_csv")
    parser.add_argument(
        "--model-factory",
        default=DEFAULT_MODEL_FACTORY_PATH,
        dest="model_factory_path",
        help=(
            "Import path like 'my_project.models:create_llms'. "
            f"Defaults to {DEFAULT_MODEL_FACTORY_PATH}."
        ),
    )
    parser.add_argument("--model-name", required=True)
    parser.add_argument("--reasoning-effort", default=None)
    parser.add_argument("--n-trials", type=int, default=1)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--trial-timeout-seconds", type=float, default=1800.0)
    parser.add_argument("--max-trial-retries", type=int, default=3)
    parser.add_argument("--model-call-timeout-seconds", type=float, default=900.0)
    parser.add_argument("--max-model-call-retries", type=int, default=3)
    parser.add_argument(
        "--log-level",
        type=str.upper,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
    )
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--agent-kwargs",
        default="{}",
        help="JSON object passed to the agent constructor.",
    )
    return parser


async def run_agents(config: AgentRunConfig) -> Path:
    """Run an agent across all tasks.

    Args:
        config: Runner configuration.

    Returns:
        The created run directory.
    """
    if not config.model_factory_path:
        raise ValueError(
            "model_factory_path must be provided. "
            f"Use {DEFAULT_MODEL_FACTORY_PATH} for the built-in eval factory."
        )

    tasks = load_tasks(config.problems_path, config.logs_dir)
    agent_class = resolve_agent_class(config.agent)
    agent_name = getattr(agent_class, "name", agent_class.__name__)

    run_id = config.run_id or _make_run_id(agent_name, config.model_name)
    run_dir = Path(config.output_dir) / run_id
    agent_dir = run_dir / agent_name
    agent_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Starting agent run run_id=%s agent=%s tasks=%d output_dir=%s",
        run_id,
        agent_name,
        len(tasks),
        run_dir,
    )

    manifest = {
        "schema_version": "1.1",
        "run_id": run_id,
        "created_at_utc": utc_now_iso(),
        "problems_path": config.problems_path,
        "logs_dir": config.logs_dir,
        "output_dir": str(run_dir),
        "agent": agent_name,
        "agent_class": f"{agent_class.__module__}:{agent_class.__qualname__}",
        "model_factory_path": config.model_factory_path,
        "model_name": config.model_name,
        "reasoning_effort": config.reasoning_effort,
        "n_trials": config.n_trials,
        "concurrency": config.concurrency,
        "trial_timeout_seconds": config.trial_timeout_seconds,
        "max_trial_retries": config.max_trial_retries,
        "model_call_timeout_seconds": config.model_call_timeout_seconds,
        "max_model_call_retries": config.max_model_call_retries,
        "task_count": len(tasks),
    }
    write_json(run_dir / "run_manifest.json", manifest)

    semaphore = asyncio.Semaphore(config.concurrency)
    model_builder = resolve_model_builder(config.model_factory_path)
    progress = {
        "skipped": 0,
        "completed": 0,
        "partial": 0,
        "failed": 0,
    }
    progress_bar = tqdm(
        total=len(tasks),
        desc="Progress",
        unit="task",
        dynamic_ncols=True,
        leave=True,
    )

    def _refresh_progress_bar() -> None:
        progress_bar.set_postfix(
            completed=progress["completed"],
            partial=progress["partial"],
            failed=progress["failed"],
            skipped=progress["skipped"],
            refresh=False,
        )

    async def _handle_task(task: TaskSpec) -> None:
        output_path = agent_dir / f"{task.task_id}.json"
        if output_path.exists() and not config.overwrite:
            progress["skipped"] += 1
            _refresh_progress_bar()
            progress_bar.update(1)
            return

        async with semaphore:
            record = await _run_task_record(
                task=task,
                output_path=output_path,
                run_id=run_id,
                agent_name=agent_name,
                agent_class=agent_class,
                model_builder=model_builder,
                config=config,
            )
            write_json(output_path, record)
            if record.summary.status == "completed":
                progress["completed"] += 1
            elif record.summary.status == "partial":
                progress["partial"] += 1
            elif record.summary.status == "failed":
                progress["failed"] += 1
            else:
                progress["skipped"] += 1
            _refresh_progress_bar()
            progress_bar.update(1)

    try:
        await asyncio.gather(*[_handle_task(task) for task in tasks])
    finally:
        progress_bar.close()
    logger.info("Completed agent run run_id=%s output_dir=%s", run_id, run_dir)
    return run_dir


async def _run_task_record(
    *,
    task: TaskSpec,
    output_path: Path,
    run_id: str,
    agent_name: str,
    agent_class: type[BaseTaskAgent],
    model_builder: Any,
    config: AgentRunConfig,
) -> TaskRunRecord:
    """Run all trials for one task and build its result record.

    Args:
        task: Task specification.
        output_path: Per-task output file.
        run_id: Run identifier.
        agent_name: Resolved agent name.
        agent_class: Agent class.
        model_builder: Resolved model-builder callable.
        config: Runner configuration.

    Returns:
        Saved task result record.
    """
    created_at = utc_now_iso()
    record = TaskRunRecord(
        task=task,
        agent=AgentMetadata(
            agent_name=agent_name,
            agent_class=f"{agent_class.__module__}:{agent_class.__qualname__}",
            model_factory_path=config.model_factory_path,
            model_name=config.model_name,
            reasoning_effort=config.reasoning_effort,
            agent_kwargs=config.agent_kwargs,
        ),
        execution=ExecutionMetadata(
            run_id=run_id,
            problems_path=config.problems_path,
            logs_dir=config.logs_dir,
            output_path=str(output_path),
            n_trials=config.n_trials,
            concurrency=config.concurrency,
            trial_timeout_seconds=config.trial_timeout_seconds,
            max_trial_retries=config.max_trial_retries,
            model_call_timeout_seconds=config.model_call_timeout_seconds,
            max_model_call_retries=config.max_model_call_retries,
            created_at_utc=created_at,
            updated_at_utc=created_at,
        ),
        summary=TaskSummary(
            status="failed",
            trial_count=config.n_trials,
            completed_trial_count=0,
            failed_trial_count=config.n_trials,
        ),
    )

    try:
        agent_instance = agent_class(
            model_builder=model_builder,
            model_name=config.model_name,
            reasoning_effort=config.reasoning_effort,
            model_call_timeout_seconds=config.model_call_timeout_seconds,
            max_model_call_retries=config.max_model_call_retries,
            agent_kwargs=config.agent_kwargs,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("[%s] Agent initialization failed", task.task_id)
        record.task_error = error_info_from_exception(exc)
        record.execution.updated_at_utc = utc_now_iso()
        record.summary = _build_task_summary(record)
        return record

    async def _run_trials() -> None:
        for trial_index in range(config.n_trials):
            trial = await _run_single_trial(
                agent_instance=agent_instance,
                task=task,
                total_trials=config.n_trials,
                trial_index=trial_index,
                attempt_timeout_seconds=config.trial_timeout_seconds,
                max_retries=config.max_trial_retries,
            )
            record.trials.append(trial)

    try:
        await _run_trials()
    except Exception as exc:  # noqa: BLE001
        logger.exception("[%s] Task failed outside per-trial handling", task.task_id)
        record.task_error = error_info_from_exception(exc)

    record.execution.updated_at_utc = utc_now_iso()
    record.summary = _build_task_summary(record)
    return record


async def _run_single_trial(
    *,
    agent_instance: BaseTaskAgent,
    task: TaskSpec,
    total_trials: int,
    trial_index: int,
    attempt_timeout_seconds: float,
    max_retries: int,
) -> TrialRecord:
    """Run one task trial with retries.

    Whole-trial retries are reserved for whole-attempt timeouts and exhausted
    transient model/provider failures.

    Args:
        agent_instance: Agent instance.
        task: Task specification.
        total_trials: Total number of trials for the task.
        trial_index: Zero-based trial index.
        attempt_timeout_seconds: Timeout for one agent attempt.
        max_retries: Retry budget for the trial.

    Returns:
        Trial record.
    """
    from tenacity import AsyncRetrying, retry_if_exception, stop_after_attempt
    from tenacity import wait_exponential, wait_random

    started_at = utc_now_iso()
    started_perf = time.perf_counter()
    attempts = 0
    display_trial_index = trial_index + 1
    system_prompt = agent_instance.build_system_prompt(task)

    def _log_before_sleep(retry_state: Any) -> None:
        error: BaseException | None = None
        if retry_state.outcome is not None and retry_state.outcome.failed:
            error = retry_state.outcome.exception()
        sleep_seconds = float(
            getattr(getattr(retry_state, "next_action", None), "sleep", 0.0) or 0.0
        )
        logger.warning(
            "[%s] Trial %d/%d attempt %d failed with %s; retrying in %.1fs",
            task.task_id,
            display_trial_index,
            total_trials,
            int(retry_state.attempt_number),
            format_exception_summary(error),
            sleep_seconds,
        )

    try:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(max_retries + 1),
            wait=wait_exponential(multiplier=1, min=10, max=30) + wait_random(0, 10),
            retry=retry_if_exception(_should_retry_trial),
            before_sleep=_log_before_sleep,
            reraise=True,
        ):
            with attempt:
                attempts = int(attempt.retry_state.attempt_number)
                execution = await asyncio.wait_for(
                    execute_task_agent(
                        agent_definition=agent_instance,
                        task=task,
                    ),
                    timeout=attempt_timeout_seconds,
                )
                finished_at = utc_now_iso()
                return TrialRecord(
                    trial_index=trial_index,
                    status="completed",
                    started_at_utc=started_at,
                    finished_at_utc=finished_at,
                    duration_seconds=time.perf_counter() - started_perf,
                    attempt_count=attempts,
                    final_answer=execution.final_answer,
                    system_prompt=execution.system_prompt,
                    trajectory_events=execution.trajectory_events,
                    raw_messages=execution.raw_messages,
                    num_messages=execution.num_messages,
                    num_model_turns=execution.num_model_turns,
                    num_tool_calls=execution.num_tool_calls,
                    token_usage=execution.token_usage,
                    effective_duration_seconds=execution.effective_duration_seconds,
                    metadata=execution.metadata,
                )
    except asyncio.TimeoutError as exc:
        error = error_info_from_exception(exc)
        status = "timed_out"
        logger.warning(
            "[%s] Trial %d/%d timed out after %.2fs (attempts=%d timeout=%.1fs)",
            task.task_id,
            display_trial_index,
            total_trials,
            time.perf_counter() - started_perf,
            max(1, attempts),
            attempt_timeout_seconds,
        )
    except Exception as exc:  # noqa: BLE001
        error = error_info_from_exception(exc)
        status = "failed"
        if is_retryable_transient_model_error(exc):
            logger.warning(
                "[%s] Trial %d/%d failed after transient model retries were exhausted: %s",
                task.task_id,
                display_trial_index,
                total_trials,
                format_exception_summary(exc),
            )
        else:
            logger.exception(
                "[%s] Trial %d/%d failed",
                task.task_id,
                display_trial_index,
                total_trials,
            )

    return TrialRecord(
        trial_index=trial_index,
        status=status,  # type: ignore
        started_at_utc=started_at,
        finished_at_utc=utc_now_iso(),
        duration_seconds=time.perf_counter() - started_perf,
        attempt_count=max(1, attempts),
        system_prompt=system_prompt,
        error=error,  # type: ignore
    )


def _should_retry_trial(exc: BaseException) -> bool:
    """Decide whether a whole trial should be restarted.

    Args:
        exc: Trial-level exception.

    Returns:
        ``True`` if the entire agent attempt should be retried.
    """
    return isinstance(exc, asyncio.TimeoutError) or is_retryable_transient_model_error(
        exc
    )


def _build_task_summary(record: TaskRunRecord) -> TaskSummary:
    """Build a top-level summary from trial results.

    Args:
        record: Task result record.

    Returns:
        Summary payload.
    """
    completed = [trial for trial in record.trials if trial.status == "completed"]
    failed = [trial for trial in record.trials if trial.status != "completed"]

    if completed and not failed and record.task_error is None:
        status = "completed"
    elif completed:
        status = "partial"
    elif record.task_error is not None or failed:
        status = "failed"
    else:
        status = "skipped"

    best_answer = next(
        (trial.final_answer for trial in completed if trial.final_answer),
        None,
    )
    return TaskSummary(
        status=status,
        trial_count=record.execution.n_trials,
        completed_trial_count=len(completed),
        failed_trial_count=max(record.execution.n_trials - len(completed), len(failed)),
        best_final_answer=best_answer,
    )


def _make_run_id(agent_name: str, model_name: str) -> str:
    """Build a simple run identifier.

    Args:
        agent_name: Agent name.
        model_name: Model name.

    Returns:
        Run identifier.
    """
    timestamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    safe_model = model_name.replace("/", "-").replace(":", "-")
    return f"{timestamp}_{agent_name}_{safe_model}"


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
    agent_kwargs = json.loads(args.agent_kwargs)
    config = AgentRunConfig(**{**vars(args), "agent_kwargs": agent_kwargs})

    _configure_logging(config.log_level)

    logger.info("=== Agent evaluation run started ===")
    run_dir = asyncio.run(run_agents(config))
    logger.info("=== Agent evaluation run finished ===")
    print(run_dir)


if __name__ == "__main__":
    main()

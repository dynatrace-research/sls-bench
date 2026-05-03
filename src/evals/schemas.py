from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class TokenUsage(BaseModel):
    """Aggregated token usage for a run or a single model response.

    Attributes:
        input_tokens: Prompt or input tokens.
        output_tokens: Completion or output tokens.
        cached_input_tokens: Cache-hit input tokens when the provider exposes them.
        reasoning_tokens: Explicit reasoning tokens when the provider exposes them.
    """

    input_tokens: int = 0
    output_tokens: int = 0
    cached_input_tokens: int = 0
    reasoning_tokens: int = 0

    def add_usage_metadata(self, usage_metadata: dict[str, Any] | None) -> None:
        """Merge LangChain-style usage metadata into this object.

        Args:
            usage_metadata: Usage metadata attached to a message.
        """
        if not usage_metadata:
            return

        input_details = usage_metadata.get("input_token_details") or {}
        output_details = usage_metadata.get("output_token_details") or {}

        raw_input = int(usage_metadata.get("input_tokens") or 0)
        cache_read = int(input_details.get("cache_read") or 0)
        if "cache_creation" in input_details:
            # Bedrock/Claude: input_tokens excludes cached tokens, add them back.
            cache_creation = int(input_details.get("cache_creation") or 0)
            self.input_tokens += raw_input + cache_creation + cache_read
        else:
            # Azure/OpenAI: input_tokens already includes cached tokens.
            self.input_tokens += raw_input

        self.output_tokens += int(usage_metadata.get("output_tokens") or 0)
        self.cached_input_tokens += cache_read
        self.reasoning_tokens += int(output_details.get("reasoning") or 0)


class ErrorInfo(BaseModel):
    """Serializable error information."""

    error_type: str
    message: str
    traceback: str | None = None


class TaskSpec(BaseModel):
    """A single benchmark task.

    Attributes:
        task_id: Stable task identifier.
        logs_path: Path to the CSV log file for the task.
        question: User-facing question.
        ground_truth_answer: Reference answer used by graders.
    """

    task_id: str
    logs_path: str
    question: str
    ground_truth_answer: str


class TrajectoryEvent(BaseModel):
    """A normalized event extracted from a LangChain message trajectory."""

    index: int
    kind: Literal["system", "human", "assistant", "tool", "unknown"]
    content: str | None = None
    tool_name: str | None = None
    tool_call_id: str | None = None
    tool_calls: list[dict[str, Any]] = Field(default_factory=list)
    usage: TokenUsage | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class AgentExecution(BaseModel):
    """Normalized result returned by an agent invocation."""

    system_prompt: str
    final_answer: str | None = None
    trajectory_events: list[TrajectoryEvent] = Field(default_factory=list)
    raw_messages: list[dict[str, Any]] = Field(default_factory=list)
    num_messages: int = 0
    num_model_turns: int = 0
    num_tool_calls: int = 0
    token_usage: TokenUsage = Field(default_factory=TokenUsage)
    effective_duration_seconds: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class GraderResult(BaseModel):
    """A single grader output."""

    name: str
    result: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] | None = None
    score: float | None = None


class TrialRecord(BaseModel):
    """One agent trial for a task."""

    trial_index: int
    status: Literal["completed", "failed", "timed_out"]
    started_at_utc: str
    finished_at_utc: str
    duration_seconds: float
    attempt_count: int
    final_answer: str | None = None
    system_prompt: str | None = None
    trajectory_events: list[TrajectoryEvent] = Field(default_factory=list)
    raw_messages: list[dict[str, Any]] = Field(default_factory=list)
    num_messages: int = 0
    num_model_turns: int = 0
    num_tool_calls: int = 0
    token_usage: TokenUsage = Field(default_factory=TokenUsage)
    effective_duration_seconds: float | None = None
    error: ErrorInfo | None = None
    grader_results: dict[str, GraderResult] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class TaskSummary(BaseModel):
    """Human-readable top-level task summary."""

    status: Literal["completed", "partial", "failed", "skipped"]
    trial_count: int
    completed_trial_count: int
    failed_trial_count: int
    best_final_answer: str | None = None


class AgentMetadata(BaseModel):
    """Metadata describing the agent used for the run."""

    agent_name: str
    agent_class: str
    model_factory_path: str
    model_name: str
    reasoning_effort: str | None = None
    agent_kwargs: dict[str, Any] = Field(default_factory=dict)


class ExecutionMetadata(BaseModel):
    """Runtime configuration for the task run."""

    run_id: str
    problems_path: str
    logs_dir: str
    output_path: str
    n_trials: int
    concurrency: int
    trial_timeout_seconds: float
    max_trial_retries: int
    model_call_timeout_seconds: float | None = None
    max_model_call_retries: int = 0
    created_at_utc: str
    updated_at_utc: str


class GradingMetadata(BaseModel):
    """Metadata about the latest grading pass."""

    model_config = ConfigDict(extra="allow")

    graders: list[str] = Field(default_factory=list)
    graded_at_utc: str | None = None
    judge_model_factory_path: str | None = None
    judge_model_name: str | None = None
    judge_reasoning_effort: str | None = None
    judge_system_prompt_path: str | None = None


class TaskRunRecord(BaseModel):
    """Saved artifact for one `(task, agent)` result file."""

    model_config = ConfigDict(extra="allow")

    schema_version: str = "1.1"
    task: TaskSpec
    agent: AgentMetadata
    execution: ExecutionMetadata
    summary: TaskSummary
    trials: list[TrialRecord] = Field(default_factory=list)
    task_error: ErrorInfo | None = None
    grading: GradingMetadata | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

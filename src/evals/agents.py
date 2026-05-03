from __future__ import annotations

import asyncio
import atexit
import contextlib
import contextvars
import inspect
import io
import logging
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Callable, Optional

from langchain.agents.middleware import AgentMiddleware
from langchain_aws.middleware.prompt_caching import BedrockPromptCachingMiddleware

from evals.schemas import AgentExecution, TaskSpec
from evals.utils import (
    build_chat_model,
    import_from_string,
    is_retryable_transient_model_error,
    summarize_messages,
    truncate_text,
)

ModelBuilder = Callable[..., Any]

_logger = logging.getLogger(__name__)

FINAL_ANSWER_SENTINEL = "<|FINAL_ANSWER|>"
_SENTINEL_MODEL_PREFIXES = ("deepseek",)
_BEDROCK_MODEL_CLASS_NAMES = frozenset({"ChatBedrock", "ChatBedrockConverse"})


class BaseTaskAgent:
    """Base class for task-specific LangChain agents."""

    name = "base"

    def __init__(
        self,
        *,
        model_builder: ModelBuilder,
        model_name: str,
        reasoning_effort: str | None,
        model_call_timeout_seconds: float | None = 60.0,
        max_model_call_retries: int = 3,
        agent_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the agent definition.

        Args:
            model_builder: Callable that builds a LangChain chat model.
            model_name: Model identifier.
            reasoning_effort: Model reasoning effort, or ``None`` to use the factory default.
            model_call_timeout_seconds: Timeout applied to each individual LLM call.
            max_model_call_retries: Retry budget for each individual LLM call.
            agent_kwargs: Extra agent-specific settings.
        """
        self.model_builder = model_builder
        self.model_name = model_name
        self.reasoning_effort = reasoning_effort
        self.model_call_timeout_seconds = model_call_timeout_seconds
        self.max_model_call_retries = max(0, int(max_model_call_retries))
        self.agent_kwargs = agent_kwargs or {}
        self._cleanup_callbacks: list[Callable[[], None]] = []

    def build_system_prompt(self, task: TaskSpec) -> str:
        """Build the system prompt for a task.

        Args:
            task: Task specification.

        Returns:
            System prompt.
        """
        raise NotImplementedError

    def build_tools(self, task: TaskSpec) -> list[Any]:
        """Build tools for a task.

        Args:
            task: Task specification.

        Returns:
            LangChain tool objects.
        """
        return []

    def build_middlewares(self, task: TaskSpec) -> list[Any]:
        """Build agent middleware for a task.

        Args:
            task: Task specification.

        Returns:
            Middleware objects passed into ``create_agent``.
        """
        del task
        return _build_model_middlewares(
            timeout_seconds=self.model_call_timeout_seconds,
            max_retries=self.max_model_call_retries,
        )

    def _register_cleanup(self, callback: Callable[[], None]) -> None:
        self._cleanup_callbacks.append(callback)

    def cleanup(self) -> None:
        callbacks = self._cleanup_callbacks
        self._cleanup_callbacks = []
        while callbacks:
            callback = callbacks.pop()
            try:
                callback()
            except Exception:  # noqa: BLE001
                _logger.exception("Agent cleanup failed")

    def create_langchain_agent(self, task: TaskSpec) -> tuple[Any, str]:
        """Create the underlying LangChain runnable for a task.

        Args:
            task: Task specification.

        Returns:
            A tuple of ``(agent_runnable, system_prompt)``.
        """
        from langchain.agents import create_agent

        model = build_chat_model(
            self.model_builder,
            self.model_name,
            self.reasoning_effort,
        )
        system_prompt = self.build_system_prompt(task)
        middlewares = self.build_middlewares(task)
        if _is_bedrock_chat_model(model):
            middlewares = [
                BedrockPromptCachingMiddleware(unsupported_model_behavior="ignore"),
                *middlewares,
            ]
        runnable = create_agent(
            model=model,
            tools=self.build_tools(task),
            system_prompt=system_prompt,
            middleware=middlewares,
        )
        return runnable, system_prompt


class SimpleCsvAgent(BaseTaskAgent):
    """Minimal example agent for CSV log analysis tasks."""

    name = "simple_csv"

    def build_system_prompt(self, task: TaskSpec) -> str:
        """Build the example system prompt.

        Args:
            task: Task specification.

        Returns:
            Prompt string.
        """
        return (
            "You are a log-analysis agent. "
            f"You have access to exactly one CSV log file at `{task.logs_path}`. "
            "Use tools to inspect the file incrementally and answer the user's question. "
            "Do not invent fields or hidden systems. Keep tool outputs concise and avoid loading "
            "the entire file unless you truly need it. When you are done, respond with only the "
            "final answer."
        )

    def build_tools(self, task: TaskSpec) -> list[Any]:
        """Build task-bound CSV tools.

        Args:
            task: Task specification.

        Returns:
            Tool list.
        """
        return [self._build_python_csv_tool(task)]

    def _build_python_csv_tool(self, task: TaskSpec) -> Any:
        """Create a small Python analysis tool.

        Args:
            task: Task specification.

        Returns:
            LangChain tool.
        """
        from langchain_core.tools import tool

        logs_path = Path(task.logs_path)

        @tool("python_csv")
        def python_csv(code: str) -> str:
            """Execute Python with ``pd`` and ``csv_path`` available for CSV analysis."""
            import json
            import math
            import statistics

            import pandas as pd

            stdout = io.StringIO()
            namespace: dict[str, Any] = {
                "Path": Path,
                "csv_path": str(logs_path),
                "json": json,
                "math": math,
                "pd": pd,
                "statistics": statistics,
            }
            try:
                with contextlib.redirect_stdout(stdout):
                    exec(code, namespace, namespace)
            except Exception as exc:  # noqa: BLE001
                details = "".join(
                    traceback.format_exception(type(exc), exc, exc.__traceback__)
                )
                output = f"Python execution failed.\n\n{details}"
                return truncate_text(output)

            printed = stdout.getvalue().strip()
            result = namespace.get("_result", None)
            chunks: list[str] = []
            if printed:
                chunks.append(printed)
            if result is not None:
                chunks.append(repr(result))
            if not chunks:
                chunks.append(
                    "Execution finished. Set `_result` or print concise output."
                )
            return truncate_text("\n\n".join(chunks))

        return python_csv


# ---------------------------------------------------------------------------
# Notebook session (Jupyter ipykernel)
# ---------------------------------------------------------------------------

_active_sessions: list["_NotebookSession"] = []


def _cleanup_sessions() -> None:
    """Shut down any remaining notebook sessions at process exit."""
    while _active_sessions:
        session = _active_sessions.pop()
        try:
            session.shutdown()
        except Exception:  # noqa: BLE001
            pass


atexit.register(_cleanup_sessions)


class _NotebookSession:
    """A live ipykernel session for code execution."""

    def __init__(self, init_code: Optional[str] = None) -> None:
        from jupyter_client.manager import KernelManager

        self._lifecycle_lock = threading.Lock()
        self._closed = False
        self._manager = KernelManager()
        self._manager.start_kernel()
        self._client = self._manager.blocking_client()
        self._client.start_channels()
        self._client.wait_for_ready(timeout=60)

        if init_code:
            self.execute(init_code, timeout=60)

        _active_sessions.append(self)

    def execute(self, code: str, timeout: float = 120.0) -> dict[str, Optional[str]]:
        """Execute code and return ``{stdout, stderr, result}``."""
        with self._lifecycle_lock:
            if self._closed:
                return {
                    "stdout": None,
                    "stderr": "Notebook session is closed.",
                    "result": None,
                }

            msg_id = self._client.execute(code, allow_stdin=False, store_history=True)
            stdout_parts: list[str] = []
            stderr_parts: list[str] = []
            result_repr: Optional[str] = None
            seen_iopub_error = False

            try:
                while True:
                    try:
                        msg = self._client.get_iopub_msg(timeout=timeout)
                    except Exception as exc:  # noqa: BLE001
                        stderr_parts.append(
                            f"Timeout or error waiting for output: {exc}"
                        )
                        break

                    if msg.get("parent_header", {}).get("msg_id") != msg_id:
                        continue

                    msg_type = msg.get("header", {}).get("msg_type")
                    content = msg.get("content", {})

                    if (
                        msg_type == "status"
                        and content.get("execution_state") == "idle"
                    ):
                        break

                    if msg_type == "stream":
                        text = content.get("text", "")
                        if content.get("name", "stdout") == "stderr":
                            stderr_parts.append(text)
                        else:
                            stdout_parts.append(text)
                    elif msg_type in {"execute_result", "display_data"}:
                        data = content.get("data", {})
                        if "text/plain" in data:
                            result_repr = str(data["text/plain"])
                    elif msg_type == "error":
                        seen_iopub_error = True
                        tb = content.get("traceback", [])
                        if tb:
                            stderr_parts.append("\n".join(tb))
                        else:
                            stderr_parts.append(
                                f"{content.get('ename', '')}: {content.get('evalue', '')}"
                            )

                try:
                    while True:
                        reply = self._client.get_shell_msg(timeout=timeout)
                        if reply.get("parent_header", {}).get("msg_id") == msg_id:
                            break
                except Exception as exc:  # noqa: BLE001
                    stderr_parts.append(
                        f"Timeout or error waiting for shell reply: {exc}"
                    )
                    reply = {}

                status = reply.get("content", {}).get("status")
                if status == "error" and not seen_iopub_error:
                    ename = reply["content"].get("ename", "Error")
                    evalue = reply["content"].get("evalue", "")
                    stderr_parts.append(f"{ename}: {evalue}")

            except Exception as exc:  # noqa: BLE001
                stderr_parts.append(f"Unexpected error during execution: {exc}")

            return {
                "stdout": "".join(stdout_parts) or None,
                "stderr": "".join(stderr_parts) or None,
                "result": result_repr,
            }

    def shutdown(self) -> None:
        """Stop channels and shut down the kernel."""
        with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._client.stop_channels()
            finally:
                try:
                    self._manager.shutdown_kernel(now=True)
                except Exception:  # noqa: BLE001
                    pass
        try:
            _active_sessions.remove(self)
        except ValueError:
            pass


NOTEBOOK_INIT_CODE = """
import IPython
import warnings

ip = IPython.get_ipython()

ip.colors = 'NoColor'
ip.InteractiveTB.set_mode(mode='Plain')
ip.config.TerminalInteractiveShell.separate_in = ''
ip.config.TerminalInteractiveShell.separate_out = ''
ip.config.TerminalInteractiveShell.separate_out2 = ''

warnings.filterwarnings('ignore')

import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 50)
"""


class NotebookCsvAgent(BaseTaskAgent):
    """Agent that uses a live Jupyter kernel for CSV log analysis."""

    name = "notebook_csv"

    def build_system_prompt(self, task: TaskSpec) -> str:
        """Build the system prompt.

        Args:
            task: Task specification.

        Returns:
            Prompt string.
        """
        base = (
            "You are a log analysis agent. Your task is to answer a user's question about "
            "log data.\n\n"
            "# Tools\n\n"
            "`python_notebook` — executes Python in a stateful IPython kernel. "
            "State (variables, imports, DataFrames) is preserved across calls. "
            "pandas is pre-imported as `pd`.\n\n"
            "# Log Data\n\n"
            f"A single CSV log file is available at `{task.logs_path}`. "
            "Load it with `pd.read_csv(...)` when needed.\n\n"
            "# Rules\n\n"
            "- Do NOT print large objects from python_notebook directly. Assign to variables, check sizes, "
            "and inspect in smaller parts to keep tool output concise.\n"
            "- Do NOT create, modify, or delete any files.\n"
        )
        if self._needs_sentinel_workaround:
            # for deepseek v3.2 as it can't stop calling python_notebook
            base += (
                "- When you have the answer, print it as a single string ending with "
                f"`{FINAL_ANSWER_SENTINEL}` using the python_notebook tool. "
                'Example: `print(f"The answer is 42{FINAL_ANSWER_SENTINEL}")`. '
                "Do NOT return the answer as plain text — always use python_notebook to print it.\n"
            )
        else:
            base += (
                "- When you have the answer, respond with only the final answer text; "
                "no explanations or auxiliary text - just the complete final answer.\n"
            )
        return base

    @property
    def _needs_sentinel_workaround(self) -> bool:
        """Whether the model requires the final-answer sentinel workaround."""
        name = self.model_name.lower()
        return any(name.startswith(p) for p in _SENTINEL_MODEL_PREFIXES)

    def build_middlewares(self, task: TaskSpec) -> list[Any]:
        """Build middlewares, adding sentinel detection for models that need it.

        Args:
            task: Task specification.

        Returns:
            Middleware list.
        """
        base = super().build_middlewares(task)
        if self._needs_sentinel_workaround:
            return [_FinalAnswerSentinelMiddleware()] + base
        return base

    def build_tools(self, task: TaskSpec) -> list[Any]:
        """Build a notebook tool backed by a fresh Jupyter kernel.

        Args:
            task: Task specification.

        Returns:
            Tool list.
        """
        return [self._build_notebook_tool(task)]

    def _build_notebook_tool(self, task: TaskSpec) -> Any:
        """Create the ``python_notebook`` tool.

        Args:
            task: Task specification.

        Returns:
            LangChain tool.
        """
        from langchain_core.tools import tool as lc_tool

        session = _NotebookSession(init_code=NOTEBOOK_INIT_CODE)
        self._register_cleanup(session.shutdown)
        logs_path = task.logs_path

        @lc_tool("python_notebook")
        def python_notebook(code: str) -> str:
            """Execute Python code in a stateful IPython notebook.

            State is preserved across calls. pandas is available as ``pd``.
            """
            print(f"Executing code in notebook session:\n{code}\n---")
            result = session.execute(code, timeout=180.0)
            chunks: list[str] = []
            if result["stdout"]:
                chunks.append(result["stdout"])
            if result["result"]:
                chunks.append(result["result"])
            if result["stderr"]:
                chunks.append(f"[stderr]\n{result['stderr']}")
            if not chunks:
                chunks.append("Execution finished with no output.")
            return truncate_text("\n\n".join(chunks))

        return python_notebook


class _FinalAnswerSentinelMiddleware(AgentMiddleware):
    """Middleware that detects a sentinel token in tool output and terminates the loop.

    When a tool message contains ``FINAL_ANSWER_SENTINEL``, the text preceding it
    is captured.  On the next model call the middleware short-circuits by returning
    an ``AIMessage`` with the captured answer (no tool calls), so the agent loop
    ends naturally.
    """

    def __init__(self) -> None:
        self._final_answer: str | None = None

    # -- tool call wrappers ---------------------------------------------------

    async def awrap_tool_call(self, request: Any, handler: Any) -> Any:
        result = await handler(request)
        self._check_sentinel(result)
        return result

    def wrap_tool_call(self, request: Any, handler: Any) -> Any:
        result = handler(request)
        self._check_sentinel(result)
        return result

    # -- model call wrappers --------------------------------------------------

    async def awrap_model_call(self, request: Any, handler: Any) -> Any:
        if self._final_answer is not None:
            return self._make_final_message()
        return await handler(request)

    def wrap_model_call(self, request: Any, handler: Any) -> Any:
        if self._final_answer is not None:
            return self._make_final_message()
        return handler(request)

    # -- internals ------------------------------------------------------------

    def _check_sentinel(self, result: Any) -> None:
        from langchain_core.messages import ToolMessage

        if not isinstance(result, ToolMessage):
            return
        content = result.content
        if isinstance(content, str) and FINAL_ANSWER_SENTINEL in content:
            answer = content.split(FINAL_ANSWER_SENTINEL, 1)[0].strip()
            _logger.info(
                "Final-answer sentinel detected in tool output; capturing answer."
            )
            self._final_answer = answer

    def _make_final_message(self) -> Any:
        from langchain_core.messages import AIMessage

        _logger.info("Short-circuiting model call with captured final answer.")
        return AIMessage(content=self._final_answer or "")


BUILTIN_AGENTS: dict[str, type[BaseTaskAgent]] = {
    SimpleCsvAgent.name: SimpleCsvAgent,
    NotebookCsvAgent.name: NotebookCsvAgent,
}


def resolve_agent_class(spec: str) -> type[BaseTaskAgent]:
    """Resolve an agent class from a registry name or import path.

    Args:
        spec: Built-in name or ``module:object`` import path.

    Returns:
        Agent class.
    """
    if spec in BUILTIN_AGENTS:
        return BUILTIN_AGENTS[spec]
    resolved = import_from_string(spec)
    if not isinstance(resolved, type):
        raise TypeError(f"Expected an agent class for {spec}, got {type(resolved)}")
    return resolved


def resolve_model_builder(spec: str) -> ModelBuilder:
    """Resolve a model-builder callable.

    Args:
        spec: Import string pointing to the builder.

    Returns:
        Model-builder callable.
    """
    resolved = import_from_string(spec)
    if not callable(resolved):
        raise TypeError(f"Expected a callable model builder for {spec}")
    return resolved


async def execute_task_agent(
    agent_definition: BaseTaskAgent,
    task: TaskSpec,
) -> AgentExecution:
    """Run a task agent and normalize its output.

    Args:
        agent_definition: Agent definition.
        task: Task specification.

    Returns:
        Normalized execution result.
    """
    timer = _EffectiveTimer()
    _effective_timer_var.set(timer)

    try:
        runnable, system_prompt = agent_definition.create_langchain_agent(task)
        payload = {"messages": [{"role": "user", "content": task.question}]}
        result = await runnable.ainvoke(payload, {"recursion_limit": 200})

        raw_messages = (
            list(result.get("messages") or []) if isinstance(result, dict) else []
        )
        summary = summarize_messages(raw_messages)

        return AgentExecution(
            system_prompt=system_prompt,
            final_answer=summary.final_answer,
            trajectory_events=summary.trajectory_events,
            raw_messages=summary.raw_messages,
            num_messages=len(raw_messages),
            num_model_turns=sum(
                1 for event in summary.trajectory_events if event.kind == "assistant"
            ),
            num_tool_calls=sum(
                len(event.tool_calls) for event in summary.trajectory_events
            ),
            token_usage=summary.token_usage,
            effective_duration_seconds=timer.total,
            metadata={},
        )
    finally:
        agent_definition.cleanup()


class _EffectiveTimer:
    """Thread-safe accumulator for effective (successful-only) call durations."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.model_seconds: float = 0.0
        self.tool_seconds: float = 0.0

    @property
    def total(self) -> float:
        with self._lock:
            return self.model_seconds + self.tool_seconds

    def record(self, category: str, seconds: float) -> None:
        with self._lock:
            if category == "model":
                self.model_seconds += seconds
            elif category == "tool":
                self.tool_seconds += seconds


_effective_timer_var: contextvars.ContextVar[_EffectiveTimer | None] = (
    contextvars.ContextVar("_effective_timer_var", default=None)
)


def _is_bedrock_chat_model(model: Any) -> bool:
    model_class = type(model)
    return (
        model_class.__name__ in _BEDROCK_MODEL_CLASS_NAMES
        and model_class.__module__.startswith("langchain_aws.")
    )


def _build_model_middlewares(
    *,
    timeout_seconds: float | None,
    max_retries: int,
) -> list[Any]:
    """Build model middleware for timeouts and transient retries.

    Args:
        timeout_seconds: Per-model-call timeout.
        max_retries: Retry budget per model call.

    Returns:
        Middleware instances.
    """
    from langchain.agents.middleware import ModelRetryMiddleware

    middlewares: list[Any] = []

    if max_retries > 0:
        middlewares.append(
            ModelRetryMiddleware(
                max_retries=max_retries,
                retry_on=is_retryable_transient_model_error,
                on_failure="error",
                backoff_factor=2.0,
                initial_delay=20.0,
                max_delay=60.0,
                jitter=True,
            )
        )

    class _TimingMiddleware(AgentMiddleware):
        """Track durations of successful model and tool calls."""

        async def awrap_model_call(
            self,
            request: Any,
            handler: Callable[[Any], Any],
        ) -> Any:
            timer = _effective_timer_var.get(None)
            t0 = time.perf_counter()
            result = await handler(request)
            if timer is not None:
                timer.record("model", time.perf_counter() - t0)
            return result

        def wrap_model_call(
            self,
            request: Any,
            handler: Callable[[Any], Any],
        ) -> Any:
            timer = _effective_timer_var.get(None)
            t0 = time.perf_counter()
            result = handler(request)
            if timer is not None:
                timer.record("model", time.perf_counter() - t0)
            return result

        async def awrap_tool_call(
            self,
            request: Any,
            handler: Callable[[Any], Any],
        ) -> Any:
            timer = _effective_timer_var.get(None)
            t0 = time.perf_counter()
            result = await handler(request)
            if timer is not None:
                timer.record("tool", time.perf_counter() - t0)
            return result

        def wrap_tool_call(
            self,
            request: Any,
            handler: Callable[[Any], Any],
        ) -> Any:
            timer = _effective_timer_var.get(None)
            t0 = time.perf_counter()
            result = handler(request)
            if timer is not None:
                timer.record("tool", time.perf_counter() - t0)
            return result

    middlewares.append(_TimingMiddleware())

    if timeout_seconds is not None:

        class _ModelTimeoutMiddleware(AgentMiddleware):
            async def awrap_model_call(
                self,
                request: Any,
                handler: Callable[[Any], Any],
            ) -> Any:
                """Apply a timeout to async model calls."""
                response = handler(request)
                if inspect.isawaitable(response):
                    try:
                        result = await asyncio.wait_for(
                            response, timeout=timeout_seconds
                        )
                        return result
                    except Exception as exc:  # noqa: BLE001
                        _logger.warning(
                            "Model call failed (timeout or other error): %s", exc
                        )
                        raise
                return response

            def wrap_model_call(
                self,
                request: Any,
                handler: Callable[[Any], Any],
            ) -> Any:
                """Pass through sync model calls unchanged."""
                return handler(request)

        middlewares.append(_ModelTimeoutMiddleware())

    return middlewares

from __future__ import annotations

from pathlib import Path

from evals.schemas import TaskSpec
from evals.utils import read_jsonl


def load_tasks(problems_path: str | Path, logs_dir: str | Path) -> list[TaskSpec]:
    """Load tasks from a JSONL file and resolve their log-file paths.

    The loader expects the current minimal schema:
    - each JSON object has `id`, `question`, and `answer`
    - the corresponding log file is `<logs_dir>/<id>.csv`

    Args:
        problems_path: Path to the problems JSONL file.
        logs_dir: Directory containing `id.csv` log files.

    Returns:
        Loaded tasks.
    """
    resolved_problems_path = Path(problems_path)
    resolved_logs_dir = Path(logs_dir)

    rows = read_jsonl(resolved_problems_path)
    tasks: list[TaskSpec] = []

    for row in rows:
        task_id = str(row["id"])
        question = str(row["question"])
        answer = str(row["answer"])
        logs_path = resolved_logs_dir / f"{task_id}.csv"
        if not logs_path.exists():
            raise FileNotFoundError(f"Missing log file for task {task_id}: {logs_path}")
        tasks.append(
            TaskSpec(
                task_id=task_id,
                logs_path=str(logs_path),
                question=question,
                ground_truth_answer=answer,
            )
        )

    return tasks

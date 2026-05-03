from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Awaitable, Iterable, TypeVar

import pandas as pd
from tqdm.asyncio import tqdm_asyncio


T = TypeVar("T")


class LLMOutputParsingError(RuntimeError):
    def __init__(self, message: str, raw_output: str | None = None) -> None:
        super().__init__(message)
        self.raw_output = raw_output


# ---------------------------------------------------------------------------
# IO helpers
# ---------------------------------------------------------------------------


def load_prompt(prompt_path: Path) -> str:
    if not prompt_path.exists():
        raise FileNotFoundError(f"Prompt file not found: {prompt_path}")
    return prompt_path.read_text(encoding="utf-8")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_results_json(output_root: Path, results: list[dict[str, Any]]) -> Path:
    ensure_dir(output_root)
    results_path = output_root / "generation_results.json"
    with results_path.open("w", encoding="utf-8") as handle:
        json.dump(results, handle, indent=2, ensure_ascii=False)
    return results_path


def write_summary_csv(
    output_root: Path,
    summary_rows: list[dict[str, Any]],
    metadata: pd.DataFrame,
) -> None:
    ensure_dir(output_root)
    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(output_root / "generation_summary.csv", index=False, encoding="utf-8")
    metadata.to_csv(output_root / "metadata.csv", index=False, encoding="utf-8")


def write_verifier_summary_json(output_root: Path, summary: dict[str, Any]) -> Path:
    ensure_dir(output_root)
    summary_path = output_root / "verifier-summary.json"
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=False)
    return summary_path


# ---------------------------------------------------------------------------
# Async helpers
# ---------------------------------------------------------------------------


async def gather_with_progress(
    tasks: Iterable[Awaitable[T | None]], desc: str
) -> list[T]:
    results = await tqdm_asyncio.gather(*tasks, desc=desc)
    return [result for result in results if result is not None]


# ---------------------------------------------------------------------------
# Verifier output parsing
# ---------------------------------------------------------------------------

JSON_RE = re.compile(r"```json\s*(.*?)```", re.DOTALL | re.IGNORECASE)


def _normalize_score(value: Any) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)) and value in (0, 1):
        return int(value)
    if isinstance(value, str) and value.strip() in {"0", "1"}:
        return int(value.strip())
    return None


def extract_requirement_keys(
    prompt_text: str,
    pattern: re.Pattern[str] = re.compile(r"^###\s+(S\d+)\b", re.MULTILINE),
) -> list[str]:
    keys = pattern.findall(prompt_text)
    unique_keys: list[str] = []
    seen: set[str] = set()

    for key in keys:
        if key in seen:
            continue
        seen.add(key)
        unique_keys.append(key)

    if not unique_keys:
        raise ValueError("No verification requirement keys found in verifier prompt")

    return unique_keys


def parse_verifier_output(
    raw_output: str,
    requirement_keys: list[str],
) -> tuple[dict[str, Any], list[str], int]:
    if not raw_output or not raw_output.strip():
        raise ValueError("Empty LLM output")

    json_match = JSON_RE.search(raw_output)
    json_str = json_match.group(1).strip() if json_match else raw_output.strip()
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Non-parseable JSON from LLM: {e}") from e

    if not isinstance(data, dict):
        raise ValueError("Verifier output is not a JSON object")

    failed: list[str] = []
    total_score = 0
    for key in requirement_keys:
        entry = data.get(key)
        if not isinstance(entry, dict):
            raise ValueError(f"Missing {key} object")
        score = _normalize_score(entry.get("score"))
        if score is None:
            raise ValueError(f"Invalid score for {key}")
        total_score += score
        if score != 1:
            failed.append(key)

    return data, failed, total_score


# ---------------------------------------------------------------------------
# Algorithmic + LLM verification helpers
# ---------------------------------------------------------------------------


def build_combined_verifier_output(
    algorithmic_output: dict[str, dict[str, Any]],
    llm_output: dict[str, Any],
    algorithmic_keys: list[str],
    llm_keys: list[str],
) -> dict[str, Any]:
    combined: dict[str, Any] = {}

    for key in algorithmic_keys:
        combined[key] = algorithmic_output.get(key, {"score": 1, "reason": None})

    for key in llm_keys:
        entry = llm_output.get(key)
        if isinstance(entry, dict):
            combined[key] = entry
        else:
            combined[key] = {
                "score": 0,
                "reason": f"Missing LLM verifier result for {key}.",
            }

    return combined


def compute_verifier_summary(
    results: list[dict[str, Any]],
    all_requirement_keys: list[str],
) -> dict[str, Any]:
    valid_results = [result for result in results if result.get("error") is None]
    valid_count = len(valid_results)
    passed_count = sum(1 for result in valid_results if result.get("passed") is True)
    failed_count = valid_count - passed_count
    error_count = len(results) - valid_count

    requirement_stats: dict[str, dict[str, int | float | None]] = {}
    for key in all_requirement_keys:
        passed = 0
        for result in valid_results:
            output = result.get("verifier_output") or {}
            entry = output.get(key) or {}
            score = _normalize_score(entry.get("score"))
            if score == 1:
                passed += 1
        requirement_stats[key] = {
            "passed": passed,
            "pass_rate": passed / valid_count if valid_count else None,
        }

    return {
        "total": {
            "evaluated": len(results),
            "valid": valid_count,
            "errors": error_count,
            "passed": passed_count,
            "failed": failed_count,
            "pass_rate": passed_count / valid_count if valid_count else None,
        },
        "requirements": requirement_stats,
    }

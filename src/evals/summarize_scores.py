from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path
from statistics import mean
from typing import Any


GRADER_KEY = "llm_rubric"

OVERALL_SCORE_MAP: dict[str, float] = {
    "strong_match": 1.0,
    "good_match": 0.75,
    "partial_match": 0.5,
    "weak_match": 0.25,
    "mismatch": 0.0,
}

DIMENSION_SCORE_MAP: dict[str, float] = {
    "yes": 1.0,
    "partial_minor": 0.67,
    "partial_major": 0.33,
    "no": 0.0,
}

DIMENSIONS: list[str] = [
    "structure",
    "system_reconstruction",
    "timeline_structure",
    "timeline_progression",
    "diagnosis",
    "support_alignment",
    "uncertainty",
]

DIMENSION_LABELS: dict[str, str] = {
    "structure": "Report structure",
    "system_reconstruction": "System reconstruction",
    "timeline_structure": "Timeline structure",
    "timeline_progression": "Timeline progression",
    "diagnosis": "Diagnosis",
    "support_alignment": "Support alignment",
    "uncertainty": "Uncertainty",
}

OVERALL_VERDICT_ORDER = ["strong_match", "good_match", "partial_match", "weak_match", "mismatch"]
OVERALL_VERDICT_LABELS = {
    "strong_match": "strong",
    "good_match": "good",
    "partial_match": "partial",
    "weak_match": "weak",
    "mismatch": "mismatch",
}

DIMENSION_VERDICT_ORDER = ["yes", "partial_minor", "partial_major", "no"]
DIMENSION_VERDICT_HEADERS = ["yes", "p+", "p-", "no"]

WIDTH = 78
DIM_COL = 23
BAR_WIDTH = 22


# ---------------------------------------------------------------------------
# ANSI helpers (auto-disabled when not on a TTY or NO_COLOR is set)
# ---------------------------------------------------------------------------

_USE_COLOR = sys.stdout.isatty() and os.environ.get("NO_COLOR") is None


def _ansi(s: str, code: str) -> str:
    if not _USE_COLOR or not code:
        return s
    return f"\033[{code}m{s}\033[0m"


def _bold(s: str) -> str:
    return _ansi(s, "1")


def _dim(s: str) -> str:
    return _ansi(s, "2")


def _color_for_score(value: float) -> str:
    if value >= 0.75:
        return "32"  # green
    if value >= 0.50:
        return "36"  # cyan
    if value >= 0.25:
        return "33"  # yellow
    return "31"  # red


def _bar(value: float, width: int = BAR_WIDTH) -> str:
    """Unicode progress bar with sub-cell precision; filled portion colored."""
    value = max(0.0, min(1.0, value))
    filled = value * width
    full_blocks = int(filled)
    remainder = filled - full_blocks
    partial_idx = round(remainder * 8)
    if partial_idx == 8:
        full_blocks += 1
        partial_idx = 0
    full_blocks = min(full_blocks, width)

    partials = ["", "▏", "▎", "▍", "▌", "▋", "▊", "▉"]
    filled_part = "█" * full_blocks
    if partial_idx > 0 and full_blocks < width:
        filled_part += partials[partial_idx]
        empty_count = width - full_blocks - 1
    else:
        empty_count = width - full_blocks
    empty_part = "░" * empty_count

    if _USE_COLOR:
        code = _color_for_score(value)
        return f"\033[{code}m{filled_part}\033[0m{_dim(empty_part)}"
    return filled_part + empty_part


# ---------------------------------------------------------------------------
# Layout helpers
# ---------------------------------------------------------------------------


def _rule(char: str = "─") -> str:
    return _dim(char * WIDTH)


def _header(title: str, subtitle: str = "", right: str = "") -> None:
    print()
    print(_dim("━" * WIDTH))
    title_line = " " + _bold(title)
    if right:
        # Right-align `right` on the same line, accounting for ANSI codes in title.
        visible_title_len = 1 + len(title)  # space + bare title
        pad = max(1, WIDTH - visible_title_len - len(right) - 1)
        title_line = title_line + " " * pad + _dim(right)
    print(title_line)
    if subtitle:
        print(" " + _dim(subtitle))
    print(_dim("━" * WIDTH))


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def _load_judge_results(run_dir: Path) -> list[dict[str, Any]]:
    agent_dirs = [p for p in run_dir.iterdir() if p.is_dir()]
    if not agent_dirs:
        return []
    agent_dir = agent_dirs[0]

    out: list[dict[str, Any]] = []
    for task_file in sorted(agent_dir.glob("*.json")):
        data = json.loads(task_file.read_text())
        trials = data.get("trials", [])
        if not trials:
            continue

        grader_result = trials[0].get("grader_results", {}).get(GRADER_KEY)
        if not grader_result:
            continue

        result_obj = grader_result.get("result", {})
        overall = result_obj.get("overall_verdict")
        dim_scores = result_obj.get("dimension_scores", {})

        if not isinstance(overall, str) or overall not in OVERALL_SCORE_MAP:
            continue

        per_dim_verdict: dict[str, str] = {}
        valid = True
        for dim in DIMENSIONS:
            entry = dim_scores.get(dim)
            if not isinstance(entry, dict):
                valid = False
                break
            verdict = entry.get("verdict")
            if verdict not in DIMENSION_SCORE_MAP:
                valid = False
                break
            per_dim_verdict[dim] = verdict
        if not valid:
            continue

        out.append({
            "task": task_file.stem,
            "overall_verdict": overall,
            "dim_verdict": per_dim_verdict,
        })

    return out


# ---------------------------------------------------------------------------
# Per-judge / combined rendering
# ---------------------------------------------------------------------------


def _print_overall_distribution(counter: Counter, total: int) -> None:
    print(_dim("  Overall verdict distribution"))
    for verdict in OVERALL_VERDICT_ORDER:
        n = counter.get(verdict, 0)
        pct = (n / total) * 100.0 if total else 0.0
        bar = _bar(pct / 100.0, width=BAR_WIDTH) if n else _dim("░" * BAR_WIDTH)
        label = OVERALL_VERDICT_LABELS[verdict]
        count_str = f"{n:>3d}  {pct:>5.1f}%"
        if n == 0:
            count_str = _dim(count_str)
        print(f"    {label:<10s} {bar}  {count_str}")


def _print_dimension_table(
    dim_means: dict[str, float],
    dim_counts: dict[str, Counter],
) -> None:
    headers = " ".join(f"{h:>4s}" for h in DIMENSION_VERDICT_HEADERS)
    print(_dim(
        f"  {'Dimension':<{DIM_COL}s} {'Score':>5s}  {'':<{BAR_WIDTH}s}  {headers}"
    ))
    print(_dim(
        f"  {'─' * DIM_COL} {'─' * 5}  {'─' * BAR_WIDTH}  {'─' * (5 * 4 - 1)}"
    ))
    for dim in DIMENSIONS:
        bar = _bar(dim_means[dim])
        counts = dim_counts[dim]
        count_strs = []
        for verdict in DIMENSION_VERDICT_ORDER:
            v = counts.get(verdict, 0)
            cell = f"{v:>4d}"
            count_strs.append(cell if v else _dim(cell))
        label = DIMENSION_LABELS.get(dim, dim)
        print(
            f"  {label:<{DIM_COL}s} {dim_means[dim]:>5.3f}  {bar}  "
            + " ".join(count_strs)
        )


def _print_dimension_table_means_only(dim_means: dict[str, float]) -> None:
    print(_dim(
        f"  {'Dimension':<{DIM_COL}s} {'Score':>5s}  {'':<{BAR_WIDTH}s}"
    ))
    print(_dim(
        f"  {'─' * DIM_COL} {'─' * 5}  {'─' * BAR_WIDTH}"
    ))
    for dim in DIMENSIONS:
        label = DIMENSION_LABELS.get(dim, dim)
        bar = _bar(dim_means[dim])
        print(f"  {label:<{DIM_COL}s} {dim_means[dim]:>5.3f}  {bar}")


def _summarize_judge(label: str, run_dir: Path) -> dict[str, Any] | None:
    if not run_dir.is_dir():
        _header(f"Judge: {label}", subtitle=f"{run_dir}  (not found, skipping)")
        return None

    results = _load_judge_results(run_dir)
    if not results:
        _header(f"Judge: {label}", subtitle=f"{run_dir}  (no graded results)")
        return None

    n = len(results)

    overall_counts = Counter(r["overall_verdict"] for r in results)
    overall_mean = mean(OVERALL_SCORE_MAP[r["overall_verdict"]] for r in results)

    dim_counts: dict[str, Counter] = {dim: Counter() for dim in DIMENSIONS}
    dim_means: dict[str, float] = {}
    for dim in DIMENSIONS:
        for r in results:
            dim_counts[dim][r["dim_verdict"][dim]] += 1
        dim_means[dim] = mean(DIMENSION_SCORE_MAP[r["dim_verdict"][dim]] for r in results)

    _header(
        f"Judge: {label}",
        subtitle=str(run_dir),
        right=f"{n} task{'s' if n != 1 else ''}",
    )

    print()
    overall_bar = _bar(overall_mean)
    print(f"  {_bold('Overall score')}  {_bold(f'{overall_mean:.3f}')}  {overall_bar}")
    print()
    _print_overall_distribution(overall_counts, n)
    print()
    _print_dimension_table(dim_means, dim_counts)

    return {
        "label": label,
        "n": n,
        "overall_mean": overall_mean,
        "dim_means": dim_means,
    }


def _print_combined(judge_summaries: list[dict[str, Any]]) -> None:
    if not judge_summaries:
        return
    if len(judge_summaries) < 2:
        print()
        print(_dim("  (Only one judge available — cross-judge average not shown.)"))
        return

    overall_avg = mean(s["overall_mean"] for s in judge_summaries)
    dim_avg = {
        dim: mean(s["dim_means"][dim] for s in judge_summaries)
        for dim in DIMENSIONS
    }

    labels = ", ".join(s["label"] for s in judge_summaries)
    _header(
        "Final score",
        subtitle=f"averaged across judges: {labels}",
    )
    print()
    bar = _bar(overall_avg)
    print(f"  {_bold('Overall')}        {_bold(f'{overall_avg:.3f}')}  {bar}")
    print()
    _print_dimension_table_means_only(dim_avg)
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Summarize per-judge and cross-judge scores for a graded run."
    )
    parser.add_argument("--run-id", required=True, help="Run id (directory name under each judge root).")
    parser.add_argument(
        "--judge",
        action="append",
        metavar="LABEL=PATH",
        help=(
            "Judge label and root directory, e.g. 'gpt=data/runs/graded-judge-gpt'. "
            "Repeatable. The run id is appended to each path."
        ),
    )
    args = parser.parse_args()

    if not args.judge:
        args.judge = [
            "gpt=data/runs/graded-judge-gpt",
            "claude=data/runs/graded-judge-claude",
        ]

    judge_summaries: list[dict[str, Any]] = []
    for spec in args.judge:
        if "=" not in spec:
            parser.error(f"--judge expects LABEL=PATH, got {spec!r}")
        label, root = spec.split("=", 1)
        run_dir = Path(root) / args.run_id
        summary = _summarize_judge(label, run_dir)
        if summary is not None:
            judge_summaries.append(summary)

    _print_combined(judge_summaries)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

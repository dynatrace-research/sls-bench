from __future__ import annotations

import ast
import csv
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


EXPECTED_CSV_COLUMNS = ["timestamp", "level", "message", "trace_id", "service", "host"]
EXPECTED_ROW_COUNT_MIN = 20_000
EXPECTED_ROW_COUNT_MAX = 100_000
ALLOWED_THIRD_PARTY_IMPORTS = {"numpy", "pandas"}
SCRIPT_RUN_TIMEOUT_SECONDS = 30


def grade(
    script: str, timeout: int = SCRIPT_RUN_TIMEOUT_SECONDS
) -> tuple[list[dict[str, Any]], str | None]:
    """Verify algorithmic requirements A1-A4.

    Returns (violations, runtime_error) where violations is a list of
    ``{"requirement": "Ax", "score": 0, "reason": str}`` dicts for failed
    checks and *runtime_error* is set when the script fails to execute or
    produces structurally invalid output.
    """
    alg: dict[str, dict[str, Any]] = {}

    # -- A3: deterministic (static check) --
    if re.search(r"\brandom\.seed\s*\(", script):
        alg["A3"] = {"score": 1, "reason": None}
    else:
        alg["A3"] = {
            "score": 0,
            "reason": "Script does not call random.seed() for reproducibility.",
        }

    # -- A4: self-contained, allows stdlib + numpy/pandas (static check) --
    a4_issues: list[str] = []
    try:
        tree = ast.parse(script)
        stdlib = sys.stdlib_module_names
        allowed_imports = stdlib | ALLOWED_THIRD_PARTY_IMPORTS
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    top = alias.name.split(".")[0]
                    if top not in allowed_imports:
                        a4_issues.append(
                            f"disallowed import '{alias.name}' (only stdlib, numpy, pandas are allowed)"
                        )
            elif isinstance(node, ast.ImportFrom) and node.module:
                top = node.module.split(".")[0]
                if top not in allowed_imports:
                    a4_issues.append(
                        f"disallowed 'from {node.module} import ...' (only stdlib, numpy, pandas are allowed)"
                    )
    except SyntaxError as e:
        a4_issues.append(f"syntax error prevents import check: {e}")
    alg["A4"] = (
        {"score": 0, "reason": "; ".join(sorted(set(a4_issues)))}
        if a4_issues
        else {"score": 1, "reason": None}
    )

    # -- Run the script for A1 (volume) and A2 (CSV format) --
    tmp_dir = tempfile.mkdtemp(prefix="logscript_alg_")
    runtime_error: str | None = None
    try:
        script_path = Path(tmp_dir) / "script.py"
        script_path.write_text(script, encoding="utf-8")
        proc = subprocess.run(
            ["python", str(script_path)],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=tmp_dir,
        )
        if proc.returncode != 0:
            runtime_error = proc.stderr.strip() or proc.stdout.strip()
            reason = f"Script failed (exit code {proc.returncode})."
            alg["A1"] = {"score": 0, "reason": reason}
            alg["A2"] = {"score": 0, "reason": reason}
            return _to_violations(alg), runtime_error

        csv_path = Path(tmp_dir) / "logs.csv"
        if not csv_path.exists():
            runtime_error = "Script ran but did not produce logs.csv."
            alg["A1"] = {"score": 0, "reason": runtime_error}
            alg["A2"] = {"score": 0, "reason": runtime_error}
            return _to_violations(alg), runtime_error

        a2_issues: list[str] = []
        row_count = 0
        with csv_path.open("r", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            header = next(reader, None)
            if header is None:
                runtime_error = "logs.csv is empty (no header row)."
                alg["A1"] = {"score": 0, "reason": runtime_error}
                alg["A2"] = {"score": 0, "reason": runtime_error}
                return _to_violations(alg), runtime_error

            if header != EXPECTED_CSV_COLUMNS:
                missing = set(EXPECTED_CSV_COLUMNS) - set(header)
                extra = set(header) - set(EXPECTED_CSV_COLUMNS)
                parts = [f"Expected columns {EXPECTED_CSV_COLUMNS}, got {header}."]
                if missing:
                    parts.append(f"Missing: {sorted(missing)}.")
                if extra:
                    parts.append(f"Unexpected: {sorted(extra)}.")
                a2_issues.append(" ".join(parts))

            prev_ts = ""
            unsorted = 0
            for row in reader:
                row_count += 1
                if row and row[0] < prev_ts:
                    unsorted += 1
                if row:
                    prev_ts = row[0]
            if unsorted > 0:
                a2_issues.append(
                    f"Timestamps not sorted ascending ({unsorted} out-of-order rows)."
                )

        # A1 - log volume
        if EXPECTED_ROW_COUNT_MIN <= row_count <= EXPECTED_ROW_COUNT_MAX:
            alg["A1"] = {"score": 1, "reason": None}
        else:
            reason = (
                f"Row count {row_count} outside "
                f"[{EXPECTED_ROW_COUNT_MIN}, {EXPECTED_ROW_COUNT_MAX}]."
            )
            alg["A1"] = {"score": 0, "reason": reason}
            runtime_error = reason

        # A2 - CSV format
        if a2_issues:
            reason = " ".join(a2_issues)
            alg["A2"] = {"score": 0, "reason": reason}
            runtime_error = runtime_error or reason
        else:
            alg["A2"] = {"score": 1, "reason": None}

    except subprocess.TimeoutExpired:
        runtime_error = f"Script timed out after {timeout}s."
        alg.setdefault("A1", {"score": 0, "reason": runtime_error})
        alg.setdefault("A2", {"score": 0, "reason": runtime_error})
    except Exception as e:
        runtime_error = f"Failed to run script: {e}"
        alg.setdefault("A1", {"score": 0, "reason": runtime_error})
        alg.setdefault("A2", {"score": 0, "reason": runtime_error})
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return _to_violations(alg), runtime_error


def _to_violations(alg: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert the internal A-key dict to a list of violation dicts (score==0 only)."""
    return [
        {"requirement": key, "score": 0, "reason": entry["reason"]}
        for key, entry in sorted(alg.items())
        if entry["score"] != 1
    ]

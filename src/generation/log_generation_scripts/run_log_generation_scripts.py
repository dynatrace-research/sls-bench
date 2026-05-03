import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from tqdm import tqdm


DEFAULT_SCRIPTS_DIR = Path("./data/logs/log-generation-scripts")
DEFAULT_OUTPUT_DIR = Path("./data/logs/log-files")
DEFAULT_TIMEOUT_SECONDS = 300


def _to_text(output: str | bytes | None) -> str:
    if output is None:
        return ""
    if isinstance(output, bytes):
        return output.decode("utf-8", errors="replace")
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run all .py scripts in a folder and save each produced logs.csv "
            "as <script_name>.csv in the output folder."
        )
    )
    parser.add_argument(
        "--scripts-dir",
        type=Path,
        default=DEFAULT_SCRIPTS_DIR,
        help="Folder with generated Python scripts.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Folder where renamed CSV files are written.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Per-script timeout in seconds.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop at first failed script.",
    )
    return parser.parse_args()


def discover_scripts(scripts_dir: Path) -> list[Path]:
    if not scripts_dir.exists():
        raise FileNotFoundError(f"Scripts directory does not exist: {scripts_dir}")
    if not scripts_dir.is_dir():
        raise NotADirectoryError(f"Scripts path is not a directory: {scripts_dir}")

    scripts = sorted(path for path in scripts_dir.glob("*.py") if path.is_file())
    if not scripts:
        raise ValueError(f"No Python scripts found in: {scripts_dir}")
    return scripts


def run_script_and_collect_csv(
    script_path: Path,
    output_dir: Path,
    timeout_seconds: int,
) -> tuple[bool, Path, str, str, str | None]:
    output_csv_path = output_dir / f"{script_path.stem}.csv"

    with tempfile.TemporaryDirectory(
        prefix=f"loggen_{script_path.stem}_"
    ) as tmp_dir_raw:
        tmp_dir = Path(tmp_dir_raw)
        try:
            result = subprocess.run(
                [sys.executable, str(script_path)],
                cwd=tmp_dir,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            stdout = _to_text(exc.stdout).strip()
            stderr = _to_text(exc.stderr).strip()
            return (
                False,
                output_csv_path,
                stdout,
                stderr,
                f"Timed out after {timeout_seconds}s",
            )

        stdout = result.stdout.strip()
        stderr = result.stderr.strip()

        if result.returncode != 0:
            return (
                False,
                output_csv_path,
                stdout,
                stderr,
                f"Exited with code {result.returncode}",
            )

        generated_csv = tmp_dir / "logs.csv"
        if not generated_csv.exists():
            return (
                False,
                output_csv_path,
                stdout,
                stderr,
                "Script finished but logs.csv was not generated",
            )

        shutil.copy2(generated_csv, output_csv_path)
        return True, output_csv_path, stdout, stderr, None


def print_script_logs(script_name: str, stdout: str, stderr: str) -> None:
    if stdout:
        tqdm.write(f"[{script_name}] stdout:\n{stdout}")
    if stderr:
        tqdm.write(f"[{script_name}] stderr:\n{stderr}")


def main() -> None:
    args = parse_args()
    scripts_dir = args.scripts_dir.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    scripts = discover_scripts(scripts_dir)
    tqdm.write(f"Found {len(scripts)} script(s) in {scripts_dir}")
    tqdm.write(f"Output directory: {output_dir}")

    failures: list[tuple[str, str]] = []
    success_count = 0

    for script_path in tqdm(scripts, desc="Running scripts", unit="script"):
        tqdm.write(f"Running: {script_path.name}")
        (
            success,
            output_csv_path,
            stdout,
            stderr,
            error,
        ) = run_script_and_collect_csv(
            script_path=script_path,
            output_dir=output_dir,
            timeout_seconds=args.timeout_seconds,
        )

        print_script_logs(script_path.name, stdout, stderr)

        if success:
            success_count += 1
            tqdm.write(f"[{script_path.name}] OK -> {output_csv_path}")
            continue

        failures.append((script_path.name, error or "Unknown error"))
        tqdm.write(f"[{script_path.name}] FAILED: {error}")
        if args.fail_fast:
            break

    total = success_count + len(failures)
    print()
    print(f"Finished: {success_count}/{total} scripts succeeded.")
    if failures:
        print("Failed scripts:")
        for name, reason in failures:
            print(f"- {name}: {reason}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()

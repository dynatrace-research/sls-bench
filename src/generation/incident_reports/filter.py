import argparse
import asyncio
import json
import re
import shutil
from enum import Enum
from pathlib import Path

import pandas as pd
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.rate_limiters import InMemoryRateLimiter
from pydantic import BaseModel, ValidationError
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio

from models.factory import AzureModelConfig, create_model
from models.helpers import TokenUsage, extract_response


CONCURRENCY = 20
MAX_RETRY_ATTEMPTS = 10
LLM_TIMEOUT_SECONDS = 60 * 1

MODEL_NAME = "gpt-5.2"
REASONING_EFFORT = "medium"


class PostmortemLabel(str, Enum):
    GOOD = "GOOD_POSTMORTEM"
    BAD = "BAD_POSTMORTEM"


class FilterResult(BaseModel):
    label: PostmortemLabel
    missing: list[str]
    reasons: list[str]


FILTER_PROMPT = f"""
Classify the document as exactly one label: {PostmortemLabel.GOOD.value} or {PostmortemLabel.BAD.value}.

## Strict rule
Label {PostmortemLabel.GOOD.value} ONLY if ALL requirements (1-7) below are explicitly satisfied in the text.
If anything is missing, vague, implied, or not tied to concrete evidence, label {PostmortemLabel.BAD.value}.

The main goal is to find postmortems that allow to infer the system structure and write a "scenario" that explains what happened and why, so that we can use the structure and scenario to write a python script that generates logs that we could have observed before, during, and after the incident described.

## Requirements

1) Postmortem (post-incident analysis)
Must be a retrospective/postmortem describing an incident after the fact (not just a status update, announcement, changelog, forum thread, Q&A, or article).

2) Cloud/distributed service failure
Must be about a failure of a backend/service hosted in a cloud-based or distributed environment.
Reject if it is mainly: client/desktop issue, purely organizational/process, security-only without operational failure symptoms, single-host/local issue, etc.

3) Explains what happened, why, and observed system behaviors
Must explicitly describe:
- what happened (impact + concrete symptoms),
- why it happened (root cause or causal chain; not speculation-only),
- observed system behaviors during the incident (specific technical behaviors, not generic "degraded").

4) Clear system/component details (names + role)
Must name the relevant system/components AND describe what they do (role/functionality) so the system or parts of the system relevant for the incident can be inferred.
Generic labels like "the service", "backend", "infrastructure" without named components and roles are insufficient.

5) Sufficient detail to reconstruct the scenario over time
Must provide enough detail to reconstruct how the failure unfolded over time:
- either timestamps OR an ordered sequence of events,
AND
- clear progression (detection -> investigation/triage -> mitigation -> recovery) with concrete event descriptions (not just "we fixed it").

6) Log-reflectable incident (explicit incident-time signals)
The problem must be clearly reflectable in system logs/traces (not only business impact).
Must include at least TWO explicit incident-time signals that are specific enough to log, such as:
- explicit error codes/types (e.g., HTTP 5xx/429, gRPC status, DB error, specific exception),
- explicit timeouts/connection failures/retry storms,
- crash/restart/OOM/resource exhaustion with a described symptom,
- failed health checks, leader elections, deployment/rollback failures,
- queue lag/backlog growth, throttling/rate limiting.
If signals are only generic (e.g., "errors occurred", "performance issues") without concrete loggable manifestations, reject.

7) Metrics-based detection (or explicitly metric-convertible)
Must explicitly state EITHER:
- the incident was detected via metrics/monitoring/alerts/anomaly (error rate/latency/saturation/SLO/etc.), OR
- a concrete numeric or categorical signal that is easily and directly convertible to a metric is explicitly mentioned (e.g., "HTTP 500 rate increased", "p95 latency rose", "CPU hit 100%", "queue depth grew", "timeouts spiked").
If detection is only via user reports/support tickets and the text does not explicitly provide a metric-like signal, label as {PostmortemLabel.BAD.value}.

## Output (JSON only)
Return only:
```json
{{{{
  "label": "{PostmortemLabel.GOOD.value} | {PostmortemLabel.BAD.value}",
  "missing": [],
  "reasons": [
    "Reason 1: concrete evidence or explicit gap",
    "Reason 2: concrete evidence or explicit gap"
  ]
}}}}
```

## Output field rules
- If label is {PostmortemLabel.GOOD.value}: "missing" must be [].
- If label is {PostmortemLabel.BAD.value}: "missing" must list every failed check using only:
["postmortem","cloud_distributed","what_happened","why","observed_behaviors","system_structure","timeline","log_signals","metrics_signal"]
- "reasons": 1-6 short bullets. Each must quote or tightly paraphrase evidence, or state exactly what is absent. No extra commentary.
"""


def load_metadata_and_content(
    input_dir: Path,
) -> tuple[pd.DataFrame, list[tuple[str, Path]]]:
    """Load metadata and get content files from input directory."""
    metadata_path = input_dir / "metadata.csv"
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")
    metadata = pd.read_csv(metadata_path, encoding="utf-8")

    content_files = []
    for _, row in metadata.iterrows():
        content_id = row["id"]
        path = input_dir / f"{content_id}.txt"
        if path.exists():
            content_files.append((content_id, path))

    return metadata, content_files


def parse_llm_output(raw_output: str) -> FilterResult:
    """Parse and validate LLM JSON output."""
    if not raw_output or not raw_output.strip():
        raise ValueError("Empty LLM output")

    json_match = re.search(r"```json\s*(.*?)\s*```", raw_output, re.DOTALL)
    if json_match:
        json_str = json_match.group(1)
    else:
        json_str = raw_output.strip()

    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Non-parseable JSON from LLM: {e}") from e

    return FilterResult(**data)


def _error_result(
    content_id: str,
    content_path: Path,
    raw_output: str | None,
    error: str,
    token_usage: TokenUsage,
) -> dict:
    return {
        "id": content_id,
        "path": str(content_path),
        "label": None,
        "missing": None,
        "reasons": None,
        "raw_output": raw_output,
        "error": error,
        "token_usage": token_usage,
    }


def _add_usage(a: TokenUsage, b: TokenUsage) -> TokenUsage:
    return TokenUsage(
        input_tokens=a.input_tokens + b.input_tokens,
        output_tokens=a.output_tokens + b.output_tokens,
        cached_input_tokens=a.cached_input_tokens + b.cached_input_tokens,
        reasoning_tokens=a.reasoning_tokens + b.reasoning_tokens,
    )


async def predict(
    model: BaseChatModel, prompt: str, content: str
) -> tuple[FilterResult, str, TokenUsage]:
    """Call LLM with timeout + retries and parse output within retry loop."""
    total_usage = TokenUsage()
    messages = [SystemMessage(content=prompt), HumanMessage(content=content)]

    async for attempt in AsyncRetrying(
        stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=1, min=5, max=60),
        reraise=True,
    ):
        with attempt:
            response = await asyncio.wait_for(
                model.ainvoke(messages),
                timeout=LLM_TIMEOUT_SECONDS,
            )
            extracted = extract_response(response)
            total_usage = _add_usage(total_usage, extracted.token_usage)
            result = parse_llm_output(extracted.final_answer)
            return result, extracted.final_answer, total_usage

    raise RuntimeError("Unreachable: retry loop exited without returning")


async def filter_content(
    content_id: str,
    content_path: Path,
    model: BaseChatModel,
    semaphore: asyncio.Semaphore,
) -> dict:
    """Read content, run LLM classification, return result dict."""
    async with semaphore:
        raw_output: str | None = None
        token_usage = TokenUsage()
        try:
            content = content_path.read_text(encoding="utf-8")
            result, raw_output, token_usage = await predict(model, FILTER_PROMPT, content)

            if result.label == PostmortemLabel.GOOD:
                missing, reasons = [], result.reasons
            else:
                missing, reasons = result.missing, result.reasons

            tqdm.write(f"Processed {content_id}: label={result.label} | {token_usage}")
            return {
                "id": content_id,
                "path": str(content_path),
                "label": result.label,
                "missing": missing,
                "reasons": reasons,
                "raw_output": raw_output,
                "error": None,
                "token_usage": token_usage,
            }
        except (ValidationError, ValueError) as e:
            tqdm.write(f"Error for {content_id}: {e} | {token_usage}")
            return _error_result(
                content_id, content_path, raw_output, str(e), token_usage
            )
        except Exception as e:
            tqdm.write(f"Failed {content_id}: {type(e).__name__}: {e} | {token_usage}")
            return _error_result(
                content_id,
                content_path,
                raw_output,
                f"{type(e).__name__}: {e}",
                token_usage,
            )


async def filter_all(
    content_files: list[tuple[str, Path]], model: BaseChatModel
) -> list[dict]:
    """Filter all content files concurrently."""
    semaphore = asyncio.Semaphore(CONCURRENCY)
    tasks = [
        filter_content(content_id, path, model, semaphore)
        for content_id, path in content_files
    ]
    results = await tqdm_asyncio.gather(*tasks, desc="Filtering")
    return [r for r in results if r]


def save_results(results: list[dict], output_dir: Path, metadata: pd.DataFrame) -> None:
    """Save all outputs to file and split content into pass/fail folders."""
    output_dir.mkdir(parents=True, exist_ok=True)
    pass_dir = output_dir / "pass"
    fail_dir = output_dir / "fail"
    pass_dir.mkdir(exist_ok=True)
    fail_dir.mkdir(exist_ok=True)

    results_path = output_dir / "filter_results.json"
    with open(results_path, "w", encoding="utf-8") as f:
        json.dump(
            results,
            f,
            indent=2,
            ensure_ascii=False,
            default=lambda o: o.model_dump() if hasattr(o, "model_dump") else str(o),
        )

    summary_rows = []
    for r in results:
        u: TokenUsage = r.get("token_usage") or TokenUsage()
        summary_rows.append(
            {
                "id": r["id"],
                "label": r["label"],
                "missing": (
                    json.dumps(r["missing"], ensure_ascii=False)
                    if r["missing"]
                    else None
                ),
                "reasons": (
                    json.dumps(r["reasons"], ensure_ascii=False)
                    if r["reasons"]
                    else None
                ),
                "error": r["error"],
                "input_tokens": u.input_tokens,
                "output_tokens": u.output_tokens,
                "cached_input_tokens": u.cached_input_tokens,
                "reasoning_tokens": u.reasoning_tokens,
            }
        )
    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(output_dir / "filter_summary.csv", index=False, encoding="utf-8")

    pass_ids = []
    fail_ids = []
    error_count = 0
    for r in results:
        content_id = r["id"]
        source_path = Path(r["path"])
        if r["label"] == PostmortemLabel.GOOD:
            shutil.copy(source_path, pass_dir / f"{content_id}.txt")
            pass_ids.append(content_id)
        elif r["label"] == PostmortemLabel.BAD:
            shutil.copy(source_path, fail_dir / f"{content_id}.txt")
            fail_ids.append(content_id)
        else:
            error_count += 1

    pass_metadata = metadata[metadata["id"].isin(pass_ids)]
    fail_metadata = metadata[metadata["id"].isin(fail_ids)]
    pass_metadata.to_csv(pass_dir / "metadata.csv", index=False, encoding="utf-8")
    fail_metadata.to_csv(fail_dir / "metadata.csv", index=False, encoding="utf-8")

    total = TokenUsage()
    for r in results:
        u = r.get("token_usage") or TokenUsage()
        total = _add_usage(total, u)

    print(
        f"\nResults: {len(pass_ids)} pass, {len(fail_ids)} fail, {error_count} errors"
    )
    print(f"Token totals: {total}")
    print(f"Outputs saved to {output_dir}")


def main():
    parser = argparse.ArgumentParser(description="Filter incident reports via LLM")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    args.input_dir = args.input_dir.resolve()
    args.output_dir = args.output_dir.resolve()

    metadata, content_files = load_metadata_and_content(args.input_dir)

    model = create_model(
        AzureModelConfig(
            model_name=MODEL_NAME,
            reasoning_effort=REASONING_EFFORT,
            rate_limiter=InMemoryRateLimiter(requests_per_second=1, max_bucket_size=2),
        )
    )
    results = asyncio.run(filter_all(content_files, model))
    save_results(results, args.output_dir, metadata)


if __name__ == "__main__":
    main()

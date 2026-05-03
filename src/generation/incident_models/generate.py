import argparse
import re
import json
import shutil
import asyncio
import logging
from functools import partial
from pathlib import Path
from typing import Literal, cast

from pydantic import BaseModel, Field
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.rate_limiters import InMemoryRateLimiter
from tqdm.asyncio import tqdm_asyncio

from models.factory import AzureModelConfig, create_model
from models.helpers import ainvoke_model, TokenUsage
from loop import Loop, LoopConfig, LoopResult
from loop import GenerationResult, VerificationResult

from generation.incident_models.alg_verifier import grade as algorithmic_grade


logger = logging.getLogger(__name__)


class IncidentReport(BaseModel):
    id: str
    content: str


class IncidentModel(BaseModel):
    id: str
    system_description: str
    scenario: str


def load_incident_reports(path: Path) -> list[IncidentReport]:
    incident_reports = []
    for p in sorted(path.glob("*.txt")):
        incident_reports.append(
            IncidentReport(id=p.stem, content=p.read_text(encoding="utf-8"))
        )
    return incident_reports


def save_loop_result(result: LoopResult[IncidentModel], path: Path) -> None:
    system_description_dir = path / "system-descriptions"
    scenario_dir = path / "scenarios"
    system_description_dir.mkdir(parents=True, exist_ok=True)
    scenario_dir.mkdir(parents=True, exist_ok=True)

    inc_model = result.output
    file_name = f"{inc_model.id}.txt"
    (system_description_dir / file_name).write_text(inc_model.system_description, encoding="utf-8")
    (scenario_dir / file_name).write_text(inc_model.scenario, encoding="utf-8")

    metadata = {"id": result.output.id, "passed": result.passed, "history": result.history}
    with (path / "metadata.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(metadata, ensure_ascii=False) + "\n")


def create_llms(
    *configs: tuple[str, str],
) -> tuple[BaseChatModel, ...]:
    """Create chat models for generation and eval entrypoints.

    A config routes to Bedrock when its reasoning selector is one of the
    accepted Bedrock sentinels, such as ``""`` (backward compatible) or
    ``"bedrock"``. All other values are treated as Azure reasoning efforts.

    Args:
        *configs: Sequence of ``(model_name, reasoning_selector)`` pairs.

    Returns:
        Instantiated chat models in the same order as the input configs.
    """
    rate_limiter = InMemoryRateLimiter(requests_per_second=1, max_bucket_size=1)
    return tuple(
        create_model(
            AzureModelConfig(
                model_name=model_name,
                reasoning_effort=cast(Literal["low", "medium", "high"], reasoning),
                rate_limiter=rate_limiter,
            )
        )
        for model_name, reasoning in configs
    )


def load_prompts(*paths: Path) -> list[str]:
    return [path.read_text(encoding="utf-8") for path in paths]


def prep_generator_llm_input(
    input: IncidentReport,
    previous_output: IncidentModel | None = None,
    feedback: str | None = None,
) -> str:
    sections = [
        "# User Inputs\n\n"
        "## Post-Mortem\n"
        "<postmortem>\n"
        f"{input.content}\n"
        "</postmortem>\n"
    ]
    if previous_output:
        sections.extend(
            [
                "\n## Previously Generated System Description\n",
                f"<previous_system_description>\n{previous_output.system_description}\n</previous_system_description>\n",
                "\n## Previously Generated Scenario\n",
                f"<previous_scenario>\n{previous_output.scenario}\n</previous_scenario>\n",
                f"\n## Verifier Feedback\n",
                f"<verifier_feedback>\n{feedback}\n</verifier_feedback>\n",
            ]
        )
    return "\n".join(sections)


def prep_verifier_llm_input(input: IncidentReport, output: IncidentModel) -> str:
    return (
        "# User Inputs\n\n"
        "## Post-Mortem\n"
        "<postmortem>\n"
        f"{input.content}\n"
        "</postmortem>\n\n"
        "## System Description\n"
        "<system_description>\n"
        f"{output.system_description}\n"
        "</system_description>\n\n"
        "## Scenario\n"
        "<scenario>\n"
        f"{output.scenario}\n"
        "</scenario>\n"
    )


def extract_tag_content(text: str, tag: str) -> str | None:
    """Return the content between <tag>...</tag>, or None if not found."""
    match = re.search(rf"<{tag}>\s*(.*?)\s*</{tag}>", text, re.DOTALL | re.IGNORECASE)
    return match.group(1).strip() if match else None


def create_generator(llm: BaseChatModel, system_prompt: str):

    async def generator(
        input: IncidentReport,
        previous_output: IncidentModel | None,
        feedback: str | None,
    ) -> GenerationResult[IncidentModel]:
        user_input = prep_generator_llm_input(input, previous_output, feedback)

        response = await ainvoke_model(llm, system_prompt, user_input)

        sys_descr = extract_tag_content(response.final_answer, "system_description")
        scenario = extract_tag_content(response.final_answer, "scenario")
        if sys_descr is None or scenario is None:
            raise RuntimeError("Missing <system_description> or <scenario> tags")
        token_usage = response.token_usage.model_dump()

        return GenerationResult(
            output=IncidentModel(
                id=input.id,
                system_description=sys_descr,
                scenario=scenario,
            ),
            metadata={"generator_token_usage": token_usage},
        )

    return generator


def create_verifier(llm: BaseChatModel, system_prompt: str):

    async def verifier(
        input: IncidentReport, output: IncidentModel
    ) -> VerificationResult:
        alg_violations = algorithmic_grade(output.system_description, output.scenario)
        if alg_violations:
            feedback = json.dumps(
                {
                    x["requirement"]: {"score": x["score"], "reason": x["reason"]}
                    for x in alg_violations
                },
                ensure_ascii=False,
            )
            logger.info(
                f"[{input.id}] Verifier failed: algorithmic violations {list(json.loads(feedback).keys())}"
            )
            return VerificationResult(
                passed=False,
                feedback=feedback,
                metadata={"verifier_alg_feedback": feedback},
            )

        user_input = prep_verifier_llm_input(input, output)
        response = await ainvoke_model(llm, system_prompt, user_input)
        token_usage = response.token_usage.model_dump()
        llm_feedback = json.loads(response.final_answer.strip())
        llm_violations = {
            req_id: req_fbk
            for req_id, req_fbk in llm_feedback.items()
            if req_fbk["score"] == 0
        }
        if llm_violations:
            feedback = json.dumps(llm_violations, ensure_ascii=False)
            logger.info(
                f"[{input.id}] Verifier failed: LLM violations {list(llm_violations.keys())}"
            )
            return VerificationResult(
                passed=False,
                feedback=feedback,
                metadata={
                    "verifier_llm_feedback": feedback,
                    "verifier_token_usage": token_usage,
                },
            )

        return VerificationResult(
            passed=True, metadata={"verifier_token_usage": token_usage}
        )

    return verifier


def clean_output_dir(path: Path) -> None:
    for sub in ("system-descriptions", "scenarios"):
        d = path / sub
        if d.exists():
            shutil.rmtree(d)
    meta = path / "metadata.jsonl"
    if meta.exists():
        meta.unlink()


async def run(
    incident_reports: list[IncidentReport],
    loop: Loop,
    output_path: Path,
) -> list[LoopResult[IncidentModel]]:
    hook = partial(save_loop_result, path=output_path)
    loop_results = await tqdm_asyncio.gather(
        *[loop.run(ir, ir.id, post_run_hook=hook) for ir in incident_reports]
    )
    return loop_results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/incident-reports/filtered/pass"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./data/incident-models"),
    )
    args = parser.parse_args()
    args.input_dir = args.input_dir.resolve()
    args.output_dir = args.output_dir.resolve()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    logger.info("=== Incident model generation pipeline ===")

    logger.info("Cleaning output directory...")
    clean_output_dir(args.output_dir)

    logger.info("Loading data...")
    incident_reports = load_incident_reports(args.input_dir)

    logger.info("Loading models and prompts...")
    gen_llm, ver_llm = create_llms(("gpt-5.2", "high"), ("gpt-5.2", "high"))
    gen_system_prompt, ver_system_prompt = load_prompts(
        Path("src/prompts/generation/incident-models/generator-prompt-v3.2.md"),
        Path("src/prompts/generation/incident-models/verifier-prompt-v3.2.md"),
    )

    logger.info("Creating and running loop...")
    loop_config = LoopConfig(
        max_correction_rounds=20,
        max_reruns=1,
        verifier_passes_required=10,
        concurrency=10,
        generator_timeout=900,
        verifier_timeout=600,
    )
    generator = create_generator(gen_llm, gen_system_prompt)
    verifier = create_verifier(ver_llm, ver_system_prompt)
    loop = Loop(generator, verifier, loop_config)
    loop_results = asyncio.run(run(incident_reports, loop, args.output_dir))

    logger.info("=== Done ===")

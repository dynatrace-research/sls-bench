import argparse
import re
import json
import shutil
import asyncio
import logging
from functools import partial
from pathlib import Path
from typing import Literal

from pydantic import BaseModel
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.rate_limiters import InMemoryRateLimiter
from tqdm.asyncio import tqdm_asyncio

from models.factory import create_model, AzureModelConfig
from models.helpers import ainvoke_model, TokenUsage
from loop import Loop, LoopConfig, LoopResult
from loop import GenerationResult, VerificationResult

from generation.log_generation_scripts.alg_verifier import grade as algorithmic_grade


logger = logging.getLogger(__name__)


CODE_FENCE_RE = re.compile(r"```(?:python)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)


class IncidentModel(BaseModel):
    id: str
    system_description: str
    scenario: str


class LogGenerationScript(BaseModel):
    id: str
    script: str


def load_incident_models(path: Path) -> list[IncidentModel]:
    sd_dir = path / "system-descriptions"
    sc_dir = path / "scenarios"
    models: list[IncidentModel] = []
    for sd_file in sorted(sd_dir.glob("*.txt")):
        sc_file = sc_dir / sd_file.name
        if not sc_file.exists():
            logger.warning("No matching scenario for %s, skipping", sd_file.name)
            continue
        models.append(
            IncidentModel(
                id=sd_file.stem,
                system_description=sd_file.read_text(encoding="utf-8"),
                scenario=sc_file.read_text(encoding="utf-8"),
            )
        )
    return models


def save_loop_result(result: LoopResult[LogGenerationScript], path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

    script_obj = result.output
    file_name = f"{script_obj.id}.py"
    (path / file_name).write_text(script_obj.script.rstrip() + "\n", encoding="utf-8")

    metadata = {"id": script_obj.id, "passed": result.passed, "history": result.history}
    with (path / "metadata.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(metadata, ensure_ascii=False) + "\n")


def create_llms(
    *configs: tuple[str, Literal["low", "medium", "high"]],
) -> tuple[BaseChatModel, ...]:
    rate_limiter = InMemoryRateLimiter(requests_per_second=1, max_bucket_size=1)
    return tuple(
        create_model(
            AzureModelConfig(
                model_name=model_name,
                reasoning_effort=reasoning_effort,
                rate_limiter=rate_limiter,
            )
        )
        for model_name, reasoning_effort in configs
    )


def load_prompts(*paths: Path) -> list[str]:
    return [path.read_text(encoding="utf-8") for path in paths]


def prep_generator_llm_input(
    input: IncidentModel,
    previous_output: LogGenerationScript | None = None,
    feedback: str | None = None,
) -> str:
    sections = [
        "# User Inputs\n\n"
        "## System Description\n"
        "<system_description>\n"
        f"{input.system_description}\n"
        "</system_description>\n\n"
        "## Scenario\n"
        "<scenario>\n"
        f"{input.scenario}\n"
        "</scenario>\n\n"
    ]
    if previous_output:
        sections.extend(
            [
                "## Previously Generated Script\n",
                "<previous_script>\n",
                previous_output.script,
                "\n</previous_script>\n\n",
            ]
        )
        if feedback:
            feedback_data = json.loads(feedback)
            verifier_feedback = feedback_data.get("verifier_feedback")
            runtime_feedback = feedback_data.get("runtime_error")
            if verifier_feedback:
                sections.extend(
                    [
                        "## Verifier Feedback\n",
                        "<verifier_feedback>\n",
                        (
                            verifier_feedback
                            if isinstance(verifier_feedback, str)
                            else json.dumps(
                                verifier_feedback,
                                indent=2,
                                ensure_ascii=False,
                            )
                        ),
                        "\n</verifier_feedback>\n",
                    ]
                )
            if runtime_feedback:
                sections.extend(
                    [
                        "## Runtime Execution Feedback\n",
                        "<runtime_feedback>\n",
                        runtime_feedback,
                        "\n</runtime_feedback>\n",
                    ]
                )
    return "\n".join(sections)


def prep_verifier_llm_input(input: IncidentModel, output: LogGenerationScript) -> str:
    return (
        "\n# User Inputs\n\n"
        "## System Description\n\n"
        "<system_description>\n"
        f"{input.system_description}\n"
        "</system_description>\n\n"
        "## Scenario \n\n"
        "<scenario>\n"
        f"{input.scenario}\n"
        "</scenario>\n\n"
        "## Python Script to Evaluate\n\n"
        "<script>\n"
        f"{output.script}\n"
        "</script>\n"
    )


def create_generator(llm: BaseChatModel, system_prompt: str):

    async def generator(
        input: IncidentModel,
        previous_output: LogGenerationScript | None,
        feedback: str | None,
    ) -> GenerationResult[LogGenerationScript]:
        user_input = prep_generator_llm_input(input, previous_output, feedback)

        response = await ainvoke_model(llm, system_prompt, user_input)

        match = CODE_FENCE_RE.search(response.final_answer)
        script = match.group(1).strip() if match else response.final_answer.strip()
        if not script:
            raise RuntimeError("Empty script output from LLM")

        token_usage = response.token_usage.model_dump()

        return GenerationResult(
            output=LogGenerationScript(id=input.id, script=script),
            metadata={"generator_token_usage": token_usage},
        )

    return generator


def create_verifier(llm: BaseChatModel, system_prompt: str):

    async def verifier(
        input: IncidentModel, output: LogGenerationScript
    ) -> VerificationResult:
        alg_violations, runtime_error = algorithmic_grade(output.script)
        if alg_violations:
            verifier_feedback = {
                x["requirement"]: {"score": x["score"], "reason": x["reason"]}
                for x in alg_violations
            }
            logger.info(
                f"[{input.id}] Verifier failed: algorithmic violations {list(verifier_feedback.keys())}"
            )
            feedback = json.dumps(
                {
                    "verifier_feedback": verifier_feedback,
                    "runtime_error": runtime_error,
                },
                ensure_ascii=False,
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
            feedback = json.dumps(
                {
                    "verifier_feedback": llm_violations,
                    "runtime_error": None,
                },
                ensure_ascii=False,
            )
            logger.info(
                f"[{input.id}] Verifier failed: LLM violations {list(llm_violations.keys())}"
            )
            return VerificationResult(
                passed=False,
                feedback=feedback,
                metadata={
                    "verifier_llm_feedback": json.dumps(
                        llm_violations,
                        ensure_ascii=False,
                    ),
                    "verifier_token_usage": token_usage,
                },
            )

        return VerificationResult(
            passed=True, metadata={"verifier_token_usage": token_usage}
        )

    return verifier


def clean_output_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


async def run(
    incident_models: list[IncidentModel],
    loop: Loop,
    output_path: Path,
) -> list[LoopResult[LogGenerationScript]]:
    hook = partial(save_loop_result, path=output_path)
    loop_results = await tqdm_asyncio.gather(
        *[loop.run(im, im.id, post_run_hook=hook) for im in incident_models]
    )
    return loop_results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("./data/incident-models"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/logs/log-generation-scripts"),
    )
    args = parser.parse_args()
    args.input_dir = args.input_dir.resolve()
    args.output_dir = args.output_dir.resolve()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    logger.info("=== Log generation script pipeline ===")

    logger.info("Cleaning output directory...")
    clean_output_dir(args.output_dir)

    logger.info("Loading data...")
    incident_models = load_incident_models(args.input_dir)

    logger.info("Loading models and prompts...")
    gen_llm, ver_llm = create_llms(("gpt-5.2", "high"), ("gpt-5.2", "high"))
    gen_system_prompt, ver_system_prompt = load_prompts(
        Path("src/prompts/generation/log-generation-scripts/generator-prompt-v3.md"),
        Path("src/prompts/generation/log-generation-scripts/verifier-prompt-v3.md"),
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
    loop_results = asyncio.run(run(incident_models, loop, args.output_dir))

    logger.info("=== Done ===")

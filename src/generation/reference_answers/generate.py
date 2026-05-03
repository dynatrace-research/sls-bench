import argparse
import re
import json
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
from models.helpers import ainvoke_model
from loop import Loop, LoopConfig, LoopResult
from loop import GenerationResult, VerificationResult


logger = logging.getLogger(__name__)


QUESTION_RE = re.compile(r"<question>\s*(.*?)\s*</question>", re.DOTALL | re.IGNORECASE)
ANSWER_RE = re.compile(r"<answer>\s*(.*?)\s*</answer>", re.DOTALL | re.IGNORECASE)
CODE_FENCE_RE = re.compile(r"```json\s*(.*?)```", re.DOTALL | re.IGNORECASE)


class IncidentModel(BaseModel):
    id: str
    system_description: str
    scenario: str


class Problem(BaseModel):
    id: str
    question: str
    answer: str


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


def save_loop_result(result: LoopResult[Problem], path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

    problem = result.output
    with (path / "problems.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(problem.model_dump(), ensure_ascii=False) + "\n")

    metadata = {"id": problem.id, "passed": result.passed, "history": result.history}
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
    previous_output: Problem | None = None,
    feedback: str | None = None,
) -> str:
    sections = [
        "# User Inputs\n\n"
        "## System Description\n\n"
        "<system_description>\n"
        f"{input.system_description}\n"
        "</system_description>\n\n"
        "## Scenario\n\n"
        "<scenario>\n"
        f"{input.scenario}\n"
        "</scenario>\n"
    ]
    if previous_output:
        sections.extend(
            [
                "\n## Previously Generated Problem and Feedback\n",
                f"<previous_question>\n{previous_output.question}\n</previous_question>\n",
                f"<previous_answer>\n{previous_output.answer}\n</previous_answer>\n",
                f"<verifier_feedback>\n{feedback}\n</verifier_feedback>\n",
            ]
        )
    return "\n".join(sections)


def prep_verifier_llm_input(input: IncidentModel, output: Problem) -> str:
    return (
        "# User Inputs\n\n"
        "## System Description\n\n"
        f"<system_description>\n{input.system_description}\n</system_description>\n\n"
        "## Scenario\n\n"
        f"<scenario>\n{input.scenario}\n</scenario>\n\n"
        "## Generated Problem (THIS IS WHAT YOU MUST EVALUATE)\n\n"
        f"<question>\n{output.question}\n</question>\n\n"
        f"<answer>\n{output.answer}\n</answer>\n"
    )


def parse_generator_output(raw: str, input_id: str) -> Problem:
    question = QUESTION_RE.search(raw)
    answer = ANSWER_RE.search(raw)

    missing = []
    if not question:
        missing.append("<question>")
    if not answer:
        missing.append("<answer>")
    if missing:
        raise RuntimeError(f"Missing tags: {', '.join(missing)}")

    return Problem(
        id=input_id,
        question=question.group(1).strip(),  # type: ignore
        answer=answer.group(1).strip(),  # type: ignore
    )


def create_generator(llm: BaseChatModel, system_prompt: str):

    async def generator(
        input: IncidentModel,
        previous_output: Problem | None,
        feedback: str | None,
    ) -> GenerationResult[Problem]:
        user_input = prep_generator_llm_input(input, previous_output, feedback)

        response = await ainvoke_model(llm, system_prompt, user_input)

        problem = parse_generator_output(response.final_answer, input.id)
        token_usage = response.token_usage.model_dump()

        return GenerationResult(
            output=problem,
            metadata={"generator_token_usage": token_usage},
        )

    return generator


def create_verifier(llm: BaseChatModel, system_prompt: str):

    async def verifier(input: IncidentModel, output: Problem) -> VerificationResult:
        user_input = prep_verifier_llm_input(input, output)
        response = await ainvoke_model(llm, system_prompt, user_input)
        token_usage = response.token_usage.model_dump()

        raw_answer = response.final_answer.strip()
        json_match = CODE_FENCE_RE.search(raw_answer)
        json_str = json_match.group(1).strip() if json_match else raw_answer
        llm_feedback = json.loads(json_str)

        llm_violations = {
            req_id: req_fbk
            for req_id, req_fbk in llm_feedback.items()
            if req_fbk["score"] == 0
        }
        if llm_violations:
            logger.info(
                f"[{input.id}] Verifier failed: LLM violations {list(llm_violations.keys())}"
            )
            feedback = json.dumps(llm_violations, ensure_ascii=False)
            return VerificationResult(
                passed=False,
                feedback=feedback,
                metadata={
                    "verifier_llm_feedback": feedback,
                    "verifier_token_usage": token_usage,
                },
            )

        return VerificationResult(
            passed=True,
            metadata={
                "verifier_token_usage": token_usage,
            },
        )

    return verifier


def clean_output_dir(path: Path) -> None:
    for fname in ("problems.jsonl", "metadata.jsonl"):
        f = path / fname
        if f.exists():
            f.unlink()


async def run(
    incident_models: list[IncidentModel],
    loop: Loop,
    output_path: Path,
) -> list[LoopResult[Problem]]:
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
        default=Path("data/reference-answers"),
    )
    args = parser.parse_args()
    args.input_dir = args.input_dir.resolve()
    args.output_dir = args.output_dir.resolve()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    logger.info("=== Reference answer generation pipeline ===")

    logger.info("Cleaning output directory...")
    clean_output_dir(args.output_dir)

    logger.info("Loading data...")
    incident_models = load_incident_models(args.input_dir)

    logger.info("Loading models and prompts...")
    gen_llm, ver_llm = create_llms(("gpt-5.2", "high"), ("gpt-5.2", "high"))
    gen_system_prompt, ver_system_prompt = load_prompts(
        Path("src/prompts/generation/reference-answers/generator-prompt-v2.md"),
        Path("src/prompts/generation/reference-answers/verifier-prompt-v2.md"),
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

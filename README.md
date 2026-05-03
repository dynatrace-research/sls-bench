# SLS-Bench

Generation and evaluation code for **SLS-Bench**, a benchmark for incident log summarization with synthetic observability data.

Each benchmark problem is a CSV log file (~20k–100k lines) plus a reference incident summary. An agent reads the logs and writes its own summary; an LLM judge scores it against the reference. See the paper for details.

## Setup

```sh
uv sync
uv pip install -e .
```

Create a `.env` in the repo root with credentials for the model providers you'll use.

For Azure OpenAI (GPT models):

```
AZURE_OPENAI_API_KEY=...
AZURE_OPENAI_ENDPOINT=...
AZURE_OPENAI_API_VERSION=...
```

For AWS Bedrock (Anthropic and open-weight models), use the standard boto3 credential chain — env vars, an `AWS_PROFILE` from `~/.aws/credentials`, or an attached IAM role:

```
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_SESSION_TOKEN=...        # if using temporary credentials
AWS_REGION=us-east-1         # a region with Bedrock model access enabled
BEDROCK_ENDPOINT_URL=...     # optional, only for custom endpoints / proxies
```

Make sure your AWS account has Bedrock model access enabled for the models you'll evaluate. Routing is decided by model name in `src/evals/model_factory.py`: anything containing `gpt` goes to Azure, otherwise Bedrock.

## Repository layout

```
src/
  generation/       # Pipeline that turns incident reports into benchmark problems
    incident_reports/       # fetch, dedupe, filter URLs of public postmortems
    incident_models/        # report -> structured incident model (YAML)
    log_generation_scripts/ # incident model -> deterministic Python simulator -> CSV logs
    reference_answers/      # incident model -> reference summary
  evals/            # Agent runner, judges, score aggregation
  models/           # Model client factory (Azure OpenAI, Bedrock)
  prompts/          # Generation and evaluation prompts
scripts/            # Shell scripts to run the pipelines
data/
  incident-reports/ # Raw, deduplicated, and filtered postmortems
  incident-models/  # Generated YAML system descriptions and scenarios
  logs/             # Generated simulator scripts and CSV log files
  reference-answers/# Questions and reference summaries
  runs/             # Agent outputs and grader outputs
  complete/         # Fully-generated artifacts for reference
```

## Generation pipeline

Run end-to-end to rebuild the benchmark from the seed URL list.

```sh
sh scripts/incident-reports.sh    # fetch -> dedupe -> filter
sh scripts/incident-models.sh     # filtered reports -> incident models (YAML)
sh scripts/log-files.sh           # incident models -> simulator scripts -> CSV logs
sh scripts/reference-answers.sh   # incident models -> questions + reference summaries
```

The generation pipeline uses a generator–verifier loop with GPT-5.2 (high). The full run cost ~$780 in API tokens for the released benchmark. 
Intermediate artifacts (incident models and simulators) can be found in `data/complete/`.

All benchmark problems (questions, answers, and logs) can be found in our data repository; after downloading them, place `problems.jsonl` to `data/reference-answers`, and extract `logs.zip` to `data/logs/log-files`.

## Evaluation pipeline

```sh
sh scripts/eval.sh <model> <effort>
# e.g.
sh scripts/eval.sh gpt-5.4-mini low
sh scripts/eval.sh global.anthropic.claude-opus-4-6-v1 low
sh scripts/eval.sh minimax.minimax-m2 medium  # reasoning effort is ignored if unapplicable
```

`<effort>` is `low`, `medium`, or `high`. For non-reasoning models pass any value; it's ignored.

The script does four things:

1. Runs the agent (ReAct loop with a stateful Python interpreter) on all 90 problems and writes outputs to `data/runs/ungraded/<run-id>/`.
2. Copies those outputs into `data/runs/graded-judge-gpt/` and `data/runs/graded-judge-claude/`.
3. Grades each copy with the corresponding judge (GPT-5.2 medium and Sonnet 4.6 low) using the rubric at `src/prompts/evals/inv-report-eval-v3.3.md`.
4. Prints the averaged final score.

## Adding a new model

`src/evals/model_factory.py` decides the provider from the model name: anything containing `gpt` goes to Azure, otherwise Bedrock. To use a different routing or a new provider, point `--model-factory-path` at your own factory module (see `run_agents.py --help`).

## Minimal example

Run an agent on a single problem and grade the prediction:

```python
import asyncio
import json
from pathlib import Path

from evals.agents import NotebookCsvAgent, execute_task_agent
from evals.graders import RubricLLMGrader
from evals.model_factory import create_chat_model
from evals.schemas import TaskSpec

PROBLEMS = Path("data/reference-answers/problems.jsonl")
LOGS_DIR = Path("data/logs/log-files")
JUDGE_PROMPT = Path("src/prompts/evals/inv-report-eval-v3.3.md")

TASK_ID = "0a7c69ff"
MODEL, EFFORT = "gpt-5.4-mini", "low"
JUDGE_MODEL, JUDGE_EFFORT = "gpt-5.2", "medium"

async def main():
    problems = [json.loads(l) for l in PROBLEMS.read_text().splitlines() if l.strip()]
    problem = next(p for p in problems if p["id"] == TASK_ID)

    task = TaskSpec(
        task_id=problem["id"],
        logs_path=str(LOGS_DIR / f"{problem['id']}.csv"),
        question=problem["question"],
        ground_truth_answer=problem["answer"],
    )

    agent = NotebookCsvAgent(
        model_builder=create_chat_model,
        model_name=MODEL,
        reasoning_effort=EFFORT,
        model_call_timeout_seconds=900,
    )
    execution = await execute_task_agent(agent_definition=agent, task=task)

    grader = RubricLLMGrader(
        model_builder=create_chat_model,
        model_name=JUDGE_MODEL,
        reasoning_effort=JUDGE_EFFORT,
        system_prompt=JUDGE_PROMPT.read_text(),
    )
    grade = await grader.grade(task, execution.final_answer)

    print("prediction:\n", execution.final_answer)
    print("\nverdict:", grade.result.get("overall_verdict"))

if __name__ == "__main__":
    asyncio.run(main())
```

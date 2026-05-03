#!/bin/bash

set -euo pipefail

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

confirm_if_dirs_exist() {
  local existing=()

  for dir in "$@"; do
    if [[ -d "$dir" ]]; then
      existing+=("$dir")
    fi
  done

  if [[ ${#existing[@]} -eq 0 ]]; then
    return
  fi

  echo "The following output directories already exist:"
  for dir in "${existing[@]}"; do
    echo "  - $dir"
  done

  read -r -p "Continue running the pipeline? [y/N] " reply
  case "$reply" in
    y|Y|yes|YES)
      ;;
    *)
      echo "Aborted. Please confirm manually before re-running."
      exit 1
      ;;
  esac
}

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 MODEL EFFORT" >&2
  exit 1
fi

MODEL="$1"
EFFORT="$2"

RUN_ID="${MODEL}-${EFFORT}"
DIR_NAME="$RUN_ID"

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

UNGRADED_DIR="./data/runs/ungraded"
GPT_JUDGE_DIR="./data/runs/graded-judge-gpt"
CLAUDE_JUDGE_DIR="./data/runs/graded-judge-claude"

confirm_if_dirs_exist \
  "$UNGRADED_DIR/$RUN_ID" \
  "$GPT_JUDGE_DIR/$DIR_NAME" \
  "$CLAUDE_JUDGE_DIR/$DIR_NAME"

echo "=== Step 1: Run agents ==="
python src/evals/run_agents.py \
  --problems-path data/reference-answers/problems.jsonl \
  --logs-dir data/logs/log-files \
  --output-dir "$UNGRADED_DIR" \
  --run-id "$RUN_ID" \
  --agent notebook_csv \
  --model-name "$MODEL" \
  --reasoning-effort "$EFFORT"

echo "=== Step 2: Sync ungraded run to judge directories ==="
mkdir -p "$GPT_JUDGE_DIR" "$CLAUDE_JUDGE_DIR"
"$SCRIPT_DIR/sync-judges.sh" "$UNGRADED_DIR"

echo "=== Step 3: Run graders ==="
python src/evals/run_graders.py \
  --run-dir "$GPT_JUDGE_DIR/$DIR_NAME" \
  --grader llm_rubric \
  --judge-model-name gpt-5.2 \
  --judge-reasoning-effort medium \
  --judge-system-prompt-path src/prompts/evals/inv-report-eval-v3.3.md

python src/evals/run_graders.py \
  --run-dir "$CLAUDE_JUDGE_DIR/$DIR_NAME" \
  --grader llm_rubric \
  --judge-model-name us.anthropic.claude-sonnet-4-6 \
  --judge-reasoning-effort low \
  --judge-system-prompt-path src/prompts/evals/inv-report-eval-v3.3.md

echo "=== Step 4: Summarize scores ==="
python src/evals/summarize_scores.py \
  --run-id "$RUN_ID" \
  --judge "gpt=$GPT_JUDGE_DIR" \
  --judge "claude=$CLAUDE_JUDGE_DIR"

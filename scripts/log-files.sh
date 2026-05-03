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

INPUT_DIR="${1:-./data/incident-models}"
SCRIPTS_DIR="${2:-./data/logs/log-generation-scripts}"
LOG_FILES_DIR="${3:-./data/logs/log-files}"

confirm_if_dirs_exist "$SCRIPTS_DIR" "$LOG_FILES_DIR"

echo "=== Step 1: Generate log-generation scripts ==="
python ./src/generation/log_generation_scripts/generate.py \
 --input-dir "$INPUT_DIR" \
 --output-dir "$SCRIPTS_DIR"

echo "=== Step 2: Run log-generation scripts ==="
python ./src/generation/log_generation_scripts/run_log_generation_scripts.py \
  --scripts-dir "$SCRIPTS_DIR" \
  --output-dir "$LOG_FILES_DIR"

#!/bin/bash

set -euo pipefail

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

confirm_if_output_exists() {
  local output_dir="$1"

  if [[ ! -d "$output_dir" ]]; then
    return
  fi

  read -r -p "Output directory '$output_dir' already exists. Continue? [y/N] " reply
  case "$reply" in
    y|Y|yes|YES)
      ;;
    *)
      echo "Aborted. Please confirm manually before re-running."
      exit 1
      ;;
  esac
}

INPUT_DIR="${1:-./data/incident-reports/filtered/pass}"
OUTPUT_DIR="${2:-./data/incident-models}"

confirm_if_output_exists "$OUTPUT_DIR"

python ./src/generation/incident_models/generate.py \
  --input-dir "$INPUT_DIR" \
  --output-dir "$OUTPUT_DIR"

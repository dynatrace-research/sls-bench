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

INPUT_DIR="./data/incident-reports"

URLS_FILE_PATH="${INPUT_DIR}/candidate_urls.csv"

RAW_REPORTS_DIR="${INPUT_DIR}/raw"
DEDUPED_REPORTS_DIR="${INPUT_DIR}/deduped"
FILTERED_REPORTS_DIR="${INPUT_DIR}/filtered"

confirm_if_dirs_exist \
  "$RAW_REPORTS_DIR" \
  "$DEDUPED_REPORTS_DIR" \
  "$FILTERED_REPORTS_DIR"

python ./src/generation/incident_reports/fetch.py --urls-file "$URLS_FILE_PATH" --output-dir "$RAW_REPORTS_DIR"
python ./src/generation/incident_reports/deduplicate.py --input-dir "$RAW_REPORTS_DIR" --output-dir "$DEDUPED_REPORTS_DIR"
python ./src/generation/incident_reports/filter.py --input-dir "$DEDUPED_REPORTS_DIR" --output-dir "$FILTERED_REPORTS_DIR"

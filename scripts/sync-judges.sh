#!/bin/sh

set -eu

RUNS_DIR="${RUNS_DIR:-./data/runs}"
UNGRADED_DIR="${1:-$RUNS_DIR/ungraded}"

if [ ! -d "$UNGRADED_DIR" ]; then
  echo "Ungraded runs directory not found: $UNGRADED_DIR" >&2
  exit 1
fi

judge_root_found=0
for judge_root in "$RUNS_DIR"/graded-judge-*; do
  if [ -d "$judge_root" ]; then
    judge_root_found=1
    break
  fi
done

if [ "$judge_root_found" -ne 1 ]; then
  echo "No graded-judge-* directories found under $RUNS_DIR" >&2
  exit 1
fi

copied_manifests=0
skipped_manifests=0
copied_notebooks=0
skipped_notebooks=0

for src_run_dir in "$UNGRADED_DIR"/*; do
  [ -d "$src_run_dir" ] || continue

  run_name="$(basename "$src_run_dir")"
  src_manifest="$src_run_dir/run_manifest.json"
  src_notebook_dir="$src_run_dir/notebook_csv"

  for judge_root in "$RUNS_DIR"/graded-judge-*; do
    [ -d "$judge_root" ] || continue

    dst_run_dir="$judge_root/$run_name"
    dst_manifest="$dst_run_dir/run_manifest.json"
    dst_notebook_dir="$dst_run_dir/notebook_csv"

    mkdir -p "$dst_notebook_dir"

    if [ -f "$src_manifest" ]; then
      if [ ! -e "$dst_manifest" ]; then
        cp "$src_manifest" "$dst_manifest"
        copied_manifests=$((copied_manifests + 1))
        echo "Copied manifest: $judge_root/$run_name/run_manifest.json"
      else
        skipped_manifests=$((skipped_manifests + 1))
      fi
    fi

    if [ -d "$src_notebook_dir" ]; then
      for src_notebook_file in "$src_notebook_dir"/*.json "$src_notebook_dir"/*.jsonl; do
        [ -f "$src_notebook_file" ] || continue
        dst_notebook_file="$dst_notebook_dir/$(basename "$src_notebook_file")"
        if [ ! -e "$dst_notebook_file" ]; then
          cp "$src_notebook_file" "$dst_notebook_file"
          copied_notebooks=$((copied_notebooks + 1))
          echo "Copied notebook file: $judge_root/$run_name/notebook_csv/$(basename "$src_notebook_file")"
        else
          skipped_notebooks=$((skipped_notebooks + 1))
        fi
      done
    fi
  done
done

echo "Done. Copied $copied_manifests manifest file(s) and $copied_notebooks notebook file(s)."
echo "Skipped existing $skipped_manifests manifest file(s) and $skipped_notebooks notebook file(s)."

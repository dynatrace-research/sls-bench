import argparse
from pathlib import Path

import pandas as pd
from tqdm import tqdm


SIMILARITY_THRESHOLD = 0.8


def load_content(input_dir: Path) -> tuple[pd.DataFrame, dict[str, str]]:
    """Load metadata and content from input directory."""
    metadata_path = input_dir / "metadata.csv"
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")
    metadata = pd.read_csv(metadata_path, encoding="utf-8")

    contents = {}
    for _, row in metadata.iterrows():
        content_id = row["id"]
        path = input_dir / f"{content_id}.txt"
        if path.exists():
            contents[content_id] = path.read_text(encoding="utf-8")

    return metadata, contents


def tokenize(text: str) -> set[str]:
    """Tokenize text by splitting on whitespace."""
    return set(text.lower().split())


def jaccard_similarity(set_a: set[str], set_b: set[str]) -> float:
    """Compute Jaccard similarity between two sets."""
    if not set_a or not set_b:
        return 0.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union


def deduplicate(
    contents: dict[str, str], threshold: float
) -> tuple[list[str], list[tuple[str, str, float]]]:
    """
    Deduplicate content by removing items too similar to others.
    Returns list of kept IDs and list of removed (id, similar_to_id, similarity) tuples.
    """
    ids = list(contents.keys())
    token_sets = {cid: tokenize(text) for cid, text in contents.items()}

    kept = []
    removed = []
    removed_ids = set()

    for i, id_i in enumerate(tqdm(ids, desc="Deduplicating")):
        if id_i in removed_ids:
            continue

        kept.append(id_i)

        for j in range(i + 1, len(ids)):
            id_j = ids[j]
            if id_j in removed_ids:
                continue

            sim = jaccard_similarity(token_sets[id_i], token_sets[id_j])
            if sim > threshold:
                removed_ids.add(id_j)
                removed.append((id_j, id_i, sim))

    return kept, removed


def save_results(
    kept_ids: list[str],
    removed: list[tuple[str, str, float]],
    metadata: pd.DataFrame,
    contents: dict[str, str],
    output_dir: Path,
) -> None:
    """Save deduplicated content and metadata."""
    output_dir.mkdir(parents=True, exist_ok=True)

    for content_id in kept_ids:
        content_path = output_dir / f"{content_id}.txt"
        content_path.write_text(contents[content_id], encoding="utf-8")

    kept_metadata = metadata[metadata["id"].isin(kept_ids)]
    kept_metadata.to_csv(output_dir / "metadata.csv", index=False, encoding="utf-8")

    removal_df = pd.DataFrame(
        removed, columns=["removed_id", "similar_to", "similarity"]  # type: ignore[arg-type]
    )
    removal_df.to_csv(output_dir / "removal_log.csv", index=False, encoding="utf-8")

    print(f"\nResults: {len(kept_ids)} kept, {len(removed)} removed")
    print(f"Outputs saved to {output_dir}")


def main():
    parser = argparse.ArgumentParser(description="Deduplicate incident report content")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    args.input_dir = args.input_dir.resolve()
    args.output_dir = args.output_dir.resolve()

    metadata, contents = load_content(args.input_dir)
    kept_ids, removed = deduplicate(contents, SIMILARITY_THRESHOLD)
    save_results(kept_ids, removed, metadata, contents, args.output_dir)


if __name__ == "__main__":
    main()

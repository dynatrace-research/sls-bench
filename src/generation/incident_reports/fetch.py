import argparse
import asyncio
from datetime import datetime
from hashlib import md5
from pathlib import Path

import httpx
import pandas as pd
import trafilatura
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio


CONCURRENCY = 50
TIMEOUT = 60.0
MAX_ATTEMPTS = 5


@retry(
    stop=stop_after_attempt(MAX_ATTEMPTS),
    wait=wait_exponential(multiplier=1, min=15, max=60),
)
async def fetch_html(client: httpx.AsyncClient, url: str) -> str:
    """Fetch HTML with retries."""
    resp = await client.get(url, follow_redirects=True)
    resp.raise_for_status()
    return resp.text


def extract_text(html: str, url: str) -> str | None:
    """Extract main text from HTML using trafilatura."""
    text = trafilatura.extract(
        html,
        url=url,
        include_comments=False,
        include_tables=False,
        include_links=False,
        favor_recall=True,
        output_format="txt",
    )
    return text.strip() if text and text.strip() else None


async def process_url(
    url: str, client: httpx.AsyncClient, semaphore: asyncio.Semaphore, delay: float
) -> dict | None:
    """Fetch and extract text from a single URL."""
    await asyncio.sleep(delay)
    async with semaphore:
        try:
            html = await fetch_html(client, url)
            content = extract_text(html, url)
            return {"url": url, "content": content} if content else None
        except Exception as e:
            tqdm.write(f"Failed: {url} ({type(e).__name__})")
            return None


async def fetch_all(urls: list[str]) -> list[dict]:
    """Fetch and extract content from all URLs concurrently."""
    semaphore = asyncio.Semaphore(CONCURRENCY)
    delay_step = 0.1
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(TIMEOUT),
        limits=httpx.Limits(max_connections=CONCURRENCY),
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
        verify=False,
    ) as client:
        tasks = [
            process_url(url, client, semaphore, delay=i * delay_step)
            for i, url in enumerate(urls)
        ]
        results = await tqdm_asyncio.gather(*tasks, desc="Fetching")
    return [r for r in results if r and len(r["content"]) < 35000]


def save_results(results: list[dict], output_dir: Path) -> None:
    """Save text files and metadata CSV (updates existing entries if present)."""
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = output_dir / "metadata.csv"
    retrieved_date = datetime.now().strftime("%d-%m-%Y")

    existing = (
        pd.read_csv(metadata_path, encoding="utf-8")
        if metadata_path.exists()
        else pd.DataFrame()
    )

    new_rows = []
    for r in results:
        content_id = md5(r["url"].encode()).hexdigest()[:8]
        (output_dir / f"{content_id}.txt").write_text(r["content"], encoding="utf-8")
        new_rows.append(
            {"id": content_id, "url": r["url"], "retrieved_date": retrieved_date}
        )

    new_df = pd.DataFrame(new_rows)
    if not existing.empty:
        existing = existing[~existing["id"].isin(new_df["id"])]
        new_df = pd.concat([existing, new_df], ignore_index=True)

    new_df.to_csv(metadata_path, index=False, encoding="utf-8")


def read_urls(csv_path: Path) -> list[str]:
    """Read unique, non-empty URLs from a CSV file."""
    return (
        pd.read_csv(csv_path, encoding="utf-8")["url"]
        .dropna()
        .astype(str)
        .str.strip()
        .loc[lambda s: s != ""]
        .drop_duplicates()
        .tolist()
    )


def main():
    parser = argparse.ArgumentParser(
        description="Fetch incident report content from URLs"
    )
    parser.add_argument("--urls-file", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    args.urls_file = args.urls_file.resolve()
    args.output_dir = args.output_dir.resolve()

    urls = read_urls(args.urls_file)
    results = asyncio.run(fetch_all(urls))
    save_results(results, args.output_dir)
    print(f"Extracted {len(results)} documents to {args.output_dir}")


if __name__ == "__main__":
    main()

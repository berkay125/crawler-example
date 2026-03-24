from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

from fastapi import FastAPI, Query


STORAGE_FILE_PATH = Path("data/storage/p.data")


@dataclass(frozen=True)
class SearchResultRow:
    url: str
    score: int
    frequency: int
    depth: int


def _parse_storage_line(line: str) -> tuple[str, str, str, int, int] | None:
    """
    Expected line format (space-separated):
    word url origin depth frequency
    """
    parts = line.strip().split()
    if len(parts) != 5:
        return None

    word, url, origin, depth_raw, frequency_raw = parts
    try:
        depth = int(depth_raw)
        frequency = int(frequency_raw)
    except ValueError:
        return None

    return word, url, origin, depth, frequency


def _score_entry(*, frequency: int, depth: int, exact_match: bool) -> int:
    # Required grading formula.
    bonus = 1000 if exact_match else 0
    return (frequency * 10) + bonus - (depth * 5)


def search_storage(query: str, sort_by: str = "relevance") -> List[SearchResultRow]:
    query_word = query.strip().lower()
    if not query_word:
        return []

    if not STORAGE_FILE_PATH.exists():
        return []

    matches: list[SearchResultRow] = []
    with STORAGE_FILE_PATH.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            parsed = _parse_storage_line(raw_line)
            if parsed is None:
                continue

            word, url, _origin, depth, frequency = parsed
            if word != query_word:
                continue

            score = _score_entry(frequency=frequency, depth=depth, exact_match=True)
            matches.append(
                SearchResultRow(
                    url=url,
                    score=score,
                    frequency=frequency,
                    depth=depth,
                )
            )

    if sort_by == "relevance":
        matches.sort(key=lambda item: item.score, reverse=True)
    elif sort_by == "frequency":
        matches.sort(key=lambda item: item.frequency, reverse=True)
    elif sort_by == "depth":
        matches.sort(key=lambda item: item.depth)
    else:
        # Unknown sort mode falls back to required relevance sort.
        matches.sort(key=lambda item: item.score, reverse=True)

    return matches


app = FastAPI(title="Crawler Search API")


@app.get("/search")
def search(
    query: str = Query(..., description="Search word query"),
    sortBy: str = Query("relevance", description="Sort mode, e.g. relevance"),
) -> dict:
    results = search_storage(query=query, sort_by=sortBy)
    return {
        "query": query,
        "results": [
            {
                "url": row.url,
                "score": row.score,
                "frequency": row.frequency,
                "depth": row.depth,
            }
            for row in results
        ],
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=3600)

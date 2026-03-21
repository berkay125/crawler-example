from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import List, Tuple


def _search_sync(query: str, db_path: str, limit: int) -> List[Tuple[str, str, int]]:
    """Runs an FTS5 search query synchronously using SQLite bm25 ranking."""
    if not query.strip():
        return []

    conn = sqlite3.connect(str(Path(db_path)), check_same_thread=False)
    conn.row_factory = sqlite3.Row

    try:
        # Reassert WAL mode on this connection so searches can run during indexing.
        conn.execute("PRAGMA journal_mode=WAL;")

        rows = conn.execute(
            """
            SELECT
                url,
                origin_url,
                depth,
                bm25(pages_fts) AS rank
            FROM pages_fts
            WHERE pages_fts MATCH ?
            ORDER BY rank ASC
            LIMIT ?
            """,
            (query, limit),
        ).fetchall()
    finally:
        conn.close()

    return [(row["url"], row["origin_url"], row["depth"]) for row in rows]


async def search(query: str) -> List[Tuple[str, str, int]]:
    """PRD-required async search API returning (relevant_url, origin_url, depth)."""
    return await asyncio.to_thread(_search_sync, query, "crawler.db", 25)


async def search_with_db(
    query: str,
    db_path: str = "crawler.db",
    limit: int = 25,
) -> List[Tuple[str, str, int]]:
    """Optional utility for CLI/tests that need a non-default DB path."""
    return await asyncio.to_thread(_search_sync, query, db_path, limit)

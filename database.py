from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional


@dataclass(frozen=True)
class CrawlTask:
    """Represents one URL to be crawled."""

    url: str
    origin_url: str
    depth: int


class CrawlDatabase:
    """Thin sqlite3 wrapper for crawler state, pages, and FTS search index."""

    def __init__(self, db_path: str = "crawler.db") -> None:
        self.db_path = str(Path(db_path))
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._configure_pragmas()
        self._create_schema()

    def _configure_pragmas(self) -> None:
        with self._lock:
            # WAL allows readers and writers to proceed concurrently.
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;")
            self._conn.execute("PRAGMA temp_store=MEMORY;")
            self._conn.execute("PRAGMA foreign_keys=ON;")
            self._conn.commit()

    def _create_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS visited_urls (
                    url TEXT PRIMARY KEY,
                    origin_url TEXT NOT NULL,
                    depth INTEGER NOT NULL,
                    discovered_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS pages (
                    url TEXT PRIMARY KEY,
                    origin_url TEXT NOT NULL,
                    depth INTEGER NOT NULL,
                    title TEXT,
                    content TEXT,
                    status_code INTEGER,
                    error TEXT,
                    fetched_at TEXT NOT NULL
                );

                -- Queue snapshot used for graceful shutdown / instant resume.
                CREATE TABLE IF NOT EXISTS queue_checkpoint (
                    url TEXT PRIMARY KEY,
                    origin_url TEXT NOT NULL,
                    depth INTEGER NOT NULL,
                    saved_at TEXT NOT NULL
                );

                CREATE VIRTUAL TABLE IF NOT EXISTS pages_fts USING fts5(
                    url UNINDEXED,
                    origin_url UNINDEXED,
                    depth UNINDEXED,
                    title,
                    content,
                    tokenize='porter unicode61'
                );
                """
            )
            self._conn.commit()

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def register_discovered_url(self, task: CrawlTask) -> bool:
        """
        Inserts URL into the visited set exactly once.

        Returns True when URL is first seen and should be enqueued.
        Returns False when URL was already discovered before.
        """
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT OR IGNORE INTO visited_urls(url, origin_url, depth, discovered_at)
                VALUES (?, ?, ?, ?)
                """,
                (task.url, task.origin_url, task.depth, self._utc_now_iso()),
            )
            self._conn.commit()
            return cursor.rowcount == 1

    def save_page(
        self,
        task: CrawlTask,
        *,
        title: str,
        content: str,
        status_code: int,
        error: Optional[str] = None,
    ) -> None:
        """Upserts fetched page metadata and maintains matching FTS row."""
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO pages(url, origin_url, depth, title, content, status_code, error, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    origin_url=excluded.origin_url,
                    depth=excluded.depth,
                    title=excluded.title,
                    content=excluded.content,
                    status_code=excluded.status_code,
                    error=excluded.error,
                    fetched_at=excluded.fetched_at
                """,
                (
                    task.url,
                    task.origin_url,
                    task.depth,
                    title,
                    content,
                    status_code,
                    error,
                    self._utc_now_iso(),
                ),
            )

            # Keep one authoritative FTS row per URL.
            self._conn.execute("DELETE FROM pages_fts WHERE url = ?", (task.url,))
            self._conn.execute(
                """
                INSERT INTO pages_fts(url, origin_url, depth, title, content)
                VALUES (?, ?, ?, ?, ?)
                """,
                (task.url, task.origin_url, task.depth, title, content),
            )
            self._conn.commit()

    def save_queue_checkpoint(self, tasks: Iterable[CrawlTask]) -> int:
        """Overwrites queue checkpoint with the provided pending tasks."""
        rows = [
            (task.url, task.origin_url, task.depth, self._utc_now_iso()) for task in tasks
        ]

        with self._lock:
            self._conn.execute("DELETE FROM queue_checkpoint")
            if rows:
                self._conn.executemany(
                    """
                    INSERT INTO queue_checkpoint(url, origin_url, depth, saved_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    rows,
                )
            self._conn.commit()

        return len(rows)

    def load_queue_checkpoint(self) -> List[CrawlTask]:
        """Loads queued tasks persisted during the previous graceful shutdown."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT url, origin_url, depth
                FROM queue_checkpoint
                ORDER BY saved_at ASC
                """
            ).fetchall()

        return [
            CrawlTask(url=row["url"], origin_url=row["origin_url"], depth=row["depth"])
            for row in rows
        ]

    def clear_queue_checkpoint(self) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM queue_checkpoint")
            self._conn.commit()

    def get_crawl_stats(self) -> dict[str, int]:
        with self._lock:
            crawled = self._conn.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
            errors = self._conn.execute(
                "SELECT COUNT(*) FROM pages WHERE error IS NOT NULL"
            ).fetchone()[0]
            discovered = self._conn.execute("SELECT COUNT(*) FROM visited_urls").fetchone()[0]

        return {"crawled": crawled, "errors": errors, "discovered": discovered}

    def close(self) -> None:
        with self._lock:
            self._conn.close()

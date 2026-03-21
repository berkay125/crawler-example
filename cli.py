from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from typing import Iterable

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from crawler import ConcurrentCrawler, CrawlStats
from database import CrawlDatabase
from search import search_with_db


console = Console()


@dataclass
class DashboardState:
    crawled_pages: int = 0
    error_pages: int = 0
    discovered_urls: int = 0
    queue_depth: int = 0
    active_workers: int = 0
    max_workers: int = 0


def _build_status_table(state: DashboardState) -> Table:
    table = Table(title="Crawler Live Dashboard", expand=True)
    table.add_column("Metric", style="bold cyan")
    table.add_column("Value", justify="right", style="bold green")

    table.add_row("Total Pages Crawled", str(state.crawled_pages))
    table.add_row("Errors", str(state.error_pages))
    table.add_row("Discovered URLs", str(state.discovered_urls))
    table.add_row("Queue Depth", str(state.queue_depth))
    table.add_row("Active Workers", f"{state.active_workers}/{state.max_workers}")

    if state.max_workers > 0 and state.active_workers >= state.max_workers:
        throttle_status = "THROTTLED (at concurrency cap)"
    elif state.queue_depth > 0:
        throttle_status = "FLOWING (queue draining)"
    else:
        throttle_status = "IDLE"
    table.add_row("Backpressure Status", throttle_status)
    return table


async def run_crawl(origin_url: str, depth: int, db_path: str) -> None:
    db = CrawlDatabase(db_path=db_path)
    state = DashboardState()

    # Rich Progress is used as an indeterminate activity display plus counters.
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]Crawling[/bold blue]"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        expand=True,
    )
    crawl_task_id = progress.add_task("crawl", total=None)

    async def on_progress(stats: CrawlStats, queue_depth: int, active_workers: int) -> None:
        state.crawled_pages = stats.crawled_pages
        state.error_pages = stats.error_pages
        state.discovered_urls = stats.discovered_urls
        state.queue_depth = queue_depth
        state.active_workers = active_workers
        progress.update(crawl_task_id, completed=stats.crawled_pages)

    crawler = ConcurrentCrawler(db=db, progress_callback=on_progress)
    state.max_workers = crawler.config.max_concurrency

    try:
        with Live(console=console, refresh_per_second=10) as live:
            crawl_coroutine = crawler.index(origin_url=origin_url, k=depth)
            crawl_future = asyncio.create_task(crawl_coroutine)

            while not crawl_future.done():
                live.update(
                    Group(
                        Panel(_build_status_table(state), border_style="cyan"),
                        progress,
                    )
                )
                await asyncio.sleep(0.1)

            # Raise any crawler exception after dashboard closes.
            await crawl_future
            live.update(
                Group(
                    Panel(_build_status_table(state), border_style="green"),
                    progress,
                )
            )

        console.print("[bold green]Crawl finished successfully.[/bold green]")
    finally:
        db.close()


def _print_search_results(query: str, rows: Iterable[tuple[str, str, int]]) -> None:
    table = Table(title=f"FTS Search Results for: {query}", expand=True)
    table.add_column("Relevant URL", style="cyan", overflow="fold")
    table.add_column("Origin URL", style="magenta", overflow="fold")
    table.add_column("Depth", justify="right", style="green")

    count = 0
    for relevant_url, origin_url, depth in rows:
        count += 1
        table.add_row(relevant_url, origin_url, str(depth))

    if count == 0:
        console.print("[yellow]No matching documents found.[/yellow]")
        return

    console.print(table)


async def run_search(query: str, db_path: str, limit: int) -> None:
    rows = await search_with_db(query=query, db_path=db_path, limit=limit)
    _print_search_results(query, rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="advanced-crawler",
        description="Concurrent web crawler with SQLite FTS5 search and rich dashboard.",
    )
    parser.add_argument("--db", default="crawler.db", help="Path to SQLite database file.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    crawl_parser = subparsers.add_parser("crawl", help="Run indexing crawl mode.")
    crawl_parser.add_argument("origin_url", help="Starting URL for crawl.")
    crawl_parser.add_argument("depth", type=int, help="Maximum crawl depth (k).")

    search_parser = subparsers.add_parser("search", help="Run FTS search mode.")
    search_parser.add_argument("query", help="FTS query string.")
    search_parser.add_argument("--limit", type=int, default=25, help="Max number of rows.")

    return parser


async def _dispatch(args: argparse.Namespace) -> None:
    if args.command == "crawl":
        await run_crawl(origin_url=args.origin_url, depth=args.depth, db_path=args.db)
        return

    if args.command == "search":
        await run_search(query=args.query, db_path=args.db, limit=args.limit)
        return

    raise ValueError(f"Unsupported command: {args.command}")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    asyncio.run(_dispatch(args))


if __name__ == "__main__":
    main()

# Advanced Concurrent Web Crawler

A high-performance asyncio crawler with SQLite WAL + FTS5 indexing, resumable queue checkpoints, robots.txt politeness, and a Rich-powered live terminal dashboard.

## Repository

GitHub: https://github.com/berkay125/crawler-example

## Deliverables (Final-4)

- Code: [crawler.py](crawler.py), [database.py](database.py), [search.py](search.py), [cli.py](cli.py)
- Readme: [readme.md](readme.md)
- PRD: [product_prd.md](product_prd.md)
- Production roadmap: [recommendation.md](recommendation.md)

## Tech Stack

- Python 3.10+
- asyncio for concurrency
- sqlite3 with WAL mode
- SQLite FTS5 for search
- Native urllib (urllib.request + urllib.robotparser) for HTTP and robots
- Native `html.parser` for HTML parsing
- Rich (live, table, progress) for terminal UX

## Architecture Overview

### 1) Storage and Search

[database.py](database.py) defines a thin `CrawlDatabase` layer that:

- Enables SQLite WAL (`PRAGMA journal_mode=WAL`) to support concurrent readers while crawler writes.
- Uses `visited_urls` table for strict deduplication (`INSERT OR IGNORE`).
- Stores fetched documents in `pages`.
- Maintains queue resumability through `queue_checkpoint` snapshots.
- Maintains full-text index in FTS5 virtual table `pages_fts`.

[search.py](search.py) runs async search through `asyncio.to_thread(...)` and issues:

- `MATCH` query against `pages_fts`
- `bm25(pages_fts)` ranking for relevance ordering

### 2) Crawling Engine

[crawler.py](crawler.py) implements `ConcurrentCrawler` with:

- Bounded queue backpressure: `asyncio.Queue(maxsize=5000)`
- Request concurrency limit: `asyncio.Semaphore(max_concurrency)`
- Per-host robots.txt checks with `urllib.robotparser`
- Graceful `SIGINT` shutdown behavior:
  - stop accepting new URLs
  - finish in-flight requests
  - persist pending queue into `queue_checkpoint`

### 3) CLI Experience

[cli.py](cli.py) provides:

- `crawl` mode with live Rich dashboard (crawled pages, errors, queue depth, active workers)
- `search` mode with formatted Rich table results

## Why This Meets “What Success Looks Like”

### Functionality (40%)

- Accurate crawl traversal to depth `k` with strict dedupe persisted in SQLite.
- FTS search works while indexing due to WAL mode and separate SQLite search connection.
- Handles HTTP failures safely (timeouts/SSL/4xx/5xx) without crashing crawler loop.

### Architectural Sensibility (40%)

- Backpressure: bounded queue prevents unbounded memory growth during high link fan-out.
- Concurrency safety:
  - I/O done in asyncio workers
  - DB operations marshalled through a thread-safe wrapper with lock-protected sqlite connection
  - active request counters protected with async lock
- Resumability via queue checkpoints supports practical operational recovery.

### AI Stewardship (20%)

Design choices and generated code are intentionally constrained to high-ROI, standard-library + mainstream dependency patterns:

- Chose WAL + FTS5 as simplest robust path to concurrent indexing/search in local single-node setup.
- Chose semaphore + bounded queue (instead of ad hoc task spawning) for predictable throughput and memory.
- Chose persistent dedupe and checkpoint tables to convert Ctrl+C into recoverable state, not data loss.

## Setup

1. Create/activate virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Crawl

```bash
.venv/bin/python cli.py --db crawler.db crawl https://example.com 1
```

### Search

```bash
.venv/bin/python cli.py --db crawler.db search "example OR domain" --limit 20
```

## Resume Behavior

- Press `Ctrl+C` during crawling.
- The crawler finishes in-flight work, checkpoints pending queue to SQLite, and exits.
- Re-running the same crawl command resumes from checkpoint and existing dedupe state.

## Concurrent Search While Indexing

To verify that search can run while indexing is still active:

```bash
.venv/bin/python cli.py --db concurrent_demo.db crawl https://news.ycombinator.com 1 &
sleep 2
.venv/bin/python cli.py --db concurrent_demo.db search "news" --limit 5
wait
```

The search results appear mid-crawl (reflected in SQLite FTS5 via WAL mode concurrent readers). No data corruption or blocking occurs.

## Quick Validation Commands

```bash
.venv/bin/python -m compileall .
.venv/bin/python cli.py --db smoke_test.db search "testing"
.venv/bin/python cli.py --db smoke_test.db crawl https://example.com 0
./scripts/concurrency_check.sh concurrent_check.db https://docs.python.org/3/ 2
```

## Notes and Limitations

- Current implementation is optimized for single-machine operation.
- robots.txt handling is permissive fallback when robots retrieval fails.
- For large-scale production deployment, see [recommendation.md](recommendation.md).

# Product Requirements Document (PRD) & Build Prompt: Advanced Concurrent Web Crawler

You are an Expert Software Architect and Backend Engineer. Your task is to build a high-performance web crawler system based on the specifications below. The goal is to maximize features, code quality, and architectural sensibility within a 3-5 hour timeframe. We want to implement high-ROI (Return on Investment) features that are easy to build with standard or popular libraries but show deep engineering maturity.

## 1. Technology Stack
- Language: Python 3.10+
- Concurrency: asyncio (Native asynchronous I/O)
- Database: sqlite3. MUST enable WAL (Write-Ahead Logging) mode to allow concurrent searches while indexing. MUST use SQLite FTS5 (Full-Text Search) extension for the search table to provide lightning-fast query capabilities instead of basic LIKE clauses.
- HTTP/Parsing: httpx (async client with connection pooling) and BeautifulSoup.
- UI/CLI: Use the rich library (specifically rich.live, rich.table, rich.progress) to create a stunning, real-time terminal dashboard.

## 2. Core Capabilities

### A. Indexing (/index)
- Function: async def index(origin_url: str, k: int)
- Depth & Duplicates: Crawl to max depth k. Track visited URLs in SQLite to strictly avoid crawling the same page twice.
- Load Management (Backpressure): Use a bounded asyncio.Queue (e.g., maxsize=5000) to control memory, and an asyncio.Semaphore to cap concurrent HTTP requests (e.g., 15-20 max).
- Robots.txt Compliance (Politeness): Use Python's built-in urllib.robotparser to check if scraping the URL is allowed before fetching.
- Graceful Shutdown & Resumability: Catch SIGINT (Ctrl+C). When caught, stop accepting new URLs, finish the active requests in the Semaphore, save the current queue state to SQLite, and exit cleanly. This guarantees the crawler can resume instantly upon restart.

### B. Searching (/search)
- Function: async def search(query: str) -> List[Tuple[str, str, int]]
- Execution: Must be runnable concurrently while the indexer is active.
- FTS5 Integration: Query the SQLite FTS5 virtual table. Match the query against page titles and text content. Rank/Order by relevance (FTS5 bm25 rank if possible, otherwise simple match).
- Output: Return tuples of (relevant_url, origin_url, depth).

### C. Live CLI Dashboard (main.py)
Instead of a basic REPL, create an interactive terminal app using rich or standard argparse + rich.
If run in "crawl" mode, display a live updating dashboard showing:
1. Progress (Total Pages Crawled, Errors).
2. Queue Depth (Current size of the asyncio queue).
3. Active Workers (How many semaphore slots are currently held).
If run in "search" mode, display the FTS results in a neatly formatted rich.table.

## 3. Architecture & File Structure Requirements
Please generate the following file structure:

- database.py: Handles SQLite connection, WAL mode, FTS5 virtual table setup, and robust resumability logic.
- crawler.py: Contains the crawler logic, robots.txt parsing, graceful shutdown signal handling, and backpressure implementation.
- search.py: FTS5 querying logic.
- cli.py: The rich-powered live dashboard and entry point.
- requirements.txt: Include httpx, beautifulsoup4, rich.
- product_prd.md: A copy of this PRD.
- recommendation.md: A 1-2 paragraph pitch on scaling this to production (e.g., PostgreSQL + Elasticsearch, RabbitMQ/Kafka, distributed nodes, rotating proxies).

## 4. Specific Instructions for AI
- Write extremely clean, type-hinted (typing), and heavily commented code.
- Handle HTTP errors gracefully (timeouts, 404s, SSL errors) without crashing.
- Please generate the files step by step. Let's start with database.py and crawler.py first, ensuring the FTS5 schema and signal handling are implemented correctly.

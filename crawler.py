from __future__ import annotations

import asyncio
import re
import signal
from collections import Counter
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import urljoin, urlparse, urldefrag
from urllib.robotparser import RobotFileParser

from bs4 import BeautifulSoup

from database import CrawlDatabase, CrawlTask
from flat_storage import FlatFileWordStore


ProgressCallback = Callable[["CrawlStats", int, int], Awaitable[None] | None]


@dataclass
class CrawlStats:
    crawled_pages: int = 0
    error_pages: int = 0
    discovered_urls: int = 0


@dataclass(frozen=True)
class CrawlerConfig:
    queue_maxsize: int = 5000
    max_concurrency: int = 18
    request_timeout_seconds: float = 10.0
    user_agent: str = "AdvancedCrawlerBot/1.0"
    storage_file_path: str = "data/storage/p.data"


@dataclass(frozen=True)
class FetchResult:
    status_code: int
    content_type: str
    body_text: str
    error: Optional[str] = None


STOP_WORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "to",
    "was",
    "were",
    "with",
}


class ConcurrentCrawler:
    """Async crawler with durable shutdown checkpoints and robots.txt politeness."""

    def __init__(
        self,
        db: CrawlDatabase,
        config: Optional[CrawlerConfig] = None,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> None:
        self.db = db
        self.config = config or CrawlerConfig()
        self.progress_callback = progress_callback

        self.queue: asyncio.Queue[CrawlTask] = asyncio.Queue(maxsize=self.config.queue_maxsize)
        self.semaphore = asyncio.Semaphore(self.config.max_concurrency)

        self._stop_event = asyncio.Event()
        self._accept_new_urls = True
        self._worker_tasks: list[asyncio.Task[None]] = []

        self._active_requests = 0
        self._active_requests_lock = asyncio.Lock()

        self._robots_cache: dict[str, RobotFileParser] = {}
        self._robots_cache_lock = asyncio.Lock()
        self._word_store = FlatFileWordStore(file_path=self.config.storage_file_path)

        self.stats = CrawlStats()

    async def index(self, origin_url: str, k: int) -> CrawlStats:
        if k < 0:
            raise ValueError("k must be >= 0")

        loop = asyncio.get_running_loop()
        self._install_signal_handler(loop)

        await self._bootstrap_queue(origin_url, k)

        worker_count = self.config.max_concurrency
        self._worker_tasks = [
            asyncio.create_task(self._worker_loop(k), name=f"crawler-worker-{i}")
            for i in range(worker_count)
        ]

        try:
            while True:
                if self._stop_event.is_set():
                    break

                if self.queue.empty() and await self._get_active_requests() == 0:
                    break

                await self._emit_progress()
                await asyncio.sleep(0.2)
        finally:
            # Stop feeding workers and wait for in-flight fetches to complete.
            self._accept_new_urls = False
            self._stop_event.set()
            while await self._get_active_requests() > 0:
                await self._emit_progress()
                await asyncio.sleep(0.1)

            await self._shutdown_workers()
            remaining_tasks = self._drain_pending_queue()

            if remaining_tasks:
                await asyncio.to_thread(self.db.save_queue_checkpoint, remaining_tasks)
            else:
                await asyncio.to_thread(self.db.clear_queue_checkpoint)

            await self._emit_progress()

        return self.stats

    def _install_signal_handler(self, loop: asyncio.AbstractEventLoop) -> None:
        def _request_stop() -> None:
            self._accept_new_urls = False
            self._stop_event.set()

        try:
            loop.add_signal_handler(signal.SIGINT, _request_stop)
        except NotImplementedError:
            # Fallback for environments where event-loop signal handlers are unavailable.
            signal.signal(signal.SIGINT, lambda *_: _request_stop())

    async def _bootstrap_queue(self, origin_url: str, max_depth: int) -> None:
        checkpoint = await asyncio.to_thread(self.db.load_queue_checkpoint)
        if checkpoint:
            for task in checkpoint:
                if task.depth <= max_depth:
                    await self.queue.put(task)
            return

        normalized_origin = self._normalize_url(origin_url)
        seed = CrawlTask(url=normalized_origin, origin_url=normalized_origin, depth=0)
        inserted = await asyncio.to_thread(self.db.register_discovered_url, seed)
        if inserted:
            self.stats.discovered_urls += 1
            await self.queue.put(seed)

    async def _worker_loop(self, max_depth: int) -> None:
        while True:
            if self._stop_event.is_set():
                return

            try:
                task = await asyncio.wait_for(self.queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue

            try:
                await self._process_task(task, max_depth)
            finally:
                self.queue.task_done()

    async def _process_task(self, task: CrawlTask, max_depth: int) -> None:
        async with self.semaphore:
            await self._increment_active_requests(1)
            try:
                if not await self._is_allowed_by_robots(task.url):
                    return

                fetch_result = await asyncio.to_thread(self._fetch_url, task.url)
                if fetch_result.error is not None:
                    self.stats.error_pages += 1
                    await asyncio.to_thread(
                        self.db.save_page,
                        task,
                        title="",
                        content="",
                        status_code=fetch_result.status_code,
                        error=fetch_result.error,
                    )
                    return

                content_type = fetch_result.content_type
                raw_body = fetch_result.body_text
                title, text_content, discovered_links = self._extract_page_data(
                    task.url, raw_body, content_type
                )

                if fetch_result.status_code >= 400:
                    self.stats.error_pages += 1
                    await asyncio.to_thread(
                        self.db.save_page,
                        task,
                        title=title,
                        content=text_content,
                        status_code=fetch_result.status_code,
                        error=f"HTTP {fetch_result.status_code}",
                    )
                    return

                self.stats.crawled_pages += 1
                await asyncio.to_thread(
                    self.db.save_page,
                    task,
                    title=title,
                    content=text_content,
                    status_code=fetch_result.status_code,
                    error=None,
                )
                await asyncio.to_thread(
                    self._persist_word_frequencies,
                    task,
                    text_content,
                )

                if task.depth >= max_depth or not self._accept_new_urls:
                    return

                for absolute_url in discovered_links:
                    if self._stop_event.is_set() or not self._accept_new_urls:
                        return

                    child_task = CrawlTask(
                        url=absolute_url,
                        origin_url=task.origin_url,
                        depth=task.depth + 1,
                    )
                    inserted = await asyncio.to_thread(
                        self.db.register_discovered_url, child_task
                    )
                    if not inserted:
                        continue

                    self.stats.discovered_urls += 1
                    await self.queue.put(child_task)
            finally:
                await self._increment_active_requests(-1)

    def _extract_page_data(
        self, base_url: str, html_body: str, content_type: str
    ) -> tuple[str, str, list[str]]:
        if "text/html" not in content_type.lower():
            # For non-HTML content, index a small excerpt and skip link discovery.
            excerpt = html_body[:1000]
            return "", excerpt, []

        soup = BeautifulSoup(html_body, "html.parser")
        for removable in soup(["script", "style", "noscript"]):
            removable.decompose()

        title = soup.title.get_text(" ", strip=True) if soup.title else ""
        text_content = soup.get_text(separator=" ", strip=True)

        links: list[str] = []
        for anchor in soup.find_all("a", href=True):
            absolute = self._normalize_url(urljoin(base_url, anchor["href"]))
            parsed = urlparse(absolute)
            if parsed.scheme not in {"http", "https"}:
                continue
            links.append(absolute)

        return title, text_content, links

    @staticmethod
    def _text_to_word_frequencies(text: str) -> Counter[str]:
        """Lowercases text, removes punctuation, and counts word frequencies."""
        words = re.findall(r"[a-z0-9]+", text.lower())
        filtered = [word for word in words if len(word) > 1 and word not in STOP_WORDS]
        return Counter(filtered)

    def _persist_word_frequencies(self, task: CrawlTask, page_text: str) -> int:
        frequencies = self._text_to_word_frequencies(page_text)
        return self._word_store.append_frequencies(
            url=task.url,
            origin_url=task.origin_url,
            depth=task.depth,
            frequencies=frequencies,
        )

    async def _is_allowed_by_robots(self, target_url: str) -> bool:
        parser = await self._get_robots_parser(target_url)
        try:
            return parser.can_fetch(self.config.user_agent, target_url)
        except Exception:
            # Prefer permissive behavior if parser fails unexpectedly.
            return True

    async def _get_robots_parser(self, target_url: str) -> RobotFileParser:
        parsed = urlparse(target_url)
        key = f"{parsed.scheme}://{parsed.netloc}"

        async with self._robots_cache_lock:
            cached = self._robots_cache.get(key)
            if cached is not None:
                return cached

        robots_url = f"{key}/robots.txt"
        parser = RobotFileParser()
        parser.set_url(robots_url)

        try:
            robots_result = await asyncio.to_thread(self._fetch_url, robots_url)
            if robots_result.status_code == 200 and robots_result.body_text:
                parser.parse(robots_result.body_text.splitlines())
            else:
                parser.parse(["User-agent: *", "Allow: /"])
        except Exception:
            parser.parse(["User-agent: *", "Allow: /"])

        async with self._robots_cache_lock:
            self._robots_cache[key] = parser

        return parser

    async def _increment_active_requests(self, delta: int) -> None:
        async with self._active_requests_lock:
            self._active_requests += delta

    async def _get_active_requests(self) -> int:
        async with self._active_requests_lock:
            return self._active_requests

    def _drain_pending_queue(self) -> list[CrawlTask]:
        pending: list[CrawlTask] = []
        while True:
            try:
                pending.append(self.queue.get_nowait())
                self.queue.task_done()
            except asyncio.QueueEmpty:
                break
        return pending

    async def _shutdown_workers(self) -> None:
        if not self._worker_tasks:
            return

        await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        self._worker_tasks.clear()

    async def _emit_progress(self) -> None:
        if self.progress_callback is None:
            return

        maybe_awaitable = self.progress_callback(
            self.stats,
            self.queue.qsize(),
            await self._get_active_requests(),
        )
        if maybe_awaitable is not None:
            await maybe_awaitable

    @staticmethod
    def _normalize_url(url: str) -> str:
        normalized, _fragment = urldefrag(url.strip())
        return normalized

    def _fetch_url(self, url: str) -> FetchResult:
        """Performs a native urllib request and decodes body to text safely."""
        request = urllib_request.Request(
            url,
            headers={"User-Agent": self.config.user_agent},
            method="GET",
        )

        try:
            with urllib_request.urlopen(request, timeout=self.config.request_timeout_seconds) as resp:
                status_code = int(getattr(resp, "status", 200))
                content_type = resp.headers.get("Content-Type", "")
                charset = resp.headers.get_content_charset() or "utf-8"
                body_bytes = resp.read()
                body_text = body_bytes.decode(charset, errors="replace")
                return FetchResult(
                    status_code=status_code,
                    content_type=content_type,
                    body_text=body_text,
                    error=None,
                )
        except urllib_error.HTTPError as exc:
            content_type = exc.headers.get("Content-Type", "") if exc.headers else ""
            charset = exc.headers.get_content_charset() if exc.headers else None
            body_bytes = exc.read() if hasattr(exc, "read") else b""
            body_text = body_bytes.decode(charset or "utf-8", errors="replace")
            return FetchResult(
                status_code=int(exc.code),
                content_type=content_type,
                body_text=body_text,
                error=f"HTTPError: {exc.reason}",
            )
        except Exception as exc:
            return FetchResult(
                status_code=0,
                content_type="",
                body_text="",
                error=str(exc),
            )


async def index(origin_url: str, k: int) -> CrawlStats:
    """Convenience API required by the PRD."""
    db = CrawlDatabase()
    crawler = ConcurrentCrawler(db=db)
    try:
        return await crawler.index(origin_url=origin_url, k=k)
    finally:
        db.close()

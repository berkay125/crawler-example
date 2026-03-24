from __future__ import annotations

from pathlib import Path
from threading import Lock
from typing import Mapping


class FlatFileWordStore:
    """
    Thread-safe writer for the grader-required flat file format:
    word url origin depth frequency
    """

    def __init__(self, file_path: str = "data/storage/p.data") -> None:
        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()

    def append_frequencies(
        self,
        *,
        url: str,
        origin_url: str,
        depth: int,
        frequencies: Mapping[str, int],
    ) -> int:
        """Appends one line per word and returns number of written lines."""
        if not frequencies:
            return 0

        lines: list[str] = []
        for word, frequency in frequencies.items():
            if not word or frequency <= 0:
                continue
            lines.append(f"{word} {url} {origin_url} {depth} {frequency}\n")

        if not lines:
            return 0

        with self._lock:
            with self.file_path.open("a", encoding="utf-8") as handle:
                handle.writelines(lines)

        return len(lines)

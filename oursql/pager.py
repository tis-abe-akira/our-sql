"""
oursql/pager.py
Pager: fixed-size page I/O layer.

Each page is exactly `page_size` bytes (default 4096 = 4 KB).
Page n is stored at byte offset n * page_size in the file.

This is the lowest-level component; HeapFile and PageBTree both
sit on top of Pager.
"""

from __future__ import annotations
import os
from pathlib import Path


class Pager:
    """
    Read and write fixed-size pages to a binary file.

    The file is created if it does not exist.
    All pages are the same size (page_size bytes).
    New pages are appended to the end of the file.
    """

    PAGE_SIZE: int = 4096  # 4 KB

    def __init__(self, filepath: str | Path, page_size: int = PAGE_SIZE) -> None:
        self.filepath = Path(filepath)
        self.page_size = page_size
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        # Open in read/write binary mode; create if missing
        mode = "r+b" if self.filepath.exists() else "w+b"
        self._file = open(self.filepath, mode)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def read_page(self, page_id: int) -> bytes:
        """
        Read page_id from disk.
        Returns PAGE_SIZE bytes (zero-padded if shorter than expected).
        Raises IndexError if page_id >= num_pages().
        """
        if page_id >= self.num_pages():
            raise IndexError(f"page_id {page_id} out of range (num_pages={self.num_pages()})")
        self._file.seek(page_id * self.page_size)
        data = self._file.read(self.page_size)
        # Pad to page_size (last page might be short due to truncation)
        return data.ljust(self.page_size, b"\x00")

    def write_page(self, page_id: int, data: bytes) -> None:
        """
        Write exactly page_size bytes to page_id.
        Raises ValueError if data is not exactly page_size bytes.
        Raises IndexError if page_id > num_pages() (gap not allowed).
        """
        if len(data) != self.page_size:
            raise ValueError(
                f"data must be exactly {self.page_size} bytes, got {len(data)}"
            )
        if page_id > self.num_pages():
            raise IndexError(
                f"page_id {page_id} would create a gap (num_pages={self.num_pages()})"
            )
        self._file.seek(page_id * self.page_size)
        self._file.write(data)
        self._file.flush()

    def allocate_page(self) -> int:
        """
        Append a new blank page to the file and return its page_id.
        The new page is zeroed out.
        """
        page_id = self.num_pages()
        self._file.seek(0, 2)  # seek to end
        self._file.write(b"\x00" * self.page_size)
        self._file.flush()
        return page_id

    def num_pages(self) -> int:
        """Return the current number of pages in the file."""
        self._file.seek(0, 2)
        file_size = self._file.tell()
        return file_size // self.page_size

    def close(self) -> None:
        """Flush and close the underlying file."""
        self._file.flush()
        self._file.close()

    def __enter__(self) -> "Pager":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

"""
oursql/storage.py
HeapStorage: an in-memory row store for OurSQL.

Rows are stored in a Python list indexed by row_id.
A deleted row is replaced with None (tombstone pattern).
"""

from __future__ import annotations
from typing import Any


class HeapStorage:
    """
    Simple in-memory heap storage.

    Each row is stored at position row_id (list index).
    Deleted rows are marked as None (tombstone).
    """

    def __init__(self) -> None:
        self._data: list[dict[str, Any] | None] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def insert(self, row: dict[str, Any]) -> int:
        """Append row and return its row_id."""
        row_id = len(self._data)
        self._data.append(dict(row))  # store a copy
        return row_id

    def get(self, row_id: int) -> dict[str, Any] | None:
        """Return the row at row_id, or None if deleted / out of range."""
        if row_id < 0 or row_id >= len(self._data):
            return None
        return self._data[row_id]

    def update(self, row_id: int, row: dict[str, Any]) -> None:
        """Overwrite the row at row_id. Raises if row_id is invalid/deleted."""
        if row_id < 0 or row_id >= len(self._data) or self._data[row_id] is None:
            raise KeyError(f"row_id {row_id} does not exist or has been deleted")
        self._data[row_id] = dict(row)

    def delete(self, row_id: int) -> None:
        """Mark the row as deleted (tombstone). Raises if already deleted."""
        if row_id < 0 or row_id >= len(self._data) or self._data[row_id] is None:
            raise KeyError(f"row_id {row_id} does not exist or has been deleted")
        self._data[row_id] = None

    def scan(self) -> list[dict[str, Any]]:
        """Return all non-deleted rows (full table scan)."""
        return [row for row in self._data if row is not None]

    def __len__(self) -> int:
        """Return the total number of slots (including tombstones)."""
        return len(self._data)

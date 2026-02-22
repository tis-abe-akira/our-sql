"""
oursql/table.py
Table implementations for OurSQL.

Two concrete classes share a common interface:

  InMemoryTable (Phase 1)
    BPlusTree (oursql.btree) + HeapStorage (oursql.storage)
    Lives entirely in RAM; data is lost on process exit.

  DiskTable (Phase 2)
    PageBTree (oursql.page_btree) + HeapFile (oursql.heap_file)
    Persists data to files; data survives process restarts.

Both expose the same DML surface:
  insert / select / select_range / select_all / update / delete / row_count
"""

from __future__ import annotations
from pathlib import Path
from typing import Any

# Phase 1 backends
from oursql.btree import BPlusTree
from oursql.storage import HeapStorage

# Phase 2 backends
from oursql.page_btree import PageBTree
from oursql.heap_file import HeapFile


# ──────────────────────────────────────────────────────────────────────
# Phase 1: In-memory table
# ──────────────────────────────────────────────────────────────────────

class InMemoryTable:
    """
    In-memory table (Phase 1).
    OurSQLTable is an alias for backward compatibility.
    """

    def __init__(self, name: str, schema: dict[str, str], btree_order: int = 4) -> None:
        self.name = name
        self.schema = schema
        self._pk_column: str = next(iter(schema))
        self._index: BPlusTree = BPlusTree(order=btree_order)
        self._storage: HeapStorage = HeapStorage()

    # DML ---------------------------------------------------------------

    def insert(self, row_data: dict[str, Any]) -> int:
        pk_value = row_data.get(self._pk_column)
        if pk_value is None and self._pk_column not in row_data:
            raise KeyError(f"Primary key column '{self._pk_column}' is missing from row data")
        if self._index.search(pk_value) is not None:
            raise ValueError(f"Duplicate primary key: {pk_value}")
        row_id = self._storage.insert(row_data)
        self._index.insert(pk_value, row_id)
        return row_id

    def select(self, pk_value: Any) -> dict[str, Any] | None:
        row_id = self._index.search(pk_value)
        if row_id is None:
            return None
        return self._storage.get(row_id)

    def select_range(self, start_pk: Any, end_pk: Any) -> list[dict[str, Any]]:
        row_ids = self._index.range_scan(start_pk, end_pk)
        result = []
        for rid in row_ids:
            row = self._storage.get(rid)
            if row is not None:
                result.append(row)
        return result

    def select_all(self) -> list[dict[str, Any]]:
        return self._storage.scan()

    def update(self, pk_value: Any, updates: dict[str, Any]) -> bool:
        if self._pk_column in updates and updates[self._pk_column] != pk_value:
            raise ValueError("Cannot change the primary key via update(). Delete and re-insert instead.")
        row_id = self._index.search(pk_value)
        if row_id is None:
            return False
        existing = self._storage.get(row_id)
        if existing is None:
            return False
        merged = {**existing, **updates}
        self._storage.update(row_id, merged)
        return True

    def delete(self, pk_value: Any) -> bool:
        row_id = self._index.search(pk_value)
        if row_id is None:
            return False
        self._index.delete(pk_value)
        self._storage.delete(row_id)
        return True

    def row_count(self) -> int:
        return len(self._storage.scan())

    def __repr__(self) -> str:
        return f"InMemoryTable(name={self.name!r}, pk={self._pk_column!r}, rows={self.row_count()})"


# Backward-compatible alias (Phase 1 tests use OurSQLTable)
OurSQLTable = InMemoryTable


# ──────────────────────────────────────────────────────────────────────
# Phase 2: Disk-backed table
# ──────────────────────────────────────────────────────────────────────

class DiskTable:
    """
    Disk-backed table (Phase 2).

    Files created under `table_dir/`:
      heap.db   — HeapFile (row data)
      pk.idx    — PageBTree (primary key index)

    Only integer primary keys are supported in Phase 2.
    """

    def __init__(
        self,
        name: str,
        schema: dict[str, str],
        table_dir: str | Path,
        btree_order: int = 4,
    ) -> None:
        self.name = name
        self.schema = schema
        self._pk_column: str = next(iter(schema))
        self._dir = Path(table_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._index = PageBTree(self._dir / "pk.idx", order=btree_order)
        self._storage = HeapFile(self._dir / "heap.db")

    # DML ---------------------------------------------------------------

    def insert(self, row_data: dict[str, Any]) -> tuple[int, int]:
        """Insert a row. Returns the RID (page_id, slot_id)."""
        pk_value = row_data.get(self._pk_column)
        if pk_value is None and self._pk_column not in row_data:
            raise KeyError(f"Primary key column '{self._pk_column}' is missing from row data")
        if not isinstance(pk_value, int):
            raise TypeError(f"Phase 2 only supports integer primary keys, got {type(pk_value).__name__}")
        if self._index.search(pk_value) is not None:
            raise ValueError(f"Duplicate primary key: {pk_value}")

        rid = self._storage.insert(row_data)      # (page_id, slot_id)
        self._index.insert(pk_value, rid)
        return rid

    def select(self, pk_value: int) -> dict[str, Any] | None:
        rid = self._index.search(pk_value)
        if rid is None:
            return None
        return self._storage.get(*rid)

    def select_range(self, start_pk: int, end_pk: int) -> list[dict[str, Any]]:
        rids = self._index.range_scan(start_pk, end_pk)
        result = []
        for rid in rids:
            row = self._storage.get(*rid)
            if row is not None:
                result.append(row)
        return result

    def select_all(self) -> list[dict[str, Any]]:
        return self._storage.scan()

    def update(self, pk_value: int, updates: dict[str, Any]) -> bool:
        """
        Update a row in-place.
        Raises ValueError if the updated row is larger than the original
        (HeapFile limitation in Phase 2; delete + re-insert as workaround).
        """
        if self._pk_column in updates and updates[self._pk_column] != pk_value:
            raise ValueError("Cannot change the primary key via update(). Delete and re-insert instead.")
        rid = self._index.search(pk_value)
        if rid is None:
            return False
        existing = self._storage.get(*rid)
        if existing is None:
            return False
        merged = {**existing, **updates}
        self._storage.update(*rid, merged)
        return True

    def delete(self, pk_value: int) -> bool:
        rid = self._index.search(pk_value)
        if rid is None:
            return False
        self._index.delete(pk_value)
        self._storage.delete(*rid)
        return True

    def row_count(self) -> int:
        return len(self._storage.scan())

    def close(self) -> None:
        """Close underlying file handles."""
        self._index.close()
        self._storage.close()

    def __repr__(self) -> str:
        return f"DiskTable(name={self.name!r}, pk={self._pk_column!r}, dir={self._dir})"

"""
oursql/table.py
OurSQLTable: combines BPlusTree (primary key index) and HeapStorage.

The first column of the schema is always treated as the primary key.
"""

from __future__ import annotations
from typing import Any

from oursql.btree import BPlusTree
from oursql.storage import HeapStorage


class OurSQLTable:
    """
    A single table backed by a B+Tree primary key index and a HeapStorage.

    Schema example:
        {"id": "int", "name": "text", "age": "int"}
    The first key in the schema dict is treated as the primary key.
    """

    def __init__(self, name: str, schema: dict[str, str], btree_order: int = 4) -> None:
        self.name = name
        self.schema = schema
        self._pk_column: str = next(iter(schema))  # first column = PK
        self._index: BPlusTree = BPlusTree(order=btree_order)
        self._storage: HeapStorage = HeapStorage()

    # ------------------------------------------------------------------
    # DML
    # ------------------------------------------------------------------

    def insert(self, row_data: dict[str, Any]) -> int:
        """
        Insert a row.

        Steps:
          1. Validate that the PK column is present.
          2. Append row to HeapStorage → get row_id.
          3. Register (pk_value, row_id) in the B+Tree.

        Returns the assigned row_id.
        Raises KeyError if the primary key is missing from row_data.
        Raises ValueError if the primary key already exists.
        """
        pk_value = row_data.get(self._pk_column)
        if pk_value is None and self._pk_column not in row_data:
            raise KeyError(f"Primary key column '{self._pk_column}' is missing from row data")

        if self._index.search(pk_value) is not None:
            raise ValueError(f"Duplicate primary key: {pk_value}")

        row_id = self._storage.insert(row_data)
        self._index.insert(pk_value, row_id)
        return row_id

    def select(self, pk_value: Any) -> dict[str, Any] | None:
        """
        Look up a row by primary key.

        Uses the B+Tree index → O(log n).
        Returns the row dict, or None if not found.
        """
        row_id = self._index.search(pk_value)
        if row_id is None:
            return None
        return self._storage.get(row_id)

    def select_range(self, start_pk: Any, end_pk: Any) -> list[dict[str, Any]]:
        """
        Return all rows whose primary key is in [start_pk, end_pk].
        Uses B+Tree leaf linked-list for efficient range scan.
        """
        row_ids = self._index.range_scan(start_pk, end_pk)
        result = []
        for rid in row_ids:
            row = self._storage.get(rid)
            if row is not None:
                result.append(row)
        return result

    def select_all(self) -> list[dict[str, Any]]:
        """
        Return every row in the table (full table scan via HeapStorage).
        O(n) — no index used.
        """
        return self._storage.scan()

    def update(self, pk_value: Any, updates: dict[str, Any]) -> bool:
        """
        Update a row identified by primary key.

        Merges `updates` into the existing row.
        Returns True if found and updated, False if not found.
        Raises ValueError if the caller tries to change the primary key.
        """
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
        """
        Delete a row by primary key.

        1. Removes the key from the B+Tree.
        2. Marks the HeapStorage slot as a tombstone.

        Returns True if found and deleted, False otherwise.
        """
        row_id = self._index.search(pk_value)
        if row_id is None:
            return False

        self._index.delete(pk_value)
        self._storage.delete(row_id)
        return True

    # ------------------------------------------------------------------
    # Metadata / introspection
    # ------------------------------------------------------------------

    def row_count(self) -> int:
        """Return the number of live rows."""
        return len(self._storage.scan())

    def __repr__(self) -> str:
        return f"OurSQLTable(name={self.name!r}, pk={self._pk_column!r}, rows={self.row_count()})"

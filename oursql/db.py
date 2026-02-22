"""
oursql/db.py
OurSQLDB: top-level DDL manager.

  OurSQLDB()              → Phase 1 in-memory mode (backward compatible)
  OurSQLDB("./data")      → Phase 2 disk-backed mode

In disk mode, table schemas are persisted to catalog.json and row data
is stored under data/<table_name>/.
"""

from __future__ import annotations
from pathlib import Path

from oursql.table import InMemoryTable, DiskTable, OurSQLTable
from oursql.catalog import Catalog


class OurSQLDB:
    """
    Database container providing DDL operations.

    Args:
        data_dir: If None, uses Phase 1 in-memory tables.
                  If a path string/Path, uses Phase 2 disk-backed tables
                  and persists schemas to catalog.json in that directory.
    """

    def __init__(self, data_dir: str | Path | None = None) -> None:
        self._data_dir = Path(data_dir) if data_dir is not None else None
        self._disk_mode = self._data_dir is not None

        if self._disk_mode:
            self._catalog = Catalog(self._data_dir)
            # Re-open existing tables from catalog
            self._tables: dict[str, InMemoryTable | DiskTable] = {}
            for name in self._catalog.list_tables():
                entry = self._catalog.get_table(name)
                self._tables[name] = DiskTable(
                    name=name,
                    schema=entry["schema"],
                    table_dir=self._data_dir / name,
                    btree_order=entry.get("btree_order", 4),
                )
        else:
            self._catalog = None
            self._tables: dict[str, InMemoryTable] = {}

    # ------------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------------

    def create_table(
        self,
        name: str,
        schema: dict[str, str],
        btree_order: int = 4,
    ) -> InMemoryTable | DiskTable:
        """
        Create a new table and return it.
        Raises ValueError if a table with the same name already exists.
        """
        if name in self._tables:
            raise ValueError(f"Table '{name}' already exists")

        if self._disk_mode:
            self._catalog.create_table(name, schema, btree_order)
            table = DiskTable(
                name=name,
                schema=schema,
                table_dir=self._data_dir / name,
                btree_order=btree_order,
            )
        else:
            table = InMemoryTable(name=name, schema=schema, btree_order=btree_order)

        self._tables[name] = table
        return table

    def get_table(self, name: str) -> InMemoryTable | DiskTable | None:
        """Return the table by name, or None if it does not exist."""
        return self._tables.get(name)

    def drop_table(self, name: str) -> bool:
        """
        Drop a table.
        Returns True if the table existed and was dropped, False otherwise.
        """
        if name not in self._tables:
            return False
        table = self._tables.pop(name)
        if self._disk_mode:
            if hasattr(table, "close"):
                table.close()
            self._catalog.drop_table(name)
        return True

    def list_tables(self) -> list[str]:
        """Return a sorted list of all table names."""
        return sorted(self._tables.keys())

    def close(self) -> None:
        """Flush and close all disk-backed tables."""
        for table in self._tables.values():
            if hasattr(table, "close"):
                table.close()

    def __enter__(self) -> "OurSQLDB":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __repr__(self) -> str:
        mode = "disk" if self._disk_mode else "memory"
        tables = ", ".join(self.list_tables()) or "(none)"
        return f"OurSQLDB(mode={mode}, tables=[{tables}])"

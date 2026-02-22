"""
oursql/db.py
OurSQLDB: top-level DDL manager.

Holds a collection of OurSQLTable instances keyed by table name.
"""

from __future__ import annotations

from oursql.table import OurSQLTable


class OurSQLDB:
    """
    Simple in-memory database container.

    Provides DDL-level operations:
      - create_table
      - get_table
      - drop_table
      - list_tables
    """

    def __init__(self) -> None:
        self._tables: dict[str, OurSQLTable] = {}

    def create_table(
        self,
        name: str,
        schema: dict[str, str],
        btree_order: int = 4,
    ) -> OurSQLTable:
        """
        Create a new table and register it.

        Args:
            name:        Table name (must be unique).
            schema:      Ordered dict of {column_name: type_str}.
                         The first entry is always the primary key.
            btree_order: B+Tree minimum degree (default 4).

        Returns the newly created OurSQLTable.
        Raises ValueError if a table with the same name already exists.
        """
        if name in self._tables:
            raise ValueError(f"Table '{name}' already exists")
        table = OurSQLTable(name=name, schema=schema, btree_order=btree_order)
        self._tables[name] = table
        return table

    def get_table(self, name: str) -> OurSQLTable | None:
        """Return the table by name, or None if it does not exist."""
        return self._tables.get(name)

    def drop_table(self, name: str) -> bool:
        """
        Drop a table.
        Returns True if the table existed and was dropped, False otherwise.
        """
        if name not in self._tables:
            return False
        del self._tables[name]
        return True

    def list_tables(self) -> list[str]:
        """Return a sorted list of all table names."""
        return sorted(self._tables.keys())

    def __repr__(self) -> str:
        tables = ", ".join(self.list_tables()) or "(none)"
        return f"OurSQLDB(tables=[{tables}])"

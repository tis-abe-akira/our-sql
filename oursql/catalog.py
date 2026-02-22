"""
oursql/catalog.py
Catalog: persists table schema definitions to catalog.json.

Format:
{
  "users": {
    "schema": {"id": "int", "name": "text"},
    "btree_order": 4
  },
  ...
}
"""

from __future__ import annotations
import json
from pathlib import Path


class Catalog:
    """
    Reads and writes table definitions to/from a JSON catalog file.
    """

    FILENAME = "catalog.json"

    def __init__(self, data_dir: str | Path) -> None:
        self._dir = Path(data_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._path = self._dir / self.FILENAME
        self._data: dict = self._load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def create_table(
        self, name: str, schema: dict[str, str], btree_order: int = 4
    ) -> None:
        """Register a new table definition. Raises if already exists."""
        if name in self._data:
            raise ValueError(f"Table '{name}' already exists in catalog")
        self._data[name] = {"schema": dict(schema), "btree_order": btree_order}
        self._save()

    def get_table(self, name: str) -> dict | None:
        """Return the table definition dict, or None if not found."""
        return self._data.get(name)

    def drop_table(self, name: str) -> bool:
        """Remove a table definition. Returns True if it existed."""
        if name not in self._data:
            return False
        del self._data[name]
        self._save()
        return True

    def list_tables(self) -> list[str]:
        """Return a sorted list of all registered table names."""
        return sorted(self._data.keys())

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _load(self) -> dict:
        if not self._path.exists():
            return {}
        with open(self._path, encoding="utf-8") as f:
            return json.load(f)

    def _save(self) -> None:
        with open(self._path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, ensure_ascii=False, indent=2)

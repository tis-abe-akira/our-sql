"""tests/test_table.py — Integration tests for OurSQLTable."""

import pytest
from oursql.table import OurSQLTable

SCHEMA = {"id": "int", "name": "text", "age": "int"}


def make_table() -> OurSQLTable:
    return OurSQLTable("users", SCHEMA)


# ---------------------------------------------------------------------------
# Insert
# ---------------------------------------------------------------------------

class TestInsert:
    def test_insert_returns_row_id(self):
        t = make_table()
        rid = t.insert({"id": 1, "name": "Alice", "age": 30})
        assert isinstance(rid, int)

    def test_insert_duplicate_pk_raises(self):
        t = make_table()
        t.insert({"id": 1, "name": "Alice"})
        with pytest.raises(ValueError, match="Duplicate"):
            t.insert({"id": 1, "name": "Bob"})

    def test_insert_missing_pk_raises(self):
        t = make_table()
        with pytest.raises(KeyError):
            t.insert({"name": "NoID"})

    def test_row_count_after_inserts(self):
        t = make_table()
        for i in range(5):
            t.insert({"id": i, "name": f"User{i}"})
        assert t.row_count() == 5


# ---------------------------------------------------------------------------
# Select (primary key)
# ---------------------------------------------------------------------------

class TestSelect:
    def test_select_existing_row(self):
        t = make_table()
        t.insert({"id": 42, "name": "Alice", "age": 30})
        row = t.select(42)
        assert row == {"id": 42, "name": "Alice", "age": 30}

    def test_select_nonexistent_returns_none(self):
        t = make_table()
        assert t.select(999) is None

    def test_select_uses_index(self):
        """Smoke test: verify B+Tree path is exercised (many rows)."""
        t = make_table()
        import random
        keys = random.sample(range(1000), 200)
        for k in keys:
            t.insert({"id": k, "name": f"user{k}"})
        for k in random.sample(keys, 50):
            assert t.select(k)["id"] == k


# ---------------------------------------------------------------------------
# Select Range
# ---------------------------------------------------------------------------

class TestSelectRange:
    def test_select_range_basic(self):
        t = make_table()
        for i in range(1, 11):
            t.insert({"id": i, "name": f"User{i}"})
        rows = t.select_range(3, 7)
        assert len(rows) == 5
        ids = sorted(r["id"] for r in rows)
        assert ids == [3, 4, 5, 6, 7]

    def test_select_range_empty(self):
        t = make_table()
        for i in [1, 2, 3]:
            t.insert({"id": i, "name": f"U{i}"})
        assert t.select_range(10, 20) == []


# ---------------------------------------------------------------------------
# Select All (full scan)
# ---------------------------------------------------------------------------

class TestSelectAll:
    def test_select_all_returns_all_rows(self):
        t = make_table()
        for i in range(1, 4):
            t.insert({"id": i, "name": f"U{i}"})
        rows = t.select_all()
        assert len(rows) == 3

    def test_select_all_empty_table(self):
        t = make_table()
        assert t.select_all() == []


# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------

class TestUpdate:
    def test_update_existing_row(self):
        t = make_table()
        t.insert({"id": 1, "name": "Alice", "age": 30})
        ok = t.update(1, {"name": "Alicia"})
        assert ok is True
        assert t.select(1)["name"] == "Alicia"
        assert t.select(1)["age"] == 30  # untouched field preserved

    def test_update_nonexistent_returns_false(self):
        t = make_table()
        assert t.update(99, {"name": "Ghost"}) is False

    def test_update_pk_raises(self):
        t = make_table()
        t.insert({"id": 1, "name": "Alice"})
        with pytest.raises(ValueError, match="primary key"):
            t.update(1, {"id": 99, "name": "Bob"})

    def test_update_same_pk_value_is_ok(self):
        """Setting the PK to the same value should not raise."""
        t = make_table()
        t.insert({"id": 1, "name": "Alice"})
        ok = t.update(1, {"id": 1, "name": "Alicia"})
        assert ok is True


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------

class TestDelete:
    def test_delete_existing_row(self):
        t = make_table()
        t.insert({"id": 5, "name": "Bob"})
        ok = t.delete(5)
        assert ok is True
        assert t.select(5) is None

    def test_delete_nonexistent_returns_false(self):
        t = make_table()
        assert t.delete(99) is False

    def test_delete_reduces_row_count(self):
        t = make_table()
        for i in range(1, 4):
            t.insert({"id": i, "name": f"U{i}"})
        t.delete(2)
        assert t.row_count() == 2

    def test_delete_then_reinsert_same_pk(self):
        t = make_table()
        t.insert({"id": 1, "name": "Alice"})
        t.delete(1)
        # Should be allowed now
        t.insert({"id": 1, "name": "New Alice"})
        assert t.select(1)["name"] == "New Alice"


# ---------------------------------------------------------------------------
# Repr
# ---------------------------------------------------------------------------

class TestRepr:
    def test_repr_shows_name_and_pk(self):
        t = make_table()
        r = repr(t)
        assert "users" in r
        assert "id" in r

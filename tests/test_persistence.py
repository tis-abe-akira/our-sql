"""tests/test_persistence.py — End-to-end persistence tests for Phase 2."""

import pytest
from oursql.db import OurSQLDB


@pytest.fixture
def data_dir(tmp_path):
    return tmp_path / "oursql_data"


class TestDiskTableCRUD:
    def test_insert_and_select(self, data_dir):
        with OurSQLDB(data_dir) as db:
            users = db.create_table("users", {"id": "int", "name": "text"})
            users.insert({"id": 1, "name": "Alice"})
            assert users.select(1) == {"id": 1, "name": "Alice"}

    def test_update(self, data_dir):
        with OurSQLDB(data_dir) as db:
            users = db.create_table("users", {"id": "int", "name": "text"})
            users.insert({"id": 1, "name": "Alice"})
            users.update(1, {"name": "Al"})   # shorter → fits in-place
            assert users.select(1)["name"] == "Al"

    def test_delete(self, data_dir):
        with OurSQLDB(data_dir) as db:
            users = db.create_table("users", {"id": "int", "name": "text"})
            users.insert({"id": 1, "name": "Alice"})
            assert users.delete(1) is True
            assert users.select(1) is None

    def test_select_all(self, data_dir):
        with OurSQLDB(data_dir) as db:
            t = db.create_table("t", {"id": "int", "v": "text"})
            for i in range(5):
                t.insert({"id": i, "v": f"val{i}"})
            assert len(t.select_all()) == 5

    def test_select_range(self, data_dir):
        with OurSQLDB(data_dir) as db:
            t = db.create_table("t", {"id": "int"})
            for i in range(1, 11):
                t.insert({"id": i})
            rows = t.select_range(3, 7)
            assert sorted(r["id"] for r in rows) == [3, 4, 5, 6, 7]


class TestRestartPersistence:
    """Close the DB and re-open it; data must still be there."""

    def test_data_survives_close_reopen(self, data_dir):
        # Session 1: write
        with OurSQLDB(data_dir) as db:
            users = db.create_table("users", {"id": "int", "name": "text"})
            users.insert({"id": 1, "name": "Alice"})
            users.insert({"id": 2, "name": "Bob"})
            users.insert({"id": 3, "name": "Charlie"})

        # Session 2: read
        with OurSQLDB(data_dir) as db2:
            users2 = db2.get_table("users")
            assert users2 is not None
            assert users2.select(1) == {"id": 1, "name": "Alice"}
            assert users2.select(2) == {"id": 2, "name": "Bob"}
            assert users2.select(3) == {"id": 3, "name": "Charlie"}

    def test_deleted_rows_not_visible_after_restart(self, data_dir):
        with OurSQLDB(data_dir) as db:
            t = db.create_table("t", {"id": "int", "name": "text"})
            t.insert({"id": 1, "name": "Alice"})
            t.insert({"id": 2, "name": "Bob"})
            t.delete(2)

        with OurSQLDB(data_dir) as db2:
            t2 = db2.get_table("t")
            assert t2.select(2) is None
            assert t2.select(1)["name"] == "Alice"

    def test_row_count_after_restart(self, data_dir):
        with OurSQLDB(data_dir) as db:
            t = db.create_table("t", {"id": "int"})
            for i in range(10):
                t.insert({"id": i})
            t.delete(5)

        with OurSQLDB(data_dir) as db2:
            t2 = db2.get_table("t")
            assert t2.row_count() == 9

    def test_catalog_lists_tables_after_restart(self, data_dir):
        with OurSQLDB(data_dir) as db:
            db.create_table("users", {"id": "int"})
            db.create_table("products", {"sku": "int"})

        with OurSQLDB(data_dir) as db2:
            assert set(db2.list_tables()) == {"users", "products"}

    def test_drop_table_persists(self, data_dir):
        with OurSQLDB(data_dir) as db:
            db.create_table("tmp", {"id": "int"})
            db.drop_table("tmp")

        with OurSQLDB(data_dir) as db2:
            assert db2.get_table("tmp") is None


class TestMemoryModeUnchanged:
    """Phase 1 in-memory mode should still work identically."""

    def test_memory_mode_basic(self):
        db = OurSQLDB()  # no data_dir → in-memory
        users = db.create_table("users", {"id": "int", "name": "text"})
        users.insert({"id": 1, "name": "Alice"})
        assert users.select(1) == {"id": 1, "name": "Alice"}
        assert users.delete(1) is True
        assert users.select(1) is None

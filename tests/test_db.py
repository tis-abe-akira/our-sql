"""tests/test_db.py — E2E tests for OurSQLDB (DDL)."""

import pytest
from oursql.db import OurSQLDB
from oursql.table import OurSQLTable


@pytest.fixture
def db():
    return OurSQLDB()


class TestCreateTable:
    def test_create_returns_table(self, db):
        t = db.create_table("users", {"id": "int", "name": "text"})
        assert isinstance(t, OurSQLTable)
        assert t.name == "users"

    def test_create_duplicate_raises(self, db):
        db.create_table("users", {"id": "int"})
        with pytest.raises(ValueError, match="already exists"):
            db.create_table("users", {"id": "int"})

    def test_list_tables_after_create(self, db):
        db.create_table("users", {"id": "int"})
        db.create_table("products", {"sku": "text"})
        assert db.list_tables() == ["products", "users"]  # sorted


class TestGetTable:
    def test_get_existing_table(self, db):
        db.create_table("orders", {"id": "int"})
        t = db.get_table("orders")
        assert t is not None
        assert t.name == "orders"

    def test_get_nonexistent_returns_none(self, db):
        assert db.get_table("unknown") is None


class TestDropTable:
    def test_drop_existing(self, db):
        db.create_table("tmp", {"id": "int"})
        assert db.drop_table("tmp") is True
        assert db.get_table("tmp") is None

    def test_drop_nonexistent_returns_false(self, db):
        assert db.drop_table("nope") is False

    def test_drop_then_recreate(self, db):
        db.create_table("tmp", {"id": "int"})
        db.drop_table("tmp")
        t = db.create_table("tmp", {"id": "int", "val": "text"})
        assert t is not None


class TestEndToEnd:
    def test_full_crud_workflow(self, db):
        """Create table, insert rows, select, update, delete."""
        users = db.create_table("users", {"id": "int", "name": "text"})

        users.insert({"id": 1, "name": "Alice"})
        users.insert({"id": 2, "name": "Bob"})
        users.insert({"id": 3, "name": "Charlie"})

        # SELECT by PK
        assert users.select(2) == {"id": 2, "name": "Bob"}

        # UPDATE
        users.update(1, {"name": "Alicia"})
        assert users.select(1)["name"] == "Alicia"

        # DELETE
        users.delete(3)
        assert users.select(3) is None

        # Full scan should return 2 rows
        assert len(users.select_all()) == 2

    def test_multiple_tables_independent(self, db):
        users = db.create_table("users", {"id": "int", "name": "text"})
        products = db.create_table("products", {"sku": "int", "title": "text"})

        users.insert({"id": 1, "name": "Alice"})
        products.insert({"sku": 100, "title": "Widget"})

        assert users.row_count() == 1
        assert products.row_count() == 1

        # Dropping users does not affect products
        db.drop_table("users")
        assert db.get_table("users") is None
        assert products.row_count() == 1


class TestRepr:
    def test_repr_shows_tables(self, db):
        db.create_table("foo", {"id": "int"})
        db.create_table("bar", {"id": "int"})
        r = repr(db)
        assert "foo" in r
        assert "bar" in r

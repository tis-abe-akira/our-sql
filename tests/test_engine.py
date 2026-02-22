"""tests/test_engine.py — E2E tests for SQLEngine."""

import pytest
from oursql.db import OurSQLDB
from oursql.engine import SQLEngine, SQLError


@pytest.fixture
def engine():
    db = OurSQLDB()   # in-memory mode
    eng = SQLEngine(db)
    eng.execute("CREATE TABLE users (id INT, name TEXT)")
    return eng


@pytest.fixture
def populated(engine):
    engine.execute("INSERT INTO users VALUES (1, 'Alice')")
    engine.execute("INSERT INTO users VALUES (2, 'Bob')")
    engine.execute("INSERT INTO users VALUES (3, 'Charlie')")
    return engine


# ── DDL ──────────────────────────────────────────────────────────────

class TestDDL:
    def test_create_table(self):
        db = OurSQLDB()
        eng = SQLEngine(db)
        result = eng.execute("CREATE TABLE t (id INT, val TEXT)")
        assert result["status"] == "OK"

    def test_create_duplicate_raises(self):
        db = OurSQLDB()
        eng = SQLEngine(db)
        eng.execute("CREATE TABLE t (id INT)")
        with pytest.raises(SQLError):
            eng.execute("CREATE TABLE t (id INT)")

    def test_drop_table(self, engine):
        result = engine.execute("DROP TABLE users")
        assert result["status"] == "OK"

    def test_drop_nonexistent_raises(self, engine):
        with pytest.raises(SQLError):
            engine.execute("DROP TABLE no_such_table")

    def test_create_and_drop_round_trip(self):
        db = OurSQLDB()
        eng = SQLEngine(db)
        eng.execute("CREATE TABLE tmp (id INT)")
        eng.execute("DROP TABLE tmp")
        eng.execute("CREATE TABLE tmp (id INT, name TEXT)")  # should succeed
        assert eng.execute("SELECT * FROM tmp") == []


# ── INSERT ────────────────────────────────────────────────────────────

class TestInsert:
    def test_insert_ok(self, engine):
        result = engine.execute("INSERT INTO users VALUES (1, 'Alice')")
        assert result == {"status": "OK", "affected": 1}

    def test_insert_duplicate_pk_raises(self, populated):
        with pytest.raises(Exception):
            populated.execute("INSERT INTO users VALUES (1, 'Dup')")

    def test_insert_wrong_column_count_raises(self, engine):
        with pytest.raises(SQLError):
            engine.execute("INSERT INTO users VALUES (1)")

    def test_insert_null(self, engine):
        result = engine.execute("INSERT INTO users VALUES (99, NULL)")
        assert result["affected"] == 1
        rows = engine.execute("SELECT * FROM users WHERE id = 99")
        assert rows[0]["name"] is None


# ── SELECT ────────────────────────────────────────────────────────────

class TestSelect:
    def test_select_all(self, populated):
        rows = populated.execute("SELECT * FROM users")
        assert len(rows) == 3

    def test_select_star_returns_full_rows(self, populated):
        rows = populated.execute("SELECT * FROM users WHERE id = 1")
        assert rows == [{"id": 1, "name": "Alice"}]

    def test_select_specific_columns(self, populated):
        rows = populated.execute("SELECT name FROM users WHERE id = 2")
        assert rows == [{"name": "Bob"}]

    def test_select_where_eq_miss(self, populated):
        rows = populated.execute("SELECT * FROM users WHERE id = 99")
        assert rows == []

    def test_select_where_gt(self, populated):
        rows = populated.execute("SELECT * FROM users WHERE id > 1")
        ids = sorted(r["id"] for r in rows)
        assert ids == [2, 3]

    def test_select_where_lt(self, populated):
        rows = populated.execute("SELECT * FROM users WHERE id < 3")
        ids = sorted(r["id"] for r in rows)
        assert ids == [1, 2]

    def test_select_where_gte(self, populated):
        rows = populated.execute("SELECT * FROM users WHERE id >= 2")
        ids = sorted(r["id"] for r in rows)
        assert ids == [2, 3]

    def test_select_where_lte(self, populated):
        rows = populated.execute("SELECT * FROM users WHERE id <= 2")
        ids = sorted(r["id"] for r in rows)
        assert ids == [1, 2]

    def test_select_where_ne(self, populated):
        rows = populated.execute("SELECT * FROM users WHERE id != 2")
        ids = sorted(r["id"] for r in rows)
        assert ids == [1, 3]

    def test_select_where_string(self, populated):
        rows = populated.execute("SELECT * FROM users WHERE name = 'Alice'")
        assert len(rows) == 1
        assert rows[0]["id"] == 1

    def test_select_nonexistent_table_raises(self, engine):
        with pytest.raises(SQLError):
            engine.execute("SELECT * FROM no_such_table")

    def test_select_empty_table(self, engine):
        rows = engine.execute("SELECT * FROM users")
        assert rows == []


# ── UPDATE ────────────────────────────────────────────────────────────

class TestUpdate:
    def test_update_by_pk(self, populated):
        populated.execute("UPDATE users SET name = 'Alicia' WHERE id = 1")
        rows = populated.execute("SELECT * FROM users WHERE id = 1")
        assert rows[0]["name"] == "Alicia"

    def test_update_returns_affected(self, populated):
        result = populated.execute("UPDATE users SET name = 'X' WHERE id = 1")
        assert result == {"status": "OK", "affected": 1}

    def test_update_nonexistent_pk(self, populated):
        result = populated.execute("UPDATE users SET name = 'X' WHERE id = 99")
        assert result["affected"] == 0

    def test_update_by_non_pk_col(self, populated):
        # name = 'Alice' matches id=1 only
        populated.execute("UPDATE users SET name = 'Wonderland' WHERE name = 'Alice'")
        rows = populated.execute("SELECT * FROM users WHERE id = 1")
        assert rows[0]["name"] == "Wonderland"


# ── DELETE ────────────────────────────────────────────────────────────

class TestDelete:
    def test_delete_by_pk(self, populated):
        populated.execute("DELETE FROM users WHERE id = 2")
        rows = populated.execute("SELECT * FROM users")
        assert len(rows) == 2
        assert all(r["id"] != 2 for r in rows)

    def test_delete_returns_affected(self, populated):
        result = populated.execute("DELETE FROM users WHERE id = 1")
        assert result == {"status": "OK", "affected": 1}

    def test_delete_nonexistent_returns_zero(self, populated):
        result = populated.execute("DELETE FROM users WHERE id = 99")
        assert result["affected"] == 0

    def test_delete_all_no_where(self, populated):
        populated.execute("DELETE FROM users")
        rows = populated.execute("SELECT * FROM users")
        assert rows == []


# ── Miscellaneous ─────────────────────────────────────────────────────

class TestMisc:
    def test_semicolon_in_sql(self, engine):
        engine.execute("INSERT INTO users VALUES (1, 'Alice');")
        rows = engine.execute("SELECT * FROM users;")
        assert len(rows) == 1

    def test_case_insensitive_sql(self, engine):
        engine.execute("insert into users values (1, 'Alice')")
        rows = engine.execute("select * from users where id = 1")
        assert rows[0]["name"] == "Alice"

    def test_parse_error_raises_sql_error(self, engine):
        with pytest.raises(SQLError):
            engine.execute("INVALID SQL HERE")

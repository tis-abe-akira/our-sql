"""tests/test_engine_disk.py — E2E tests for SQLEngine + disk-backed OurSQLDB."""

import pytest
from oursql.db import OurSQLDB
from oursql.engine import SQLEngine, SQLError


@pytest.fixture
def data_dir(tmp_path):
    return tmp_path / "oursql_disk"


def make_engine(data_dir):
    db = OurSQLDB(data_dir)
    return SQLEngine(db), db


class TestDiskSQLBasic:
    def test_create_and_insert_select(self, data_dir):
        eng, db = make_engine(data_dir)
        eng.execute("CREATE TABLE users (id INT, name TEXT)")
        eng.execute("INSERT INTO users VALUES (1, 'Alice')")
        rows = eng.execute("SELECT * FROM users WHERE id = 1")
        assert rows == [{"id": 1, "name": "Alice"}]
        db.close()

    def test_select_all(self, data_dir):
        eng, db = make_engine(data_dir)
        eng.execute("CREATE TABLE t (id INT, val TEXT)")
        for i in range(5):
            eng.execute(f"INSERT INTO t VALUES ({i}, 'v{i}')")
        rows = eng.execute("SELECT * FROM t")
        assert len(rows) == 5
        db.close()

    def test_delete(self, data_dir):
        eng, db = make_engine(data_dir)
        eng.execute("CREATE TABLE t (id INT, v TEXT)")
        eng.execute("INSERT INTO t VALUES (1, 'a')")
        eng.execute("INSERT INTO t VALUES (2, 'b')")
        eng.execute("DELETE FROM t WHERE id = 1")
        rows = eng.execute("SELECT * FROM t")
        assert len(rows) == 1
        assert rows[0]["id"] == 2
        db.close()


class TestRestartWithSQL:
    """Close DB, reopen, and query via SQL — data must survive."""

    def test_data_survives_restart(self, data_dir):
        # Session 1
        eng1, db1 = make_engine(data_dir)
        eng1.execute("CREATE TABLE users (id INT, name TEXT)")
        eng1.execute("INSERT INTO users VALUES (1, 'Alice')")
        eng1.execute("INSERT INTO users VALUES (2, 'Bob')")
        db1.close()

        # Session 2
        eng2, db2 = make_engine(data_dir)
        rows = eng2.execute("SELECT * FROM users WHERE id = 1")
        assert rows == [{"id": 1, "name": "Alice"}]
        db2.close()

    def test_insert_in_session2_visible(self, data_dir):
        eng1, db1 = make_engine(data_dir)
        eng1.execute("CREATE TABLE t (id INT, v TEXT)")
        eng1.execute("INSERT INTO t VALUES (1, 'first')")
        db1.close()

        eng2, db2 = make_engine(data_dir)
        eng2.execute("INSERT INTO t VALUES (2, 'second')")
        rows = eng2.execute("SELECT * FROM t")
        assert len(rows) == 2
        db2.close()

    def test_delete_persists(self, data_dir):
        eng1, db1 = make_engine(data_dir)
        eng1.execute("CREATE TABLE t (id INT, v TEXT)")
        eng1.execute("INSERT INTO t VALUES (1, 'x')")
        eng1.execute("INSERT INTO t VALUES (2, 'y')")
        eng1.execute("DELETE FROM t WHERE id = 2")
        db1.close()

        eng2, db2 = make_engine(data_dir)
        rows = eng2.execute("SELECT * FROM t")
        assert len(rows) == 1
        assert rows[0]["id"] == 1
        db2.close()

    def test_update_persists(self, data_dir):
        eng1, db1 = make_engine(data_dir)
        eng1.execute("CREATE TABLE t (id INT, v TEXT)")
        eng1.execute("INSERT INTO t VALUES (1, 'old')")
        eng1.execute("UPDATE t SET v = 'new' WHERE id = 1")
        db1.close()

        eng2, db2 = make_engine(data_dir)
        rows = eng2.execute("SELECT * FROM t WHERE id = 1")
        assert rows[0]["v"] == "new"
        db2.close()

    def test_tables_listed_after_restart(self, data_dir):
        eng1, db1 = make_engine(data_dir)
        eng1.execute("CREATE TABLE a (id INT)")
        eng1.execute("CREATE TABLE b (id INT)")
        db1.close()

        _, db2 = make_engine(data_dir)
        assert set(db2.list_tables()) == {"a", "b"}
        db2.close()

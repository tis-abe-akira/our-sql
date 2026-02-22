"""tests/test_engine_extended.py
Tests for Phase 4b/4c features:
  - AND / OR compound WHERE conditions
  - ORDER BY [ASC|DESC]
  - LIMIT
"""

import pytest
from oursql.db import OurSQLDB
from oursql.engine import SQLEngine, SQLError


@pytest.fixture
def engine():
    db = OurSQLDB()
    eng = SQLEngine(db)
    eng.execute("CREATE TABLE t (id INT, score INT, tag TEXT)")
    for i in range(1, 11):
        tag = "even" if i % 2 == 0 else "odd"
        eng.execute(f"INSERT INTO t VALUES ({i}, {i * 10}, '{tag}')")
    return eng


# ── AND / OR WHERE ────────────────────────────────────────────────────

class TestAndOrWhere:
    def test_and_basic(self, engine):
        rows = engine.execute("SELECT * FROM t WHERE id > 2 AND id < 6")
        ids = sorted(r["id"] for r in rows)
        assert ids == [3, 4, 5]

    def test_or_basic(self, engine):
        rows = engine.execute("SELECT * FROM t WHERE id = 1 OR id = 3")
        ids = sorted(r["id"] for r in rows)
        assert ids == [1, 3]

    def test_and_three_conditions(self, engine):
        # id > 2 AND id < 8 AND score >= 60
        rows = engine.execute(
            "SELECT * FROM t WHERE id > 2 AND id < 8 AND score >= 60"
        )
        ids = sorted(r["id"] for r in rows)
        assert ids == [6, 7]

    def test_or_then_and(self, engine):
        # (id = 1) OR (id > 5 AND id < 8)  →  [1, 6, 7]
        rows = engine.execute(
            "SELECT * FROM t WHERE id = 1 OR id > 5 AND id < 8"
        )
        ids = sorted(r["id"] for r in rows)
        # AND binds tighter: id=1 OR (id>5 AND id<8)
        assert ids == [1, 6, 7]

    def test_and_with_string_column(self, engine):
        rows = engine.execute("SELECT * FROM t WHERE tag = 'even' AND id <= 6")
        ids = sorted(r["id"] for r in rows)
        assert ids == [2, 4, 6]

    def test_or_with_string_column(self, engine):
        rows = engine.execute("SELECT * FROM t WHERE id = 1 OR tag = 'even'")
        ids = sorted(r["id"] for r in rows)
        assert ids == [1, 2, 4, 6, 8, 10]

    def test_update_with_and(self, engine):
        engine.execute("UPDATE t SET tag = 'special' WHERE id > 3 AND id < 6")
        rows = engine.execute("SELECT * FROM t WHERE tag = 'special'")
        ids = sorted(r["id"] for r in rows)
        assert ids == [4, 5]

    def test_delete_with_or(self, engine):
        engine.execute("DELETE FROM t WHERE id = 1 OR id = 10")
        rows = engine.execute("SELECT * FROM t")
        assert all(r["id"] not in (1, 10) for r in rows)
        assert len(rows) == 8


# ── ORDER BY ─────────────────────────────────────────────────────────

class TestOrderBy:
    def test_order_asc(self, engine):
        rows = engine.execute("SELECT * FROM t ORDER BY id ASC")
        ids = [r["id"] for r in rows]
        assert ids == sorted(ids)

    def test_order_desc(self, engine):
        rows = engine.execute("SELECT * FROM t ORDER BY id DESC")
        ids = [r["id"] for r in rows]
        assert ids == sorted(ids, reverse=True)

    def test_order_default_is_asc(self, engine):
        rows = engine.execute("SELECT * FROM t ORDER BY score")
        scores = [r["score"] for r in rows]
        assert scores == sorted(scores)

    def test_order_by_non_pk(self, engine):
        rows = engine.execute("SELECT * FROM t ORDER BY score DESC")
        scores = [r["score"] for r in rows]
        assert scores == sorted(scores, reverse=True)

    def test_order_with_where(self, engine):
        rows = engine.execute("SELECT * FROM t WHERE id <= 5 ORDER BY id DESC")
        ids = [r["id"] for r in rows]
        assert ids == [5, 4, 3, 2, 1]


# ── LIMIT ────────────────────────────────────────────────────────────

class TestLimit:
    def test_limit_basic(self, engine):
        rows = engine.execute("SELECT * FROM t LIMIT 3")
        assert len(rows) == 3

    def test_limit_zero(self, engine):
        rows = engine.execute("SELECT * FROM t LIMIT 0")
        assert rows == []

    def test_limit_larger_than_result(self, engine):
        rows = engine.execute("SELECT * FROM t LIMIT 100")
        assert len(rows) == 10

    def test_limit_with_order(self, engine):
        rows = engine.execute("SELECT * FROM t ORDER BY id DESC LIMIT 3")
        ids = [r["id"] for r in rows]
        assert ids == [10, 9, 8]

    def test_limit_with_where(self, engine):
        rows = engine.execute("SELECT * FROM t WHERE id > 5 ORDER BY id ASC LIMIT 2")
        ids = [r["id"] for r in rows]
        assert ids == [6, 7]


# ── Combined ──────────────────────────────────────────────────────────

class TestCombined:
    def test_where_and_order_limit(self, engine):
        rows = engine.execute(
            "SELECT * FROM t WHERE tag = 'even' ORDER BY id DESC LIMIT 3"
        )
        ids = [r["id"] for r in rows]
        assert ids == [10, 8, 6]

    def test_where_or_order_asc_limit(self, engine):
        rows = engine.execute(
            "SELECT * FROM t WHERE id < 3 OR id > 8 ORDER BY id ASC LIMIT 4"
        )
        ids = [r["id"] for r in rows]
        assert ids == [1, 2, 9, 10]

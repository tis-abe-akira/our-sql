"""tests/test_heap_file.py — Unit tests for HeapFile."""

import pytest
from oursql.heap_file import HeapFile


@pytest.fixture
def hf(tmp_path):
    f = HeapFile(tmp_path / "heap.db")
    yield f
    f.close()


class TestInsertGet:
    def test_insert_returns_rid(self, hf):
        rid = hf.insert({"id": 1, "name": "Alice"})
        assert isinstance(rid, tuple)
        assert len(rid) == 2

    def test_get_existing(self, hf):
        rid = hf.insert({"id": 1, "name": "Alice"})
        row = hf.get(*rid)
        assert row == {"id": 1, "name": "Alice"}

    def test_get_invalid_page(self, hf):
        assert hf.get(99, 0) is None

    def test_multiple_inserts(self, hf):
        rids = []
        for i in range(10):
            rids.append(hf.insert({"id": i, "name": f"User{i}"}))
        for i, rid in enumerate(rids):
            assert hf.get(*rid)["id"] == i


class TestUpdate:
    def test_update_shorter_value(self, hf):
        rid = hf.insert({"id": 1, "name": "Alice"})
        hf.update(*rid, {"id": 1, "name": "Al"})
        assert hf.get(*rid)["name"] == "Al"

    def test_update_larger_raises(self, hf):
        rid = hf.insert({"id": 1, "name": "Al"})
        with pytest.raises(ValueError):
            hf.update(*rid, {"id": 1, "name": "A" * 1000})

    def test_update_deleted_raises(self, hf):
        rid = hf.insert({"id": 1})
        hf.delete(*rid)
        with pytest.raises(KeyError):
            hf.update(*rid, {"id": 1})


class TestDelete:
    def test_delete_sets_tombstone(self, hf):
        rid = hf.insert({"id": 1})
        hf.delete(*rid)
        assert hf.get(*rid) is None

    def test_delete_twice_raises(self, hf):
        rid = hf.insert({"id": 1})
        hf.delete(*rid)
        with pytest.raises(KeyError):
            hf.delete(*rid)


class TestScan:
    def test_scan_all_live(self, hf):
        for i in range(5):
            hf.insert({"id": i})
        assert len(hf.scan()) == 5

    def test_scan_excludes_deleted(self, hf):
        hf.insert({"id": 1})
        rid2 = hf.insert({"id": 2})
        hf.insert({"id": 3})
        hf.delete(*rid2)
        rows = hf.scan()
        assert len(rows) == 2
        assert {r["id"] for r in rows} == {1, 3}

    def test_scan_multi_page(self, hf):
        """Insert enough rows to span multiple pages."""
        # Each row ~50 bytes → ~80 per page → need >80 rows for 2 pages
        for i in range(200):
            hf.insert({"id": i, "val": "x" * 30})
        assert len(hf.scan()) == 200


class TestPersistence:
    def test_data_survives_reopen(self, tmp_path):
        path = tmp_path / "heap.db"
        with HeapFile(path) as hf:
            rid = hf.insert({"id": 42, "name": "Alice"})
        with HeapFile(path) as hf2:
            assert hf2.get(*rid) == {"id": 42, "name": "Alice"}

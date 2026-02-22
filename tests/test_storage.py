"""tests/test_storage.py — Unit tests for HeapStorage."""

import pytest
from oursql.storage import HeapStorage


class TestInsert:
    def test_insert_returns_row_id_zero(self):
        s = HeapStorage()
        rid = s.insert({"id": 1, "name": "Alice"})
        assert rid == 0

    def test_insert_increments_row_id(self):
        s = HeapStorage()
        rid0 = s.insert({"id": 1})
        rid1 = s.insert({"id": 2})
        rid2 = s.insert({"id": 3})
        assert rid0 == 0
        assert rid1 == 1
        assert rid2 == 2

    def test_insert_stores_copy(self):
        s = HeapStorage()
        row = {"id": 1, "name": "Alice"}
        rid = s.insert(row)
        row["name"] = "Modified"  # mutate original
        assert s.get(rid)["name"] == "Alice"  # storage unaffected


class TestGet:
    def test_get_existing_row(self):
        s = HeapStorage()
        rid = s.insert({"id": 42, "val": "hello"})
        assert s.get(rid) == {"id": 42, "val": "hello"}

    def test_get_out_of_range(self):
        s = HeapStorage()
        assert s.get(99) is None

    def test_get_negative_index(self):
        s = HeapStorage()
        assert s.get(-1) is None


class TestUpdate:
    def test_update_changes_value(self):
        s = HeapStorage()
        rid = s.insert({"id": 1, "name": "Alice"})
        s.update(rid, {"id": 1, "name": "Alicia"})
        assert s.get(rid)["name"] == "Alicia"

    def test_update_deleted_raises(self):
        s = HeapStorage()
        rid = s.insert({"id": 1})
        s.delete(rid)
        with pytest.raises(KeyError):
            s.update(rid, {"id": 1})

    def test_update_invalid_rid_raises(self):
        s = HeapStorage()
        with pytest.raises(KeyError):
            s.update(99, {"id": 99})


class TestDelete:
    def test_delete_sets_tombstone(self):
        s = HeapStorage()
        rid = s.insert({"id": 1})
        s.delete(rid)
        assert s.get(rid) is None

    def test_delete_twice_raises(self):
        s = HeapStorage()
        rid = s.insert({"id": 1})
        s.delete(rid)
        with pytest.raises(KeyError):
            s.delete(rid)

    def test_delete_invalid_rid_raises(self):
        s = HeapStorage()
        with pytest.raises(KeyError):
            s.delete(99)


class TestScan:
    def test_scan_returns_all_live_rows(self):
        s = HeapStorage()
        s.insert({"id": 1})
        s.insert({"id": 2})
        s.insert({"id": 3})
        rows = s.scan()
        assert len(rows) == 3

    def test_scan_excludes_tombstones(self):
        s = HeapStorage()
        s.insert({"id": 1})
        rid2 = s.insert({"id": 2})
        s.insert({"id": 3})
        s.delete(rid2)
        rows = s.scan()
        assert len(rows) == 2
        ids = {r["id"] for r in rows}
        assert ids == {1, 3}

    def test_scan_empty(self):
        s = HeapStorage()
        assert s.scan() == []


class TestLen:
    def test_len_includes_tombstones(self):
        s = HeapStorage()
        s.insert({"id": 1})
        s.insert({"id": 2})
        rid = s.insert({"id": 3})
        s.delete(rid)
        assert len(s) == 3  # 2 live + 1 tombstone

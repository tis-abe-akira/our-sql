"""tests/test_page_btree.py — Unit tests for PageBTree."""

import pytest
from oursql.page_btree import PageBTree


@pytest.fixture
def bt(tmp_path):
    t = PageBTree(tmp_path / "pk.idx", order=4)
    yield t
    t.close()


class TestInsertSearch:
    def test_single_insert_search(self, bt):
        bt.insert(10, (0, 0))
        assert bt.search(10) == (0, 0)

    def test_missing_key_returns_none(self, bt):
        bt.insert(5, (0, 0))
        assert bt.search(99) is None

    def test_multiple_inserts(self, bt):
        for k in [3, 7, 1, 9, 5, 2, 8, 4, 6]:
            bt.insert(k, (0, k))
        for k in range(1, 10):
            assert bt.search(k) == (0, k)

    def test_large_insert(self, tmp_path):
        bt = PageBTree(tmp_path / "large.idx", order=4)
        n = 100
        for k in range(n):
            bt.insert(k, (0, k))
        for k in range(n):
            assert bt.search(k) == (0, k)
        bt.close()

    def test_descending_insert(self, bt):
        for k in reversed(range(1, 15)):
            bt.insert(k, (k, 0))
        for k in range(1, 15):
            assert bt.search(k) == (k, 0)


class TestDelete:
    def test_delete_existing(self, bt):
        bt.insert(5, (0, 5))
        assert bt.delete(5) is True
        assert bt.search(5) is None

    def test_delete_nonexistent(self, bt):
        bt.insert(1, (0, 1))
        assert bt.delete(99) is False

    def test_delete_all(self, bt):
        for k in range(1, 8):
            bt.insert(k, (0, k))
        for k in range(1, 8):
            assert bt.delete(k) is True
        for k in range(1, 8):
            assert bt.search(k) is None

    def test_delete_multiple(self, bt):
        for k in range(1, 10):
            bt.insert(k, (0, k))
        bt.delete(3)
        bt.delete(7)
        assert bt.search(3) is None
        assert bt.search(7) is None
        for k in [1, 2, 4, 5, 6, 8, 9]:
            assert bt.search(k) == (0, k)


class TestRangeScan:
    def test_range_basic(self, bt):
        for k in range(1, 11):
            bt.insert(k, (0, k))
        rids = bt.range_scan(3, 7)
        assert sorted(r[1] for r in rids) == [3, 4, 5, 6, 7]

    def test_range_no_match(self, bt):
        for k in [1, 2, 3]:
            bt.insert(k, (0, k))
        assert bt.range_scan(10, 20) == []

    def test_range_single(self, bt):
        for k in range(1, 6):
            bt.insert(k, (0, k))
        assert bt.range_scan(3, 3) == [(0, 3)]


class TestPersistence:
    def test_data_survives_reopen(self, tmp_path):
        path = tmp_path / "pk.idx"
        bt = PageBTree(path)
        bt.insert(42, (0, 7))
        bt.close()

        bt2 = PageBTree(path)
        assert bt2.search(42) == (0, 7)
        bt2.close()

    def test_many_keys_survive_reopen(self, tmp_path):
        path = tmp_path / "pk.idx"
        bt = PageBTree(path)
        for k in range(50):
            bt.insert(k, (0, k))
        bt.close()

        bt2 = PageBTree(path)
        for k in range(50):
            assert bt2.search(k) == (0, k)
        bt2.close()

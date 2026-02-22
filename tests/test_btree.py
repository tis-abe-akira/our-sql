"""tests/test_btree.py — Unit tests for BPlusTree."""

import pytest
from oursql.btree import BPlusTree, BTreeNode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_tree(order: int = 4) -> BPlusTree:
    return BPlusTree(order=order)


def insert_many(tree: BPlusTree, pairs: list[tuple]) -> None:
    for key, val in pairs:
        tree.insert(key, val)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestConstruction:
    def test_default_order(self):
        t = BPlusTree()
        assert t.order == 4

    def test_custom_order(self):
        t = BPlusTree(order=2)
        assert t.order == 2

    def test_invalid_order(self):
        with pytest.raises(ValueError):
            BPlusTree(order=1)

    def test_empty_tree_root_is_leaf(self):
        t = make_tree()
        assert t.root.is_leaf


# ---------------------------------------------------------------------------
# Insert & Search (basic)
# ---------------------------------------------------------------------------

class TestInsertSearch:
    def test_single_insert_search(self):
        t = make_tree()
        t.insert(10, "row10")
        assert t.search(10) == "row10"

    def test_missing_key_returns_none(self):
        t = make_tree()
        t.insert(5, "row5")
        assert t.search(99) is None

    def test_multiple_inserts_sorted_retrieval(self):
        t = make_tree()
        keys = [3, 7, 1, 9, 5, 2, 8, 4, 6]
        for k in keys:
            t.insert(k, f"row{k}")
        for k in keys:
            assert t.search(k) == f"row{k}"

    def test_all_items_sorted(self):
        t = make_tree()
        keys = [5, 2, 8, 1, 9, 3]
        for k in keys:
            t.insert(k, k * 10)
        items = t.all_items()
        assert [k for k, _ in items] == sorted(keys)

    def test_search_empty_tree(self):
        t = make_tree()
        assert t.search(1) is None


# ---------------------------------------------------------------------------
# Split behaviour
# ---------------------------------------------------------------------------

class TestSplit:
    """With order=2, max keys per node = 2*2-1 = 3. Split triggers at 4th key."""

    def test_root_split_creates_new_root(self):
        t = BPlusTree(order=2)
        for k in [1, 2, 3, 4]:
            t.insert(k, k)
        # Root should no longer be a leaf
        assert not t.root.is_leaf

    def test_values_still_correct_after_split(self):
        t = BPlusTree(order=2)
        for k in [10, 20, 30, 40, 50]:
            t.insert(k, k * 2)
        for k in [10, 20, 30, 40, 50]:
            assert t.search(k) == k * 2

    def test_large_insert_all_findable(self):
        t = BPlusTree(order=3)
        n = 50
        for k in range(n):
            t.insert(k, k + 1000)
        for k in range(n):
            assert t.search(k) == k + 1000

    def test_descending_insert(self):
        t = BPlusTree(order=2)
        for k in reversed(range(1, 10)):
            t.insert(k, k)
        for k in range(1, 10):
            assert t.search(k) == k


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------

class TestDelete:
    def test_delete_only_key(self):
        t = make_tree()
        t.insert(5, "five")
        assert t.delete(5) is True
        assert t.search(5) is None

    def test_delete_nonexistent_returns_false(self):
        t = make_tree()
        t.insert(1, "one")
        assert t.delete(99) is False

    def test_delete_from_empty(self):
        t = make_tree()
        assert t.delete(1) is False

    def test_delete_all_keys(self):
        t = BPlusTree(order=2)
        keys = list(range(1, 8))
        for k in keys:
            t.insert(k, k)
        for k in keys:
            assert t.delete(k) is True
        for k in keys:
            assert t.search(k) is None

    def test_delete_multiple_leaves(self):
        t = BPlusTree(order=2)
        for k in range(1, 10):
            t.insert(k, k * 10)
        t.delete(3)
        t.delete(7)
        assert t.search(3) is None
        assert t.search(7) is None
        # Others still present
        for k in [1, 2, 4, 5, 6, 8, 9]:
            assert t.search(k) == k * 10

    def test_delete_triggers_borrow_and_search_still_works(self):
        t = BPlusTree(order=2)
        for k in [10, 20, 30, 40, 50]:
            t.insert(k, k)
        t.delete(20)
        for k in [10, 30, 40, 50]:
            assert t.search(k) == k


# ---------------------------------------------------------------------------
# Range Scan
# ---------------------------------------------------------------------------

class TestRangeScan:
    def test_full_range(self):
        t = make_tree()
        for k in range(1, 6):
            t.insert(k, k * 100)
        result = t.range_scan(1, 5)
        assert sorted(result) == [100, 200, 300, 400, 500]

    def test_partial_range(self):
        t = make_tree()
        for k in range(1, 11):
            t.insert(k, k)
        result = t.range_scan(3, 7)
        assert sorted(result) == [3, 4, 5, 6, 7]

    def test_single_element_range(self):
        t = make_tree()
        for k in range(1, 6):
            t.insert(k, k)
        assert t.range_scan(3, 3) == [3]

    def test_range_no_match(self):
        t = make_tree()
        for k in [1, 2, 3]:
            t.insert(k, k)
        assert t.range_scan(10, 20) == []

    def test_range_after_splits(self):
        t = BPlusTree(order=2)
        for k in range(1, 20):
            t.insert(k, k)
        result = t.range_scan(5, 10)
        assert sorted(result) == list(range(5, 11))

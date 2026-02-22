"""
oursql/page_btree.py
PageBTree: disk-backed B+Tree where each node lives in one Pager page.

Node page layout (4096 bytes):
  [0:1]   is_leaf    (uint8,  0=internal 1=leaf)
  [1:3]   num_keys   (uint16)
  [3:7]   next_page  (int32, -1=none; leaf linked-list pointer)
  [7 …]   entries    — fixed-size records

Leaf entry  (16 bytes): key(8) + page_id(4) + slot_id(4)
Internal entry (12 bytes): key(8) + child_page_id(4)
  Internal node layout: child0 | key0 | child1 | key1 | ... | child_n

Only integer (int64) keys are supported in Phase 2.

The first page (page_id=0) is always the root.
Root page_id is tracked in a tiny 4-byte header page at page_id=0:
  Actually we keep it simpler: page 0 is always logically the root.
  When the root splits we write a new root page and update page 0.

Implementation note:
  We store the root page_id in a 1-page "meta" file companion,
  but for simplicity page 0 = root always; on root split we
  write the new root content INTO page 0 and give the old root
  a new page_id.
"""

from __future__ import annotations
import struct
from pathlib import Path
from typing import Any

from oursql.pager import Pager

# ── Page binary layout constants ──────────────────────────────────────
_HDR_FMT   = struct.Struct("<BHi")       # is_leaf(1) num_keys(2) next_page(4)
_HDR_SIZE  = _HDR_FMT.size              # 7 bytes
_LEAF_ENT  = struct.Struct("<qii")       # key(8) page_id(4) slot_id(4)  = 16
_INT_ENT   = struct.Struct("<qi")        # key(8) child_pid(4)            = 12
_CHILD_PTR = struct.Struct("<i")         # child_pid(4)                   = 4
_NO_PAGE   = -1

# Maximum entries per page (conservative: leave 16 bytes header slack)
_PAGE_SIZE  = Pager.PAGE_SIZE
_MAX_LEAF   = (_PAGE_SIZE - _HDR_SIZE) // _LEAF_ENT.size       # ~255 for 4 KB
_MAX_INT    = (_PAGE_SIZE - _HDR_SIZE - _CHILD_PTR.size) // (_INT_ENT.size + _CHILD_PTR.size)

RID = tuple[int, int]   # (page_id, slot_id) in HeapFile


# ── Node (in-memory representation) ──────────────────────────────────

class _Node:
    __slots__ = ("page_id", "is_leaf", "keys", "rids", "children", "next_page")

    def __init__(self, page_id: int, is_leaf: bool) -> None:
        self.page_id   = page_id
        self.is_leaf   = is_leaf
        self.keys      : list[int]   = []
        self.rids      : list[RID]   = []   # leaf only
        self.children  : list[int]   = []   # internal: list of child page_ids
        self.next_page : int         = _NO_PAGE   # leaf linked-list

    def is_full(self, order: int) -> bool:
        limit = _MAX_LEAF if self.is_leaf else _MAX_INT
        return len(self.keys) >= limit


# ── PageBTree ─────────────────────────────────────────────────────────

class PageBTree:
    """Disk-backed B+Tree for integer primary keys."""

    def __init__(self, filepath: str | Path, order: int = 4) -> None:
        self._pager = Pager(filepath)
        self.order   = order
        # Bootstrap: allocate root page if file is new
        if self._pager.num_pages() == 0:
            root = _Node(page_id=0, is_leaf=True)
            self._write_node(root)

    # ── Public API ────────────────────────────────────────────────────

    def insert(self, key: int, rid: RID) -> None:
        root = self._read_node(0)
        if self._node_full(root):
            # Split root: move old root to a new page, create new root at page 0
            old_root_pid = self._pager.allocate_page()
            old_root     = self._read_node(0)
            old_root.page_id = old_root_pid
            self._write_node(old_root)

            new_root = _Node(page_id=0, is_leaf=False)
            new_root.children.append(old_root_pid)
            self._write_node(new_root)

            self._split_child(new_root, 0)
            self._insert_non_full(new_root, key, rid)
        else:
            self._insert_non_full(root, key, rid)

    def search(self, key: int) -> RID | None:
        leaf = self._find_leaf(0, key)
        for i, k in enumerate(leaf.keys):
            if k == key:
                return leaf.rids[i]
        return None

    def delete(self, key: int) -> bool:
        found = self._delete(0, key, parent_pid=None, parent_idx=None)
        # Shrink root if needed
        root = self._read_node(0)
        if not root.is_leaf and len(root.keys) == 0 and root.children:
            child_pid = root.children[0]
            child     = self._read_node(child_pid)
            child.page_id = 0
            self._write_node(child)
        return found

    def range_scan(self, start: int, end: int) -> list[RID]:
        leaf = self._find_leaf(0, start)
        result: list[RID] = []
        while leaf is not None:
            for i, k in enumerate(leaf.keys):
                if k > end:
                    return result
                if k >= start:
                    result.append(leaf.rids[i])
            nxt = leaf.next_page
            leaf = self._read_node(nxt) if nxt != _NO_PAGE else None
        return result

    def close(self) -> None:
        self._pager.close()

    # ── Serialisation ─────────────────────────────────────────────────

    def _read_node(self, page_id: int) -> _Node:
        page = self._pager.read_page(page_id)
        is_leaf_b, num_keys, next_page = _HDR_FMT.unpack_from(page, 0)
        node = _Node(page_id=page_id, is_leaf=bool(is_leaf_b))
        node.next_page = next_page
        off = _HDR_SIZE

        if node.is_leaf:
            for _ in range(num_keys):
                k, p, s = _LEAF_ENT.unpack_from(page, off)
                node.keys.append(k)
                node.rids.append((p, s))
                off += _LEAF_ENT.size
        else:
            # Read first child pointer
            (child_pid,) = _CHILD_PTR.unpack_from(page, off)
            node.children.append(child_pid)
            off += _CHILD_PTR.size
            for _ in range(num_keys):
                k, child_pid = _INT_ENT.unpack_from(page, off)
                node.keys.append(k)
                node.children.append(child_pid)
                off += _INT_ENT.size

        return node

    def _write_node(self, node: _Node) -> None:
        page = bytearray(self._pager.page_size)
        _HDR_FMT.pack_into(page, 0, int(node.is_leaf), len(node.keys), node.next_page)
        off = _HDR_SIZE

        if node.is_leaf:
            for k, (p, s) in zip(node.keys, node.rids):
                _LEAF_ENT.pack_into(page, off, k, p, s)
                off += _LEAF_ENT.size
        else:
            _CHILD_PTR.pack_into(page, off, node.children[0])
            off += _CHILD_PTR.size
            for k, child_pid in zip(node.keys, node.children[1:]):
                _INT_ENT.pack_into(page, off, k, child_pid)
                off += _INT_ENT.size

        # If the page already exists, overwrite; otherwise allocate up to it
        while self._pager.num_pages() <= node.page_id:
            self._pager.allocate_page()
        self._pager.write_page(node.page_id, bytes(page))

    # ── Insert helpers ────────────────────────────────────────────────

    def _node_full(self, node: _Node) -> bool:
        limit = _MAX_LEAF if node.is_leaf else _MAX_INT
        return len(node.keys) >= limit

    def _insert_non_full(self, node: _Node, key: int, rid: RID) -> None:
        if node.is_leaf:
            i = len(node.keys)
            node.keys.append(0)
            node.rids.append((0, 0))
            while i > 0 and key < node.keys[i - 1]:
                node.keys[i] = node.keys[i - 1]
                node.rids[i] = node.rids[i - 1]
                i -= 1
            node.keys[i] = key
            node.rids[i] = rid
            self._write_node(node)
        else:
            i = 0
            while i < len(node.keys) and key >= node.keys[i]:
                i += 1
            child = self._read_node(node.children[i])
            if self._node_full(child):
                self._split_child(node, i)
                if key > node.keys[i]:
                    i += 1
                child = self._read_node(node.children[i])
            self._insert_non_full(child, key, rid)

    def _split_child(self, parent: _Node, child_idx: int) -> None:
        child_pid = parent.children[child_idx]
        child     = self._read_node(child_pid)
        new_pid   = self._pager.allocate_page()
        new_node  = _Node(page_id=new_pid, is_leaf=child.is_leaf)
        mid       = len(child.keys) // 2

        if child.is_leaf:
            new_node.keys     = child.keys[mid:]
            new_node.rids     = child.rids[mid:]
            new_node.next_page = child.next_page
            child.keys        = child.keys[:mid]
            child.rids        = child.rids[:mid]
            child.next_page   = new_pid
            push_up_key       = new_node.keys[0]
        else:
            push_up_key       = child.keys[mid]
            new_node.keys     = child.keys[mid + 1:]
            new_node.children = child.children[mid + 1:]
            child.keys        = child.keys[:mid]
            child.children    = child.children[:mid + 1]

        self._write_node(child)
        self._write_node(new_node)

        parent.keys.insert(child_idx, push_up_key)
        parent.children.insert(child_idx + 1, new_pid)
        self._write_node(parent)

    # ── Search helpers ────────────────────────────────────────────────

    def _find_leaf(self, page_id: int, key: int) -> _Node:
        node = self._read_node(page_id)
        if node.is_leaf:
            return node
        i = 0
        while i < len(node.keys) and key >= node.keys[i]:
            i += 1
        return self._find_leaf(node.children[i], key)

    # ── Delete helpers ────────────────────────────────────────────────

    def _delete(
        self,
        page_id: int,
        key: int,
        parent_pid: int | None,
        parent_idx: int | None,
    ) -> bool:
        node = self._read_node(page_id)
        min_keys = max(1, _MAX_LEAF // 2) if node.is_leaf else max(1, _MAX_INT // 2)

        if node.is_leaf:
            try:
                i = node.keys.index(key)
            except ValueError:
                return False
            node.keys.pop(i)
            node.rids.pop(i)
            self._write_node(node)
            return True

        i = 0
        while i < len(node.keys) and key >= node.keys[i]:
            i += 1
        child_pid = node.children[i]
        child     = self._read_node(child_pid)

        if len(child.keys) <= min_keys:
            self._fix_child_page(node, i)
            # Re-read after possible merge/borrow
            node  = self._read_node(page_id)
            i = 0
            while i < len(node.keys) and key >= node.keys[i]:
                i += 1
            child_pid = node.children[i]

        return self._delete(child_pid, key, page_id, i)

    def _fix_child_page(self, parent: _Node, idx: int) -> None:
        child_pid = parent.children[idx]
        child     = self._read_node(child_pid)
        min_keys  = max(1, (_MAX_LEAF if child.is_leaf else _MAX_INT) // 2)

        left_pid  = parent.children[idx - 1] if idx > 0 else None
        right_pid = parent.children[idx + 1] if idx < len(parent.children) - 1 else None
        left      = self._read_node(left_pid) if left_pid is not None else None
        right     = self._read_node(right_pid) if right_pid is not None else None

        if left and len(left.keys) > min_keys:
            self._borrow_left_page(parent, idx, child, left)
        elif right and len(right.keys) > min_keys:
            self._borrow_right_page(parent, idx, child, right)
        elif left:
            self._merge_pages(parent, idx - 1, left, child)
        else:
            self._merge_pages(parent, idx, child, right)

    def _borrow_left_page(
        self, parent: _Node, idx: int, child: _Node, left: _Node
    ) -> None:
        if child.is_leaf:
            child.keys.insert(0, left.keys.pop())
            child.rids.insert(0, left.rids.pop())
            parent.keys[idx - 1] = child.keys[0]
        else:
            child.keys.insert(0, parent.keys[idx - 1])
            parent.keys[idx - 1] = left.keys.pop()
            child.children.insert(0, left.children.pop())
        self._write_node(left)
        self._write_node(child)
        self._write_node(parent)

    def _borrow_right_page(
        self, parent: _Node, idx: int, child: _Node, right: _Node
    ) -> None:
        if child.is_leaf:
            child.keys.append(right.keys.pop(0))
            child.rids.append(right.rids.pop(0))
            parent.keys[idx] = right.keys[0]
        else:
            child.keys.append(parent.keys[idx])
            parent.keys[idx] = right.keys.pop(0)
            child.children.append(right.children.pop(0))
        self._write_node(right)
        self._write_node(child)
        self._write_node(parent)

    def _merge_pages(
        self, parent: _Node, left_idx: int, left: _Node, right: _Node
    ) -> None:
        if left.is_leaf:
            left.keys.extend(right.keys)
            left.rids.extend(right.rids)
            left.next_page = right.next_page
        else:
            left.keys.append(parent.keys[left_idx])
            left.keys.extend(right.keys)
            left.children.extend(right.children)

        parent.keys.pop(left_idx)
        parent.children.pop(left_idx + 1)
        self._write_node(left)
        self._write_node(parent)

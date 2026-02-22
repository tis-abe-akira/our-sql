"""
oursql/btree.py
B+Tree implementation for OurSQL primary key index.

Terminology:
  order (t): minimum degree. Each non-root node has at least t-1 keys
  and at most 2t-1 keys. A node is "full" when it has 2t-1 keys.

Leaf nodes store (key, row_id) pairs and are linked in a singly-linked
list for efficient range scans.

Internal nodes store separator keys and child pointers but no row_ids.
"""

from __future__ import annotations
from typing import Any


class BTreeNode:
    """A single node in the B+Tree."""

    def __init__(self, is_leaf: bool = False) -> None:
        self.is_leaf: bool = is_leaf
        self.keys: list[Any] = []
        # leaf  → row_ids corresponding to each key
        # internal → NOT used (child pointers are in self.children)
        self.values: list[Any] = []
        # internal → child BTreeNode references (len == len(keys) + 1)
        self.children: list["BTreeNode"] = []
        # leaf → pointer to next leaf node (for range scans)
        self.next: "BTreeNode | None" = None

    def is_full(self, order: int) -> bool:
        return len(self.keys) >= 2 * order - 1

    def __repr__(self) -> str:  # pragma: no cover
        kind = "Leaf" if self.is_leaf else "Internal"
        return f"{kind}({self.keys})"


class BPlusTree:
    """
    B+Tree with configurable order (minimum degree t).

    - Leaves hold actual (key, row_id) pairs.
    - Internal nodes hold separator keys and child pointers only.
    - All leaves are connected via a singly-linked list (self.next).
    """

    def __init__(self, order: int = 4) -> None:
        if order < 2:
            raise ValueError("order must be >= 2")
        self.order = order          # minimum degree t
        self.root: BTreeNode = BTreeNode(is_leaf=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def insert(self, key: Any, row_id: Any) -> None:
        """Insert (key, row_id) into the tree. Splits nodes as needed."""
        root = self.root
        if root.is_full(self.order):
            # Root is full → create a new root and split old root
            new_root = BTreeNode(is_leaf=False)
            new_root.children.append(self.root)
            self._split_child(new_root, 0)
            self.root = new_root
        self._insert_non_full(self.root, key, row_id)

    def search(self, key: Any) -> Any | None:
        """Return the row_id for key, or None if not found."""
        leaf = self._find_leaf(self.root, key)
        for i, k in enumerate(leaf.keys):
            if k == key:
                return leaf.values[i]
        return None

    def delete(self, key: Any) -> bool:
        """
        Delete key from the tree.
        Returns True if the key was found and deleted, False otherwise.
        Uses borrow-from-sibling or merge to fix underflows.
        """
        found = self._delete(self.root, key, parent=None, parent_idx=None)
        # If root became empty internal node after merge, shrink the tree
        if not self.root.is_leaf and len(self.root.keys) == 0:
            self.root = self.root.children[0]
        return found

    def range_scan(self, start_key: Any, end_key: Any) -> list[Any]:
        """
        Return list of row_ids for all keys in [start_key, end_key].
        Traverses the leaf linked-list for efficiency.
        """
        result: list[Any] = []
        leaf = self._find_leaf(self.root, start_key)
        while leaf is not None:
            for i, k in enumerate(leaf.keys):
                if k > end_key:
                    return result
                if k >= start_key:
                    result.append(leaf.values[i])
            leaf = leaf.next
        return result

    def all_items(self) -> list[tuple[Any, Any]]:
        """Return all (key, row_id) pairs in sorted order (leaf scan)."""
        items: list[tuple[Any, Any]] = []
        node = self.root
        while not node.is_leaf:
            node = node.children[0]
        while node is not None:
            for k, v in zip(node.keys, node.values):
                items.append((k, v))
            node = node.next
        return items

    # ------------------------------------------------------------------
    # Internal helpers — insert
    # ------------------------------------------------------------------

    def _insert_non_full(self, node: BTreeNode, key: Any, row_id: Any) -> None:
        if node.is_leaf:
            # Insert into leaf in sorted order
            i = len(node.keys) - 1
            node.keys.append(None)
            node.values.append(None)
            while i >= 0 and key < node.keys[i]:
                node.keys[i + 1] = node.keys[i]
                node.values[i + 1] = node.values[i]
                i -= 1
            node.keys[i + 1] = key
            node.values[i + 1] = row_id
        else:
            # Find the child to descend into
            i = len(node.keys) - 1
            while i >= 0 and key < node.keys[i]:
                i -= 1
            i += 1
            if node.children[i].is_full(self.order):
                self._split_child(node, i)
                if key > node.keys[i]:
                    i += 1
            self._insert_non_full(node.children[i], key, row_id)

    def _split_child(self, parent: BTreeNode, child_idx: int) -> None:
        """
        Split parent.children[child_idx] (which is full) into two nodes.
        The median key is pushed up into parent.
        """
        t = self.order
        child = parent.children[child_idx]
        new_node = BTreeNode(is_leaf=child.is_leaf)
        mid = t - 1  # index of the median key

        if child.is_leaf:
            # For leaves: copy mid key UP (B+Tree: keep in leaf too)
            new_node.keys = child.keys[mid:]
            new_node.values = child.values[mid:]
            child.keys = child.keys[:mid]
            child.values = child.values[:mid]
            # Maintain leaf linked-list
            new_node.next = child.next
            child.next = new_node
            # Push up the first key of the new (right) leaf
            push_up_key = new_node.keys[0]
        else:
            # For internal nodes: median key moves up, not copied
            push_up_key = child.keys[mid]
            new_node.keys = child.keys[mid + 1:]
            new_node.children = child.children[mid + 1:]
            child.keys = child.keys[:mid]
            child.children = child.children[:mid + 1]

        # Insert median key and new child into parent
        parent.keys.insert(child_idx, push_up_key)
        parent.children.insert(child_idx + 1, new_node)

    # ------------------------------------------------------------------
    # Internal helpers — search / leaf navigation
    # ------------------------------------------------------------------

    def _find_leaf(self, node: BTreeNode, key: Any) -> BTreeNode:
        """Descend the tree to find the leaf node that should contain key."""
        if node.is_leaf:
            return node
        i = 0
        while i < len(node.keys) and key >= node.keys[i]:
            i += 1
        return self._find_leaf(node.children[i], key)

    # ------------------------------------------------------------------
    # Internal helpers — delete
    # ------------------------------------------------------------------

    def _delete(
        self,
        node: BTreeNode,
        key: Any,
        parent: BTreeNode | None,
        parent_idx: int | None,
    ) -> bool:
        t = self.order
        min_keys = t - 1  # minimum keys for non-root node

        if node.is_leaf:
            # Try to find and remove the key
            try:
                i = node.keys.index(key)
            except ValueError:
                return False
            node.keys.pop(i)
            node.values.pop(i)
            return True

        # Internal node: find child to descend into
        i = 0
        while i < len(node.keys) and key >= node.keys[i]:
            i += 1
        child = node.children[i]

        # Ensure child has at least t keys before descending
        if len(child.keys) <= min_keys:
            self._fix_child(node, i)
            # After fix, the tree structure may have changed —
            # re-determine the right child index
            i = 0
            while i < len(node.keys) and key >= node.keys[i]:
                i += 1
            child = node.children[i]

        return self._delete(child, key, node, i)

    def _fix_child(self, parent: BTreeNode, idx: int) -> None:
        """
        Ensure parent.children[idx] has at least self.order keys
        by borrowing from a sibling or merging with one.
        """
        t = self.order
        child = parent.children[idx]
        left_sib = parent.children[idx - 1] if idx > 0 else None
        right_sib = parent.children[idx + 1] if idx < len(parent.children) - 1 else None

        if left_sib and len(left_sib.keys) >= t:
            self._borrow_from_left(parent, idx)
        elif right_sib and len(right_sib.keys) >= t:
            self._borrow_from_right(parent, idx)
        else:
            # Merge
            if left_sib:
                self._merge(parent, idx - 1)  # merge left_sib and child
            else:
                self._merge(parent, idx)      # merge child and right_sib

    def _borrow_from_left(self, parent: BTreeNode, idx: int) -> None:
        child = parent.children[idx]
        left = parent.children[idx - 1]

        if child.is_leaf:
            # Move last key/value of left to front of child
            child.keys.insert(0, left.keys.pop())
            child.values.insert(0, left.values.pop())
            # Update parent separator
            parent.keys[idx - 1] = child.keys[0]
        else:
            # Rotate via parent separator key
            child.keys.insert(0, parent.keys[idx - 1])
            parent.keys[idx - 1] = left.keys.pop()
            child.children.insert(0, left.children.pop())

    def _borrow_from_right(self, parent: BTreeNode, idx: int) -> None:
        child = parent.children[idx]
        right = parent.children[idx + 1]

        if child.is_leaf:
            # Move first key/value of right to end of child
            child.keys.append(right.keys.pop(0))
            child.values.append(right.values.pop(0))
            # Update parent separator
            parent.keys[idx] = right.keys[0]
        else:
            # Rotate via parent separator key
            child.keys.append(parent.keys[idx])
            parent.keys[idx] = right.keys.pop(0)
            child.children.append(right.children.pop(0))

    def _merge(self, parent: BTreeNode, left_idx: int) -> None:
        """
        Merge parent.children[left_idx] and parent.children[left_idx+1].
        The separator key in parent is removed (absorbed into the merge).
        """
        left = parent.children[left_idx]
        right = parent.children[left_idx + 1]

        if left.is_leaf:
            # Leaf merge: concatenate keys/values, relink
            left.keys.extend(right.keys)
            left.values.extend(right.values)
            left.next = right.next
        else:
            # Internal merge: pull down parent separator
            left.keys.append(parent.keys[left_idx])
            left.keys.extend(right.keys)
            left.children.extend(right.children)

        # Remove separator key and right child from parent
        parent.keys.pop(left_idx)
        parent.children.pop(left_idx + 1)

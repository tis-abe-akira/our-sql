"""
oursql/heap_file.py
HeapFile: page-based, slot-managed row storage.

Page layout (4096 bytes):
  [0:2]   num_slots  — uint16, number of slots on this page
  [2:4]   num_slots reserved (padding, always 0)
  [4 …]   slot directory: (offset: uint16, length: uint16) per slot
           offset=0, length=0 → tombstone (deleted)
  [… end] row data growing from the end of the page toward the header

Rows are serialised as UTF-8 JSON bytes.
A row's RID is (page_id, slot_id).
"""

from __future__ import annotations
import json
import struct
from pathlib import Path
from typing import Any

from oursql.pager import Pager

# Struct formats (little-endian)
_HDR = struct.Struct("<HH")          # num_slots, reserved
_SLOT = struct.Struct("<HH")         # offset, length
_HDR_SIZE = _HDR.size                # 4 bytes
_SLOT_SIZE = _SLOT.size              # 4 bytes

RID = tuple[int, int]                # (page_id, slot_id)


def _encode(row: dict[str, Any]) -> bytes:
    return json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode()


def _decode(data: bytes) -> dict[str, Any]:
    return json.loads(data.decode())


class HeapFile:
    """
    Stores rows in a sequence of Pager-backed pages with slot directories.
    Each row gets a (page_id, slot_id) identifier (RID).
    """

    def __init__(self, filepath: str | Path) -> None:
        self._pager = Pager(filepath)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def insert(self, row: dict[str, Any]) -> RID:
        """Add row, return its RID (page_id, slot_id)."""
        data = _encode(row)
        page_id = self._find_page_with_space(len(data))
        page = bytearray(self._pager.read_page(page_id))
        slot_id = self._write_slot(page, data)
        self._pager.write_page(page_id, bytes(page))
        return (page_id, slot_id)

    def get(self, page_id: int, slot_id: int) -> dict[str, Any] | None:
        """Return the row at (page_id, slot_id), or None if deleted / invalid."""
        if page_id >= self._pager.num_pages():
            return None
        page = self._pager.read_page(page_id)
        num_slots, _ = _HDR.unpack_from(page, 0)
        if slot_id >= num_slots:
            return None
        offset, length = _SLOT.unpack_from(page, _HDR_SIZE + slot_id * _SLOT_SIZE)
        if length == 0:          # tombstone
            return None
        return _decode(page[offset: offset + length])

    def update(self, page_id: int, slot_id: int, row: dict[str, Any]) -> None:
        """
        Update a slot in-place.  If the new row fits in the old space, we reuse
        it; otherwise we delete the old slot, append to a page with space, and
        return the *new* RID.  Due to Phase 2 scope, callers are responsible for
        updating the B+Tree pointer after an update that moves the slot.
        For simplicity we reject oversized updates and raise ValueError.
        """
        new_data = _encode(row)
        page = bytearray(self._pager.read_page(page_id))
        num_slots, _ = _HDR.unpack_from(page, 0)
        if slot_id >= num_slots:
            raise KeyError(f"slot {slot_id} out of range on page {page_id}")
        offset, length = _SLOT.unpack_from(page, _HDR_SIZE + slot_id * _SLOT_SIZE)
        if length == 0:
            raise KeyError(f"slot {slot_id} on page {page_id} has been deleted")
        if len(new_data) > length:
            raise ValueError(
                "Updated row is larger than original — delete + re-insert instead"
            )
        # Overwrite in place (zero-pad unused trailing bytes)
        page[offset: offset + length] = new_data.ljust(length, b"\x00")
        # Update slot length to actual new length
        _SLOT.pack_into(page, _HDR_SIZE + slot_id * _SLOT_SIZE, offset, len(new_data))
        self._pager.write_page(page_id, bytes(page))

    def delete(self, page_id: int, slot_id: int) -> None:
        """Mark slot as deleted (tombstone: offset=0, length=0)."""
        page = bytearray(self._pager.read_page(page_id))
        num_slots, _ = _HDR.unpack_from(page, 0)
        if slot_id >= num_slots:
            raise KeyError(f"slot {slot_id} out of range on page {page_id}")
        offset, length = _SLOT.unpack_from(page, _HDR_SIZE + slot_id * _SLOT_SIZE)
        if length == 0:
            raise KeyError(f"slot {slot_id} on page {page_id} is already deleted")
        # Tombstone
        _SLOT.pack_into(page, _HDR_SIZE + slot_id * _SLOT_SIZE, 0, 0)
        self._pager.write_page(page_id, bytes(page))

    def scan(self) -> list[dict[str, Any]]:
        """Full table scan — returns all live rows across all pages."""
        rows: list[dict[str, Any]] = []
        for page_id in range(self._pager.num_pages()):
            page = self._pager.read_page(page_id)
            num_slots, _ = _HDR.unpack_from(page, 0)
            for slot_id in range(num_slots):
                offset, length = _SLOT.unpack_from(
                    page, _HDR_SIZE + slot_id * _SLOT_SIZE
                )
                if length > 0:
                    rows.append(_decode(page[offset: offset + length]))
        return rows

    def close(self) -> None:
        self._pager.close()

    def __enter__(self) -> "HeapFile":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _find_page_with_space(self, need: int) -> int:
        """
        Find a page that has enough free space for `need` bytes of row data
        plus one slot-directory entry.  Allocates a new page if none found.
        """
        required = need + _SLOT_SIZE
        for page_id in range(self._pager.num_pages()):
            page = self._pager.read_page(page_id)
            if self._free_space(page) >= required:
                return page_id
        # No suitable page found — allocate a new one
        return self._pager.allocate_page()

    def _free_space(self, page: bytes) -> int:
        """
        Return the number of free bytes on the page.
        Free space = page_size - header - slot_dir - used_data_area
        """
        num_slots, _ = _HDR.unpack_from(page, 0)
        slot_dir_end = _HDR_SIZE + num_slots * _SLOT_SIZE
        # Find the lowest data offset (data grows from the end)
        min_offset = self._pager.page_size
        for i in range(num_slots):
            offset, length = _SLOT.unpack_from(page, _HDR_SIZE + i * _SLOT_SIZE)
            if length > 0 and offset < min_offset:
                min_offset = offset
        return min_offset - slot_dir_end

    def _write_slot(self, page: bytearray, data: bytes) -> int:
        """
        Write row data into the page, append a slot entry, return slot_id.
        Data is written from the end of the page growing toward the header.
        """
        num_slots, _ = _HDR.unpack_from(page, 0)
        # Current lowest data offset
        min_offset = self._pager.page_size
        for i in range(num_slots):
            offset, length = _SLOT.unpack_from(page, _HDR_SIZE + i * _SLOT_SIZE)
            if length > 0 and offset < min_offset:
                min_offset = offset
        # Place new data just before the current lowest data
        data_offset = min_offset - len(data)
        page[data_offset: data_offset + len(data)] = data
        # Append slot directory entry
        slot_id = num_slots
        _SLOT.pack_into(page, _HDR_SIZE + slot_id * _SLOT_SIZE, data_offset, len(data))
        # Update header
        _HDR.pack_into(page, 0, num_slots + 1, 0)
        return slot_id

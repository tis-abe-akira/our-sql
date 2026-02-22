"""tests/test_pager.py — Unit tests for Pager."""

import pytest
from pathlib import Path
from oursql.pager import Pager


@pytest.fixture
def tmp_file(tmp_path):
    return tmp_path / "test.db"


class TestAllocate:
    def test_new_file_has_zero_pages(self, tmp_file):
        p = Pager(tmp_file)
        assert p.num_pages() == 0
        p.close()

    def test_allocate_returns_page_id(self, tmp_file):
        p = Pager(tmp_file)
        pid = p.allocate_page()
        assert pid == 0
        p.close()

    def test_allocate_increments(self, tmp_file):
        p = Pager(tmp_file)
        assert p.allocate_page() == 0
        assert p.allocate_page() == 1
        assert p.allocate_page() == 2
        assert p.num_pages() == 3
        p.close()


class TestReadWrite:
    def test_write_and_read_back(self, tmp_file):
        p = Pager(tmp_file)
        p.allocate_page()
        data = b"hello" + b"\x00" * (Pager.PAGE_SIZE - 5)
        p.write_page(0, data)
        assert p.read_page(0) == data
        p.close()

    def test_read_out_of_range_raises(self, tmp_file):
        p = Pager(tmp_file)
        with pytest.raises(IndexError):
            p.read_page(0)
        p.close()

    def test_write_wrong_size_raises(self, tmp_file):
        p = Pager(tmp_file)
        p.allocate_page()
        with pytest.raises(ValueError):
            p.write_page(0, b"too short")
        p.close()

    def test_data_persists_across_reopen(self, tmp_file):
        data = b"X" * Pager.PAGE_SIZE
        with Pager(tmp_file) as p:
            p.allocate_page()
            p.write_page(0, data)
        with Pager(tmp_file) as p:
            assert p.read_page(0) == data

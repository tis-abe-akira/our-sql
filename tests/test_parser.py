"""tests/test_parser.py — Unit tests for the SQL Parser."""

import pytest
from oursql.parser import (
    parse, ParseError,
    SelectStmt, InsertStmt, UpdateStmt, DeleteStmt,
    CreateTableStmt, DropTableStmt,
    WhereClause, ColumnDef,
)


class TestSelect:
    def test_select_star(self):
        stmt = parse("SELECT * FROM users")
        assert isinstance(stmt, SelectStmt)
        assert stmt.table == "users"
        assert stmt.columns == ["*"]
        assert stmt.where is None

    def test_select_columns(self):
        stmt = parse("SELECT id, name FROM users")
        assert stmt.columns == ["id", "name"]

    def test_select_where_eq(self):
        stmt = parse("SELECT * FROM users WHERE id = 1")
        assert isinstance(stmt.where, WhereClause)
        assert stmt.where.column == "id"
        assert stmt.where.op == "="
        assert stmt.where.value == 1

    def test_select_where_gt(self):
        stmt = parse("SELECT * FROM users WHERE id > 3")
        assert stmt.where.op == ">"
        assert stmt.where.value == 3

    def test_select_where_gte(self):
        stmt = parse("SELECT * FROM users WHERE id >= 5")
        assert stmt.where.op == ">="

    def test_select_where_ne(self):
        stmt = parse("SELECT * FROM users WHERE id != 2")
        assert stmt.where.op == "!="

    def test_select_where_string(self):
        stmt = parse("SELECT * FROM users WHERE name = 'Alice'")
        assert stmt.where.value == "Alice"

    def test_select_with_semicolon(self):
        stmt = parse("SELECT * FROM users;")
        assert isinstance(stmt, SelectStmt)

    def test_select_case_insensitive(self):
        stmt = parse("select * from users where id = 1")
        assert isinstance(stmt, SelectStmt)


class TestInsert:
    def test_insert_int_string(self):
        stmt = parse("INSERT INTO users VALUES (1, 'Alice')")
        assert isinstance(stmt, InsertStmt)
        assert stmt.table == "users"
        assert stmt.values == [1, "Alice"]

    def test_insert_multiple_values(self):
        stmt = parse("INSERT INTO products VALUES (10, 'Widget', 99)")
        assert stmt.values == [10, "Widget", 99]

    def test_insert_null(self):
        stmt = parse("INSERT INTO users VALUES (1, NULL)")
        assert stmt.values[1] is None


class TestUpdate:
    def test_update_single_col(self):
        stmt = parse("UPDATE users SET name = 'Bob' WHERE id = 1")
        assert isinstance(stmt, UpdateStmt)
        assert stmt.table == "users"
        assert stmt.assignments == {"name": "Bob"}
        assert stmt.where.value == 1

    def test_update_multiple_cols(self):
        stmt = parse("UPDATE users SET name = 'Bob', age = 30 WHERE id = 1")
        assert stmt.assignments == {"name": "Bob", "age": 30}

    def test_update_no_where(self):
        stmt = parse("UPDATE users SET name = 'X'")
        assert stmt.where is None


class TestDelete:
    def test_delete_with_where(self):
        stmt = parse("DELETE FROM users WHERE id = 5")
        assert isinstance(stmt, DeleteStmt)
        assert stmt.table == "users"
        assert stmt.where.value == 5

    def test_delete_no_where(self):
        stmt = parse("DELETE FROM users")
        assert stmt.where is None


class TestCreateTable:
    def test_create_two_cols(self):
        stmt = parse("CREATE TABLE users (id INT, name TEXT)")
        assert isinstance(stmt, CreateTableStmt)
        assert stmt.table == "users"
        assert stmt.columns == [ColumnDef("id", "int"), ColumnDef("name", "text")]

    def test_create_single_col(self):
        stmt = parse("CREATE TABLE counters (n INT)")
        assert len(stmt.columns) == 1

    def test_create_unknown_type_raises(self):
        with pytest.raises(ParseError):
            parse("CREATE TABLE t (id BLOB)")


class TestDropTable:
    def test_drop(self):
        stmt = parse("DROP TABLE users")
        assert isinstance(stmt, DropTableStmt)
        assert stmt.table == "users"

    def test_drop_with_semicolon(self):
        stmt = parse("DROP TABLE users;")
        assert isinstance(stmt, DropTableStmt)


class TestErrors:
    def test_empty_sql_raises(self):
        with pytest.raises(ParseError):
            parse("")

    def test_unknown_keyword_raises(self):
        with pytest.raises(ParseError):
            parse("TRUNCATE TABLE users")

    def test_missing_from_raises(self):
        with pytest.raises(ParseError):
            parse("SELECT * users")

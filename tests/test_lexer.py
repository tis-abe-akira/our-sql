"""tests/test_lexer.py — Unit tests for the SQL Lexer."""

import pytest
from oursql.lexer import tokenize, TokenType, LexError


def types(sql):
    return [t.type for t in tokenize(sql) if t.type != TokenType.EOF]

def values(sql):
    return [t.value for t in tokenize(sql) if t.type != TokenType.EOF]


class TestKeywords:
    def test_select_keyword(self):
        toks = tokenize("SELECT")
        assert toks[0].type == TokenType.KEYWORD
        assert toks[0].value == "SELECT"

    def test_case_insensitive(self):
        toks = tokenize("select from where")
        assert all(t.type == TokenType.KEYWORD for t in toks if t.type != TokenType.EOF)
        assert toks[0].value == "SELECT"

    def test_all_dml_keywords(self):
        sql = "SELECT FROM WHERE INSERT INTO VALUES UPDATE SET DELETE"
        kws = [t.value for t in tokenize(sql) if t.type == TokenType.KEYWORD]
        assert kws == sql.split()


class TestIdentifiers:
    def test_simple_ident(self):
        toks = tokenize("users")
        assert toks[0].type == TokenType.IDENT
        assert toks[0].value == "users"

    def test_ident_with_underscore(self):
        toks = tokenize("user_id")
        assert toks[0].type == TokenType.IDENT

    def test_ident_mixed_case_preserved(self):
        toks = tokenize("myTable")
        assert toks[0].value == "myTable"


class TestNumbers:
    def test_integer(self):
        toks = tokenize("42")
        assert toks[0].type == TokenType.NUMBER
        assert toks[0].value == "42"

    def test_float(self):
        toks = tokenize("3.14")
        assert toks[0].type == TokenType.NUMBER
        assert toks[0].value == "3.14"


class TestStrings:
    def test_simple_string(self):
        toks = tokenize("'hello'")
        assert toks[0].type == TokenType.STRING
        assert toks[0].value == "hello"

    def test_string_with_spaces(self):
        toks = tokenize("'hello world'")
        assert toks[0].value == "hello world"

    def test_escaped_quote(self):
        toks = tokenize("'it''s ok'")
        assert toks[0].value == "it's ok"

    def test_unterminated_string_raises(self):
        with pytest.raises(LexError):
            tokenize("'unterminated")


class TestSymbols:
    def test_single_symbols(self):
        sql = "( ) , ; = * <"
        syms = [t.value for t in tokenize(sql) if t.type == TokenType.SYMBOL]
        assert syms == ["(", ")", ",", ";", "=", "*", "<"]

    def test_multi_char_symbols(self):
        sql = "<= >= !="
        syms = [t.value for t in tokenize(sql) if t.type == TokenType.SYMBOL]
        assert syms == ["<=", ">=", "!="]

    def test_lt_gt(self):
        syms = [t.value for t in tokenize("< >") if t.type == TokenType.SYMBOL]
        assert syms == ["<", ">"]


class TestComments:
    def test_line_comment_skipped(self):
        sql = "-- this is a comment\nSELECT"
        toks = [t for t in tokenize(sql) if t.type != TokenType.EOF]
        assert len(toks) == 1
        assert toks[0].value == "SELECT"


class TestFullStatements:
    def test_select_star(self):
        sql = "SELECT * FROM users"
        v = values(sql)
        assert v == ["SELECT", "*", "FROM", "users"]

    def test_select_where(self):
        sql = "SELECT id FROM users WHERE id = 1"
        v = values(sql)
        assert v == ["SELECT", "id", "FROM", "users", "WHERE", "id", "=", "1"]

    def test_insert_values(self):
        sql = "INSERT INTO users VALUES (1, 'Alice')"
        v = values(sql)
        assert v == ["INSERT", "INTO", "users", "VALUES", "(", "1", ",", "Alice", ")"]


    def test_create_table(self):
        sql = "CREATE TABLE t (id INT, name TEXT)"
        v = values(sql)
        assert "CREATE" in v and "TABLE" in v and "INT" in v and "TEXT" in v


class TestErrors:
    def test_unknown_character(self):
        with pytest.raises(LexError):
            tokenize("SELECT @ FROM")

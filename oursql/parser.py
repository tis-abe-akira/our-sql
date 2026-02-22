"""
oursql/parser.py
Recursive-descent parser for OurSQL.

Consumes a list of Tokens (from oursql.lexer) and produces an AST node.
Each statement maps to a distinct dataclass.

Grammar (simplified):
  stmt        = select_stmt | insert_stmt | update_stmt
              | delete_stmt | create_stmt | drop_stmt
  select_stmt = SELECT col_list FROM IDENT [where_clause] [;]
  insert_stmt = INSERT INTO IDENT VALUES ( value_list ) [;]
  update_stmt = UPDATE IDENT SET assign_list [where_clause] [;]
  delete_stmt = DELETE FROM IDENT [where_clause] [;]
  create_stmt = CREATE TABLE IDENT ( col_def_list ) [;]
  drop_stmt   = DROP TABLE IDENT [;]
  where_clause= WHERE IDENT op literal
  col_list    = * | IDENT (, IDENT)*
  assign_list = IDENT = literal (, IDENT = literal)*
  col_def_list= IDENT type (, IDENT type)*
  op          = = | != | < | > | <= | >=
  literal     = NUMBER | STRING | NULL
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any

from oursql.lexer import Token, TokenType, tokenize


# ── AST nodes ─────────────────────────────────────────────────────────

@dataclass
class WhereClause:
    column: str
    op: str             # '=', '!=', '<', '>', '<=', '>='
    value: Any          # int | float | str | None


@dataclass
class SelectStmt:
    table: str
    columns: list[str]  # ['*'] means all columns
    where: WhereClause | None = None


@dataclass
class InsertStmt:
    table: str
    values: list[Any]   # positional values matching table schema order


@dataclass
class UpdateStmt:
    table: str
    assignments: dict[str, Any]   # {column: new_value}
    where: WhereClause | None = None


@dataclass
class DeleteStmt:
    table: str
    where: WhereClause | None = None


@dataclass
class ColumnDef:
    name: str
    type: str   # 'INT' | 'TEXT'


@dataclass
class CreateTableStmt:
    table: str
    columns: list[ColumnDef]


@dataclass
class DropTableStmt:
    table: str


Stmt = (
    SelectStmt | InsertStmt | UpdateStmt | DeleteStmt
    | CreateTableStmt | DropTableStmt
)


# ── ParseError ────────────────────────────────────────────────────────

class ParseError(Exception):
    pass


# ── Parser ────────────────────────────────────────────────────────────

class Parser:
    """Recursive-descent parser. Produces a single AST node per call."""

    def __init__(self, tokens: list[Token]) -> None:
        self._tokens = tokens
        self._pos = 0

    # ── Public API ────────────────────────────────────────────────────

    def parse(self) -> Stmt:
        """Parse one statement and return its AST node."""
        tok = self._peek()
        if tok.type == TokenType.EOF:
            raise ParseError("Empty SQL statement")

        if tok.type != TokenType.KEYWORD:
            raise ParseError(f"Expected keyword, got {tok.value!r}")

        kw = tok.value
        if kw == "SELECT":
            stmt = self._parse_select()
        elif kw == "INSERT":
            stmt = self._parse_insert()
        elif kw == "UPDATE":
            stmt = self._parse_update()
        elif kw == "DELETE":
            stmt = self._parse_delete()
        elif kw == "CREATE":
            stmt = self._parse_create()
        elif kw == "DROP":
            stmt = self._parse_drop()
        else:
            raise ParseError(f"Unknown statement keyword: {kw!r}")

        self._skip_optional(TokenType.SYMBOL, ";")
        self._expect(TokenType.EOF)
        return stmt

    # ── Statement parsers ─────────────────────────────────────────────

    def _parse_select(self) -> SelectStmt:
        # SELECT col_list FROM table [WHERE ...]
        self._expect_kw("SELECT")
        columns = self._parse_col_list()
        self._expect_kw("FROM")
        table = self._expect(TokenType.IDENT).value
        where = self._parse_where_opt()
        return SelectStmt(table=table, columns=columns, where=where)

    def _parse_insert(self) -> InsertStmt:
        # INSERT INTO table VALUES ( v1, v2, ... )
        self._expect_kw("INSERT")
        self._expect_kw("INTO")
        table = self._expect(TokenType.IDENT).value
        self._expect_kw("VALUES")
        self._expect_sym("(")
        values = self._parse_value_list()
        self._expect_sym(")")
        return InsertStmt(table=table, values=values)

    def _parse_update(self) -> UpdateStmt:
        # UPDATE table SET col=val [, col=val]* [WHERE ...]
        self._expect_kw("UPDATE")
        table = self._expect(TokenType.IDENT).value
        self._expect_kw("SET")
        assignments = self._parse_assignment_list()
        where = self._parse_where_opt()
        return UpdateStmt(table=table, assignments=assignments, where=where)

    def _parse_delete(self) -> DeleteStmt:
        # DELETE FROM table [WHERE ...]
        self._expect_kw("DELETE")
        self._expect_kw("FROM")
        table = self._expect(TokenType.IDENT).value
        where = self._parse_where_opt()
        return DeleteStmt(table=table, where=where)

    def _parse_create(self) -> CreateTableStmt:
        # CREATE TABLE name ( col_def [, col_def]* )
        self._expect_kw("CREATE")
        self._expect_kw("TABLE")
        table = self._expect(TokenType.IDENT).value
        self._expect_sym("(")
        cols: list[ColumnDef] = []
        while True:
            col_name = self._expect(TokenType.IDENT).value
            col_type = self._expect(TokenType.KEYWORD).value  # INT or TEXT
            if col_type not in ("INT", "TEXT"):
                raise ParseError(f"Unknown column type: {col_type!r}")
            cols.append(ColumnDef(name=col_name, type=col_type.lower()))
            if not self._match_sym(","):
                break
        self._expect_sym(")")
        return CreateTableStmt(table=table, columns=cols)

    def _parse_drop(self) -> DropTableStmt:
        # DROP TABLE name
        self._expect_kw("DROP")
        self._expect_kw("TABLE")
        table = self._expect(TokenType.IDENT).value
        return DropTableStmt(table=table)

    # ── Sub-parsers ───────────────────────────────────────────────────

    def _parse_col_list(self) -> list[str]:
        """Parse * or comma-separated identifiers."""
        if self._match_sym("*"):
            return ["*"]
        cols = [self._expect(TokenType.IDENT).value]
        while self._match_sym(","):
            cols.append(self._expect(TokenType.IDENT).value)
        return cols

    def _parse_value_list(self) -> list[Any]:
        values = [self._parse_literal()]
        while self._match_sym(","):
            values.append(self._parse_literal())
        return values

    def _parse_assignment_list(self) -> dict[str, Any]:
        assignments: dict[str, Any] = {}
        col = self._expect(TokenType.IDENT).value
        self._expect_sym("=")
        assignments[col] = self._parse_literal()
        while self._match_sym(","):
            col = self._expect(TokenType.IDENT).value
            self._expect_sym("=")
            assignments[col] = self._parse_literal()
        return assignments

    def _parse_where_opt(self) -> WhereClause | None:
        """Parse WHERE clause if present, else return None."""
        if not self._match_kw("WHERE"):
            return None
        col = self._expect(TokenType.IDENT).value
        op  = self._parse_op()
        val = self._parse_literal()
        return WhereClause(column=col, op=op, value=val)

    def _parse_op(self) -> str:
        tok = self._peek()
        if tok.type == TokenType.SYMBOL and tok.value in ("=", "!=", "<", ">", "<=", ">=", "<>"):
            self._advance()
            return "!=" if tok.value == "<>" else tok.value
        raise ParseError(f"Expected comparison operator, got {tok.value!r}")

    def _parse_literal(self) -> Any:
        tok = self._peek()
        if tok.type == TokenType.NUMBER:
            self._advance()
            s = tok.value
            return float(s) if "." in s else int(s)
        if tok.type == TokenType.STRING:
            self._advance()
            return tok.value
        if tok.type == TokenType.KEYWORD and tok.value == "NULL":
            self._advance()
            return None
        raise ParseError(f"Expected literal value, got {tok.type.name} {tok.value!r}")

    # ── Token-stream helpers ──────────────────────────────────────────

    def _peek(self) -> Token:
        return self._tokens[self._pos]

    def _advance(self) -> Token:
        tok = self._tokens[self._pos]
        self._pos += 1
        return tok

    def _expect(self, ttype: TokenType, value: str | None = None) -> Token:
        tok = self._peek()
        if tok.type != ttype:
            raise ParseError(
                f"Expected {ttype.name}"
                + (f" {value!r}" if value else "")
                + f", got {tok.type.name} {tok.value!r} at pos {tok.pos}"
            )
        if value is not None and tok.value != value:
            raise ParseError(
                f"Expected {value!r}, got {tok.value!r} at pos {tok.pos}"
            )
        return self._advance()

    def _expect_kw(self, kw: str) -> Token:
        return self._expect(TokenType.KEYWORD, kw)

    def _expect_sym(self, sym: str) -> Token:
        return self._expect(TokenType.SYMBOL, sym)

    def _match_kw(self, kw: str) -> bool:
        tok = self._peek()
        if tok.type == TokenType.KEYWORD and tok.value == kw:
            self._advance()
            return True
        return False

    def _match_sym(self, sym: str) -> bool:
        tok = self._peek()
        if tok.type == TokenType.SYMBOL and tok.value == sym:
            self._advance()
            return True
        return False

    def _skip_optional(self, ttype: TokenType, value: str) -> None:
        tok = self._peek()
        if tok.type == ttype and tok.value == value:
            self._advance()


def parse(sql: str) -> Stmt:
    """Convenience function: tokenize and parse a SQL string."""
    tokens = tokenize(sql)
    return Parser(tokens).parse()

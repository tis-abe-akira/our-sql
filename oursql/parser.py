"""
oursql/parser.py
Recursive-descent parser for OurSQL.

Produces a single AST node per SQL statement.

Grammar (Phase 4 – ORDER BY / LIMIT / AND / OR):
  stmt         = select_stmt | insert_stmt | update_stmt
               | delete_stmt | create_stmt | drop_stmt
  select_stmt  = SELECT col_list FROM IDENT
                 [WHERE condition]
                 [ORDER BY IDENT [ASC|DESC]]
                 [LIMIT NUMBER] [;]
  insert_stmt  = INSERT INTO IDENT VALUES ( value_list ) [;]
  update_stmt  = UPDATE IDENT SET assign_list [WHERE condition] [;]
  delete_stmt  = DELETE FROM IDENT [WHERE condition] [;]
  create_stmt  = CREATE TABLE IDENT ( col_def_list ) [;]
  drop_stmt    = DROP TABLE IDENT [;]

  condition    = and_cond (OR and_cond)*
  and_cond     = predicate (AND predicate)*
  predicate    = IDENT op literal

  col_list     = * | IDENT (, IDENT)*
  assign_list  = IDENT = literal (, IDENT = literal)*
  col_def_list = IDENT type (, IDENT type)*
  op           = = | != | < | > | <= | >=
  literal      = NUMBER | STRING | NULL
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any

from oursql.lexer import Token, TokenType, tokenize


# ── AST condition nodes ────────────────────────────────────────────────

@dataclass
class Predicate:
    """A single comparison: col op value."""
    column: str
    op: str          # '=', '!=', '<', '>', '<=', '>='
    value: Any       # int | float | str | None


@dataclass
class AndCondition:
    """Two conditions joined by AND."""
    left: "Condition"
    right: "Condition"


@dataclass
class OrCondition:
    """Two conditions joined by OR."""
    left: "Condition"
    right: "Condition"


# A Condition is any of the above
Condition = Predicate | AndCondition | OrCondition

# Backward-compat alias (Phase 3 tests still use WhereClause)
WhereClause = Predicate


# ── Statement AST nodes ───────────────────────────────────────────────

@dataclass
class SelectStmt:
    table: str
    columns: list[str]          # ['*'] = all columns
    where: Condition | None = None
    order_by: str | None = None
    order_dir: str = "ASC"      # 'ASC' | 'DESC'
    limit: int | None = None


@dataclass
class InsertStmt:
    table: str
    values: list[Any]


@dataclass
class UpdateStmt:
    table: str
    assignments: dict[str, Any]
    where: Condition | None = None


@dataclass
class DeleteStmt:
    table: str
    where: Condition | None = None


@dataclass
class ColumnDef:
    name: str
    type: str   # 'int' | 'text'


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

    def parse(self) -> Stmt:
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
        self._expect_kw("SELECT")
        columns = self._parse_col_list()
        self._expect_kw("FROM")
        table = self._expect(TokenType.IDENT).value
        where = self._parse_where_opt()
        order_by, order_dir = self._parse_order_by_opt()
        limit = self._parse_limit_opt()
        return SelectStmt(
            table=table, columns=columns, where=where,
            order_by=order_by, order_dir=order_dir, limit=limit,
        )

    def _parse_insert(self) -> InsertStmt:
        self._expect_kw("INSERT")
        self._expect_kw("INTO")
        table = self._expect(TokenType.IDENT).value
        self._expect_kw("VALUES")
        self._expect_sym("(")
        values = self._parse_value_list()
        self._expect_sym(")")
        return InsertStmt(table=table, values=values)

    def _parse_update(self) -> UpdateStmt:
        self._expect_kw("UPDATE")
        table = self._expect(TokenType.IDENT).value
        self._expect_kw("SET")
        assignments = self._parse_assignment_list()
        where = self._parse_where_opt()
        return UpdateStmt(table=table, assignments=assignments, where=where)

    def _parse_delete(self) -> DeleteStmt:
        self._expect_kw("DELETE")
        self._expect_kw("FROM")
        table = self._expect(TokenType.IDENT).value
        where = self._parse_where_opt()
        return DeleteStmt(table=table, where=where)

    def _parse_create(self) -> CreateTableStmt:
        self._expect_kw("CREATE")
        self._expect_kw("TABLE")
        table = self._expect(TokenType.IDENT).value
        self._expect_sym("(")
        cols: list[ColumnDef] = []
        while True:
            col_name = self._expect(TokenType.IDENT).value
            col_type = self._expect(TokenType.KEYWORD).value
            if col_type not in ("INT", "TEXT"):
                raise ParseError(f"Unknown column type: {col_type!r}")
            cols.append(ColumnDef(name=col_name, type=col_type.lower()))
            if not self._match_sym(","):
                break
        self._expect_sym(")")
        return CreateTableStmt(table=table, columns=cols)

    def _parse_drop(self) -> DropTableStmt:
        self._expect_kw("DROP")
        self._expect_kw("TABLE")
        table = self._expect(TokenType.IDENT).value
        return DropTableStmt(table=table)

    # ── Sub-parsers ───────────────────────────────────────────────────

    def _parse_col_list(self) -> list[str]:
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

    # WHERE: OR > AND > predicate  (standard SQL precedence)
    def _parse_where_opt(self) -> Condition | None:
        if not self._match_kw("WHERE"):
            return None
        return self._parse_or_condition()

    def _parse_or_condition(self) -> Condition:
        left = self._parse_and_condition()
        while self._match_kw("OR"):
            right = self._parse_and_condition()
            left = OrCondition(left=left, right=right)
        return left

    def _parse_and_condition(self) -> Condition:
        left = self._parse_predicate()
        while self._match_kw("AND"):
            right = self._parse_predicate()
            left = AndCondition(left=left, right=right)
        return left

    def _parse_predicate(self) -> Predicate:
        col = self._expect(TokenType.IDENT).value
        op  = self._parse_op()
        val = self._parse_literal()
        return Predicate(column=col, op=op, value=val)

    def _parse_order_by_opt(self) -> tuple[str | None, str]:
        tok = self._peek()
        if not (tok.type == TokenType.KEYWORD and tok.value == "ORDER"):
            return None, "ASC"
        self._advance()  # consume ORDER
        self._expect_kw("BY")
        col = self._expect(TokenType.IDENT).value
        direction = "ASC"
        tok = self._peek()
        if tok.type == TokenType.KEYWORD and tok.value in ("ASC", "DESC"):
            direction = tok.value
            self._advance()
        return col, direction

    def _parse_limit_opt(self) -> int | None:
        if not self._match_kw("LIMIT"):
            return None
        tok = self._expect(TokenType.NUMBER)
        return int(tok.value)

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

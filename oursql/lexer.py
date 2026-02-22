"""
oursql/lexer.py
Lexer (tokenizer) for OurSQL.

Converts a SQL string into a flat list of Tokens.
Supports:
  - Keywords (case-insensitive)
  - Identifiers
  - Integer and float literals
  - Single-quoted string literals  ('hello', 'it''s ok')
  - Symbols: ( ) , ; = < > <= >= != *
  - Single-line comments: -- …
  - Whitespace (silently skipped)
"""

from __future__ import annotations
from dataclasses import dataclass
from enum import Enum, auto


class TokenType(Enum):
    KEYWORD  = auto()   # SELECT, FROM, WHERE, …
    IDENT    = auto()   # table / column names
    NUMBER   = auto()   # 42, 3.14
    STRING   = auto()   # 'hello'
    SYMBOL   = auto()   # ( ) , ; = < > <= >= != *
    EOF      = auto()


# All keywords (uppercase canonical form)
KEYWORDS: frozenset[str] = frozenset({
    "SELECT", "FROM", "WHERE",
    "INSERT", "INTO", "VALUES",
    "UPDATE", "SET",
    "DELETE",
    "CREATE", "TABLE", "DROP",
    "INT", "TEXT",
    "AND", "OR", "NOT",
    "NULL",
    "ORDER", "BY", "ASC", "DESC",
    "LIMIT",
})


# Multi-character symbols (order matters: longest first)
_MULTI_SYMBOLS = ("<=", ">=", "!=", "<>")
_SINGLE_SYMBOLS = set("()=<>,;*")


@dataclass(frozen=True)
class Token:
    type: TokenType
    value: str          # raw text value
    pos: int            # byte offset in original SQL


class LexError(Exception):
    pass


class Lexer:
    def __init__(self, sql: str) -> None:
        self._sql = sql
        self._pos = 0
        self._tokens: list[Token] = []

    def tokenize(self) -> list[Token]:
        """Return all tokens including a final EOF token."""
        while self._pos < len(self._sql):
            self._skip_whitespace()
            if self._pos >= len(self._sql):
                break
            ch = self._sql[self._pos]

            # Single-line comment
            if ch == "-" and self._peek(1) == "-":
                self._skip_line_comment()

            # String literal
            elif ch == "'":
                self._read_string()

            # Number
            elif ch.isdigit():
                self._read_number()

            # Identifier or keyword
            elif ch.isalpha() or ch == "_":
                self._read_word()

            # Multi-char symbol
            elif self._sql[self._pos: self._pos + 2] in _MULTI_SYMBOLS:
                sym = self._sql[self._pos: self._pos + 2]
                self._emit(TokenType.SYMBOL, sym)
                self._pos += 2

            # Single-char symbol
            elif ch in _SINGLE_SYMBOLS:
                self._emit(TokenType.SYMBOL, ch)
                self._pos += 1

            else:
                raise LexError(
                    f"Unexpected character {ch!r} at position {self._pos}"
                )

        self._emit(TokenType.EOF, "")
        return self._tokens

    # ── internal helpers ──────────────────────────────────────────────

    def _peek(self, offset: int = 1) -> str:
        idx = self._pos + offset
        return self._sql[idx] if idx < len(self._sql) else ""

    def _emit(self, ttype: TokenType, value: str) -> None:
        self._tokens.append(Token(ttype, value, self._pos))

    def _skip_whitespace(self) -> None:
        while self._pos < len(self._sql) and self._sql[self._pos].isspace():
            self._pos += 1

    def _skip_line_comment(self) -> None:
        while self._pos < len(self._sql) and self._sql[self._pos] != "\n":
            self._pos += 1

    def _read_string(self) -> None:
        start = self._pos
        self._pos += 1  # skip opening '
        buf: list[str] = []
        while self._pos < len(self._sql):
            ch = self._sql[self._pos]
            if ch == "'":
                # Check for escaped quote ''
                if self._peek() == "'":
                    buf.append("'")
                    self._pos += 2
                else:
                    self._pos += 1  # skip closing '
                    tok = Token(TokenType.STRING, "".join(buf), start)
                    self._tokens.append(tok)
                    return
            else:
                buf.append(ch)
                self._pos += 1
        raise LexError(f"Unterminated string literal at position {start}")

    def _read_number(self) -> None:
        start = self._pos
        while self._pos < len(self._sql) and (
            self._sql[self._pos].isdigit() or self._sql[self._pos] == "."
        ):
            self._pos += 1
        self._emit(TokenType.NUMBER, self._sql[start: self._pos])

    def _read_word(self) -> None:
        start = self._pos
        while self._pos < len(self._sql) and (
            self._sql[self._pos].isalnum() or self._sql[self._pos] == "_"
        ):
            self._pos += 1
        word = self._sql[start: self._pos]
        upper = word.upper()
        if upper in KEYWORDS:
            self._emit(TokenType.KEYWORD, upper)
        else:
            self._emit(TokenType.IDENT, word)


def tokenize(sql: str) -> list[Token]:
    """Convenience function: tokenize a SQL string."""
    return Lexer(sql).tokenize()

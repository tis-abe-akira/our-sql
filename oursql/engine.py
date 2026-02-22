"""
oursql/engine.py
SQLEngine: ties the SQL parser to OurSQLDB for execution.

Supports:
  - SELECT with WHERE (AND/OR/Predicate), ORDER BY [ASC|DESC], LIMIT
  - INSERT, UPDATE, DELETE with compound WHERE
  - CREATE TABLE, DROP TABLE
"""

from __future__ import annotations
from typing import Any

from oursql.db import OurSQLDB
from oursql.parser import (
    parse,
    SelectStmt, InsertStmt, UpdateStmt, DeleteStmt,
    CreateTableStmt, DropTableStmt,
    Predicate, AndCondition, OrCondition,
    Condition,
    ParseError,
)


class SQLError(Exception):
    """Runtime SQL execution error."""


class SQLEngine:
    """
    Executes SQL statements against an OurSQLDB instance.

    Returns:
      - SELECT → list[dict]
      - INSERT / UPDATE / DELETE → {"status": "OK", "affected": n}
      - CREATE TABLE / DROP TABLE → {"status": "OK"}
    """

    def __init__(self, db: OurSQLDB) -> None:
        self._db = db

    def execute(self, sql: str) -> list[dict] | dict:
        try:
            stmt = parse(sql)
        except ParseError as e:
            raise SQLError(f"Parse error: {e}") from e

        if isinstance(stmt, SelectStmt):
            return self._exec_select(stmt)
        elif isinstance(stmt, InsertStmt):
            return self._exec_insert(stmt)
        elif isinstance(stmt, UpdateStmt):
            return self._exec_update(stmt)
        elif isinstance(stmt, DeleteStmt):
            return self._exec_delete(stmt)
        elif isinstance(stmt, CreateTableStmt):
            return self._exec_create(stmt)
        elif isinstance(stmt, DropTableStmt):
            return self._exec_drop(stmt)
        else:
            raise SQLError(f"Unsupported statement type: {type(stmt)}")

    # ── SELECT ────────────────────────────────────────────────────────

    def _exec_select(self, stmt: SelectStmt) -> list[dict]:
        table = self._get_table(stmt.table)

        # === Row retrieval ===
        # Optimise: simple PK equality → index lookup
        if (
            stmt.where is not None
            and isinstance(stmt.where, Predicate)
            and stmt.where.column == table._pk_column
            and stmt.where.op == "="
        ):
            row = table.select(stmt.where.value)
            rows = [row] if row is not None else []
        else:
            rows = table.select_all()
            if stmt.where is not None:
                rows = [r for r in rows if eval_condition(r, stmt.where)]

        # === Column projection ===
        if stmt.columns != ["*"]:
            rows = [{col: row.get(col) for col in stmt.columns} for row in rows]

        # === ORDER BY ===
        if stmt.order_by is not None:
            reverse = stmt.order_dir == "DESC"
            rows = sorted(rows, key=lambda r: (r.get(stmt.order_by) is None, r.get(stmt.order_by)), reverse=reverse)

        # === LIMIT ===
        if stmt.limit is not None:
            rows = rows[: stmt.limit]

        return rows

    # ── INSERT ────────────────────────────────────────────────────────

    def _exec_insert(self, stmt: InsertStmt) -> dict:
        table = self._get_table(stmt.table)
        schema_cols = list(table.schema.keys())

        if len(stmt.values) != len(schema_cols):
            raise SQLError(
                f"INSERT has {len(stmt.values)} values but table '{stmt.table}' "
                f"has {len(schema_cols)} columns"
            )

        row = {col: val for col, val in zip(schema_cols, stmt.values)}
        # Coerce float→int for INT columns
        for col, col_type in table.schema.items():
            if col_type == "int" and isinstance(row.get(col), float):
                row[col] = int(row[col])

        table.insert(row)
        return {"status": "OK", "affected": 1}

    # ── UPDATE ────────────────────────────────────────────────────────

    def _exec_update(self, stmt: UpdateStmt) -> dict:
        table = self._get_table(stmt.table)
        affected = 0

        if stmt.where is not None:
            pk_col = table._pk_column
            # Optimise: simple PK equality
            if (
                isinstance(stmt.where, Predicate)
                and stmt.where.column == pk_col
                and stmt.where.op == "="
            ):
                ok = table.update(stmt.where.value, stmt.assignments)
                affected = 1 if ok else 0
            else:
                candidates = [r for r in table.select_all() if eval_condition(r, stmt.where)]
                for row in candidates:
                    table.update(row[pk_col], stmt.assignments)
                    affected += 1
        else:
            for row in table.select_all():
                table.update(row[table._pk_column], stmt.assignments)
                affected += 1

        return {"status": "OK", "affected": affected}

    # ── DELETE ────────────────────────────────────────────────────────

    def _exec_delete(self, stmt: DeleteStmt) -> dict:
        table = self._get_table(stmt.table)
        affected = 0

        if stmt.where is not None:
            pk_col = table._pk_column
            if (
                isinstance(stmt.where, Predicate)
                and stmt.where.column == pk_col
                and stmt.where.op == "="
            ):
                ok = table.delete(stmt.where.value)
                affected = 1 if ok else 0
            else:
                candidates = [r for r in table.select_all() if eval_condition(r, stmt.where)]
                for row in candidates:
                    table.delete(row[pk_col])
                    affected += 1
        else:
            for row in table.select_all():
                table.delete(row[table._pk_column])
                affected += 1

        return {"status": "OK", "affected": affected}

    # ── DDL ───────────────────────────────────────────────────────────

    def _exec_create(self, stmt: CreateTableStmt) -> dict:
        schema = {col.name: col.type for col in stmt.columns}
        try:
            self._db.create_table(stmt.table, schema)
        except ValueError as e:
            raise SQLError(str(e)) from e
        return {"status": "OK"}

    def _exec_drop(self, stmt: DropTableStmt) -> dict:
        ok = self._db.drop_table(stmt.table)
        if not ok:
            raise SQLError(f"Table '{stmt.table}' does not exist")
        return {"status": "OK"}

    # ── Helpers ───────────────────────────────────────────────────────

    def _get_table(self, name: str):
        table = self._db.get_table(name)
        if table is None:
            raise SQLError(f"Table '{name}' does not exist")
        return table


# ── Condition evaluation ──────────────────────────────────────────────

def eval_condition(row: dict, cond: Condition) -> bool:
    """Recursively evaluate a condition tree against a row."""
    if isinstance(cond, Predicate):
        return _eval_predicate(row, cond)
    elif isinstance(cond, AndCondition):
        return eval_condition(row, cond.left) and eval_condition(row, cond.right)
    elif isinstance(cond, OrCondition):
        return eval_condition(row, cond.left) or eval_condition(row, cond.right)
    return False


def _eval_predicate(row: dict, pred: Predicate) -> bool:
    val = row.get(pred.column)
    rhs = pred.value

    # NULL handling
    if val is None or rhs is None:
        return (pred.op == "=" and val is rhs) or (pred.op == "!=" and val is not rhs)

    try:
        match pred.op:
            case "=":  return val == rhs
            case "!=": return val != rhs
            case "<":  return val < rhs
            case ">":  return val > rhs
            case "<=": return val <= rhs
            case ">=": return val >= rhs
            case _:    return False
    except TypeError:
        return False

"""
oursql/__main__.py
Interactive REPL for OurSQL.

Usage:
    python -m oursql                        # in-memory mode
    python -m oursql --data-dir ./mydb      # disk-backed mode

Meta-commands:
    .help    — show help
    .tables  — list tables
    .quit    — exit
    exit     — exit (also: quit, .exit, .quit)
"""

from __future__ import annotations
import argparse
import sys

from oursql.db import OurSQLDB
from oursql.engine import SQLEngine, SQLError


# ── ASCII table formatter ─────────────────────────────────────────────

def _fmt_table(rows: list[dict]) -> str:
    if not rows:
        return "(0 rows)"
    cols = list(rows[0].keys())
    widths = {c: len(c) for c in cols}
    str_rows = []
    for row in rows:
        str_row = {c: str(row[c]) if row[c] is not None else "NULL" for c in cols}
        for c in cols:
            widths[c] = max(widths[c], len(str_row[c]))
        str_rows.append(str_row)

    sep = "+" + "+".join("-" * (widths[c] + 2) for c in cols) + "+"
    header = "|" + "|".join(f" {c:<{widths[c]}} " for c in cols) + "|"
    lines = [sep, header, sep]
    for str_row in str_rows:
        lines.append("|" + "|".join(f" {str_row[c]:<{widths[c]}} " for c in cols) + "|")
    lines.append(sep)
    lines.append(f"({len(rows)} row{'s' if len(rows) != 1 else ''})")
    return "\n".join(lines)


# ── REPL ─────────────────────────────────────────────────────────────

def run_repl(engine: SQLEngine, db: OurSQLDB) -> None:
    mode = "disk" if db._disk_mode else "memory"
    print(f"OurSQL REPL  (mode={mode})  Type .help for help, exit or .quit to exit.")
    print()

    buf: list[str] = []
    prompt_main = "oursql> "
    prompt_cont = "    ... "

    while True:
        try:
            prompt = prompt_main if not buf else prompt_cont
            line = input(prompt)
        except (EOFError, KeyboardInterrupt):
            print()
            break

        stripped = line.strip()

        # Plain exit/quit commands (without dot prefix)
        if not buf and stripped.lower() in ("exit", "quit"):
            print("Bye!")
            sys.exit(0)

        # Meta-commands (only at top level)
        if not buf and stripped.startswith("."):
            _handle_meta(stripped, db)
            continue

        buf.append(line)
        # Execute when we see a ; at the end
        full = " ".join(buf)
        if ";" in full or (stripped and not stripped.endswith(",") and not buf[:-1]):
            # Heuristic: run if ; found OR single complete line without comma continuation
            sql = full.strip().rstrip(";").strip()
            buf = []
            if not sql:
                continue
            try:
                result = engine.execute(sql)
                _print_result(result)
            except SQLError as e:
                print(f"Error: {e}")
            except Exception as e:  # noqa: BLE001
                print(f"Unexpected error: {e}")


def _handle_meta(cmd: str, db: OurSQLDB) -> None:
    cmd = cmd.lower().split()[0]
    if cmd == ".quit" or cmd == ".exit":
        print("Bye!")
        sys.exit(0)
    elif cmd == ".tables":
        tables = db.list_tables()
        if tables:
            for t in tables:
                print(f"  {t}")
        else:
            print("  (no tables)")
    elif cmd == ".help":
        print("""
Meta-commands:
  .tables   List all tables
  .help     Show this help
  .quit     Exit  (also: exit, quit, .exit)

SQL examples:
  CREATE TABLE users (id INT, name TEXT);
  INSERT INTO users VALUES (1, 'Alice');
  SELECT * FROM users;
  SELECT * FROM users WHERE id > 1 AND id < 5;
  SELECT * FROM users ORDER BY name DESC;
  SELECT * FROM users ORDER BY id ASC LIMIT 3;
  UPDATE users SET name = 'Alicia' WHERE id = 1;
  DELETE FROM users WHERE id = 2;
  DROP TABLE users;
""")
    else:
        print(f"Unknown meta-command: {cmd}")


def _print_result(result: list | dict) -> None:
    if isinstance(result, list):
        print(_fmt_table(result))
    else:
        status = result.get("status", "OK")
        affected = result.get("affected")
        if affected is not None:
            print(f"{status} — {affected} row{'s' if affected != 1 else ''} affected")
        else:
            print(status)


# ── Entry point ───────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m oursql", description="OurSQL interactive REPL")
    parser.add_argument("--data-dir", metavar="PATH", default=None,
                        help="Directory for disk-backed storage (default: in-memory)")
    args = parser.parse_args()

    db = OurSQLDB(args.data_dir)
    engine = SQLEngine(db)
    try:
        run_repl(engine, db)
    finally:
        db.close()


if __name__ == "__main__":
    main()

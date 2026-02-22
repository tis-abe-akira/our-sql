# OurSQL

**OurSQL** は、データベースエンジンの仕組みを「自分の手で作る」学習プロジェクトです。  
B+Tree・ページング・SQLパーサまで一から実装しています。

## セットアップ

```bash
cd our-sql
pip install -e ".[dev]"
```

## REPL で使う（一番手軽）

```bash
# インメモリモード（終了すると消える）
python -m oursql

# ディスク永続モード（再起動後もデータが残る）
python -m oursql --data-dir ./mydb
```

```
OurSQL REPL  (mode=memory)  Type .help for help, .quit to exit.

oursql> CREATE TABLE users (id INT, name TEXT);
OK
oursql> INSERT INTO users VALUES (1, 'Alice');
oursql> INSERT INTO users VALUES (2, 'Bob');
oursql> INSERT INTO users VALUES (3, 'Charlie');
oursql> SELECT * FROM users;
+----+---------+
| id | name    |
+----+---------+
| 1  | Alice   |
| 2  | Bob     |
| 3  | Charlie |
+----+---------+
(3 rows)
oursql> SELECT * FROM users WHERE id > 1 AND id < 3;
+----+------+
| id | name |
+----+------+
| 2  | Bob  |
+----+------+
(1 row)
oursql> SELECT * FROM users ORDER BY name DESC LIMIT 2;
+----+---------+
| id | name    |
+----+---------+
| 3  | Charlie |
| 2  | Bob     |
+----+---------+
(2 rows)
oursql> .tables
  users
oursql> .quit
```

### 使えるSQL

| 文 | 例 |
|----|----|
| `CREATE TABLE` | `CREATE TABLE t (id INT, name TEXT)` |
| `INSERT INTO` | `INSERT INTO t VALUES (1, 'Alice')` |
| `SELECT` | `SELECT * FROM t` |
| `SELECT ... WHERE` | `SELECT * FROM t WHERE id = 1` |
| `AND / OR` | `WHERE id > 1 AND id < 5` |
| `ORDER BY` | `ORDER BY name DESC` |
| `LIMIT` | `LIMIT 10` |
| `UPDATE` | `UPDATE t SET name = 'Bob' WHERE id = 1` |
| `DELETE` | `DELETE FROM t WHERE id = 3` |
| `DROP TABLE` | `DROP TABLE t` |

### メタコマンド

| コマンド | 説明 |
|---------|------|
| `.tables` | テーブル一覧 |
| `.help` | ヘルプ表示 |
| `.quit` | 終了 |

---

## Python API から使う

```python
from oursql.db import OurSQLDB
from oursql.engine import SQLEngine

# ディスク永続モード
with OurSQLDB("./mydb") as db:
    engine = SQLEngine(db)
    engine.execute("CREATE TABLE users (id INT, name TEXT)")
    engine.execute("INSERT INTO users VALUES (1, 'Alice')")
    rows = engine.execute("SELECT * FROM users WHERE id = 1")
    print(rows)  # [{"id": 1, "name": "Alice"}]
```

## テスト実行

```bash
pytest -v   # 222 tests
```

## アーキテクチャ

```
SQL 文字列
   │
   ▼
Lexer → Parser → AST
                  │
                  ▼
              SQLEngine
                  │
              OurSQLDB
             /        \
   InMemoryTable    DiskTable
   (Phase 1)        (Phase 2)
   BPlusTree        PageBTree
   HeapStorage      HeapFile + Pager
```

### ディスク永続化のファイル構成

```
./mydb/
├── catalog.json        ← テーブル定義
└── users/
    ├── heap.db         ← 行データ（4KB ページ列）
    └── pk.idx          ← B+Tree 主キーインデックス
```

## ディレクトリ構成

```
our-sql/
├── oursql/
│   ├── __main__.py     # REPL エントリポイント
│   ├── lexer.py        # 字句解析器
│   ├── parser.py       # 再帰下降パーサ / AST
│   ├── engine.py       # SQL 実行エンジン
│   ├── db.py           # OurSQLDB (DDL)
│   ├── table.py        # InMemoryTable / DiskTable
│   ├── btree.py        # B+Tree (in-memory)
│   ├── storage.py      # HeapStorage (in-memory)
│   ├── page_btree.py   # PageBTree (disk)
│   ├── heap_file.py    # HeapFile (disk)
│   ├── pager.py        # 4KB ページ I/O
│   └── catalog.py      # テーブル定義の JSON 永続化
└── tests/              # 222 tests
```

## ロードマップ

| Phase | テーマ | 状態 |
|-------|--------|------|
| 1 | シングルテーブル + B+Tree インデックス | ✅ 完了 |
| 2 | ディスク永続化（ページ管理） | ✅ 完了 |
| 3 | SQL パーサ（Lexer + Parser + Engine） | ✅ 完了 |
| 4 | AND/OR、ORDER BY、LIMIT、REPL | ✅ 完了 |
| 5 | トランザクション & WAL | 予定 |
| 6 | JOIN & サブクエリ | 予定 |

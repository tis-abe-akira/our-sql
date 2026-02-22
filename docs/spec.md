# OurSQL 仕様書

## 概要

OurSQL は、データベースエンジンの仕組みを「自分の手で作る」学習プロジェクトです。  
B+Tree・ページング・SQLパーサ・REPLを一から実装しています。

---

## ゴール・学習目的

| 問い | OurSQL で得られる答え |
|------|----------------------|
| なぜ主キー検索は速いのか？ | B+Tree を辿るだけだから O(log n) |
| なぜフルスキャンは遅いのか？ | Heap ストレージを端から舐めるから O(n) |
| なぜ INSERT はインデックスがあると重くなるのか？ | データ保存 + B+Tree への挿入・ノード分割が走るから |
| ディスクはどのようにデータを保管するのか？ | 固定サイズのページに行データをスロット管理で書き込む |
| SQL 文はどのように実行されるのか？ | 字句解析 → 構文解析(AST) → 実行エンジン |

---

## アーキテクチャ

```
SQL 文字列
   │
   ▼
┌──────────┐     token列      ┌──────────┐     AST      ┌───────────┐
│  Lexer   │ ──────────────► │  Parser  │ ───────────► │ SQLEngine │
│(字句解析) │                  │(構文解析) │              │(実行エンジン)│
└──────────┘                  └──────────┘              └─────┬─────┘
                                                              │
                                                        OurSQLDB
                                                       /         \
                                               InMemoryTable   DiskTable
                                               (Phase 1)       (Phase 2)
                                               BPlusTree       PageBTree
                                               HeapStorage     HeapFile
                                                               Pager
```

### コンポーネント一覧

| コンポーネント | ファイル | 責務 |
|--------------|---------|------|
| `Lexer` | `oursql/lexer.py` | SQL文字列→トークン列 |
| `Parser` | `oursql/parser.py` | トークン列→AST（再帰下降） |
| `SQLEngine` | `oursql/engine.py` | ASTを受け取り OurSQLDB を呼ぶ実行エンジン |
| `OurSQLDB` | `oursql/db.py` | DDL管理。in-memory / disk 両モード |
| `InMemoryTable` | `oursql/table.py` | BPlusTree + HeapStorage によるインメモリテーブル |
| `DiskTable` | `oursql/table.py` | PageBTree + HeapFile によるディスクテーブル |
| `BPlusTree` | `oursql/btree.py` | インメモリ B+Tree（主キーインデックス） |
| `HeapStorage` | `oursql/storage.py` | インメモリ行ストレージ（tombstone方式） |
| `PageBTree` | `oursql/page_btree.py` | ディスクB+Tree（各ノード=1ページ） |
| `HeapFile` | `oursql/heap_file.py` | ページ+スロット方式の行ストレージ |
| `Pager` | `oursql/pager.py` | 4KB固定長ページのファイルI/O |
| `Catalog` | `oursql/catalog.py` | テーブル定義を `catalog.json` で永続化 |
| REPL | `oursql/__main__.py` | `python -m oursql` で起動するインタラクティブシェル |

---

## サポートする SQL

```sql
-- DDL
CREATE TABLE users (id INT, name TEXT);
DROP TABLE users;

-- DML
INSERT INTO users VALUES (1, 'Alice');
SELECT * FROM users;
SELECT id, name FROM users WHERE id = 1;
SELECT * FROM users WHERE id > 3 AND id < 8;
SELECT * FROM users WHERE id = 1 OR id = 5;
SELECT * FROM users ORDER BY name DESC;
SELECT * FROM users ORDER BY id ASC LIMIT 10;
UPDATE users SET name = 'Alicia' WHERE id = 1;
DELETE FROM users WHERE id = 3;
```

### WHERE 条件演算子

| 演算子 | 意味 |
|--------|------|
| `=` | 等しい |
| `!=` / `<>` | 等しくない |
| `<` | より小さい |
| `>` | より大きい |
| `<=` | 以下 |
| `>=` | 以上 |

### WHERE の論理結合（優先順位: AND > OR）

```sql
-- AND: id > 2 AND id < 6  →  [3, 4, 5]
-- OR:  id = 1 OR id = 10  →  [1, 10]
-- 混在: id = 1 OR id > 5 AND id < 8  →  id=1 OR (id>5 AND id<8)
```

---

## データモデル

### スキーマ定義

```python
schema = {
    "id":   "int",   # 主キー（必須・先頭カラム）
    "name": "text",
}
```

- 先頭カラムが主キー（固定）
- サポートする型: `INT`（`int`）, `TEXT`（`text`）

### ディスクモードのファイル構成

```
data_dir/
├── catalog.json        ← テーブル定義（スキーマ + btree_order）
└── <table_name>/
    ├── heap.db         ← 行データ（4KBページ列）
    └── pk.idx          ← B+Treeインデックス（4KBページ列）
```

---

## B+Tree 仕様（共通）

### パラメータ

| パラメータ | デフォルト値 | 説明 |
|-----------|------------|------|
| `order` (t) | 4 | 各ノードが持てる最大キー数 = `2t - 1` |

### 操作

| メソッド | 計算量 | 説明 |
|---------|--------|------|
| `insert(key, rid)` | O(log n) | キー挿入。オーバーフローでスプリット |
| `search(key)` | O(log n) | キー検索 |
| `delete(key)` | O(log n) | キー削除。アンダーフローで再配布/マージ |
| `range_scan(start, end)` | O(log n + k) | リーフリンクリストを使った範囲スキャン |

### インメモリ版 vs ディスク版の違い

| | `BPlusTree` | `PageBTree` |
|-|-------------|-------------|
| ストレージ | Python オブジェクト | 4KB ページファイル |
| ポインタ | Python オブジェクト参照 | ページID (int) |
| RID | `int` (HeapStorage の index) | `(page_id, slot_id)` |
| 主キー型 | 任意 | `int` のみ（Phase 2 制約） |

---

## HeapFile / HeapStorage 仕様

### HeapStorage（インメモリ）

```
_data: [row0, None, row2, ...]  ← None = tombstone（削除済み）
```

### HeapFile（ディスク）

**1ページのレイアウト（4096 bytes）：**

```
[0:2]   num_slots    ← スロット数（uint16）
[4:4+n*8]  slot_dir  ← (offset uint32, length uint32) × n
[末尾から] row data   ← JSON エンコードされた行データ（逆方向に積む）
```

| メソッド | 説明 |
|---------|------|
| `insert(row)` | 空きスペースのあるページにJSONで書き込み。RID=(page_id, slot_id)を返す |
| `get(page_id, slot_id)` | RIDから行を取得 |
| `update(page_id, slot_id, row)` | インプレース更新（拡大は不可） |
| `delete(page_id, slot_id)` | tombstone（offset=0, length=0）を設定 |
| `scan()` | 全ページを舐めて有効行を返す |

---

## SQL レイヤー仕様

### Lexer

- 入力: SQL 文字列
- 出力: `list[Token]`
- トークン種別: `KEYWORD` / `IDENT` / `NUMBER` / `STRING` / `SYMBOL` / `EOF`
- `--` から行末はコメントとして無視
- 文字列リテラルは `'...'`。`''` でシングルクォートをエスケープ

### Parser

再帰下降パーサ。文法（重要部分）：

```
condition  = and_cond (OR and_cond)*
and_cond   = predicate (AND predicate)*
predicate  = IDENT op literal
select     = SELECT col_list FROM IDENT
             [WHERE condition]
             [ORDER BY IDENT [ASC|DESC]]
             [LIMIT NUMBER]
```

ASTノード: `SelectStmt`, `InsertStmt`, `UpdateStmt`, `DeleteStmt`, `CreateTableStmt`, `DropTableStmt`, `Predicate`, `AndCondition`, `OrCondition`

### SQLEngine の実行最適化

- `WHERE pk = value` → B+Tree のインデックスルックアップを使用（O(log n)）
- それ以外の WHERE → フルスキャン後にフィルタリング（O(n)）
- `ORDER BY` → Python の `list.sort()` をインメモリで実行
- `LIMIT` → ソート後にスライス

---

## OurSQLDB の2モード

```python
# インメモリモード（Phase 1 互換）
db = OurSQLDB()

# ディスク永続モード（Phase 2）
db = OurSQLDB("./data")

# context manager で使うと自動で close()
with OurSQLDB("./data") as db:
    ...
```

---

## ディレクトリ構成

```
our-sql/
├── docs/
│   └── spec.md             # この仕様書
├── oursql/
│   ├── __main__.py         # REPL (python -m oursql)
│   ├── __init__.py
│   ├── lexer.py            # Lexer: SQL文字列 → トークン列
│   ├── parser.py           # Parser: トークン列 → AST
│   ├── engine.py           # SQLEngine: AST → 実行
│   ├── db.py               # OurSQLDB: DDL + デュアルモード
│   ├── table.py            # InMemoryTable / DiskTable
│   ├── btree.py            # BPlusTree (in-memory)
│   ├── storage.py          # HeapStorage (in-memory)
│   ├── page_btree.py       # PageBTree (disk)
│   ├── heap_file.py        # HeapFile (disk)
│   ├── pager.py            # Pager: 4KBページI/O
│   └── catalog.py          # Catalog: テーブル定義の永続化
└── tests/
    ├── test_btree.py
    ├── test_storage.py
    ├── test_table.py
    ├── test_db.py
    ├── test_pager.py
    ├── test_heap_file.py
    ├── test_page_btree.py
    ├── test_persistence.py
    ├── test_lexer.py
    ├── test_parser.py
    ├── test_engine.py
    ├── test_engine_disk.py
    └── test_engine_extended.py
```

---

## ロードマップ

| Phase | テーマ | 状態 |
|-------|--------|------|
| 1 | シングルテーブル + B+Tree インデックス | ✅ 完了 |
| 2 | ディスク永続化（ページ・スロット管理） | ✅ 完了 |
| 3 | SQL パーサ（Lexer + Parser + Engine） | ✅ 完了 |
| 4 | AND/OR、ORDER BY、LIMIT、REPL | ✅ 完了 |
| 5 | トランザクション & WAL | 予定 |
| 6 | JOIN & サブクエリ | 予定 |

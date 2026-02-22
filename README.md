# OurSQL

**OurSQL** は、データベースエンジンの仕組みを「自分の手で作る」学習プロジェクトです。  
Phase 1 では **B+Tree 主キーインデックス付きシングルテーブルエンジン** を Python で実装しています。

## セットアップ

```bash
cd our-sql
pip install -e ".[dev]"
```

## Quick Start

```python
from oursql.db import OurSQLDB

db = OurSQLDB()

# テーブル作成（最初のカラムが主キー）
users = db.create_table("users", {"id": "int", "name": "text"})

# INSERT — データ保存 + B+Tree へ登録
users.insert({"id": 1, "name": "Alice"})
users.insert({"id": 2, "name": "Bob"})
users.insert({"id": 3, "name": "Charlie"})

# SELECT — B+Tree を辿って O(log n) で検索
print(users.select(2))      # {"id": 2, "name": "Bob"}

# UPDATE
users.update(1, {"name": "Alicia"})
print(users.select(1))      # {"id": 1, "name": "Alicia"}

# DELETE
users.delete(3)
print(users.select(3))      # None

# SELECT ALL — フルスキャン O(n)
print(users.select_all())   # Alice と Bob の 2 件

# 範囲スキャン — B+Tree のリーフリンクリストを活用
users.insert({"id": 10, "name": "Dave"})
users.insert({"id": 20, "name": "Eve"})
users.insert({"id": 30, "name": "Frank"})
print(users.select_range(10, 20))  # Dave と Eve の 2 件
```

## テスト実行

```bash
pytest -v
```

## アーキテクチャ

```
OurSQLDB
└── OurSQLTable
    ├── BPlusTree  (主キーインデックス: O(log n) 検索・挿入・削除)
    └── HeapStorage (実データ保存: インメモリリスト + tombstone)
```

### なぜ主キー検索は速いのか？

`select(pk)` は **B+Tree を上から辿るだけ** なので O(log n)。  
`select_all()` は HeapStorage を端から舐めるため O(n)（フルスキャン）。

### なぜ INSERT でコストがかかるのか？

データ保存だけでなく、B+Tree へのキー挿入とノード分割（スプリット）が発生するから。

## ディレクトリ構成

```
our-sql/
├── docs/spec.md          # 詳細仕様書
├── oursql/
│   ├── btree.py          # B+Tree
│   ├── storage.py        # HeapStorage
│   ├── table.py          # OurSQLTable
│   └── db.py             # OurSQLDB
└── tests/
    ├── test_btree.py
    ├── test_storage.py
    ├── test_table.py
    └── test_db.py
```

## Phase ロードマップ

| Phase | テーマ |
|-------|--------|
| **1** (現在) | シングルテーブル + B+Tree インデックス |
| 2 | ディスク永続化（ページ管理） |
| 3 | SQL パーサ |
| 4 | クエリオプティマイザ |
| 5 | トランザクション & WAL |
| 6 | マルチテーブル & JOIN |

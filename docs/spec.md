# OurSQL Phase 1 仕様書

## 概要

OurSQL は、データベースエンジンの仕組みを「自分の手で作る」ことで深く理解するための学習プロジェクトです。  
Phase 1 では **シングルテーブル・エンジン** を Python で実装します。  
B+Tree を主キーインデックスとして採用し、INSERT / SELECT / UPDATE / DELETE の基本操作を実現します。

---

## ゴール・学習目的

| 問い | Phase 1 で得られる答え |
|------|----------------------|
| なぜ主キー検索は速いのか？ | B+Tree を辿るだけだから O(log n) |
| なぜフルスキャンは遅いのか？ | Heap ストレージを端から舐めるから O(n) |
| なぜ INSERT はインデックスがあると重くなるのか？ | データ保存 + B+Tree への挿入・ノード分割が走るから |
| 削除はなぜ難しいのか？ | B+Tree のマージ・再配布が必要だから |

---

## アーキテクチャ

```
┌─────────────────────────────────────────┐
│              OurSQLDB                   │  ← DDL: create_table / drop_table
│  ┌───────────────────────────────────┐  │
│  │          OurSQLTable              │  │  ← DML: insert / select / update / delete
│  │  ┌────────────┐  ┌─────────────┐ │  │
│  │  │  BPlusTree │  │ HeapStorage │ │  │
│  │  │ (主キーIdx) │  │ (実データ)  │ │  │
│  │  └────────────┘  └─────────────┘ │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
```

### コンポーネント一覧

| コンポーネント | ファイル | 責務 |
|--------------|---------|------|
| `BPlusTree` | `oursql/btree.py` | 主キーの管理（挿入・検索・削除・範囲スキャン） |
| `HeapStorage` | `oursql/storage.py` | 行データの保存（インメモリリスト + tombstone） |
| `OurSQLTable` | `oursql/table.py` | B+Tree と HeapStorage を組み合わせたテーブル操作 |
| `OurSQLDB` | `oursql/db.py` | 複数テーブルの管理（DDL） |

---

## データモデル

### スキーマ定義

```python
schema = {
    "id":   "int",   # 主キー（必須・先頭カラム）
    "name": "text",
    "age":  "int",
}
```

- 最初のカラムが主キー（PK）固定とする（Phase 1 の制約）
- サポートする型: `int`, `text`（Phase 1）

### 行データ（Row）

```python
# dict 形式で表現
row = {"id": 1, "name": "Alice", "age": 30}
```

---

## B+Tree 仕様

### パラメータ

| パラメータ | デフォルト値 | 説明 |
|-----------|------------|------|
| `order` (t) | 4 | 各ノードが持てる最大キー数 = `2t - 1` |

### ノード構造

```python
class BTreeNode:
    keys: list        # ソート済みキーのリスト
    values: list      # リーフノード時: row_id のリスト / 内部ノード時: 未使用
    children: list    # 内部ノード時: 子ノードへのポインタ
    is_leaf: bool
    next: BTreeNode   # リーフ同士のリンク（範囲スキャン用）
```

### 操作

#### `insert(key, row_id)`
1. ルートから適切なリーフを見つける
2. リーフにキーを挿入
3. ノードがオーバーフローしたら **分割（スプリット）**
4. 分割が根まで伝播した場合、新しい根を生成

#### `search(key) → row_id | None`
1. ルートから下りていき、リーフに到達
2. リーフ内でキーを線形探索 → row_id を返す
3. 見つからない場合は `None`

#### `delete(key)`
1. キーの存在するリーフを特定
2. キーを削除
3. アンダーフロー時は **再配布** または **マージ**
4. マージが根まで伝播した場合、根を更新

#### `range_scan(start_key, end_key) → list[row_id]`
1. `start_key` のリーフを見つける
2. リーフのリンクリストを辿りながら `end_key` まで収集

---

## HeapStorage 仕様

```python
class HeapStorage:
    _data: list[dict | None]  # row_id = インデックス、削除済みは None（tombstone）
```

### 操作

| メソッド | 引数 | 戻り値 | 説明 |
|---------|-----|-------|------|
| `insert(row)` | `dict` | `int` (row_id) | リストに追加して row_id を返す |
| `get(row_id)` | `int` | `dict \| None` | 行を返す（tombstone は None） |
| `update(row_id, row)` | `int, dict` | `None` | 行を上書き |
| `delete(row_id)` | `int` | `None` | tombstone（None）を設定 |
| `scan()` | — | `list[dict]` | 全有効行をリストで返す |

---

## OurSQLTable 仕様

```python
class OurSQLTable:
    def __init__(self, name: str, schema: dict): ...

    def insert(self, row_data: dict) -> int:
        """行を挿入し、row_id を返す"""

    def select(self, pk_value) -> dict | None:
        """主キーで 1 件検索"""

    def select_all(self) -> list[dict]:
        """全件取得（フルスキャン）"""

    def update(self, pk_value, updates: dict) -> bool:
        """主キーで行を特定して更新。成功したら True"""

    def delete(self, pk_value) -> bool:
        """主キーで行を特定して削除。成功したら True"""
```

### insert の流れ

```
insert({"id": 5, "name": "Bob"})
   │
   ├─① HeapStorage.insert(row) → row_id = 3
   │
   └─② BPlusTree.insert(pk=5, row_id=3)
              │
              └─ ノードがあふれたら「パッカーン！」（スプリット）
```

### select の流れ

```
select(pk=5)
   │
   ├─① BPlusTree.search(5) → row_id = 3   ← O(log n)
   │
   └─② HeapStorage.get(3) → {"id":5, "name":"Bob"}
```

### フルスキャンとの比較

```
select_all()
   └─ HeapStorage.scan() → 全行を端から舐める  ← O(n)
```

---

## OurSQLDB 仕様

```python
class OurSQLDB:
    def create_table(self, name: str, schema: dict) -> OurSQLTable:
        """テーブルを作成して返す"""

    def get_table(self, name: str) -> OurSQLTable | None:
        """テーブルを取得"""

    def drop_table(self, name: str) -> bool:
        """テーブルを削除"""
```

---

## ディレクトリ構成

```
our-sql/
├── docs/
│   └── spec.md           # この仕様書
├── oursql/
│   ├── __init__.py
│   ├── btree.py          # B+Tree
│   ├── storage.py        # HeapStorage
│   ├── table.py          # OurSQLTable
│   └── db.py             # OurSQLDB
├── tests/
│   ├── test_btree.py
│   ├── test_storage.py
│   ├── test_table.py
│   └── test_db.py
├── README.md
└── pyproject.toml        # pytest などの依存管理
```

---

## Phase 1 制約事項（スコープ外）

以下は Phase 1 では実装しない（将来の Phase で扱う）

- SQL パーサ（文字列解析）
- ファイルへの永続化（ディスク I/O）
- トランザクション / MVCC
- セカンダリインデックス
- 複合主キー
- 結合（JOIN）

---

## 将来フェーズのロードマップ（概略）

| Phase | テーマ |
|-------|--------|
| 2 | ディスク永続化（ページ・スロット管理） |
| 3 | SQL パーサ（字句解析 → 構文木） |
| 4 | クエリオプティマイザ（コストベース） |
| 5 | トランザクション & WAL |
| 6 | マルチテーブル & JOIN |

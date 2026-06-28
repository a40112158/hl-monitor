#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
One-time migration: local SQLite hl_monitor.db -> Turso/libSQL.

Runs safely in GitHub Actions. It copies schema, indexes, triggers, and data.
Default behavior resets target tables that exist in the source database to avoid duplicates.

Required env:
  TURSO_DATABASE_URL
  TURSO_AUTH_TOKEN
Optional env:
  LOCAL_SQLITE_DB=hl_monitor.db
  TURSO_MIGRATE_RESET=1          # 1 = drop target tables before import; recommended for first migration
  TURSO_MIGRATE_BATCH_SIZE=1000
"""

from __future__ import annotations

import argparse
import gzip
import os
import shutil
import sqlite3
import sys
import time
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple


def log(msg: str) -> None:
    print(msg, flush=True)


def env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def ensure_local_db(path: Path) -> Path:
    if path.exists() and path.stat().st_size > 0:
        return path

    gz_path = Path(str(path) + ".gz")
    if gz_path.exists() and gz_path.stat().st_size > 0:
        log(f"未找到 {path.name}，发现 {gz_path.name}，开始解压...")
        with gzip.open(gz_path, "rb") as src, open(path, "wb") as dst:
            shutil.copyfileobj(src, dst)
        log(f"已解压：{path} ({path.stat().st_size / 1024 / 1024:.2f} MB)")
        return path

    # Common fallbacks
    for candidate in [Path("hl_monitor.db"), Path("hl_monitor.db.gz")]:
        if candidate == path:
            continue
        if candidate.exists() and candidate.stat().st_size > 0:
            if candidate.suffix == ".gz":
                out = Path(candidate.stem)
                log(f"发现 {candidate}，开始解压到 {out}...")
                with gzip.open(candidate, "rb") as src, open(out, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                return out
            return candidate

    raise FileNotFoundError(
        f"找不到本地数据库：{path} 或 {path}.gz。请确认 GitHub 仓库根目录仍有 hl_monitor.db/hl_monitor.db.gz。"
    )


def connect_turso():
    url = os.getenv("TURSO_DATABASE_URL", "").strip()
    token = os.getenv("TURSO_AUTH_TOKEN", "").strip()
    if not url or not token:
        raise RuntimeError("缺少 TURSO_DATABASE_URL 或 TURSO_AUTH_TOKEN，请先在 GitHub Secrets 设置。")
    import libsql  # type: ignore

    log("连接 Turso 外部数据库...")
    return libsql.connect(url, auth_token=token)


def get_source_objects(src: sqlite3.Connection, object_type: str) -> List[Tuple[str, str]]:
    rows = src.execute(
        """
        SELECT name, sql
        FROM sqlite_master
        WHERE type = ?
          AND name NOT LIKE 'sqlite_%'
          AND sql IS NOT NULL
        ORDER BY name
        """,
        (object_type,),
    ).fetchall()
    return [(str(r[0]), str(r[1])) for r in rows]


def table_columns(src: sqlite3.Connection, table: str) -> List[str]:
    rows = src.execute(f"PRAGMA table_info({quote_ident(table)})").fetchall()
    return [str(r[1]) for r in rows]


def count_rows(conn, table: str) -> int:
    cur = conn.execute(f"SELECT COUNT(*) FROM {quote_ident(table)}")
    row = cur.fetchone()
    return int(row[0] if row else 0)


def execute_many(conn, sql: str, params: Sequence[Sequence[object]]) -> None:
    if not params:
        return
    if hasattr(conn, "executemany"):
        conn.executemany(sql, params)
    else:
        for p in params:
            conn.execute(sql, p)


def safe_execute(conn, sql: str, label: str = "") -> None:
    try:
        conn.execute(sql)
    except Exception as exc:
        prefix = f"{label}: " if label else ""
        raise RuntimeError(f"执行 SQL 失败：{prefix}{exc}\nSQL: {sql[:500]}") from exc


def copy_table(src: sqlite3.Connection, dst, table: str, batch_size: int) -> int:
    cols = table_columns(src, table)
    if not cols:
        log(f"  - {table}: 无列，跳过数据复制")
        return 0

    q_table = quote_ident(table)
    q_cols = ", ".join(quote_ident(c) for c in cols)
    placeholders = ", ".join(["?"] * len(cols))
    insert_sql = f"INSERT INTO {q_table} ({q_cols}) VALUES ({placeholders})"
    select_sql = f"SELECT {q_cols} FROM {q_table}"

    cur = src.execute(select_sql)
    total = 0
    t0 = time.time()

    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        payload = [tuple(r) for r in rows]
        execute_many(dst, insert_sql, payload)
        try:
            dst.commit()
        except Exception:
            pass
        total += len(payload)
        if total % max(batch_size * 10, 10000) == 0:
            log(f"  - {table}: 已复制 {total:,} 行...")

    elapsed = time.time() - t0
    log(f"  - {table}: 完成 {total:,} 行，用时 {elapsed:.1f}s")
    return total


def verify_counts(src: sqlite3.Connection, dst, tables: List[str]) -> bool:
    log("开始校验表行数...")
    ok = True
    for table in tables:
        try:
            src_count = count_rows(src, table)
            dst_count = count_rows(dst, table)
            status = "OK" if src_count == dst_count else "MISMATCH"
            log(f"  - {table}: source={src_count:,}, turso={dst_count:,} [{status}]")
            if src_count != dst_count:
                ok = False
        except Exception as exc:
            ok = False
            log(f"  - {table}: 校验失败：{exc}")
    return ok


def migrate(args: argparse.Namespace) -> int:
    local_db = ensure_local_db(Path(os.getenv("LOCAL_SQLITE_DB", "hl_monitor.db")))
    reset = env_bool("TURSO_MIGRATE_RESET", True)
    batch_size = int(os.getenv("TURSO_MIGRATE_BATCH_SIZE", "1000"))

    log(f"本地 SQLite：{local_db} ({local_db.stat().st_size / 1024 / 1024:.2f} MB)")
    log(f"目标 Turso：{os.getenv('TURSO_DATABASE_URL', '').split('?')[0]}")
    log(f"重置目标表 TURSO_MIGRATE_RESET={int(reset)}")
    log(f"批量大小 TURSO_MIGRATE_BATCH_SIZE={batch_size}")

    src = sqlite3.connect(f"file:{local_db}?mode=ro", uri=True)
    src.row_factory = sqlite3.Row
    dst = connect_turso()

    tables_with_sql = get_source_objects(src, "table")
    indexes_with_sql = get_source_objects(src, "index")
    triggers_with_sql = get_source_objects(src, "trigger")
    views_with_sql = get_source_objects(src, "view")
    tables = [name for name, _ in tables_with_sql]

    if not tables:
        raise RuntimeError("本地 SQLite 没有找到可迁移的表。")

    log(f"发现 {len(tables)} 张表：{', '.join(tables[:12])}{' ...' if len(tables) > 12 else ''}")

    if args.verify_only:
        return 0 if verify_counts(src, dst, tables) else 2

    if reset:
        log("开始清空 Turso 目标库里同名表/视图/触发器，避免重复数据...")
        # Drop views/triggers/indexes first, then tables.
        for name, _ in reversed(views_with_sql):
            try:
                dst.execute(f"DROP VIEW IF EXISTS {quote_ident(name)}")
            except Exception as exc:
                log(f"  - DROP VIEW {name} 警告：{exc}")
        for name, _ in reversed(triggers_with_sql):
            try:
                dst.execute(f"DROP TRIGGER IF EXISTS {quote_ident(name)}")
            except Exception as exc:
                log(f"  - DROP TRIGGER {name} 警告：{exc}")
        for name, _ in reversed(indexes_with_sql):
            try:
                dst.execute(f"DROP INDEX IF EXISTS {quote_ident(name)}")
            except Exception as exc:
                log(f"  - DROP INDEX {name} 警告：{exc}")
        for name in reversed(tables):
            try:
                dst.execute(f"DROP TABLE IF EXISTS {quote_ident(name)}")
            except Exception as exc:
                log(f"  - DROP TABLE {name} 警告：{exc}")
        try:
            dst.commit()
        except Exception:
            pass

    log("创建表结构...")
    for name, sql in tables_with_sql:
        safe_execute(dst, sql, f"CREATE TABLE {name}")
    try:
        dst.commit()
    except Exception:
        pass

    log("复制表数据...")
    total_rows = 0
    for table in tables:
        total_rows += copy_table(src, dst, table, batch_size)

    log("创建索引...")
    for name, sql in indexes_with_sql:
        try:
            safe_execute(dst, sql, f"CREATE INDEX {name}")
        except RuntimeError as exc:
            # If reset=false, indexes may already exist. Show warning but continue.
            log(f"  - 索引 {name} 跳过/警告：{exc}")
    try:
        dst.commit()
    except Exception:
        pass

    if views_with_sql:
        log("创建视图...")
        for name, sql in views_with_sql:
            try:
                safe_execute(dst, sql, f"CREATE VIEW {name}")
            except RuntimeError as exc:
                log(f"  - 视图 {name} 跳过/警告：{exc}")
    if triggers_with_sql:
        log("创建触发器...")
        for name, sql in triggers_with_sql:
            try:
                safe_execute(dst, sql, f"CREATE TRIGGER {name}")
            except RuntimeError as exc:
                log(f"  - 触发器 {name} 跳过/警告：{exc}")
    try:
        dst.commit()
    except Exception:
        pass

    log(f"数据复制完成，总计 {total_rows:,} 行。")
    ok = verify_counts(src, dst, tables)
    if not ok:
        log("迁移完成但校验不一致，请不要删除 GitHub 旧数据库。")
        return 2

    log("迁移成功：Turso 表行数与本地 SQLite 一致。")
    log("下一步：手动 Run monitor workflow，看到“使用 Turso 外部数据库”后，再删除 GitHub 里的 hl_monitor.db/hl_monitor.db.gz。")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify-only", action="store_true", help="只校验 Turso 与本地 SQLite 行数，不复制数据")
    args = parser.parse_args()
    try:
        return migrate(args)
    except Exception as exc:
        log(f"迁移失败：{exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

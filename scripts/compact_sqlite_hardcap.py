#!/usr/bin/env python3
"""Hard-cap SQLite database size for GitHub storage.

Keeps only recent raw snapshots, preserves open lifecycle/trade rows, and VACUUMs.
Designed for hl_monitor.db before gzip commit.
"""
import argparse
import datetime as dt
import os
import sqlite3
from pathlib import Path

RAW_TABLES = [
    "wallet_states",
    "perp_positions",
    "spot_balances",
    "coin_signals",
    "market_context",
    "coin_risk_metrics",
    "wallet_quality",
    "wallet_position_performance",
]
EVENT_TABLES = [
    "wallet_actions",
    "coin_flow_snapshots",
    "position_trade_events",
    "signal_lifecycle_events",
]
SIGNAL_EVENT_TABLES = ["signal_events", "longterm_events"]


def mb(path: Path) -> float:
    return path.stat().st_size / 1024 / 1024 if path.exists() else 0.0


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def table_exists(cur: sqlite3.Cursor, table: str) -> bool:
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None


def col_exists(cur: sqlite3.Cursor, table: str, col: str) -> bool:
    if not table_exists(cur, table):
        return False
    cur.execute(f"PRAGMA table_info({table})")
    return any(r[1] == col for r in cur.fetchall())


def execute_delete(cur: sqlite3.Cursor, sql: str, params: tuple = ()) -> int:
    """Execute a DELETE without costly COUNT(*) scans before and after it."""
    try:
        cur.execute(sql, params)
        return max(0, int(cur.rowcount or 0))
    except sqlite3.OperationalError:
        return 0


def delete_old_by_created_at(cur: sqlite3.Cursor, table: str, cutoff: str) -> int:
    if not col_exists(cur, table, "created_at"):
        return 0
    return execute_delete(cur, f"DELETE FROM {table} WHERE created_at < ?", (cutoff,))


def delete_old_by_column(cur: sqlite3.Cursor, table: str, column: str, cutoff: str) -> int:
    if not col_exists(cur, table, column):
        return 0
    return execute_delete(cur, f"DELETE FROM {table} WHERE {column} < ?", (cutoff,))


def free_space(conn: sqlite3.Connection) -> tuple[float, float]:
    page_count = int(conn.execute("PRAGMA page_count").fetchone()[0] or 0)
    free_pages = int(conn.execute("PRAGMA freelist_count").fetchone()[0] or 0)
    page_size = int(conn.execute("PRAGMA page_size").fetchone()[0] or 0)
    free_mb = free_pages * page_size / 1024 / 1024
    free_ratio = free_pages / page_count if page_count else 0.0
    return free_mb, free_ratio


def compact(db_path: Path, raw_keep: int, history_days: int, final_reports_days: int, hard: bool) -> None:
    if not db_path.exists():
        print(f"[hardcap] db not found: {db_path}")
        return

    raw_keep = max(2, int(raw_keep))
    history_days = max(30, int(history_days))
    final_reports_days = max(1, int(final_reports_days))

    cutoff = (utc_now() - dt.timedelta(days=history_days)).strftime("%Y-%m-%d %H:%M:%S")
    signal_keep_days = max(60, int(os.getenv("SIGNAL_EVENT_KEEP_DAYS", "90")))
    signal_cutoff = (utc_now() - dt.timedelta(days=signal_keep_days)).strftime("%Y-%m-%d %H:%M:%S")
    final_cutoff = (utc_now() - dt.timedelta(days=final_reports_days)).strftime("%Y-%m-%d %H:%M:%S")

    before_mb = mb(db_path)
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    try:
        if table_exists(cur, "runs") and col_exists(cur, "runs", "run_id"):
            cur.execute("SELECT run_id FROM runs ORDER BY run_id DESC LIMIT ?", (raw_keep,))
            keep_ids = [int(r[0]) for r in cur.fetchall()]
            cutoff_run = min(keep_ids) if keep_ids else None
        else:
            cutoff_run = None

        print(f"[hardcap] start db={before_mb:.2f}MB raw_keep={raw_keep} cutoff_run={cutoff_run} history_days={history_days}", flush=True)

        for t in RAW_TABLES:
            if cutoff_run is not None and col_exists(cur, t, "run_id"):
                deleted = execute_delete(cur, f"DELETE FROM {t} WHERE run_id < ?", (cutoff_run,))
                if deleted:
                    print(f"[hardcap] {t}: deleted {deleted} old rows", flush=True)

        for t in EVENT_TABLES:
            deleted = delete_old_by_created_at(cur, t, cutoff)
            if deleted:
                print(f"[hardcap] {t}: deleted {deleted} old rows", flush=True)

        for t in SIGNAL_EVENT_TABLES:
            deleted = delete_old_by_created_at(cur, t, signal_cutoff)
            if deleted:
                print(f"[hardcap] {t}: deleted {deleted} rows older than {signal_keep_days}d", flush=True)

        # final_reports can contain large text; reports files are already kept in GitHub, so DB copies can be short-lived.
        deleted = delete_old_by_created_at(cur, "final_reports", final_cutoff)
        if deleted:
            print(f"[hardcap] final_reports: deleted {deleted} old rows", flush=True)

        # Closed lifecycle/trade rows older than the history window can be pruned; open rows are preserved.
        if table_exists(cur, "position_trades") and col_exists(cur, "position_trades", "status") and col_exists(cur, "position_trades", "close_time"):
            deleted = execute_delete(cur, "DELETE FROM position_trades WHERE status='closed' AND close_time IS NOT NULL AND close_time < ?", (cutoff,))
            if deleted:
                print(f"[hardcap] position_trades: deleted {deleted} old rows", flush=True)

        if table_exists(cur, "signal_lifecycles") and col_exists(cur, "signal_lifecycles", "status") and col_exists(cur, "signal_lifecycles", "exit_time"):
            deleted = execute_delete(cur, "DELETE FROM signal_lifecycles WHERE status='closed' AND exit_time IS NOT NULL AND exit_time < ?", (signal_cutoff,))
            if deleted:
                print(f"[hardcap] signal_lifecycles: deleted {deleted} old rows", flush=True)

        # Optional hard mode: keep run index only for recent raw snapshots + history window.
        if hard and cutoff_run is not None and table_exists(cur, "runs") and col_exists(cur, "runs", "run_id") and col_exists(cur, "runs", "started_at"):
            deleted = execute_delete(cur, "DELETE FROM runs WHERE run_id < ? AND started_at < ?", (cutoff_run, cutoff))
            if deleted:
                print(f"[hardcap] runs: deleted {deleted} old rows", flush=True)

        if table_exists(cur, "push_log") and col_exists(cur, "push_log", "pushed_at"):
            push_cutoff = (utc_now() - dt.timedelta(days=14)).strftime("%Y-%m-%d %H:%M:%S")
            deleted = delete_old_by_column(cur, "push_log", "pushed_at", push_cutoff)
            if deleted:
                print(f"[hardcap] push_log: deleted {deleted} old rows", flush=True)

        conn.commit()
    finally:
        conn.close()

    # Checkpoint every run, but only rewrite the whole database when worthwhile.
    # Emergency --hard mode always VACUUMs before the final gzip size check.
    conn = sqlite3.connect(str(db_path))
    try:
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            pass
        free_mb, free_ratio = free_space(conn)
        min_free_mb = max(0.0, float(os.getenv("HARDCAP_VACUUM_MIN_FREE_MB", "16")))
        min_free_ratio = max(0.0, float(os.getenv("HARDCAP_VACUUM_MIN_FREE_RATIO", "0.10")))
        should_vacuum = hard or (free_mb >= min_free_mb and free_ratio >= min_free_ratio)
        if should_vacuum:
            conn.execute("VACUUM")
            print(f"[hardcap] VACUUM complete free={free_mb:.2f}MB ({free_ratio:.1%})", flush=True)
        else:
            print(f"[hardcap] VACUUM skipped free={free_mb:.2f}MB ({free_ratio:.1%})", flush=True)
    finally:
        conn.close()

    after_mb = mb(db_path)
    print(f"[hardcap] done db={before_mb:.2f}MB -> {after_mb:.2f}MB", flush=True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=os.getenv("HL_DB_FILE", "hl_monitor.db"))
    p.add_argument("--raw-keep", type=int, default=int(os.getenv("HARDCAP_RAW_KEEP", "12")))
    p.add_argument("--history-days", type=int, default=int(os.getenv("HARDCAP_HISTORY_DAYS", "35")))
    p.add_argument("--final-reports-days", type=int, default=int(os.getenv("HARDCAP_FINAL_REPORTS_DAYS", "7")))
    p.add_argument("--hard", action="store_true")
    args = p.parse_args()
    compact(Path(args.db), args.raw_keep, args.history_days, args.final_reports_days, args.hard)


if __name__ == "__main__":
    main()

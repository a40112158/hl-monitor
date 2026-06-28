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
    "signal_events",
    "longterm_events",
    "position_trade_events",
    "signal_lifecycle_events",
]


def mb(path: Path) -> float:
    return path.stat().st_size / 1024 / 1024 if path.exists() else 0.0


def table_exists(cur: sqlite3.Cursor, table: str) -> bool:
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None


def col_exists(cur: sqlite3.Cursor, table: str, col: str) -> bool:
    if not table_exists(cur, table):
        return False
    cur.execute(f"PRAGMA table_info({table})")
    return any(r[1] == col for r in cur.fetchall())


def count_rows(cur: sqlite3.Cursor, table: str) -> int:
    if not table_exists(cur, table):
        return 0
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return int(cur.fetchone()[0] or 0)
    except Exception:
        return 0


def delete_old_by_created_at(cur: sqlite3.Cursor, table: str, cutoff: str) -> int:
    if not col_exists(cur, table, "created_at"):
        return 0
    before = count_rows(cur, table)
    try:
        cur.execute(f"DELETE FROM {table} WHERE created_at < ?", (cutoff,))
    except sqlite3.OperationalError:
        return 0
    after = count_rows(cur, table)
    return max(0, before - after)


def compact(db_path: Path, raw_keep: int, history_days: int, final_reports_days: int, hard: bool) -> None:
    if not db_path.exists():
        print(f"[hardcap] db not found: {db_path}")
        return

    raw_keep = max(2, int(raw_keep))
    history_days = max(30, int(history_days))
    final_reports_days = max(1, int(final_reports_days))

    cutoff = (dt.datetime.utcnow() - dt.timedelta(days=history_days)).strftime("%Y-%m-%d %H:%M:%S")
    final_cutoff = (dt.datetime.utcnow() - dt.timedelta(days=final_reports_days)).strftime("%Y-%m-%d %H:%M:%S")

    before_mb = mb(db_path)
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    try:
        if table_exists(cur, "runs") and col_exists(cur, "runs", "run_id"):
            cur.execute("SELECT run_id FROM runs ORDER BY run_id DESC LIMIT ?", (raw_keep,))
            keep_ids = [int(r[0]) for r in cur.fetchall()]
            cutoff_run = min(keep_ids) if keep_ids else 10**18
        else:
            cutoff_run = 10**18

        print(f"[hardcap] start db={before_mb:.2f}MB raw_keep={raw_keep} cutoff_run={cutoff_run} history_days={history_days}", flush=True)

        for t in RAW_TABLES:
            if col_exists(cur, t, "run_id"):
                before = count_rows(cur, t)
                cur.execute(f"DELETE FROM {t} WHERE run_id < ?", (cutoff_run,))
                after = count_rows(cur, t)
                if before != after:
                    print(f"[hardcap] {t}: {before}->{after}", flush=True)

        for t in EVENT_TABLES:
            deleted = delete_old_by_created_at(cur, t, cutoff)
            if deleted:
                print(f"[hardcap] {t}: deleted {deleted} old rows", flush=True)

        # final_reports can contain large text; reports files are already kept in GitHub, so DB copies can be short-lived.
        deleted = delete_old_by_created_at(cur, "final_reports", final_cutoff)
        if deleted:
            print(f"[hardcap] final_reports: deleted {deleted} old rows", flush=True)

        # Closed lifecycle/trade rows older than the history window can be pruned; open rows are preserved.
        if table_exists(cur, "position_trades") and col_exists(cur, "position_trades", "status") and col_exists(cur, "position_trades", "close_time"):
            before = count_rows(cur, "position_trades")
            cur.execute("DELETE FROM position_trades WHERE status='closed' AND close_time IS NOT NULL AND close_time < ?", (cutoff,))
            after = count_rows(cur, "position_trades")
            if before != after:
                print(f"[hardcap] position_trades: {before}->{after}", flush=True)

        if table_exists(cur, "signal_lifecycles") and col_exists(cur, "signal_lifecycles", "status") and col_exists(cur, "signal_lifecycles", "exit_time"):
            before = count_rows(cur, "signal_lifecycles")
            cur.execute("DELETE FROM signal_lifecycles WHERE status='closed' AND exit_time IS NOT NULL AND exit_time < ?", (cutoff,))
            after = count_rows(cur, "signal_lifecycles")
            if before != after:
                print(f"[hardcap] signal_lifecycles: {before}->{after}", flush=True)

        # Optional hard mode: keep run index only for recent raw snapshots + history window.
        if hard and table_exists(cur, "runs") and col_exists(cur, "runs", "run_id") and col_exists(cur, "runs", "started_at"):
            before = count_rows(cur, "runs")
            cur.execute("DELETE FROM runs WHERE run_id < ? AND started_at < ?", (cutoff_run, cutoff))
            after = count_rows(cur, "runs")
            if before != after:
                print(f"[hardcap] runs: {before}->{after}", flush=True)

        if table_exists(cur, "push_log") and col_exists(cur, "push_log", "created_at"):
            push_cutoff = (dt.datetime.utcnow() - dt.timedelta(days=14)).strftime("%Y-%m-%d %H:%M:%S")
            deleted = delete_old_by_created_at(cur, "push_log", push_cutoff)
            if deleted:
                print(f"[hardcap] push_log: deleted {deleted} old rows", flush=True)

        conn.commit()
    finally:
        conn.close()

    # Reclaim space.
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except Exception:
        pass
    try:
        conn.execute("VACUUM")
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

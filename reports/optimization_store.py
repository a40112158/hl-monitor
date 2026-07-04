#!/usr/bin/env python3
"""Compact, long-lived samples for threshold optimization.

One independent signal per coin/direction/gap is retained. This preserves
months of learning data without keeping half-hour raw wallet snapshots.
"""

import bisect
import datetime as dt
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None, microsecond=0)


def parse_time(value: Any) -> Optional[dt.datetime]:
    if not value:
        return None
    text = str(value).replace("T", " ").replace("Z", "").split("+")[0]
    try:
        return dt.datetime.fromisoformat(text)
    except ValueError:
        return None


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    if not table_exists(conn, table):
        return False
    return any(row[1] == column for row in conn.execute(f"PRAGMA table_info({table})"))


def init_store(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS optimization_samples (
            sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_event_id INTEGER NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            coin TEXT NOT NULL,
            direction TEXT NOT NULL,
            abs_score REAL NOT NULL,
            market_regime TEXT NOT NULL DEFAULT 'unknown',
            ret_24h REAL,
            ret_72h REAL,
            ret_7d REAL,
            archived_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_opt_samples_coin_dir_time "
        "ON optimization_samples(coin, direction, created_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_opt_samples_created "
        "ON optimization_samples(created_at)"
    )


def _source_rows(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    if not table_exists(conn, "signal_events"):
        return []
    has_run_id = column_exists(conn, "signal_events", "run_id")
    if table_exists(conn, "market_context") and has_run_id:
        sql = """
            SELECT s.event_id, s.run_id, s.created_at, s.coin, s.direction, s.score,
                   s.ret_24h, s.ret_72h, s.ret_7d,
                   COALESCE(m.regime, 'unknown') AS market_regime
            FROM signal_events s
            LEFT JOIN market_context m ON m.run_id=s.run_id AND m.coin=s.coin
            ORDER BY s.created_at, s.event_id
        """
    else:
        sql = """
            SELECT event_id, 0 AS run_id, created_at, coin, direction, score,
                   ret_24h, ret_72h, ret_7d, 'unknown' AS market_regime
            FROM signal_events
            ORDER BY created_at, event_id
        """
    return conn.execute(sql).fetchall()


def archive_samples(
    db_path: Path,
    *,
    retention_days: int = 180,
    gap_hours: float = 24.0,
) -> Dict[str, int]:
    if not db_path.exists():
        return {"inserted": 0, "updated": 0, "deleted": 0, "total": 0}
    retention_days = max(35, int(retention_days))
    gap_hours = max(1.0, float(gap_hours))
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    inserted = updated = deleted = 0
    try:
        init_store(conn)
        existing = {
            int(row["source_event_id"]): dict(row)
            for row in conn.execute(
                "SELECT source_event_id, coin, direction, created_at, market_regime, "
                "ret_24h, ret_72h, ret_7d FROM optimization_samples"
            ).fetchall()
        }
        times: Dict[Tuple[str, str], List[dt.datetime]] = {}
        for row in existing.values():
            created = parse_time(row.get("created_at"))
            if created is not None:
                times.setdefault((str(row["coin"]), str(row["direction"])), []).append(created)
        for values in times.values():
            values.sort()

        for row in _source_rows(conn):
            event_id = int(row["event_id"])
            created = parse_time(row["created_at"])
            coin = str(row["coin"] or "").upper().strip()
            direction = str(row["direction"] or "unknown")
            try:
                score = abs(float(row["score"]))
            except (TypeError, ValueError):
                continue
            if created is None or not coin:
                continue
            if event_id in existing:
                old = existing[event_id]
                new_regime = str(row["market_regime"] or "unknown")
                needs_update = any(
                    old.get(key) is None and row[key] is not None
                    for key in ("ret_24h", "ret_72h", "ret_7d")
                ) or (old.get("market_regime") == "unknown" and new_regime != "unknown")
                if not needs_update:
                    continue
                cursor = conn.execute(
                    """
                    UPDATE optimization_samples
                    SET ret_24h=COALESCE(?, ret_24h),
                        ret_72h=COALESCE(?, ret_72h),
                        ret_7d=COALESCE(?, ret_7d),
                        market_regime=CASE WHEN market_regime='unknown' THEN ? ELSE market_regime END,
                        archived_at=?
                    WHERE source_event_id=?
                    """,
                    (
                        row["ret_24h"], row["ret_72h"], row["ret_7d"],
                        new_regime, utc_now().isoformat(), event_id,
                    ),
                )
                updated += max(0, int(cursor.rowcount or 0))
                continue

            key = (coin, direction)
            selected_times = times.setdefault(key, [])
            index = bisect.bisect_right(selected_times, created)
            previous = selected_times[index - 1] if index else None
            if previous is not None and (created - previous).total_seconds() < gap_hours * 3600:
                continue
            conn.execute(
                """
                INSERT INTO optimization_samples (
                    source_event_id, created_at, coin, direction, abs_score,
                    market_regime, ret_24h, ret_72h, ret_7d, archived_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id, str(row["created_at"]), coin, direction, score,
                    str(row["market_regime"] or "unknown"), row["ret_24h"],
                    row["ret_72h"], row["ret_7d"], utc_now().isoformat(),
                ),
            )
            bisect.insort(selected_times, created)
            existing[event_id] = {
                "source_event_id": event_id, "coin": coin,
                "direction": direction, "created_at": str(row["created_at"]),
            }
            inserted += 1

        cutoff = (utc_now() - dt.timedelta(days=retention_days)).strftime("%Y-%m-%d %H:%M:%S")
        cursor = conn.execute("DELETE FROM optimization_samples WHERE created_at < ?", (cutoff,))
        deleted = max(0, int(cursor.rowcount or 0))
        conn.commit()
        total = int(conn.execute("SELECT COUNT(*) FROM optimization_samples").fetchone()[0] or 0)
        return {"inserted": inserted, "updated": updated, "deleted": deleted, "total": total}
    finally:
        conn.close()


def load_samples(db_path: Path) -> List[Dict[str, Any]]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        if not table_exists(conn, "optimization_samples"):
            return []
        rows = conn.execute(
            """
            SELECT source_event_id AS event_id, created_at, coin, direction,
                   abs_score, market_regime, ret_24h, ret_72h, ret_7d
            FROM optimization_samples
            ORDER BY created_at, source_event_id
            """
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()

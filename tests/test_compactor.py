import datetime as dt
import importlib.util
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "compact_sqlite_hardcap", ROOT / "scripts" / "compact_sqlite_hardcap.py"
)
compactor = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(compactor)


class CompactDatabaseTests(unittest.TestCase):
    def test_compact_keeps_recent_snapshots_and_events(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "monitor.db"
            conn = sqlite3.connect(db)
            conn.executescript(
                """
                CREATE TABLE runs (run_id INTEGER PRIMARY KEY, started_at TEXT);
                CREATE TABLE wallet_states (run_id INTEGER, address TEXT);
                CREATE TABLE wallet_actions (created_at TEXT, address TEXT);
                """
            )
            now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
            for run_id in (1, 2, 3):
                conn.execute("INSERT INTO runs VALUES (?, ?)", (run_id, now.strftime("%Y-%m-%d %H:%M:%S")))
                conn.execute("INSERT INTO wallet_states VALUES (?, ?)", (run_id, f"wallet-{run_id}"))
            conn.execute(
                "INSERT INTO wallet_actions VALUES (?, 'old')",
                ((now - dt.timedelta(days=90)).strftime("%Y-%m-%d %H:%M:%S"),),
            )
            conn.execute(
                "INSERT INTO wallet_actions VALUES (?, 'recent')",
                (now.strftime("%Y-%m-%d %H:%M:%S"),),
            )
            conn.commit()
            conn.close()

            env = {
                "HARDCAP_VACUUM_MIN_FREE_MB": "999999",
                "HARDCAP_VACUUM_MIN_FREE_RATIO": "1",
            }
            with mock.patch.dict(os.environ, env):
                compactor.compact(db, raw_keep=2, history_days=30, final_reports_days=7, hard=False)

            conn = sqlite3.connect(db)
            try:
                self.assertEqual(
                    [row[0] for row in conn.execute("SELECT run_id FROM wallet_states ORDER BY run_id")],
                    [2, 3],
                )
                self.assertEqual(
                    [row[0] for row in conn.execute("SELECT address FROM wallet_actions ORDER BY address")],
                    ["recent"],
                )
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()

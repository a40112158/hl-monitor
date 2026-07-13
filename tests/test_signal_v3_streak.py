import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import hl_monitor_final as monitor


class SignalV3StreakTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name) / "monitor.db"
        self.patch = mock.patch.multiple(monitor, DB_FILE=str(self.db), USE_TURSO=False)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        monitor.init_db()

    def seed(self, present_runs):
        conn = sqlite3.connect(self.db)
        try:
            for run_id in (1, 2, 3):
                conn.execute(
                    "INSERT INTO runs(run_id, started_at, snapshot_complete, price_data_ok) VALUES (?, ?, 1, 1)",
                    (run_id, f"2026-07-01 0{run_id}:00:00"),
                )
            for run_id in present_runs:
                conn.execute(
                    """
                    INSERT INTO coin_signals(run_id, coin, direction, long_score, model_version)
                    VALUES (?, 'BTC', 'bullish', 9, ?)
                    """,
                    (run_id, monitor.SIGNAL_MODEL_VERSION),
                )
            conn.commit()
        finally:
            conn.close()

    def test_missing_healthy_round_breaks_long_signal_streak(self):
        self.seed((1, 3))
        self.assertEqual(
            monitor.signal_streak("BTC", "bullish", 3, min_abs_score=5, score_field="long_score"),
            1,
        )

    def test_contiguous_healthy_rounds_count_normally(self):
        self.seed((1, 2, 3))
        self.assertEqual(
            monitor.signal_streak("BTC", "bullish", 3, min_abs_score=5, score_field="long_score"),
            3,
        )


if __name__ == "__main__":
    unittest.main()

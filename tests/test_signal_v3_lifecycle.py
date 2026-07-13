import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import hl_monitor_final as monitor


class SignalV3LifecycleContractTests(unittest.TestCase):
    """Contract for one non-overlapping trade lifecycle per continuous signal.

    V3 deliberately keeps the existing ``status='open'/'closed'`` convention so
    old reports and pruning queries remain compatible.  The additive
    ``lifecycle_state`` field describes the process (active/grace/closed), while
    ``exit_type`` distinguishes a normal invalidation, reversal, and expiry.

    MFE/MAE are directional gross excursions from entry: MFE is the greatest
    observed return (never below zero) and MAE is the smallest observed return
    (never above zero).  Fees belong to realized/net return reporting, not to
    path excursion metrics.
    """

    REQUIRED_V3_COLUMNS = {
        "lifecycle_state",
        "mark_return_pct",
        "mfe_pct",
        "mae_pct",
        "expires_at",
        "exit_type",
    }

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db = self.root / "monitor.db"
        self.patch = mock.patch.multiple(
            monitor,
            DB_FILE=str(self.db),
            USE_TURSO=False,
            REPORT_DIR=str(self.root / "reports"),
            DETAILS_DIR=str(self.root / "reports" / "details"),
            SIGNAL_LIFECYCLE_MODE=True,
            DATA_ANOMALY_PROTECT_MODE=False,
        )
        self.patch.start()
        self.addCleanup(self.patch.stop)
        monitor.init_db()

    def rows(self, sql, params=()):
        conn = sqlite3.connect(self.db)
        conn.row_factory = sqlite3.Row
        try:
            return [dict(row) for row in conn.execute(sql, params).fetchall()]
        finally:
            conn.close()

    @staticmethod
    def strong_signal(direction="bullish"):
        score = 9.0 if direction == "bullish" else -9.0
        return {
            "coin": "BTC",
            "direction": direction,
            "alert_score": score,
            "threshold_score": 8.0,
            "alert_threshold_score": 8.0,
            "signal_category": "短线突发异动",
            "candidate_gate": "BLOCK",
            "candidate_state": "WATCH",
            "watchlist": "observe",
            "reason": "v3 lifecycle contract",
        }

    def update(self, run_id, signals, price, observed_at):
        with mock.patch.object(monitor, "now_str", return_value=observed_at):
            return monitor.update_signal_lifecycles(
                run_id,
                signals,
                [],
                {"BTC": price},
                data_quality_ok=True,
            )

    def test_schema_adds_v3_fields_without_replacing_legacy_status(self):
        columns = {row["name"] for row in self.rows("PRAGMA table_info(signal_lifecycles)")}

        self.assertTrue(self.REQUIRED_V3_COLUMNS.issubset(columns))
        self.assertIn("status", columns)
        self.assertIn("lifecycle_return_pct", columns)

    def test_refresh_tracks_directional_mark_mfe_and_mae_on_one_trade(self):
        signal = self.strong_signal("bullish")
        self.update(1, [signal], 100.0, "2026-07-01 00:00:00")
        self.update(2, [signal], 110.0, "2026-07-01 01:00:00")
        self.update(3, [signal], 90.0, "2026-07-01 02:00:00")

        rows = self.rows(
            """
            SELECT status, lifecycle_state, mark_return_pct, mfe_pct, mae_pct,
                   entry_px, last_seen_run_id
            FROM signal_lifecycles
            WHERE lifecycle_type='strong' AND coin='BTC' AND direction='bullish'
            """
        )
        self.assertEqual(len(rows), 1, "a continuous signal is one trade, not one trade per scan")
        row = rows[0]
        self.assertEqual(row["status"], "open")
        self.assertEqual(row["lifecycle_state"], "active")
        self.assertAlmostEqual(row["mark_return_pct"], -10.0)
        self.assertAlmostEqual(row["mfe_pct"], 10.0)
        self.assertAlmostEqual(row["mae_pct"], -10.0)
        self.assertEqual(row["last_seen_run_id"], 3)

    def test_missing_round_enters_grace_and_expiry_is_an_explicit_close(self):
        signal = self.strong_signal("bullish")
        # Tests patch the policy values so expiry behavior is deterministic and
        # does not prescribe production defaults.
        with mock.patch.object(monitor, "STRONG_SIGNAL_MISSING_ROUNDS", 99), mock.patch.object(
            monitor, "STRONG_SIGNAL_MAX_HOLD_HOURS", 2, create=True
        ):
            self.update(1, [signal], 100.0, "2026-07-01 00:00:00")
            self.update(2, [], 105.0, "2026-07-01 01:00:00")
            grace = self.rows(
                "SELECT status, lifecycle_state, missing_count FROM signal_lifecycles"
            )[0]
            self.assertEqual(grace, {
                "status": "open",
                "lifecycle_state": "grace",
                "missing_count": 1,
            })

            closed = self.update(3, [], 106.0, "2026-07-01 03:00:00")

        row = self.rows(
            """
            SELECT status, lifecycle_state, exit_type, exit_time, exit_px,
                   mark_return_pct, mfe_pct, mae_pct
            FROM signal_lifecycles
            """
        )[0]
        self.assertEqual(len(closed), 1)
        self.assertEqual(row["status"], "closed")
        self.assertEqual(row["lifecycle_state"], "closed")
        self.assertEqual(row["exit_type"], "expired")
        self.assertEqual(row["exit_time"], "2026-07-01 03:00:00")
        self.assertEqual(row["exit_px"], 106.0)
        self.assertAlmostEqual(row["mark_return_pct"], 6.0)
        self.assertAlmostEqual(row["mfe_pct"], 6.0)
        self.assertAlmostEqual(row["mae_pct"], 0.0)
        event_types = [
            row["event_type"]
            for row in self.rows(
                "SELECT event_type FROM signal_lifecycle_events ORDER BY id"
            )
        ]
        self.assertEqual(event_types[-1], "expire")

    def test_same_run_retry_is_idempotent_and_open_trade_blocks_new_sample(self):
        signal = self.strong_signal("bullish")
        observed_at = "2026-01-01 00:00:00"
        self.update(1, [signal], 100.0, observed_at)
        # A workflow retry of the same run must not add a second observation.
        self.update(1, [signal], 100.0, observed_at)

        self.assertEqual(self.rows("SELECT COUNT(*) AS n FROM signal_lifecycles")[0]["n"], 1)
        self.assertEqual(self.rows("SELECT COUNT(*) AS n FROM signal_lifecycle_events")[0]["n"], 1)

        # The fixed-horizon sample and the lifecycle represent the same entry.
        # Once that lifecycle is open, elapsed cooldown alone must not admit a
        # duplicate return sample for the still-continuous signal.
        conn = sqlite3.connect(self.db)
        try:
            conn.execute(
                """
                INSERT INTO signal_events(
                    model_version, run_id, created_at, coin, direction, score,
                    entry_px, reason
                ) VALUES (?, 1, ?, 'BTC', 'bullish', 9, 100, 'original entry')
                """,
                (monitor.SIGNAL_MODEL_VERSION, observed_at),
            )
            conn.commit()
        finally:
            conn.close()

        thresholds = {
            "DEFAULT": {
                "alert_score_push": 8.0,
                "score_push": 8.0,
                "min_watch_score": 5.0,
                "perp": 1.0,
                "spot": 1.0,
            }
        }
        with mock.patch.object(
            monitor,
            "utc_now",
            return_value=monitor.dt.datetime(2026, 7, 1, 0, 0, 0),
        ):
            created = monitor.create_signal_events(
                2, [signal], {"BTC": 120.0}, thresholds
            )

        self.assertEqual(created, 0)
        self.assertEqual(self.rows("SELECT COUNT(*) AS n FROM signal_events")[0]["n"], 1)


if __name__ == "__main__":
    unittest.main()

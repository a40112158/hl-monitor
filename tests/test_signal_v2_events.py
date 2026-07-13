import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import hl_monitor_final as monitor


class SignalV2EventContractTests(unittest.TestCase):
    """Contract tests for keeping V2 alert and long-term samples separate."""

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
    def signal(coin, *, direction="bullish", alert=0.0, final=0.0, long_score=0.0, **extra):
        row = {
            "coin": coin,
            "direction": direction,
            "alert_score": alert,
            "final_score": final,
            "long_score": long_score,
            "signal_category": "短线异动观察",
            "candidate_gate": "BLOCK",
            "candidate_state": "WATCH",
            "watchlist": "observe",
            "reason": "v2 contract test",
        }
        row.update(extra)
        return row

    @staticmethod
    def long_candidate(coin, *, gate="PASS", state="CANDIDATE", action="可进入低杠杆长期观察", **extra):
        row = {
            "coin": coin,
            "direction": "bullish",
            "direction_cn": "做多",
            "candidate_gate": gate,
            "candidate_state": state,
            "action": action,
            "long_term_score": 9.0,
            "long_score": 8.5,
            "alert_score": 1.0,
            "streak": 3,
        }
        row.update(extra)
        return row

    def test_signal_events_use_alert_score_and_exclude_formal_long_candidates(self):
        thresholds = {
            "DEFAULT": {
                "min_watch_score": 5.0,
                "score_push": 8.0,
                "perp": 1_000_000.0,
                "spot": 500_000.0,
            }
        }
        signals = [
            # A high final/long score must not turn a weak alert into an alert event.
            self.signal("WEAK", alert=3.0, final=9.0, long_score=9.0),
            # Formal long candidates belong exclusively to longterm_events.
            self.signal(
                "LONG",
                alert=9.0,
                final=9.0,
                long_score=9.0,
                signal_category="长期多单候选",
                candidate_gate="PASS",
                candidate_state="CANDIDATE",
                watchlist="long",
            ),
            # V2 alert admission is based on abs(alert_score), including shorts;
            # final_score deliberately remains below min_watch_score.
            self.signal("ALERT", direction="bearish", alert=-9.0, final=-2.0, long_score=-1.0),
        ]

        created = monitor.create_signal_events(
            1,
            signals,
            {"WEAK": 10.0, "LONG": 20.0, "ALERT": 30.0},
            thresholds,
        )

        self.assertEqual(created, 1)
        rows = self.rows(
            "SELECT coin, direction, score, model_version FROM signal_events ORDER BY event_id"
        )
        self.assertEqual(
            rows,
            [{"coin": "ALERT", "direction": "bearish", "score": -9.0, "model_version": 2}],
        )

    def test_longterm_events_require_formal_candidate_gate_and_do_not_repeat(self):
        conn = sqlite3.connect(self.db)
        try:
            conn.execute(
                """
                INSERT INTO signal_lifecycles (
                    model_version, lifecycle_type, coin, direction, status, entry_run_id,
                    entry_time, entry_px, entry_score, last_seen_run_id,
                    last_seen_at, last_score, missing_count
                ) VALUES (2, 'longterm', 'OPEN', 'bullish', 'open', 1,
                          '2026-01-01 00:00:00', 10, 9, 1,
                          '2026-01-01 00:00:00', 9, 0)
                """
            )
            conn.commit()
        finally:
            conn.close()

        candidates = [
            self.long_candidate("ACCEPT"),
            self.long_candidate("FORMING", state="FORMING"),
            self.long_candidate("BLOCKED", gate="BLOCK"),
            self.long_candidate("OBSERVE", action="只观察，不适合直接做长期单"),
            self.long_candidate("OPEN"),
        ]
        prices = {coin: 10.0 for coin in ("ACCEPT", "FORMING", "BLOCKED", "OBSERVE", "OPEN")}

        first_created = monitor.create_longterm_events(1, candidates, prices)
        # A second run inside the cooldown/open lifecycle must not add ACCEPT again.
        second_created = monitor.create_longterm_events(2, [self.long_candidate("ACCEPT")], prices)

        self.assertEqual(first_created, 1)
        self.assertEqual(second_created, 0)
        rows = self.rows(
            "SELECT coin, direction, score, model_version FROM longterm_events ORDER BY event_id"
        )
        self.assertEqual(
            rows,
            [{"coin": "ACCEPT", "direction": "bullish", "score": 9.0, "model_version": 2}],
        )

    def test_net_return_and_win_rate_apply_cost_to_each_event_first(self):
        gross_returns = [0.10, 0.15, -0.05]
        with mock.patch.object(monitor, "BACKTEST_ROUNDTRIP_COST_PCT", 0.12):
            net_returns = [monitor.net_direction_return(value) for value in gross_returns]

        self.assertEqual([round(value, 8) for value in net_returns], [-0.02, 0.03, -0.17])
        self.assertAlmostEqual(sum(value > 0 for value in net_returns) / len(net_returns), 1 / 3)
        self.assertAlmostEqual(sum(net_returns) / len(net_returns), -0.16 / 3)
        self.assertIsNone(monitor.net_direction_return(None))

    def test_v2_backtest_reports_exclude_legacy_model_rows(self):
        created_at = monitor.now_str()
        for table in ("signal_events", "longterm_events"):
            conn = sqlite3.connect(self.db)
            try:
                conn.executemany(
                    f"""
                    INSERT INTO {table} (
                        run_id, created_at, coin, direction, score, entry_px,
                        ret_24h, ret_72h, model_version, reason
                    ) VALUES (?, ?, ?, 'bullish', 9, 10, ?, ?, ?, ?)
                    """,
                    [
                        (10, created_at, "LEGACY", 50.0, 50.0, 1, "legacy row"),
                        (11, created_at, "V2", 2.0, 3.0, 2, "v2 row"),
                    ],
                )
                conn.commit()
            finally:
                conn.close()

            rows = monitor._export_event_backtest_table(
                table,
                f"{table}_v2_test.csv",
                "V2 contract report",
                days=30,
            )
            self.assertEqual([row["coin"] for row in rows], ["V2"])

    def test_coin_signal_persists_split_thresholds_and_model_version(self):
        monitor.save_coin_signals(1, [{
            "coin": "TEST", "direction": "bullish", "score": 1.0,
            "alert_score": 8.5, "long_score": 6.5, "final_score": 8.5,
            "threshold_score": 8.0, "alert_threshold_score": 8.0,
            "long_threshold_score": 9.0, "model_version": 2,
        }])
        rows = self.rows(
            "SELECT coin, alert_threshold_score, long_threshold_score, model_version FROM coin_signals"
        )
        self.assertEqual(rows, [{
            "coin": "TEST", "alert_threshold_score": 8.0,
            "long_threshold_score": 9.0, "model_version": 2,
        }])


if __name__ == "__main__":
    unittest.main()

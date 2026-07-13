import csv
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import hl_monitor_final as monitor


class SignalV3DashboardContractTests(unittest.TestCase):
    """User-facing contract for the V3 return dashboard.

    The detailed CSV and its text companion form one dashboard.  Alert and
    long-term events intentionally expose different horizons; V2 metrics are
    split by direction; and legacy V1 rows are reference-only.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db = self.root / "monitor.db"
        self.details = self.root / "reports" / "details"
        self.patch = mock.patch.multiple(
            monitor,
            DB_FILE=str(self.db),
            USE_TURSO=False,
            REPORT_DIR=str(self.root / "reports"),
            DETAILS_DIR=str(self.details),
            BACKTEST_ROUNDTRIP_COST_PCT=0.12,
        )
        self.patch.start()
        self.addCleanup(self.patch.stop)
        monitor.init_db()

    def add_event(
        self,
        table,
        *,
        coin,
        direction,
        model_version=2,
        ret_1h=None,
        ret_4h=None,
        ret_24h=None,
        ret_72h=None,
        ret_7d=None,
        ret_15d=None,
        ret_30d=None,
    ):
        conn = sqlite3.connect(self.db)
        try:
            conn.execute(
                f"""
                INSERT INTO {table} (
                    model_version, run_id, created_at, coin, direction,
                    score, entry_px, ret_1h, ret_4h, ret_24h, ret_72h,
                    ret_7d, ret_15d, ret_30d, reason
                ) VALUES (?, 1, ?, ?, ?, 9, 100, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    model_version,
                    monitor.now_str(),
                    coin,
                    direction,
                    ret_1h,
                    ret_4h,
                    ret_24h,
                    ret_72h,
                    ret_7d,
                    ret_15d,
                    ret_30d,
                    "v3 dashboard contract",
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def export(self, table, filename, title):
        rows = monitor._export_event_backtest_table(
            table,
            filename,
            title,
            days=30,
        )
        csv_path = self.details / filename
        report_path = self.details / filename.replace(".csv", "_report.txt")
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            csv_rows = list(reader)
            fields = reader.fieldnames or []
        return rows, fields, csv_rows, report_path.read_text(encoding="utf-8")

    def test_short_term_dashboard_only_shows_1h_4h_24h_and_splits_sides(self):
        self.add_event(
            "signal_events",
            coin="BTC",
            direction="bullish",
            ret_1h=1.0,
            ret_4h=None,
            ret_24h=3.0,
            # Deliberately populated: a short-term dashboard must still hide it.
            ret_72h=88.0,
        )
        self.add_event(
            "signal_events",
            coin="ETH",
            direction="bearish",
            ret_1h=None,
            ret_4h=2.0,
            ret_24h=None,
            ret_72h=77.0,
        )
        self.add_event(
            "signal_events",
            coin="LEGACY",
            direction="bullish",
            model_version=1,
            ret_1h=99.0,
            ret_4h=99.0,
            ret_24h=99.0,
        )

        rows, fields, csv_rows, report = self.export(
            "signal_events", "signal_v3.csv", "短期信号收益面板 V3"
        )

        self.assertEqual({row["coin"] for row in rows}, {"BTC", "ETH"})
        self.assertEqual({row["coin"] for row in csv_rows}, {"BTC", "ETH"})
        for horizon in ("1h", "4h", "24h"):
            self.assertIn(f"ret_{horizon}", fields)
            self.assertIn(f"net_ret_{horizon}", fields)
            self.assertIn(f"direction_win_{horizon}", fields)
        for horizon in ("72h", "7d", "15d", "30d"):
            self.assertNotIn(f"ret_{horizon}", fields)
            self.assertNotIn(f"net_ret_{horizon}", fields)
            self.assertNotIn(f"direction_win_{horizon}", fields)

        self.assertIn("多头 | V2事件=1", report)
        self.assertIn("空头 | V2事件=1", report)
        self.assertIn("1h | 成熟=1 | 待评估=0", report)
        self.assertIn("4h | 成熟=0 | 待评估=1", report)
        self.assertIn("4h | 成熟=1 | 待评估=0", report)
        self.assertIn("24h | 成熟=0 | 待评估=1", report)
        self.assertNotIn("72h | 成熟=", report)
        self.assertNotIn("7d | 成熟=", report)
        self.assertIn("V1历史参考：事件=1", report)
        self.assertIn("不计入V2指标", report)
        self.assertNotIn("LEGACY", report)
        self.assertNotIn("98.88", report)

    def test_long_term_dashboard_only_shows_72h_7d_15d_30d(self):
        self.add_event(
            "longterm_events",
            coin="SOL",
            direction="bullish",
            ret_1h=44.0,
            ret_24h=55.0,
            ret_72h=3.0,
            ret_7d=5.0,
            ret_15d=None,
            ret_30d=None,
        )
        self.add_event(
            "longterm_events",
            coin="HYPE",
            direction="bearish",
            ret_1h=66.0,
            ret_24h=77.0,
            ret_72h=None,
            ret_7d=6.0,
            ret_15d=8.0,
            ret_30d=None,
        )
        self.add_event(
            "longterm_events",
            coin="OLDLONG",
            direction="bearish",
            model_version=1,
            ret_72h=99.0,
            ret_7d=99.0,
            ret_15d=99.0,
            ret_30d=99.0,
        )

        rows, fields, csv_rows, report = self.export(
            "longterm_events", "longterm_v3.csv", "长期信号收益面板 V3"
        )

        self.assertEqual({row["coin"] for row in rows}, {"SOL", "HYPE"})
        self.assertEqual({row["coin"] for row in csv_rows}, {"SOL", "HYPE"})
        for horizon in ("72h", "7d", "15d", "30d"):
            self.assertIn(f"ret_{horizon}", fields)
            self.assertIn(f"net_ret_{horizon}", fields)
            self.assertIn(f"direction_win_{horizon}", fields)
        for horizon in ("1h", "4h", "24h"):
            self.assertNotIn(f"ret_{horizon}", fields)
            self.assertNotIn(f"net_ret_{horizon}", fields)
            self.assertNotIn(f"direction_win_{horizon}", fields)

        self.assertIn("多头 | V2事件=1", report)
        self.assertIn("空头 | V2事件=1", report)
        self.assertIn("72h | 成熟=1 | 待评估=0", report)
        self.assertIn("72h | 成熟=0 | 待评估=1", report)
        self.assertIn("15d | 成熟=0 | 待评估=1", report)
        self.assertIn("15d | 成熟=1 | 待评估=0", report)
        self.assertIn("30d | 成熟=0 | 待评估=1", report)
        self.assertNotIn("1h | 成熟=", report)
        self.assertNotIn("24h | 成熟=", report)
        self.assertIn("V1历史参考：事件=1", report)
        self.assertIn("不计入V2指标", report)
        self.assertNotIn("OLDLONG", report)
        self.assertNotIn("98.88", report)


if __name__ == "__main__":
    unittest.main()

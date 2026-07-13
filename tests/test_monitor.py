import asyncio
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("hl_monitor_final", ROOT / "hl_monitor_final.py")
monitor = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = monitor
SPEC.loader.exec_module(monitor)


class FetchWalletTests(unittest.IsolatedAsyncioTestCase):
    async def test_perp_and_spot_requests_start_concurrently(self):
        both_started = asyncio.Event()
        calls = []

        async def fake_post_info(_session, _limiter, payload):
            calls.append(payload["type"])
            if len(calls) == 2:
                both_started.set()
            await asyncio.wait_for(both_started.wait(), timeout=0.5)
            return True, {}

        with mock.patch.object(monitor, "post_info", side_effect=fake_post_info):
            wallet, perp_rows, spot_rows = await monitor.fetch_wallet(
                object(), object(), "0x" + "1" * 40, ["smart_money"], {}, {0: 1.0}, {"USDC": 1.0}
            )

        self.assertCountEqual(calls, ["clearinghouseState", "spotClearinghouseState"])
        self.assertEqual(wallet["status"], "ok")
        self.assertEqual(perp_rows, [])
        self.assertEqual(spot_rows, [])

    async def test_partial_spot_price_parse_fails_closed(self):
        response = [
            {
                "tokens": [{"index": 1, "name": "ETH"}],
                "universe": [{"tokens": [1, 0]}, {"tokens": ["bad-index", 0]}],
            },
            [{"markPx": "3000"}, {"markPx": "1"}],
        ]

        with mock.patch.object(monitor, "post_info", new=mock.AsyncMock(return_value=(True, response))):
            token_prices, coin_prices = await monitor.fetch_spot_prices(object(), None)

        self.assertEqual(token_prices, {0: 1.0})
        self.assertEqual(coin_prices, {"USDC": 1.0})
        health = monitor.global_price_data_health({"BTC": 60000.0}, token_prices, coin_prices)
        self.assertFalse(health["ok"])
        self.assertEqual(health["missing_sources"], ["spotMetaAndAssetCtxs"])


class VacuumPolicyTests(unittest.TestCase):
    def test_auto_vacuum_requires_useful_reclaimable_space(self):
        with mock.patch.multiple(
            monitor,
            DB_VACUUM_MODE="auto",
            DB_MAX_MB=85.0,
            DB_VACUUM_MIN_FREE_MB=16.0,
            DB_VACUUM_MIN_FREE_RATIO=0.10,
        ):
            self.assertFalse(monitor.should_vacuum_sqlite(70.0, 8.0, 0.20))
            self.assertFalse(monitor.should_vacuum_sqlite(70.0, 20.0, 0.05))
            self.assertTrue(monitor.should_vacuum_sqlite(70.0, 20.0, 0.20))
            self.assertTrue(monitor.should_vacuum_sqlite(90.0, 1.0, 0.01))

    def test_vacuum_mode_overrides_auto_policy(self):
        with mock.patch.object(monitor, "DB_VACUUM_MODE", "off"):
            self.assertFalse(monitor.should_vacuum_sqlite(100.0, 50.0, 0.5))
        with mock.patch.object(monitor, "DB_VACUUM_MODE", "always"):
            self.assertTrue(monitor.should_vacuum_sqlite(1.0, 0.0, 0.0))


class AutoThresholdOverlayTests(unittest.TestCase):
    def test_auto_file_only_overlays_score_push(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manual = root / "manual.json"
            auto = root / "auto.json"
            manual.write_text(json.dumps({"DEFAULT": {"score_push": 8, "perp": 100}, "BTC": {"score_push": 10}}), encoding="utf-8")
            auto.write_text(
                json.dumps({"signal_model_version": 2, "overrides": {"BTC": {"score_push": 9.75, "perp": 1}}}),
                encoding="utf-8",
            )
            with mock.patch.multiple(monitor, THRESHOLD_FILE=str(manual), AUTO_THRESHOLD_FILE=str(auto)):
                thresholds = monitor.load_thresholds()
        self.assertEqual(thresholds["BTC"]["score_push"], 9.75)
        self.assertNotIn("perp", thresholds["BTC"])
        self.assertEqual(thresholds["DEFAULT"]["perp"], 100)

    def test_v1_auto_overlay_is_ignored_by_v2_model(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manual = root / "manual.json"
            auto = root / "auto.json"
            manual.write_text(json.dumps({"DEFAULT": {"score_push": 8}, "BTC": {"score_push": 10}}), encoding="utf-8")
            auto.write_text(json.dumps({"signal_model_version": 1, "overrides": {"BTC": {"score_push": 9.75}}}), encoding="utf-8")
            with mock.patch.multiple(monitor, THRESHOLD_FILE=str(manual), AUTO_THRESHOLD_FILE=str(auto)):
                thresholds = monitor.load_thresholds()
        self.assertEqual(thresholds["BTC"]["score_push"], 10)

    def test_auto_overlay_can_be_disabled_immediately(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manual = root / "manual.json"
            auto = root / "auto.json"
            manual.write_text(json.dumps({"DEFAULT": {"score_push": 8}, "BTC": {"score_push": 10}}), encoding="utf-8")
            auto.write_text(json.dumps({"overrides": {"BTC": {"score_push": 9.75}}}), encoding="utf-8")
            with mock.patch.multiple(
                monitor,
                THRESHOLD_FILE=str(manual),
                AUTO_THRESHOLD_FILE=str(auto),
                AUTO_THRESHOLD_ENABLED=False,
            ):
                thresholds = monitor.load_thresholds()
        self.assertEqual(thresholds["BTC"]["score_push"], 10)


class CandidatePriorityTests(unittest.TestCase):
    def test_market_context_selection_preserves_flow_priority(self):
        candidates = ["ZRO", "WIF", "TIA"] + [f"C{i:02d}" for i in range(40)]
        selected = monitor.prioritized_coins(candidates, ["BTC", "ETH"], limit=30)
        self.assertEqual(selected[:3], ["ZRO", "WIF", "TIA"])
        self.assertIn("BTC", selected)
        self.assertIn("ETH", selected)
        self.assertEqual(len(selected), 30)


if __name__ == "__main__":
    unittest.main()


class PreliminaryEndpointHealthTests(unittest.TestCase):
    def _db_context(self):
        tmp = tempfile.TemporaryDirectory()
        db_path = str(Path(tmp.name) / "test.db")
        patches = mock.patch.multiple(monitor, DB_FILE=db_path, USE_TURSO=False)
        patches.start()
        self.addCleanup(patches.stop)
        self.addCleanup(tmp.cleanup)
        monitor.init_db()
        return db_path

    def _insert_wallet(self, run_id, address, status="ok", error="", spot_total=0.0, spot_usdc=0.0):
        conn = monitor.db_conn(); cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO wallet_states(
                run_id, address, groups, status, error,
                perp_account_value, perp_total_ntl_pos, perp_withdrawable, perp_account_leverage, perp_position_count,
                spot_total_value, spot_usdc_value, spot_token_count
            ) VALUES (?, ?, 'smart_money', ?, ?, 0, 0, 0, 0, 0, ?, ?, 0)
            """,
            (run_id, address, status, error, spot_total, spot_usdc),
        )
        conn.commit(); conn.close()

    def _thresholds(self):
        return {"DEFAULT": {"perp": 1000, "spot": 1000, "score_push": 8, "min_watch_score": 4}}

    def test_compute_preliminary_skips_perp_when_current_endpoint_failed(self):
        self._db_context()
        addr = "0x" + "a" * 40
        self._insert_wallet(1, addr, "ok", "")
        self._insert_wallet(2, addr, "partial", "perp=timeout")
        conn = monitor.db_conn(); cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO perp_positions(run_id, address, groups, coin, side, szi, abs_szi, mark_px, position_value)
            VALUES (1, ?, 'smart_money', 'ETH', 'long', 10, 10, 2000, 20000)
            """,
            (addr,),
        )
        conn.commit(); conn.close()
        preliminary, actions, cashflows = monitor.compute_preliminary(2, 1, self._thresholds())
        self.assertEqual(preliminary, {})
        self.assertEqual(actions, [])
        self.assertEqual(cashflows, [])

    def test_compute_preliminary_skips_spot_when_current_endpoint_failed(self):
        self._db_context()
        addr = "0x" + "b" * 40
        self._insert_wallet(1, addr, "ok", "", spot_total=1000, spot_usdc=0)
        self._insert_wallet(2, addr, "partial", "spot=timeout", spot_total=0, spot_usdc=0)
        conn = monitor.db_conn(); cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO spot_balances(run_id, address, groups, coin, token, total, mark_px, current_value)
            VALUES (1, ?, 'smart_money', 'BTC', 0, 1, 1000, 1000)
            """,
            (addr,),
        )
        conn.commit(); conn.close()
        preliminary, actions, cashflows = monitor.compute_preliminary(2, 1, self._thresholds())
        self.assertEqual(preliminary, {})
        self.assertEqual(actions, [])
        self.assertEqual(cashflows, [])

    def test_cashflow_lite_skips_when_spot_endpoint_failed(self):
        self._db_context()
        addr = "0x" + "c" * 40
        self._insert_wallet(1, addr, "ok", "", spot_total=1_000_000, spot_usdc=1_000_000)
        self._insert_wallet(2, addr, "partial", "spot=timeout", spot_total=0, spot_usdc=0)
        preliminary, actions, cashflows = monitor.compute_preliminary(2, 1, self._thresholds())
        self.assertEqual(preliminary, {})
        self.assertEqual(actions, [])
        self.assertEqual(cashflows, [])

    def test_legitimate_perp_close_still_detected_when_both_endpoints_ok(self):
        self._db_context()
        addr = "0x" + "d" * 40
        self._insert_wallet(1, addr, "ok", "")
        self._insert_wallet(2, addr, "ok", "")
        conn = monitor.db_conn(); cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO perp_positions(run_id, address, groups, coin, side, szi, abs_szi, mark_px, position_value)
            VALUES (1, ?, 'smart_money', 'ETH', 'long', 10, 10, 2000, 20000)
            """,
            (addr,),
        )
        conn.commit(); conn.close()
        preliminary, actions, cashflows = monitor.compute_preliminary(2, 1, self._thresholds())
        self.assertIn("ETH", preliminary)
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["action_type"], "close_long")
        self.assertLess(actions[0]["active_delta"], 0)

    def test_endpoint_explicit_flags_override_legacy_error_text(self):
        self._db_context()
        addr = "0x" + "e" * 40
        self._insert_wallet(1, addr, "partial", "perp=old timeout")
        self._insert_wallet(2, addr, "partial", "perp=old timeout")
        conn = monitor.db_conn(); cur = conn.cursor()
        cur.execute("UPDATE wallet_states SET perp_ok=1, spot_ok=1 WHERE run_id IN (1, 2)")
        cur.execute(
            """
            INSERT INTO perp_positions(run_id, address, groups, coin, side, szi, abs_szi, mark_px, position_value)
            VALUES (1, ?, 'smart_money', 'ETH', 'long', 10, 10, 2000, 20000)
            """,
            (addr,),
        )
        conn.commit(); conn.close()
        preliminary, actions, cashflows = monitor.compute_preliminary(2, 1, self._thresholds())
        self.assertIn("ETH", preliminary)
        self.assertEqual(actions[0]["action_type"], "close_long")

    def test_save_snapshot_persists_endpoint_flags(self):
        self._db_context()
        addr = "0x" + "f" * 40
        monitor.save_snapshot(1, [{
            "address": addr,
            "groups": "smart_money",
            "status": "partial",
            "error": "spot=timeout",
            "perp_ok": 1,
            "spot_ok": 0,
            "perp_position_count": 0,
            "spot_total_value": 0.0,
            "spot_usdc_value": 0.0,
            "spot_token_count": 0,
        }], [], [])
        conn = monitor.db_conn()
        row = conn.execute("SELECT perp_ok, spot_ok FROM wallet_states WHERE run_id=1 AND address=?", (addr,)).fetchone()
        conn.close()
        self.assertEqual(tuple(row), (1, 0))


class SignalPushTests(unittest.TestCase):
    def test_new_long_candidate_category_is_pushable_by_candidate_score(self):
        row = {
            "signal_category": "长期多单候选",
            "candidate_gate": "PASS",
            "candidate_state": "CANDIDATE",
            "watchlist": "long",
            "alert_score": 1,
            "long_score": 9,
            "long_candidate_score": 9,
            "short_candidate_score": 0,
            "threshold_score": 8,
        }
        self.assertTrue(monitor.is_pushable_signal(row))


class DataQualityGuardTests(unittest.TestCase):
    def test_protect_mode_blocks_low_ok_rate(self):
        with mock.patch.multiple(monitor, DATA_ANOMALY_PROTECT_MODE=True, MIN_OK_RATE=0.85):
            self.assertFalse(monitor.data_quality_allows_signal_writes(0.5))
            self.assertTrue(monitor.data_quality_allows_signal_writes(0.85))

    def test_protect_mode_off_allows_low_ok_rate(self):
        with mock.patch.multiple(monitor, DATA_ANOMALY_PROTECT_MODE=False, MIN_OK_RATE=0.85):
            self.assertTrue(monitor.data_quality_allows_signal_writes(0.1))

    def test_missing_global_prices_block_signal_writes(self):
        with mock.patch.multiple(monitor, DATA_ANOMALY_PROTECT_MODE=True, MIN_OK_RATE=0.85):
            self.assertFalse(monitor.data_quality_allows_signal_writes(1.0, price_data_ok=False))
            self.assertTrue(monitor.data_quality_allows_signal_writes(1.0, price_data_ok=True))

    def test_global_price_health_rejects_fallback_only_maps(self):
        failed = monitor.global_price_data_health({}, {0: 1.0}, {"USDC": 1.0})
        self.assertFalse(failed["ok"])
        self.assertCountEqual(failed["missing_sources"], ["allMids", "spotMetaAndAssetCtxs"])

        healthy = monitor.global_price_data_health(
            {"BTC": 60000.0},
            {0: 1.0, 1: 3000.0},
            {"USDC": 1.0, "ETH": 3000.0},
        )
        self.assertTrue(healthy["ok"])


class SnapshotBaselineIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.patches = mock.patch.multiple(
            monitor,
            DB_FILE=str(Path(self.tmp.name) / "monitor.db"),
            USE_TURSO=False,
        )
        self.patches.start()
        self.addCleanup(self.patches.stop)
        monitor.init_db()

    def test_previous_run_skips_incomplete_snapshot(self):
        healthy_run = monitor.create_run("healthy")
        monitor.save_snapshot(healthy_run, [], [], [], price_data_ok=True)
        incomplete_run = monitor.create_run("interrupted")
        current_run = monitor.create_run("current")

        self.assertEqual(monitor.get_previous_run_id(current_run), healthy_run)
        conn = monitor.db_conn()
        marker = conn.execute(
            "SELECT snapshot_complete FROM runs WHERE run_id=?", (incomplete_run,)
        ).fetchone()[0]
        conn.close()
        self.assertEqual(marker, 0)

    def test_interrupted_chunk_write_never_marks_snapshot_complete(self):
        interrupted_run = monitor.create_run("interrupted-during-perp")
        perp_row = {
            "address": "0x" + "9" * 40,
            "groups": "smart_money",
            "coin": "ETH",
            "side": "long",
            "szi": 1.0,
            "abs_szi": 1.0,
            "mark_px": 3000.0,
            "position_value": 3000.0,
        }
        with mock.patch.object(monitor, "_chunks", side_effect=RuntimeError("simulated interruption")):
            with self.assertRaisesRegex(RuntimeError, "simulated interruption"):
                monitor.save_snapshot(interrupted_run, [], [perp_row], [], price_data_ok=True)

        current_run = monitor.create_run("after-interruption")
        self.assertIsNone(monitor.get_previous_run_id(current_run))
        conn = monitor.db_conn()
        marker = conn.execute(
            "SELECT snapshot_complete FROM runs WHERE run_id=?", (interrupted_run,)
        ).fetchone()[0]
        conn.close()
        self.assertEqual(marker, 0)

    def test_previous_run_skips_snapshot_with_missing_prices(self):
        healthy_run = monitor.create_run("healthy")
        monitor.save_snapshot(healthy_run, [], [], [], price_data_ok=True)
        bad_price_run = monitor.create_run("bad-prices")
        monitor.save_snapshot(bad_price_run, [], [], [], price_data_ok=False)
        current_run = monitor.create_run("current")

        self.assertEqual(monitor.get_previous_run_id(current_run), healthy_run)
        conn = monitor.db_conn()
        row = conn.execute(
            "SELECT snapshot_complete, price_data_ok FROM runs WHERE run_id=?", (bad_price_run,)
        ).fetchone()
        conn.close()
        self.assertEqual(tuple(row), (1, 0))

    def test_restart_does_not_requalify_finished_bad_price_run(self):
        bad_price_run = monitor.create_run("bad-prices")
        monitor.save_snapshot(bad_price_run, [], [], [], price_data_ok=False)
        monitor.finish_run(bad_price_run, [], [], [], pushed=False)

        # A new process calls init_db() again. The compatibility migration must
        # not overwrite a deliberate price_data_ok=0 from a v10 run.
        monitor.init_db()
        current_run = monitor.create_run("after-restart")

        self.assertIsNone(monitor.get_previous_run_id(current_run))
        conn = monitor.db_conn()
        marker = conn.execute(
            "SELECT price_data_ok FROM runs WHERE run_id=?", (bad_price_run,)
        ).fetchone()[0]
        conn.close()
        self.assertEqual(marker, 0)


class SnapshotMarkerMigrationTests(unittest.TestCase):
    def test_finished_legacy_run_requires_fresh_price_baseline(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "legacy.db")
            with mock.patch.multiple(monitor, DB_FILE=db_path, USE_TURSO=False):
                conn = monitor.db_conn()
                conn.execute(
                    "CREATE TABLE runs (run_id INTEGER PRIMARY KEY, started_at TEXT, finished_at TEXT)"
                )
                conn.execute(
                    "INSERT INTO runs(run_id, started_at, finished_at) VALUES (1, '2026-07-01 00:00:00', '2026-07-01 00:10:00')"
                )
                conn.commit()
                conn.close()

                monitor.init_db()
                conn = monitor.db_conn()
                row = conn.execute(
                    "SELECT snapshot_complete, price_data_ok FROM runs WHERE run_id=1"
                ).fetchone()
                conn.close()

        self.assertEqual(tuple(row), (1, 0))


GEMINI_SPEC = importlib.util.spec_from_file_location("gemini_scan_analysis", ROOT / "gemini_scan_analysis.py")
gemini_scan = importlib.util.module_from_spec(GEMINI_SPEC)
sys.modules[GEMINI_SPEC.name] = gemini_scan
GEMINI_SPEC.loader.exec_module(gemini_scan)


class GeminiMarkdownPushTests(unittest.TestCase):
    def test_markdown_high_urgency_pushes_even_without_focus_items(self):
        result = {
            "status": "completed",
            "urgency": "high",
            "model": "test",
            "markdown_report": "【Gemini 本轮扫描分析】\n紧急度：high\n重点：测试",
            "focus_items": [],
            "risk_warnings": [],
        }
        with mock.patch.object(gemini_scan, "TG_ENABLED", True), mock.patch.object(gemini_scan, "TG_MIN_URGENCY", "watch"):
            self.assertTrue(gemini_scan.should_push_tg(result, force_due=False))
        self.assertIn("Gemini 本轮扫描分析", gemini_scan.format_tg_message(result))


class PositionLifecycleGuardTests(unittest.TestCase):
    def _db_context(self):
        tmp = tempfile.TemporaryDirectory()
        db_path = str(Path(tmp.name) / "test.db")
        patches = mock.patch.multiple(monitor, DB_FILE=db_path, USE_TURSO=False)
        patches.start()
        self.addCleanup(patches.stop)
        self.addCleanup(tmp.cleanup)
        monitor.init_db()
        return db_path

    def test_wallet_perp_ok_map_uses_explicit_endpoint_flags(self):
        self._db_context()
        addr = "0x" + "1" * 40
        conn = monitor.db_conn(); cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO wallet_states(
                run_id, address, groups, status, error, perp_ok, spot_ok,
                perp_account_value, perp_total_ntl_pos, perp_withdrawable, perp_account_leverage, perp_position_count,
                spot_total_value, spot_usdc_value, spot_token_count
            ) VALUES (1, ?, 'smart_money', 'partial', 'perp=legacy timeout', 1, 1, 0, 0, 0, 0, 0, 0, 0, 0)
            """,
            (addr,),
        )
        conn.commit(); conn.close()
        self.assertTrue(monitor._wallet_perp_ok_map(1)[addr.lower()])

    def test_skipped_artifacts_overwrite_latest_reports(self):
        self._db_context()
        with tempfile.TemporaryDirectory() as tmp:
            report_dir = Path(tmp) / "reports"
            details_dir = report_dir / "details"
            with mock.patch.multiple(monitor, REPORT_DIR=str(report_dir), DETAILS_DIR=str(details_dir), MIN_OK_RATE=0.85):
                monitor.write_quality_guard_skipped_artifacts(7, 0.25, "test")
                pos_report = (details_dir / "wallet_position_report.txt").read_text(encoding="utf-8")
                quality_report = (details_dir / "wallet_quality_report.txt").read_text(encoding="utf-8")
                quality_csv = (details_dir / "wallet_quality_latest.csv").read_text(encoding="utf-8-sig")
        self.assertIn("本轮未更新仓位生命周期", pos_report)
        self.assertIn("本轮未刷新钱包质量", quality_report)
        self.assertIn("skipped_low_data_quality", quality_csv)

    def test_low_quality_skips_leverage_long_short_and_rolling_reports(self):
        self._db_context()
        with tempfile.TemporaryDirectory() as tmp:
            report_dir = Path(tmp) / "reports"
            details_dir = report_dir / "details"
            with mock.patch.multiple(
                monitor,
                REPORT_DIR=str(report_dir),
                DETAILS_DIR=str(details_dir),
                MIN_OK_RATE=0.85,
                LEVERAGE_QUALITY_MODE=True,
                ROLLING_FLOW_MODE=True,
            ):
                monitor.write_leverage_guard_skipped_artifacts(8, 0.30, "test")
                monitor.write_long_short_guard_skipped_artifacts(8, 0.30, "test")
                monitor.write_rolling_flow_guard_skipped_artifacts(8, 0.30, source_run_id=7, note="test")
                leverage_report = (details_dir / "leverage_quality_report.txt").read_text(encoding="utf-8")
                long_short_report = (report_dir / "long_short_state_report.txt").read_text(encoding="utf-8")
                rolling_report = (report_dir / "rolling_flow_report.txt").read_text(encoding="utf-8")
                rolling_csv = (details_dir / "rolling_flow_latest.csv").read_text(encoding="utf-8-sig")
        self.assertIn("本轮未导出杠杆质量", leverage_report)
        self.assertIn("本轮未刷新长期多/空状态机", long_short_report)
        self.assertIn("本轮未刷新 rolling flow", rolling_report)
        self.assertIn("source_run_id", rolling_csv)
        self.assertIn("skipped_low_data_quality", rolling_csv)

    def test_low_quality_skips_signal_risk_and_longterm_reports(self):
        self._db_context()
        with tempfile.TemporaryDirectory() as tmp:
            report_dir = Path(tmp) / "reports"
            details_dir = report_dir / "details"
            with mock.patch.multiple(monitor, REPORT_DIR=str(report_dir), DETAILS_DIR=str(details_dir), MIN_OK_RATE=0.85):
                monitor.write_signal_guard_skipped_artifacts(9, 0.20, "test")
                monitor.write_coin_risk_guard_skipped_artifacts(9, 0.20, "test")
                monitor.write_long_term_guard_skipped_artifacts(9, 0.20, "test")
                coin_signals = (details_dir / "coin_signals_latest.csv").read_text(encoding="utf-8-sig")
                signal_explain = (details_dir / "signal_explain_latest.csv").read_text(encoding="utf-8-sig")
                risk_report = (report_dir / "coin_risk_report.txt").read_text(encoding="utf-8")
                longterm_report = (report_dir / "long_term_plan.txt").read_text(encoding="utf-8")
                longterm_csv = (details_dir / "long_term_candidates.csv").read_text(encoding="utf-8-sig")
        self.assertIn("skipped_low_data_quality", coin_signals)
        self.assertIn("skipped_low_data_quality", signal_explain)
        self.assertIn("本轮未刷新币种风险指标", risk_report)
        self.assertIn("本轮未刷新长期候选计划", longterm_report)
        self.assertIn("skipped_low_data_quality", longterm_csv)



class RunOnceDataQualityGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_low_quality_first_run_skips_position_lifecycle_and_wallet_quality(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            addr = "0x" + "2" * 40
            wallet_rows = [{
                "address": addr,
                "groups": "smart_money",
                "status": "failed",
                "error": "network timeout",
                "perp_ok": 0,
                "spot_ok": 0,
                "perp_position_count": 0,
                "spot_total_value": 0.0,
                "spot_usdc_value": 0.0,
                "spot_token_count": 0,
            }]
            args = type("Args", (), {"note": "test", "rpm": 60, "concurrency": 1})()
            with mock.patch.multiple(
                monitor,
                DB_FILE=str(root / "monitor.db"),
                USE_TURSO=False,
                REPORT_DIR=str(root / "reports"),
                DETAILS_DIR=str(root / "reports" / "details"),
                THRESHOLD_FILE=str(root / "missing_thresholds.json"),
                AUTO_THRESHOLD_FILE=str(root / "missing_auto.json"),
                MIN_WALLET_COUNT=1,
                DATA_ANOMALY_PROTECT_MODE=True,
                MIN_OK_RATE=0.85,
                PUSH_EVERY_RUN=False,
                WALLET_QUALITY_MODE=True,
            ), \
            mock.patch.object(monitor, "load_wallet_addresses", return_value={addr: ["smart_money"]}), \
            mock.patch.object(monitor, "fetch_all", return_value=(wallet_rows, [], [], {}, {}, {})), \
            mock.patch.object(monitor, "send_tg", new=mock.AsyncMock(return_value=False)), \
            mock.patch.object(monitor, "update_position_trades") as upd_pos, \
            mock.patch.object(monitor, "export_leverage_quality_files") as exp_lev, \
            mock.patch.object(monitor, "refresh_wallet_quality") as refresh_quality, \
            mock.patch.object(monitor, "evaluate_events", return_value=(0, 0)) as eval_events, \
            mock.patch.object(monitor, "export_latest_csv"), \
            mock.patch.object(monitor, "export_signal_lifecycle_files") as lifecycle_export, \
            mock.patch.object(monitor, "prune_reports"), \
            mock.patch.object(monitor, "save_daily_archive"), \
            mock.patch.object(monitor, "finish_run"), \
            mock.patch.object(monitor, "write_last_run_status"), \
            mock.patch.object(monitor, "prune_database_for_github"), \
            mock.patch.object(monitor, "should_push_daily", return_value=False):
                await monitor.run_once(args)

            upd_pos.assert_not_called()
            exp_lev.assert_not_called()
            refresh_quality.assert_not_called()
            eval_events.assert_not_called()
            lifecycle_export.assert_not_called()
            skipped_report = (root / "reports" / "details" / "wallet_position_report.txt").read_text(encoding="utf-8")
            self.assertIn("本轮未更新仓位生命周期", skipped_report)
            leverage_report = (root / "reports" / "details" / "leverage_quality_report.txt").read_text(encoding="utf-8")
            long_short_report = (root / "reports" / "long_short_state_report.txt").read_text(encoding="utf-8")
            rolling_report = (root / "reports" / "rolling_flow_report.txt").read_text(encoding="utf-8")
            rolling_csv = (root / "reports" / "details" / "rolling_flow_latest.csv").read_text(encoding="utf-8-sig")
            coin_signals_csv = (root / "reports" / "details" / "coin_signals_latest.csv").read_text(encoding="utf-8-sig")
            signal_explain_csv = (root / "reports" / "details" / "signal_explain_latest.csv").read_text(encoding="utf-8-sig")
            coin_risk_report = (root / "reports" / "coin_risk_report.txt").read_text(encoding="utf-8")
            coin_risk_csv = (root / "reports" / "details" / "coin_risk_latest.csv").read_text(encoding="utf-8-sig")
            long_term_plan = (root / "reports" / "long_term_plan.txt").read_text(encoding="utf-8")
            long_term_csv = (root / "reports" / "details" / "long_term_candidates.csv").read_text(encoding="utf-8-sig")
            signal_backtest_csv = (root / "reports" / "details" / "signal_backtest_latest.csv").read_text(encoding="utf-8-sig")
            research_csv = (root / "reports" / "details" / "research_signal_summary_latest.csv").read_text(encoding="utf-8-sig")
            research_dashboard = (root / "reports" / "research_dashboard.txt").read_text(encoding="utf-8")
            signal_lifecycle_csv = (root / "reports" / "details" / "signal_lifecycle_latest.csv").read_text(encoding="utf-8-sig")
            signal_lifecycle_report = (root / "reports" / "details" / "signal_lifecycle_report.txt").read_text(encoding="utf-8")
            self.assertIn("本轮未导出杠杆质量", leverage_report)
            self.assertIn("本轮未刷新长期多/空状态机", long_short_report)
            self.assertIn("本轮未刷新 rolling flow", rolling_report)
            self.assertIn("skipped_low_data_quality", rolling_csv)
            self.assertIn("skipped_low_data_quality", coin_signals_csv)
            self.assertIn("skipped_low_data_quality", signal_explain_csv)
            self.assertIn("本轮未刷新币种风险指标", coin_risk_report)
            self.assertIn("skipped_low_data_quality", coin_risk_csv)
            self.assertIn("本轮未刷新长期候选计划", long_term_plan)
            self.assertIn("skipped_low_data_quality", long_term_csv)
            self.assertIn("skipped_low_data_quality", signal_backtest_csv)
            self.assertIn("skipped_low_data_quality", research_csv)
            self.assertIn("本轮未刷新研究面板", research_dashboard)
            self.assertIn("skipped_low_data_quality", signal_lifecycle_csv)
            self.assertIn("本轮未更新信号生命周期", signal_lifecycle_report)


    async def test_low_quality_existing_run_skips_market_backtest_and_research_refresh(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            addr = "0x" + "3" * 40
            wallet_rows = [{
                "address": addr,
                "groups": "smart_money",
                "status": "failed",
                "error": "network timeout",
                "perp_ok": 0,
                "spot_ok": 0,
                "perp_position_count": 0,
                "spot_total_value": 0.0,
                "spot_usdc_value": 0.0,
                "spot_token_count": 0,
            }]
            args = type("Args", (), {"note": "test", "rpm": 60, "concurrency": 1})()
            with mock.patch.multiple(
                monitor,
                DB_FILE=str(root / "monitor.db"),
                USE_TURSO=False,
                REPORT_DIR=str(root / "reports"),
                DETAILS_DIR=str(root / "reports" / "details"),
                THRESHOLD_FILE=str(root / "missing_thresholds.json"),
                AUTO_THRESHOLD_FILE=str(root / "missing_auto.json"),
                MIN_WALLET_COUNT=1,
                DATA_ANOMALY_PROTECT_MODE=True,
                MIN_OK_RATE=0.85,
                PUSH_EVERY_RUN=False,
                WALLET_QUALITY_MODE=True,
                LONG_TERM_MODE=True,
            ), \
            mock.patch.object(monitor, "load_wallet_addresses", return_value={addr: ["smart_money"]}), \
            mock.patch.object(monitor, "fetch_all", return_value=(wallet_rows, [], [], {}, {}, {})), \
            mock.patch.object(monitor, "get_previous_run_id", return_value=1), \
            mock.patch.object(monitor, "send_tg", new=mock.AsyncMock(return_value=False)), \
            mock.patch.object(monitor, "update_position_trades") as upd_pos, \
            mock.patch.object(monitor, "refresh_wallet_quality") as refresh_quality, \
            mock.patch.object(monitor, "evaluate_events", return_value=(0, 0)) as eval_events, \
            mock.patch.object(monitor, "build_market_context", new=mock.AsyncMock(return_value={})) as market_ctx, \
            mock.patch.object(monitor, "build_coin_risk_metrics", new=mock.AsyncMock(return_value={})) as risk_metrics, \
            mock.patch.object(monitor, "export_backtest_files") as backtest_export, \
            mock.patch.object(monitor, "export_research_intelligence_files") as research_export, \
            mock.patch.object(monitor, "export_signal_lifecycle_files") as lifecycle_export, \
            mock.patch.object(monitor, "should_push_daily", return_value=False):
                await monitor.run_once(args)

            upd_pos.assert_not_called()
            refresh_quality.assert_not_called()
            eval_events.assert_not_called()
            market_ctx.assert_not_called()
            risk_metrics.assert_not_called()
            backtest_export.assert_not_called()
            research_export.assert_not_called()
            lifecycle_export.assert_not_called()
            final_report = (root / "reports" / "final_latest_report.txt").read_text(encoding="utf-8")
            self.assertIn("未刷新钱包质量", final_report)
            self.assertIn("未更新仓位生命周期", final_report)
            self.assertIn("已跳过市场上下文刷新", final_report)
            self.assertNotIn("BTC: 1h N/A", final_report)
            signal_backtest_csv = (root / "reports" / "details" / "signal_backtest_latest.csv").read_text(encoding="utf-8-sig")
            wallet_profile_csv = (root / "reports" / "details" / "wallet_profile_latest.csv").read_text(encoding="utf-8-sig")
            signal_lifecycle_csv = (root / "reports" / "details" / "signal_lifecycle_latest.csv").read_text(encoding="utf-8-sig")
            signal_lifecycle_report = (root / "reports" / "details" / "signal_lifecycle_report.txt").read_text(encoding="utf-8")
            self.assertIn("skipped_low_data_quality", signal_backtest_csv)
            self.assertIn("skipped_low_data_quality", wallet_profile_csv)
            self.assertIn("skipped_low_data_quality", signal_lifecycle_csv)
            self.assertIn("本轮未更新信号生命周期", signal_lifecycle_report)


class LatestEmptyExportTests(unittest.TestCase):
    def _db_context(self, root: Path):
        patches = mock.patch.multiple(
            monitor,
            DB_FILE=str(root / "test.db"),
            USE_TURSO=False,
            REPORT_DIR=str(root / "reports"),
            DETAILS_DIR=str(root / "reports" / "details"),
        )
        patches.start()
        self.addCleanup(patches.stop)
        monitor.init_db()
        (root / "reports" / "details").mkdir(parents=True, exist_ok=True)

    def test_export_latest_csv_overwrites_stale_empty_outputs_and_preserves_current_skip(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._db_context(root)
            details = root / "reports" / "details"
            (details / "perp_positions_latest.csv").write_text("OLD_PERP\n", encoding="utf-8")
            (details / "coin_signals_latest.csv").write_text("OLD_SIGNAL\n", encoding="utf-8")
            monitor.export_latest_csv(42)
            self.assertEqual((details / "perp_positions_latest.csv").read_text(encoding="utf-8-sig"), "empty\n")
            self.assertEqual((details / "coin_signals_latest.csv").read_text(encoding="utf-8-sig"), "empty\n")

            monitor.write_signal_guard_skipped_artifacts(43, 0.20, "test")
            monitor.export_latest_csv(43)
            preserved = (details / "coin_signals_latest.csv").read_text(encoding="utf-8-sig")
            self.assertIn("skipped_low_data_quality", preserved)
            self.assertIn("43", preserved)

    def test_export_latest_csv_does_not_preserve_stale_skip_with_current_id_substring(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._db_context(root)
            details = root / "reports" / "details"
            # Old low-quality marker from run 1.  Its timestamp contains "20",
            # so a loose substring check for current run_id=20 would incorrectly
            # preserve this stale file.
            (details / "coin_signals_latest.csv").write_text(
                "run_id,source_run_id,calculated_at,status,ok_rate,min_ok_rate,note\n"
                "1,,2026-07-04 00:00:00,skipped_low_data_quality,0.2,0.85,old skip\n",
                encoding="utf-8-sig",
            )
            monitor.export_latest_csv(20)
            self.assertEqual((details / "coin_signals_latest.csv").read_text(encoding="utf-8-sig"), "empty\n")

    def test_empty_signal_and_leverage_exports_clear_stale_latest_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._db_context(root)
            details = root / "reports" / "details"
            report_dir = root / "reports"
            (details / "long_short_state_latest.csv").write_text("OLD_LONG_SHORT\n", encoding="utf-8")
            monitor.export_long_short_state_files(50, [])
            self.assertEqual((details / "long_short_state_latest.csv").read_text(encoding="utf-8-sig"), "empty\n")
            self.assertIn("暂无", (report_dir / "long_short_state_report.txt").read_text(encoding="utf-8"))

            for name in ["leverage_quality_latest.csv", "wallet_leverage_profile_latest.csv", "coin_leverage_summary_latest.csv"]:
                (details / name).write_text("OLD_LEVERAGE\n", encoding="utf-8")
            with mock.patch.object(monitor, "LEVERAGE_QUALITY_MODE", True):
                monitor.export_leverage_quality_files(50)
            for name in ["leverage_quality_latest.csv", "wallet_leverage_profile_latest.csv", "coin_leverage_summary_latest.csv"]:
                self.assertEqual((details / name).read_text(encoding="utf-8-sig"), "empty\n")
            self.assertIn("没有合约持仓", (details / "leverage_quality_report.txt").read_text(encoding="utf-8"))

    def test_empty_position_trade_exports_clear_stale_latest_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._db_context(root)
            details = root / "reports" / "details"
            (details / "wallet_position_trades_latest.csv").write_text("OLD_TRADES\n", encoding="utf-8")
            (details / "wallet_position_performance_latest.csv").write_text("OLD_PERF\n", encoding="utf-8")
            with mock.patch.object(monitor, "POSITION_TRADE_MODE", True):
                monitor.export_position_trade_files(60)
            self.assertEqual((details / "wallet_position_trades_latest.csv").read_text(encoding="utf-8-sig"), "empty\n")
            self.assertEqual((details / "wallet_position_performance_latest.csv").read_text(encoding="utf-8-sig"), "empty\n")
            self.assertIn("暂无", (details / "wallet_position_report.txt").read_text(encoding="utf-8"))

import os
import asyncio
import tempfile
import unittest

import gemini_optimizer
import hl_monitor_final as monitor


class SpotIntegrityTests(unittest.TestCase):
    def test_extreme_spot_value_is_retained_but_untrusted(self):
        wallet, rows = monitor.parse_spot_state(
            "0x" + "1" * 40,
            "smart_money",
            {"balances": [{"coin": "SPAM", "token": 9, "total": "100000000", "entryNtl": "1"}]},
            {0: 1.0, 9: 100.0},
            {"USDC": 1.0, "SPAM": 100.0},
        )
        self.assertEqual(rows[0]["price_trusted"], 0)
        self.assertEqual(rows[0]["anomaly_reason"], "position_value_over_dynamic_limit")
        self.assertEqual(wallet["spot_total_value"], 0.0)

    def test_semantic_health_reports_anomaly_ratio(self):
        rows = [
            {"coin": "A", "price_trusted": 1},
            {"coin": "B", "price_trusted": 0, "anomaly_reason": "bad"},
        ]
        old_min = monitor.SPOT_MIN_TRUSTED_ROWS
        old_ratio = monitor.SPOT_MAX_ANOMALY_RATIO
        try:
            monitor.SPOT_MIN_TRUSTED_ROWS = 1
            monitor.SPOT_MAX_ANOMALY_RATIO = 0.4
            health = monitor.spot_semantic_health(rows)
        finally:
            monitor.SPOT_MIN_TRUSTED_ROWS = old_min
            monitor.SPOT_MAX_ANOMALY_RATIO = old_ratio
        self.assertFalse(health["semantic_ok"])
        self.assertEqual(health["anomalous_spot_rows"], 1)
        self.assertEqual(health["spot_anomaly_ratio"], 0.5)

    def test_low_volume_token_gets_lower_dynamic_cap(self):
        old_volume = dict(monitor.SPOT_TOKEN_DAY_VOLUME)
        old_canonical = dict(monitor.SPOT_TOKEN_CANONICAL)
        try:
            monitor.SPOT_TOKEN_DAY_VOLUME[7] = 1_000_000
            monitor.SPOT_TOKEN_CANONICAL[7] = False
            _, rows = monitor.parse_spot_state(
                "0x" + "3" * 40, "smart_money",
                {"balances": [{"coin": "LOWVOL", "token": 7, "total": 100_000}]},
                {0: 1.0, 7: 100.0}, {"LOWVOL": 100.0},
            )
        finally:
            monitor.SPOT_TOKEN_DAY_VOLUME.clear(); monitor.SPOT_TOKEN_DAY_VOLUME.update(old_volume)
            monitor.SPOT_TOKEN_CANONICAL.clear(); monitor.SPOT_TOKEN_CANONICAL.update(old_canonical)
        self.assertEqual(rows[0]["trusted_value_cap"], 8_000_000)
        self.assertEqual(rows[0]["price_trusted"], 0)


class SignalDedupTests(unittest.TestCase):
    def test_same_direction_event_respects_cooldown(self):
        with tempfile.TemporaryDirectory() as td:
            old_db, old_turso = monitor.DB_FILE, monitor.USE_TURSO
            monitor.DB_FILE, monitor.USE_TURSO = os.path.join(td, "test.db"), False
            try:
                monitor.init_db()
                signal = {"coin": "BTC", "direction": "bullish", "final_score": 12.0, "reason": "test"}
                thresholds = {"DEFAULT": {"min_watch_score": 5, "score_push": 8, "perp": 1, "spot": 1}}
                self.assertEqual(monitor.create_signal_events(1, [signal], {"BTC": 100.0}, thresholds), 1)
                self.assertEqual(monitor.create_signal_events(2, [signal], {"BTC": 101.0}, thresholds), 0)
            finally:
                monitor.DB_FILE, monitor.USE_TURSO = old_db, old_turso


class StatisticalUpgradeTests(unittest.TestCase):
    def test_wilson_lower_bound_penalizes_small_samples(self):
        lower = monitor.wilson_lower_bound(8, 10)
        self.assertIsNotNone(lower)
        self.assertLess(lower, 0.60)

    def test_recent_result_has_more_decay_weight(self):
        now = monitor.utc_now()
        rows = [
            {"created_at": now.strftime("%Y-%m-%d %H:%M:%S"), "ret_24h": 5.0},
            {"created_at": (now - monitor.dt.timedelta(days=28)).strftime("%Y-%m-%d %H:%M:%S"), "ret_24h": -5.0},
        ]
        _, _, avg, effective = monitor._decayed_metrics(rows, "ret_24h", 1.0)
        self.assertGreater(avg, 0.0)
        self.assertGreater(effective, 1.0)


class WalletClusterTests(unittest.TestCase):
    def test_repeated_synchronous_wallets_share_cluster(self):
        with tempfile.TemporaryDirectory() as td:
            old_db, old_turso = monitor.DB_FILE, monitor.USE_TURSO
            old_report, old_details = monitor.REPORT_DIR, monitor.DETAILS_DIR
            monitor.DB_FILE, monitor.USE_TURSO = os.path.join(td, "cluster.db"), False
            monitor.REPORT_DIR, monitor.DETAILS_DIR = td, os.path.join(td, "details")
            a1, a2 = "0x" + "1" * 40, "0x" + "2" * 40
            try:
                monitor.init_db()
                conn = monitor.db_conn(); cur = conn.cursor()
                for run_id in (1, 2, 3):
                    for address in (a1, a2):
                        cur.execute(
                            "INSERT INTO wallet_actions(run_id,created_at,address,coin,market,direction,action_type,active_delta) VALUES(?,?,?,?,?,?,?,?)",
                            (run_id, monitor.now_str(), address, "BTC", "perp", "bullish", "perp_change", 1000),
                        )
                conn.commit(); conn.close()
                actions = [{"address": a1}, {"address": a2}]
                mapping = monitor.assign_behavioral_wallet_clusters(actions)
                self.assertEqual(mapping[a1], mapping[a2])
                self.assertTrue(mapping[a1].startswith("cluster:"))
            finally:
                monitor.DB_FILE, monitor.USE_TURSO = old_db, old_turso
                monitor.REPORT_DIR, monitor.DETAILS_DIR = old_report, old_details


class EvidenceEnrichmentTests(unittest.TestCase):
    def test_top_action_receives_fill_and_ledger_evidence(self):
        old_post = monitor.post_info
        old_started = monitor.get_run_started_at
        async def fake_post(session, limiter, payload):
            if payload["type"] == "userFillsByTime":
                return True, [{"coin": "BTC", "dir": "Open Long", "sz": "1", "px": "100"}]
            return True, [{"delta": {"type": "deposit"}}]
        try:
            monitor.post_info = fake_post
            monitor.get_run_started_at = lambda run_id: monitor.utc_now() - monitor.dt.timedelta(hours=1 if run_id == 1 else 0)
            actions = [{"address": "0x" + "4" * 40, "coin": "BTC", "active_delta": 1000}]
            asyncio.run(monitor.enrich_recent_execution_evidence(actions, [], 1, 2))
        finally:
            monitor.post_info = old_post
            monitor.get_run_started_at = old_started
        self.assertEqual(actions[0]["fill_count"], 1)
        self.assertEqual(actions[0]["evidence_class"], "trade_confirmed")
        self.assertIn("BTC", actions[0]["execution_evidence"])
        self.assertIn("deposit", actions[0]["ledger_evidence"])


class GeminiCompletenessTests(unittest.TestCase):
    def test_truncated_markdown_is_rejected(self):
        ok, missing = gemini_optimizer.validate_scan_markdown("【执行摘要】\n内容\n【风险提示】\n截断")
        self.assertFalse(ok)
        self.assertIn("【下一轮重点检查】", missing)


if __name__ == "__main__":
    unittest.main()

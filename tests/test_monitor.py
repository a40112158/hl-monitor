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
            auto.write_text(json.dumps({"overrides": {"BTC": {"score_push": 9.75, "perp": 1}}}), encoding="utf-8")
            with mock.patch.multiple(monitor, THRESHOLD_FILE=str(manual), AUTO_THRESHOLD_FILE=str(auto)):
                thresholds = monitor.load_thresholds()
        self.assertEqual(thresholds["BTC"]["score_push"], 9.75)
        self.assertNotIn("perp", thresholds["BTC"])
        self.assertEqual(thresholds["DEFAULT"]["perp"], 100)

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

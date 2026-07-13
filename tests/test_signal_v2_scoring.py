"""Regression tests for the V2 directional and long-horizon signal scoring."""

import unittest
from unittest import mock

import hl_monitor_final as monitor


class DirectionalAdjustmentTests(unittest.TestCase):
    def test_favorable_adjustments_strengthen_both_directions(self):
        helper = getattr(monitor, "apply_directional_adjustment", None)
        self.assertIsNotNone(
            helper,
            "signal V2 must expose apply_directional_adjustment(base_signed, direction, adjustment)",
        )

        bullish = helper(4.0, "bullish", 1.0)
        bearish = helper(-4.0, "bearish", 1.0)

        self.assertGreater(bullish, 0.0)
        self.assertLess(bearish, 0.0)
        self.assertGreater(abs(bullish), 4.0)
        self.assertGreater(abs(bearish), 4.0)

    def test_unfavorable_adjustments_weaken_both_directions(self):
        helper = getattr(monitor, "apply_directional_adjustment", None)
        self.assertIsNotNone(
            helper,
            "signal V2 must expose apply_directional_adjustment(base_signed, direction, adjustment)",
        )

        bullish = helper(4.0, "bullish", -1.0)
        bearish = helper(-4.0, "bearish", -1.0)

        self.assertGreater(bullish, 0.0)
        self.assertLess(bearish, 0.0)
        self.assertLess(abs(bullish), 4.0)
        self.assertLess(abs(bearish), 4.0)


class RollingWindowMaturityTests(unittest.TestCase):
    def test_unselected_immature_30d_does_not_taint_mature_15d_selection(self):
        rolling = {
            # A fully mature and otherwise healthy 15d long-horizon candidate.
            "weighted_15d": 800.0,
            "coverage_15d": 0.80,
            "span_hours_15d": 360.0,
            "bullish_runs_15d": 12,
            "bullish_days_15d": 6,
            "runs_15d": 12,
            "gaps_15d": 0,
            "bullish_wallets_15d": 3,
            "bullish_top1_share_15d": 0.30,
            "bullish_top3_share_15d": 0.60,
            "spot_share_15d": 0.0,
            "bullish_wallet_spot_share_15d": 0.0,
            "perp_15d": 800.0,
            # This reaches the 30d flow threshold but its window is not mature.
            "weighted_30d": 1_100.0,
            "coverage_30d": 0.20,
            "span_hours_30d": 144.0,
            "bullish_runs_30d": 20,
            "bullish_days_30d": 10,
            "runs_30d": 20,
            "gaps_30d": 0,
            "bullish_wallets_30d": 3,
            "bullish_top1_share_30d": 0.30,
            "bullish_top3_share_30d": 0.60,
            "spot_share_30d": 0.0,
            "bullish_wallet_spot_share_30d": 0.0,
            "perp_30d": 1_100.0,
        }

        with mock.patch.object(monitor, "threshold", return_value=100.0), mock.patch.multiple(
            monitor,
            ROLLING_FLOW_WINDOWS_HOURS=[360.0, 720.0],
            ROLLING_REQUIRE_WINDOW_MATURITY=True,
            ROLLING_SCORE_USE_BEST_HORIZON=True,
            ROLLING_LEVERAGE_MODE=False,
        ):
            score, _reasons, parts = monitor.rolling_score_for_coin("TEST", rolling, {})

        self.assertGreater(score, 0.0)
        self.assertEqual(parts["best_window"], "15d")
        self.assertEqual(parts["rolling_immature_risk"], 0)


class LongTermStreakTests(unittest.TestCase):
    def test_current_round_counts_toward_candidate_streak(self):
        thresholds = {
            "DEFAULT": {
                "score_push": 8.0,
                "min_watch_score": 5.0,
                "perp": 1_000.0,
                "spot": 1_000.0,
            }
        }
        signal = {
            "coin": "TEST",
            "direction": "bullish",
            "long_score": 8.5,
            "alert_score": 2.0,
            "signal_category": "只观察",
            "watchlist": "observe",
            "conclusion": "只观察",
            "risk": "",
            "score_parts": {},
        }

        def prior_or_inclusive_streak(*_args, **kwargs):
            # Two qualifying rows are already persisted. The signal currently
            # being classified is the third qualifying round.
            return 3 if kwargs.get("include_current") else 2

        with mock.patch.object(monitor, "_long_short_gate", return_value=(True, [])), mock.patch.object(
            monitor, "signal_streak", side_effect=prior_or_inclusive_streak
        ), mock.patch.multiple(
            monitor,
            LONG_SHORT_STATE_MODE=True,
            LONG_SHORT_MIN_STREAK_FORMING=2,
            LONG_SHORT_MIN_STREAK_CANDIDATE=3,
        ):
            [result] = monitor.enhance_long_short_state(3, [signal], thresholds)

        self.assertEqual(result["candidate_state"], "CANDIDATE")
        self.assertEqual(result["watchlist"], "long")


if __name__ == "__main__":
    unittest.main()

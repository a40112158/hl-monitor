import datetime as dt
import importlib.util
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("analysis_layer", ROOT / "analysis_layer.py")
analysis = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(analysis)


def synthetic_rows(count=60):
    rows = []
    start = dt.datetime(2025, 1, 1)
    for i in range(count):
        weak = i % 4 == 0
        rows.append({
            "event_id": i + 1,
            "created_at": (start + dt.timedelta(hours=i * 25)).strftime("%Y-%m-%d %H:%M:%S"),
            "coin": "TEST",
            "abs_score": 8.1 if weak else 9.0,
            "ret_24h": -2.0 if weak else 3.0,
            "ret_72h": -3.0 if weak else 5.0,
            "ret_7d": -5.0 if weak else 8.0,
        })
    return rows


class RecommendationTests(unittest.TestCase):
    def test_raises_threshold_when_both_train_and_validation_improve(self):
        result = analysis.recommend_threshold(
            synthetic_rows(), 8.0, min_samples=24, validation_samples=8, step=0.25, min_gain=0.1
        )
        self.assertEqual(result["status"], "recommend_change")
        self.assertEqual(result["recommended"], 8.25)
        self.assertGreater(result["train_gain"], 0)
        self.assertGreater(result["validation_gain"], 0)

    def test_insufficient_samples_never_changes_threshold(self):
        result = analysis.recommend_threshold(
            synthetic_rows(10), 8.0, min_samples=24, validation_samples=8
        )
        self.assertEqual(result["status"], "insufficient_samples")

    def test_post_change_regression_triggers_rollback(self):
        start = dt.datetime(2025, 2, 1)
        rows = []
        for i in range(24):
            high = i % 2 == 0
            rows.append({
                "event_id": i + 1,
                "created_at": (start + dt.timedelta(days=i)).strftime("%Y-%m-%d %H:%M:%S"),
                "coin": "TEST",
                "direction": "bullish",
                "abs_score": 9.0 if high else 8.1,
                "ret_72h": -5.0 if high else 5.0,
            })
        meta = {
            "applied_at": "2025-01-31 00:00:00",
            "previous_score_push": 8.0,
            "return_key": "ret_72h",
            "hurdle": 2.0,
        }
        result = analysis.post_change_check(rows, 8.25, meta)
        self.assertEqual(result["status"], "rollback")

    def test_shadow_candidate_is_promoted_only_after_new_samples_improve(self):
        rows = synthetic_rows(60)
        shadow = {
            "started_at": "2024-12-31 00:00:00",
            "current_score_push": 8.0,
            "candidate_score_push": 8.25,
            "return_key": "ret_7d",
            "hurdle": 4.0,
        }
        result = analysis.shadow_check(rows, shadow)
        self.assertEqual(result["status"], "promote")
        self.assertGreater(result["quality_gap"], 0)


class EndToEndAnalysisTests(unittest.TestCase):
    def test_guarded_mode_writes_separate_auto_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = root / "monitor.db"
            conn = sqlite3.connect(db)
            conn.execute(
                """
                CREATE TABLE signal_events (
                    event_id INTEGER PRIMARY KEY, created_at TEXT, coin TEXT,
                    direction TEXT, score REAL, ret_24h REAL, ret_72h REAL, ret_7d REAL,
                    model_version INTEGER
                )
                """
            )
            for row in synthetic_rows():
                conn.execute(
                    "INSERT INTO signal_events VALUES (?, ?, ?, 'bullish', ?, ?, ?, ?, 2)",
                    (
                        row["event_id"], row["created_at"], row["coin"], row["abs_score"],
                        row["ret_24h"], row["ret_72h"], row["ret_7d"],
                    ),
                )
            conn.commit()
            conn.close()
            manual = root / "coin_thresholds.json"
            auto = root / "coin_thresholds_auto.json"
            manual.write_text(json.dumps({"DEFAULT": {"score_push": 8}}), encoding="utf-8")

            with mock.patch.object(analysis, "MODE", "guarded"), mock.patch.object(
                analysis, "gemini_enabled", return_value=False
            ):
                snapshot = analysis.run_analysis(db, manual, auto, root / "reports", root / "reports/details")

            saved = json.loads(auto.read_text(encoding="utf-8"))
            self.assertEqual(snapshot["changes_applied"], 0)
            self.assertEqual(snapshot["shadows_started"], 1)
            self.assertEqual(saved["shadows"]["TEST"]["candidate_score_push"], 8.25)
            self.assertEqual(json.loads(manual.read_text(encoding="utf-8"))["DEFAULT"]["score_push"], 8)


if __name__ == "__main__":
    unittest.main()

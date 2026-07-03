#!/usr/bin/env python3
"""Guarded, auditable self-optimization for HL Monitor.

The layer reads evaluated alert events and tunes bounded monitoring thresholds.
It never places orders and never edits the manual threshold file. Changes are
written to ``coin_thresholds_auto.json`` and consumed on the next monitor run.

By default it optimizes ``score_push`` and can safely tighten ``min_watch_score``.
``min_watch_score`` is only moved upward because historical low-score events that
were filtered out cannot be evaluated retroactively.
"""

import argparse
import csv
import datetime as dt
import json
import math
import os
import sqlite3
import statistics
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from gemini_optimizer import analyze_outcome as gemini_analyze_outcome
from gemini_optimizer import is_enabled as gemini_enabled
from gemini_optimizer import review_candidate as gemini_review_candidate
from optimization_store import archive_samples, load_samples


DB_FILE = os.getenv("HL_DB_FILE", "hl_monitor.db")
THRESHOLD_FILE = os.getenv("THRESHOLD_FILE", "coin_thresholds.json")
AUTO_THRESHOLD_FILE = os.getenv("AUTO_THRESHOLD_FILE", "coin_thresholds_auto.json")
REPORT_DIR = os.getenv("REPORT_DIR", "reports")
DETAILS_DIR = os.getenv("DETAILS_DIR", os.path.join(REPORT_DIR, "details"))

MODE = os.getenv("AUTO_OPTIMIZE_MODE", "shadow").strip().lower()
MIN_SAMPLES = max(12, int(os.getenv("AUTO_OPTIMIZE_MIN_SAMPLES", "24")))
VALIDATION_SAMPLES = max(4, int(os.getenv("AUTO_OPTIMIZE_VALIDATION_SAMPLES", "8")))
STEP = max(0.05, float(os.getenv("AUTO_OPTIMIZE_STEP", "0.25")))
MIN_GAIN = max(0.0, float(os.getenv("AUTO_OPTIMIZE_MIN_GAIN", "0.10")))
COOLDOWN_HOURS = max(1.0, float(os.getenv("AUTO_OPTIMIZE_COOLDOWN_HOURS", "24")))
POST_SAMPLES = max(6, int(os.getenv("AUTO_OPTIMIZE_POST_SAMPLES", "12")))
MAX_CHANGES = max(1, int(os.getenv("AUTO_OPTIMIZE_MAX_CHANGES", "3")))
REVIEW_TOP_N = max(MAX_CHANGES, int(os.getenv("AUTO_OPTIMIZE_REVIEW_TOP_N", "5")))
MIN_THRESHOLD = float(os.getenv("AUTO_OPTIMIZE_MIN_THRESHOLD", "4"))
MAX_THRESHOLD = float(os.getenv("AUTO_OPTIMIZE_MAX_THRESHOLD", "12"))
MIN_WATCH_OPTIMIZE_ENABLED = os.getenv("AUTO_OPTIMIZE_MIN_WATCH_ENABLED", "1") == "1"
MIN_WATCH_MIN_THRESHOLD = float(os.getenv("AUTO_OPTIMIZE_MIN_WATCH_MIN", "4"))
MIN_WATCH_MAX_THRESHOLD = float(os.getenv("AUTO_OPTIMIZE_MIN_WATCH_MAX", "8"))
ROLLBACK_GAP = max(MIN_GAIN, float(os.getenv("AUTO_OPTIMIZE_ROLLBACK_GAP", "0.20")))
REQUIRE_DATA_QUALITY = os.getenv("AUTO_OPTIMIZE_REQUIRE_DATA_QUALITY", "1") == "1"
DATA_QUALITY_MIN_SUCCESS_RATE = max(0.0, min(1.0, float(os.getenv("DATA_QUALITY_MIN_SUCCESS_RATE", os.getenv("MIN_OK_RATE", "0.85")))))
EVENT_GAP_HOURS = max(1.0, float(os.getenv("AUTO_OPTIMIZE_EVENT_GAP_HOURS", "24")))
MAX_PENDING_DAYS = max(7.0, float(os.getenv("AUTO_OPTIMIZE_MAX_PENDING_DAYS", "30")))
SAMPLE_RETENTION_DAYS = max(35, int(os.getenv("AUTO_OPTIMIZE_SAMPLE_RETENTION_DAYS", "180")))
HISTORY_MAX_LINES = max(200, int(os.getenv("AUTO_OPTIMIZE_HISTORY_MAX_LINES", "2000")))

HORIZONS = [
    ("7d", "ret_7d", 4.0),
    ("72h", "ret_72h", 2.0),
    ("24h", "ret_24h", 1.0),
]


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None, microsecond=0)


def now_str() -> str:
    return utc_now().strftime("%Y-%m-%d %H:%M:%S")


def as_float(value: Any) -> Optional[float]:
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


def parse_time(value: Any) -> Optional[dt.datetime]:
    if not value:
        return None
    text = str(value).replace("T", " ").replace("Z", "").split("+")[0]
    try:
        return dt.datetime.fromisoformat(text)
    except ValueError:
        return None


def load_json(path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else default
    except (OSError, json.JSONDecodeError):
        return default


def write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temp, path)


def load_events(db_path: Path) -> List[Dict[str, Any]]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='signal_events'"
        ).fetchone()
        if not exists:
            return []
        rows = conn.execute(
            """
            SELECT event_id, created_at, coin, direction, score,
                   ret_24h, ret_72h, ret_7d
            FROM signal_events
            WHERE ret_24h IS NOT NULL OR ret_72h IS NOT NULL OR ret_7d IS NOT NULL
            ORDER BY created_at, event_id
            """
        ).fetchall()
    finally:
        conn.close()

    out: List[Dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        score = as_float(row.get("score"))
        coin = str(row.get("coin") or "").upper().strip()
        if not coin or score is None:
            continue
        row["coin"] = coin
        row["abs_score"] = abs(score)
        out.append(row)
    return out


def deduplicate_events(rows: List[Dict[str, Any]], gap_hours: float = EVENT_GAP_HOURS) -> List[Dict[str, Any]]:
    """Collapse repeated half-hour signals into more independent observations."""
    kept: List[Dict[str, Any]] = []
    last_seen: Dict[Tuple[str, str], dt.datetime] = {}
    ordered = sorted(rows, key=lambda r: (str(r.get("created_at") or ""), int(r.get("event_id") or 0)))
    for row in ordered:
        created = parse_time(row.get("created_at"))
        key = (str(row.get("coin") or ""), str(row.get("direction") or ""))
        if created is None:
            continue
        previous = last_seen.get(key)
        if previous is not None and (created - previous).total_seconds() < gap_hours * 3600:
            continue
        kept.append(row)
        last_seen[key] = created
    return kept



def current_threshold(manual: Dict[str, Any], auto: Dict[str, Any], coin: str, key: str = "score_push") -> float:
    defaults = {"score_push": 8.0, "min_watch_score": 5.0}
    default = as_float((manual.get("DEFAULT") or {}).get(key)) or defaults.get(key, 0.0)
    manual_value = as_float((manual.get(coin) or {}).get(key))
    auto_value = as_float(((auto.get("overrides") or {}).get(coin) or {}).get(key))
    if key == "min_watch_score" and auto_value is not None:
        # Safe tightening only. Historical events below the old watch threshold
        # are unavailable, so automatic lowering would be statistically blind.
        base = manual_value if manual_value is not None else default
        return max(base, auto_value)
    return auto_value if auto_value is not None else (manual_value if manual_value is not None else default)


def parameter_bounds(parameter: str) -> Tuple[float, float]:
    if parameter == "min_watch_score":
        return MIN_WATCH_MIN_THRESHOLD, MIN_WATCH_MAX_THRESHOLD
    return MIN_THRESHOLD, MAX_THRESHOLD


def shadow_parameter(shadow: Dict[str, Any]) -> str:
    return str(shadow.get("parameter") or "score_push")


def meta_parameter(meta: Dict[str, Any]) -> str:
    return str(meta.get("parameter") or "score_push")


def shadow_current_value(shadow: Dict[str, Any]) -> Optional[float]:
    param = shadow_parameter(shadow)
    return (
        as_float(shadow.get("current_value"))
        or as_float(shadow.get(f"current_{param}"))
        or as_float(shadow.get("current_score_push"))
    )


def shadow_candidate_value(shadow: Dict[str, Any]) -> Optional[float]:
    param = shadow_parameter(shadow)
    return (
        as_float(shadow.get("candidate_value"))
        or as_float(shadow.get(f"candidate_{param}"))
        or as_float(shadow.get("candidate_score_push"))
    )


def previous_meta_value(meta: Dict[str, Any]) -> Optional[float]:
    return as_float(meta.get("previous_value")) or as_float(meta.get("previous_score_push"))


def data_quality_ok(report_dir: Path) -> Tuple[bool, str]:
    path = report_dir / "last_run_status.json"
    if not path.exists():
        return True, "last_run_status_missing_non_blocking"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return True, "last_run_status_not_object_non_blocking"
    except (OSError, json.JSONDecodeError) as exc:
        return True, f"last_run_status_unreadable_non_blocking:{type(exc).__name__}"
    try:
        ok_rate = float(payload.get("success_rate"))
    except (TypeError, ValueError):
        return True, "success_rate_missing_non_blocking"
    if ok_rate > 1.0:
        ok_rate /= 100.0
    if REQUIRE_DATA_QUALITY and ok_rate < DATA_QUALITY_MIN_SUCCESS_RATE:
        return False, f"success_rate {ok_rate:.2%} below required {DATA_QUALITY_MIN_SUCCESS_RATE:.2%}"
    return True, f"success_rate {ok_rate:.2%}"

def choose_horizon(rows: List[Dict[str, Any]], min_samples: int = MIN_SAMPLES) -> Optional[Tuple[str, str, float]]:
    for label, key, hurdle in HORIZONS:
        if sum(as_float(r.get(key)) is not None for r in rows) >= min_samples:
            return label, key, hurdle
    return None


def metric(rows: Iterable[Dict[str, Any]], threshold: float, return_key: str, hurdle: float) -> Optional[Dict[str, float]]:
    values = [
        as_float(r.get(return_key))
        for r in rows
        if (as_float(r.get("abs_score")) or 0.0) >= threshold and as_float(r.get(return_key)) is not None
    ]
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    mean = statistics.fmean(vals)
    median = statistics.median(vals)
    std = statistics.stdev(vals) if len(vals) > 1 else 0.0
    stderr = std / math.sqrt(len(vals))
    win_rate = sum(v > 0 for v in vals) / len(vals)
    hurdle_rate = sum(v >= hurdle for v in vals) / len(vals)
    # Conservative return plus a small reward for consistently beating the
    # horizon hurdle. The standard-error penalty resists noisy tiny samples.
    quality = mean - 0.5 * stderr + 0.25 * hurdle * (hurdle_rate - 0.5)
    return {
        "n": float(len(vals)),
        "mean": mean,
        "median": median,
        "win_rate": win_rate,
        "hurdle_rate": hurdle_rate,
        "stderr": stderr,
        "quality": quality,
    }


def split_train_validation(rows: List[Dict[str, Any]], validation_samples: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    ordered = sorted(rows, key=lambda r: (str(r.get("created_at") or ""), int(r.get("event_id") or 0)))
    validation_n = max(validation_samples, int(math.ceil(len(ordered) * 0.25)))
    validation_n = min(validation_n, max(validation_samples, len(ordered) // 2))
    return ordered[:-validation_n], ordered[-validation_n:]


def recommend_threshold(
    rows: List[Dict[str, Any]],
    current: float,
    *,
    min_samples: int = MIN_SAMPLES,
    validation_samples: int = VALIDATION_SAMPLES,
    step: float = STEP,
    min_gain: float = MIN_GAIN,
) -> Dict[str, Any]:
    horizon = choose_horizon(rows, min_samples)
    if horizon is None:
        return {"status": "insufficient_samples", "samples": len(rows), "current": current}
    horizon_label, return_key, hurdle = horizon
    mature = [r for r in rows if as_float(r.get(return_key)) is not None]
    train, validation = split_train_validation(mature, validation_samples)
    min_train = max(validation_samples, min_samples - validation_samples)
    candidates = sorted({
        round(max(MIN_THRESHOLD, current - step), 4),
        round(current, 4),
        round(min(MAX_THRESHOLD, current + step), 4),
    })

    evaluated: Dict[float, Dict[str, Any]] = {}
    for candidate in candidates:
        train_metric = metric(train, candidate, return_key, hurdle)
        validation_metric = metric(validation, candidate, return_key, hurdle)
        if not train_metric or not validation_metric:
            continue
        if train_metric["n"] < min_train or validation_metric["n"] < validation_samples:
            continue
        evaluated[candidate] = {"train": train_metric, "validation": validation_metric}

    baseline = evaluated.get(round(current, 4))
    if not baseline:
        return {
            "status": "insufficient_threshold_samples",
            "samples": len(mature),
            "current": current,
            "horizon": horizon_label,
        }

    choices = []
    for candidate, result in evaluated.items():
        if abs(candidate - current) < 1e-9:
            continue
        train_gain = result["train"]["quality"] - baseline["train"]["quality"]
        validation_gain = result["validation"]["quality"] - baseline["validation"]["quality"]
        if train_gain >= min_gain and validation_gain >= min_gain:
            choices.append((min(train_gain, validation_gain), validation_gain, candidate, result))

    if not choices:
        return {
            "status": "stable",
            "samples": len(mature),
            "current": current,
            "recommended": current,
            "horizon": horizon_label,
            "baseline": baseline,
        }

    _guarded_gain, validation_gain, candidate, result = max(choices, key=lambda x: (x[0], x[1]))
    return {
        "status": "recommend_change",
        "samples": len(mature),
        "current": current,
        "recommended": candidate,
        "horizon": horizon_label,
        "return_key": return_key,
        "hurdle": hurdle,
        "train_gain": result["train"]["quality"] - baseline["train"]["quality"],
        "validation_gain": validation_gain,
        "baseline": baseline,
        "candidate": result,
        "regime_metrics": regime_metrics(mature, current, candidate, return_key, hurdle),
    }



def recommend_min_watch_threshold(
    rows: List[Dict[str, Any]],
    current: float,
    *,
    min_samples: int = MIN_SAMPLES,
    validation_samples: int = VALIDATION_SAMPLES,
    step: float = STEP,
    min_gain: float = MIN_GAIN,
) -> Dict[str, Any]:
    """Safely tighten min_watch_score when lower-score watch signals are noisy.

    This function never recommends lowering min_watch_score. The monitor only
    stores evaluated signal_events that already passed the active watch threshold,
    so loosening the threshold would require historical samples that do not exist.
    """
    if not MIN_WATCH_OPTIMIZE_ENABLED:
        return {"status": "disabled", "current": current}
    horizon = choose_horizon(rows, min_samples)
    if horizon is None:
        return {"status": "insufficient_samples", "samples": len(rows), "current": current}
    horizon_label, return_key, hurdle = horizon
    mature = [r for r in rows if as_float(r.get(return_key)) is not None]
    train, validation = split_train_validation(mature, validation_samples)
    min_train = max(validation_samples, min_samples - validation_samples)
    _lo, hi = parameter_bounds("min_watch_score")
    candidate = round(min(hi, current + step), 4)
    if candidate <= current + 1e-9:
        return {
            "status": "stable",
            "samples": len(mature),
            "current": current,
            "recommended": current,
            "horizon": horizon_label,
        }
    baseline_train = metric(train, current, return_key, hurdle)
    baseline_validation = metric(validation, current, return_key, hurdle)
    candidate_train = metric(train, candidate, return_key, hurdle)
    candidate_validation = metric(validation, candidate, return_key, hurdle)
    if not baseline_train or not baseline_validation or not candidate_train or not candidate_validation:
        return {"status": "insufficient_threshold_samples", "samples": len(mature), "current": current, "horizon": horizon_label}
    if candidate_train["n"] < min_train or candidate_validation["n"] < validation_samples:
        return {"status": "insufficient_threshold_samples", "samples": len(mature), "current": current, "horizon": horizon_label}
    train_gain = candidate_train["quality"] - baseline_train["quality"]
    validation_gain = candidate_validation["quality"] - baseline_validation["quality"]
    if train_gain >= min_gain and validation_gain >= min_gain:
        return {
            "status": "recommend_change",
            "parameter": "min_watch_score",
            "samples": len(mature),
            "current": current,
            "recommended": candidate,
            "horizon": horizon_label,
            "return_key": return_key,
            "hurdle": hurdle,
            "train_gain": train_gain,
            "validation_gain": validation_gain,
            "baseline": {"train": baseline_train, "validation": baseline_validation},
            "candidate": {"train": candidate_train, "validation": candidate_validation},
            "regime_metrics": regime_metrics(mature, current, candidate, return_key, hurdle),
            "note": "tighten_watch_threshold_only",
        }
    return {
        "status": "stable",
        "samples": len(mature),
        "current": current,
        "recommended": current,
        "horizon": horizon_label,
        "baseline": {"train": baseline_train, "validation": baseline_validation},
        "candidate": {"train": candidate_train, "validation": candidate_validation},
        "train_gain": train_gain,
        "validation_gain": validation_gain,
    }


def rounded_metric(value: Optional[Dict[str, float]]) -> Dict[str, Any]:
    if not value:
        return {}
    return {k: (int(v) if k == "n" else round(v, 6)) for k, v in value.items()}


def regime_metrics(
    rows: List[Dict[str, Any]], current: float, candidate: float, return_key: str, hurdle: float
) -> Dict[str, Any]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("market_regime") or "unknown"), []).append(row)
    result: Dict[str, Any] = {}
    for regime, subset in grouped.items():
        baseline = metric(subset, current, return_key, hurdle)
        proposed = metric(subset, candidate, return_key, hurdle)
        if not baseline and not proposed:
            continue
        result[regime] = {
            "current": rounded_metric(baseline),
            "candidate": rounded_metric(proposed),
        }
    return result


def cooldown_active(meta: Dict[str, Any]) -> bool:
    applied = parse_time(meta.get("applied_at"))
    return bool(applied and (utc_now() - applied).total_seconds() < COOLDOWN_HOURS * 3600)



def post_change_check(
    rows: List[Dict[str, Any]], current: float, meta: Dict[str, Any]
) -> Dict[str, Any]:
    parameter = meta_parameter(meta)
    previous = previous_meta_value(meta)
    applied_at = parse_time(meta.get("applied_at"))
    return_key = str(meta.get("return_key") or "ret_72h")
    hurdle = as_float(meta.get("hurdle")) or 2.0
    if previous is None or applied_at is None:
        return {"status": "invalid_pending_state", "parameter": parameter}
    post_rows = [
        r for r in rows
        if (parse_time(r.get("created_at")) or dt.datetime.min) >= applied_at
        and as_float(r.get(return_key)) is not None
    ]
    current_metric = metric(post_rows, current, return_key, hurdle)
    previous_metric = metric(post_rows, previous, return_key, hurdle)
    if (
        not current_metric
        or not previous_metric
        or current_metric["n"] < POST_SAMPLES
        or previous_metric["n"] < POST_SAMPLES
    ):
        if (utc_now() - applied_at).total_seconds() >= MAX_PENDING_DAYS * 86400:
            return {
                "status": "rollback",
                "reason": "post_sample_timeout",
                "parameter": parameter,
                "previous": previous,
                "samples": len(post_rows),
            }
        return {"status": "awaiting_post_samples", "parameter": parameter, "samples": len(post_rows)}
    gap = current_metric["quality"] - previous_metric["quality"]
    return {
        "status": "rollback" if gap <= -ROLLBACK_GAP else "validated",
        "parameter": parameter,
        "quality_gap": gap,
        "current_metric": current_metric,
        "previous_metric": previous_metric,
        "previous": previous,
        "samples": len(post_rows),
    }


def shadow_check(rows: List[Dict[str, Any]], shadow: Dict[str, Any]) -> Dict[str, Any]:
    parameter = shadow_parameter(shadow)
    started_at = parse_time(shadow.get("started_at"))
    current = shadow_current_value(shadow)
    candidate = shadow_candidate_value(shadow)
    return_key = str(shadow.get("return_key") or "ret_72h")
    hurdle = as_float(shadow.get("hurdle")) or 2.0
    if started_at is None or current is None or candidate is None:
        return {"status": "reject", "parameter": parameter, "reason": "invalid_shadow_state"}
    post_rows = [
        row for row in rows
        if (parse_time(row.get("created_at")) or dt.datetime.min) >= started_at
        and as_float(row.get(return_key)) is not None
    ]
    current_metric = metric(post_rows, current, return_key, hurdle)
    candidate_metric = metric(post_rows, candidate, return_key, hurdle)
    age_days = max(0.0, (utc_now() - started_at).total_seconds() / 86400)
    enough = (
        current_metric is not None
        and candidate_metric is not None
        and current_metric["n"] >= POST_SAMPLES
        and candidate_metric["n"] >= POST_SAMPLES
    )
    if not enough:
        return {
            "status": "reject" if age_days >= MAX_PENDING_DAYS else "collecting",
            "parameter": parameter,
            "reason": "sample_timeout" if age_days >= MAX_PENDING_DAYS else "awaiting_samples",
            "samples": len(post_rows),
            "age_days": age_days,
        }
    quality_gap = candidate_metric["quality"] - current_metric["quality"]
    if quality_gap >= MIN_GAIN:
        status = "promote"
    elif quality_gap <= -ROLLBACK_GAP or age_days >= MAX_PENDING_DAYS:
        status = "reject"
    else:
        status = "collecting"
    return {
        "status": status,
        "parameter": parameter,
        "reason": "candidate_improved" if status == "promote" else (
            "candidate_regressed" if status == "reject" else "difference_not_mature"
        ),
        "samples": len(post_rows),
        "age_days": age_days,
        "quality_gap": quality_gap,
        "current_metric": current_metric,
        "candidate_metric": candidate_metric,
    }


def append_history(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
    if path.stat().st_size > 2 * 1024 * 1024:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        if len(lines) > HISTORY_MAX_LINES:
            path.write_text("\n".join(lines[-HISTORY_MAX_LINES:]) + "\n", encoding="utf-8")



def recommendation_priority(recommendation: Dict[str, Any]) -> Tuple[float, float, float, float]:
    """Rank optimization candidates before spending Gemini calls.

    The optimizer first uses deterministic train/validation evidence for every
    coin, then sends only the best bounded candidates to Gemini. This avoids
    reviewing coins alphabetically and protects the API quota.
    """
    train_gain = as_float(recommendation.get("train_gain")) or 0.0
    validation_gain = as_float(recommendation.get("validation_gain")) or 0.0
    samples = as_float(recommendation.get("samples")) or 0.0
    current = as_float(recommendation.get("current")) or 0.0
    recommended = as_float(recommendation.get("recommended")) or current
    guarded_gain = min(train_gain, validation_gain)
    # Larger guarded gain first, then validation gain, then sample size. For
    # equal evidence, prefer the smaller threshold movement.
    return (guarded_gain, validation_gain, samples, -abs(recommended - current))


def default_ai_review(reason: str = "No candidate to review") -> Dict[str, Any]:
    return {
        "status": "not_requested",
        "approved": True,
        "decision": "APPROVE",
        "confidence": 1.0,
        "risk_level": "low",
        "reason": reason,
        "warnings": [],
        "model": "deterministic",
    }

def run_analysis(
    db_path: Path,
    manual_path: Path,
    auto_path: Path,
    report_dir: Path,
    details_dir: Path,
) -> Dict[str, Any]:
    report_dir.mkdir(parents=True, exist_ok=True)
    details_dir.mkdir(parents=True, exist_ok=True)
    quality_ok, quality_reason = data_quality_ok(report_dir)
    if not quality_ok:
        snapshot = {
            "generated_at": now_str(),
            "mode": MODE if MODE in {"off", "shadow", "guarded"} else "shadow",
            "status": "skipped_data_quality",
            "data_quality_reason": quality_reason,
            "raw_events": 0,
            "events": 0,
            "coins": 0,
            "changes_applied": 0,
            "shadows_started": 0,
            "candidate_recommendations": 0,
            "candidate_reviews_limit": REVIEW_TOP_N,
            "gemini_review_enabled": gemini_enabled(),
            "guardrails": {"data_quality_min_success_rate": DATA_QUALITY_MIN_SUCCESS_RATE},
            "results": [],
        }
        write_json_atomic(details_dir / "auto_analysis_latest.json", snapshot)
        with (report_dir / "auto_analysis_report.txt").open("w", encoding="utf-8") as handle:
            print("【自动优化分析层】", file=handle)
            print(f"更新时间 UTC：{snapshot['generated_at']} | 状态：跳过", file=handle)
            print(f"原因：数据质量不足，{quality_reason}", file=handle)
            print("为避免坏数据污染阈值优化，本轮不做 Gemini 审核、不启动影子实验、不修改自动阈值。", file=handle)
        return snapshot

    manual = load_json(manual_path, {"DEFAULT": {"score_push": 8.0, "min_watch_score": 5.0}})
    auto = load_json(auto_path, {"version": 1, "overrides": {}, "meta": {}})
    auto.setdefault("version", 1)
    auto.setdefault("overrides", {})
    auto.setdefault("meta", {})
    auto.setdefault("shadows", {})
    archive_stats = archive_samples(
        db_path, retention_days=SAMPLE_RETENTION_DAYS, gap_hours=EVENT_GAP_HOURS
    )
    raw_events = load_events(db_path)
    stored_events = load_samples(db_path)
    events = stored_events if stored_events else deduplicate_events(raw_events)
    by_coin: Dict[str, List[Dict[str, Any]]] = {}
    for row in events:
        by_coin.setdefault(row["coin"], []).append(row)

    mode = MODE if MODE in {"off", "shadow", "guarded"} else "shadow"
    changes = 0
    shadows_started = 0
    results: List[Dict[str, Any]] = []
    pending_recommendations: List[Dict[str, Any]] = []
    history_path = details_dir / "auto_optimization_history.jsonl"

    # First pass: process existing shadows / pending applied changes immediately.
    # New recommendations are collected and sorted later, so Gemini reviews the
    # best deterministic candidates instead of whichever coin is alphabetically first.
    for coin in sorted(by_coin):
        rows = by_coin[coin]
        coin_meta = dict((auto.get("meta") or {}).get(coin) or {})

        shadow = dict((auto.get("shadows") or {}).get(coin) or {})
        if shadow:
            param = shadow_parameter(shadow)
            current = current_threshold(manual, auto, coin, param)
            check = shadow_check(rows, shadow)
            candidate_value = shadow_candidate_value(shadow)
            if check["status"] == "collecting":
                results.append({
                    "coin": coin, "parameter": param, "action": "shadow_collecting", "current": current,
                    "recommended": candidate_value,
                    "horizon": shadow.get("return_key"), "samples": check.get("samples"),
                    "quality_gap": check.get("quality_gap"),
                })
                continue
            if check["status"] == "reject":
                auto["shadows"].pop(coin, None)
                append_history(history_path, {
                    "time": now_str(), "coin": coin, "parameter": param, "action": "shadow_rejected",
                    "current": current, "candidate": candidate_value,
                    "reason": check.get("reason"), "quality_gap": check.get("quality_gap"),
                })
                results.append({
                    "coin": coin, "parameter": param, "action": "shadow_rejected", "current": current,
                    "recommended": candidate_value,
                    "samples": check.get("samples"), "quality_gap": check.get("quality_gap"),
                })
                continue
            if candidate_value is None:
                auto["shadows"].pop(coin, None)
                results.append({"coin": coin, "parameter": param, "action": "shadow_rejected", "current": current, "reason": "missing_candidate_value"})
                continue
            candidate = float(candidate_value)
            outcome_ai = gemini_analyze_outcome({
                "coin": coin,
                "parameter": param,
                "deterministic_result": "shadow_promote",
                "current_value": current,
                "candidate_value": candidate,
                "current_score_push": current if param == "score_push" else None,
                "candidate_score_push": candidate if param == "score_push" else None,
                "horizon": shadow.get("return_key"),
                "post_samples": check.get("samples"),
                "quality_gap": check.get("quality_gap"),
                "current_metric": rounded_metric(check.get("current_metric")),
                "candidate_metric": rounded_metric(check.get("candidate_metric")),
                "safety_rule": "Only the pre-approved bounded candidate can be promoted",
            })
            applied = mode == "guarded" and changes < MAX_CHANGES
            if applied:
                auto["overrides"].setdefault(coin, {})[param] = candidate
                auto["meta"][coin] = {
                    "status": "pending",
                    "parameter": param,
                    "applied_at": now_str(),
                    "previous_value": current,
                    "previous_score_push": current if param == "score_push" else None,
                    "return_key": shadow.get("return_key", "ret_72h"),
                    "hurdle": shadow.get("hurdle", 2.0),
                    "reason": "shadow_validation_passed",
                    "shadow_quality_gap": check.get("quality_gap"),
                    "ai_outcome": outcome_ai,
                }
                auto["shadows"].pop(coin, None)
                changes += 1
                append_history(history_path, {
                    "time": now_str(), "coin": coin, "parameter": param, "action": "shadow_promoted",
                    "from": current, "to": candidate, "quality_gap": check.get("quality_gap"),
                    "ai_outcome": outcome_ai,
                })
            results.append({
                "coin": coin, "parameter": param, "action": "shadow_promoted" if applied else "shadow_ready",
                "current": current, "recommended": candidate,
                "samples": check.get("samples"), "quality_gap": check.get("quality_gap"),
                "ai_outcome_decision": outcome_ai.get("decision"),
                "ai_outcome_confidence": outcome_ai.get("confidence"),
                "ai_outcome_diagnosis": outcome_ai.get("diagnosis"),
            })
            continue

        if coin_meta.get("status") == "pending":
            param = meta_parameter(coin_meta)
            current = current_threshold(manual, auto, coin, param)
            check = post_change_check(rows, current, coin_meta)
            if check["status"] in {"awaiting_post_samples", "invalid_pending_state"}:
                results.append({"coin": coin, "parameter": param, "action": "hold_pending", "current": current, **check})
                continue
            if check["status"] == "rollback":
                previous = float(check["previous"])
                outcome_ai = gemini_analyze_outcome({
                    "coin": coin,
                    "parameter": param,
                    "deterministic_result": "rollback",
                    "current_value": current,
                    "previous_value": previous,
                    "current_score_push": current if param == "score_push" else None,
                    "previous_score_push": previous if param == "score_push" else None,
                    "horizon": coin_meta.get("return_key", "ret_72h"),
                    "post_samples": check.get("samples"),
                    "quality_gap": check.get("quality_gap"),
                    "current_metric": rounded_metric(check.get("current_metric")),
                    "previous_metric": rounded_metric(check.get("previous_metric")),
                    "reason": check.get("reason", "measured_quality_regression"),
                    "safety_rule": "AI analysis cannot block deterministic rollback",
                })
                applied = mode == "guarded" and changes < MAX_CHANGES
                if applied:
                    auto["overrides"].setdefault(coin, {})[param] = previous
                    auto["meta"][coin] = {
                        "status": "pending",
                        "parameter": param,
                        "applied_at": now_str(),
                        "previous_value": current,
                        "previous_score_push": current if param == "score_push" else None,
                        "return_key": coin_meta.get("return_key", "ret_72h"),
                        "hurdle": coin_meta.get("hurdle", 2.0),
                        "reason": "automatic_rollback",
                        "ai_outcome": outcome_ai,
                    }
                    changes += 1
                    append_history(history_path, {
                        "time": now_str(), "coin": coin, "parameter": param, "action": "rollback",
                        "from": current, "to": previous, "quality_gap": check.get("quality_gap"),
                        "ai_outcome": outcome_ai,
                    })
                results.append({
                    "coin": coin, "parameter": param, "action": "rollback_applied" if applied else "rollback_recommended",
                    "current": current, "recommended": previous, "samples": check.get("samples"),
                    "quality_gap": check.get("quality_gap"),
                    "ai_outcome_decision": outcome_ai.get("decision"),
                    "ai_outcome_confidence": outcome_ai.get("confidence"),
                    "ai_outcome_diagnosis": outcome_ai.get("diagnosis"),
                })
                continue
            outcome_ai = gemini_analyze_outcome({
                "coin": coin,
                "parameter": param,
                "deterministic_result": "validated",
                "current_value": current,
                "previous_value": previous_meta_value(coin_meta),
                "current_score_push": current if param == "score_push" else None,
                "previous_score_push": coin_meta.get("previous_score_push") if param == "score_push" else None,
                "horizon": coin_meta.get("return_key", "ret_72h"),
                "post_samples": check.get("samples"),
                "quality_gap": check.get("quality_gap"),
                "current_metric": rounded_metric(check.get("current_metric")),
                "previous_metric": rounded_metric(check.get("previous_metric")),
            })
            auto["meta"][coin]["status"] = "validated"
            auto["meta"][coin]["validated_at"] = now_str()
            auto["meta"][coin]["ai_outcome"] = outcome_ai
            append_history(history_path, {
                "time": now_str(), "coin": coin, "parameter": param, "action": "validated",
                "threshold": current, "quality_gap": check.get("quality_gap"),
                "ai_outcome": outcome_ai,
            })
            results.append({
                "coin": coin, "parameter": param, "action": "validated", "current": current,
                "samples": check.get("samples"), "quality_gap": check.get("quality_gap"),
                "ai_outcome_decision": outcome_ai.get("decision"),
                "ai_outcome_confidence": outcome_ai.get("confidence"),
                "ai_outcome_diagnosis": outcome_ai.get("diagnosis"),
            })
            continue

        if coin_meta and cooldown_active(coin_meta):
            param = meta_parameter(coin_meta)
            current = current_threshold(manual, auto, coin, param)
            results.append({"coin": coin, "parameter": param, "action": "cooldown", "current": current, "samples": len(rows)})
            continue

        score_current = current_threshold(manual, auto, coin, "score_push")
        recommendation = recommend_threshold(rows, score_current)
        recommendation["parameter"] = "score_push"
        action = recommendation.get("status", "unknown")
        if action == "recommend_change":
            pending_recommendations.append({
                "coin": coin,
                "parameter": "score_push",
                "rows": rows,
                "current": score_current,
                "recommendation": recommendation,
            })
            continue

        baseline_validation = ((recommendation.get("baseline") or {}).get("validation"))
        candidate_validation = ((recommendation.get("candidate") or {}).get("validation"))
        results.append({
            "coin": coin,
            "parameter": "score_push",
            "action": action,
            "current": score_current,
            "recommended": recommendation.get("recommended", score_current),
            "horizon": recommendation.get("horizon", ""),
            "samples": recommendation.get("samples", len(rows)),
            "train_gain": recommendation.get("train_gain"),
            "validation_gain": recommendation.get("validation_gain"),
            "baseline_validation": rounded_metric(baseline_validation),
            "candidate_validation": rounded_metric(candidate_validation),
        })

        if MIN_WATCH_OPTIMIZE_ENABLED:
            watch_current = current_threshold(manual, auto, coin, "min_watch_score")
            watch_recommendation = recommend_min_watch_threshold(rows, watch_current)
            watch_recommendation["parameter"] = "min_watch_score"
            if watch_recommendation.get("status") == "recommend_change":
                pending_recommendations.append({
                    "coin": coin,
                    "parameter": "min_watch_score",
                    "rows": rows,
                    "current": watch_current,
                    "recommendation": watch_recommendation,
                })

    pending_recommendations.sort(
        key=lambda item: recommendation_priority(item["recommendation"]),
        reverse=True,
    )

    # Second pass: review only the top deterministic candidates. This is the
    # quota-saving Gemini optimization path.
    for rank, item in enumerate(pending_recommendations, start=1):
        coin = item["coin"]
        param = str(item.get("parameter") or "score_push")
        current = float(item["current"])
        recommendation = item["recommendation"]
        priority = recommendation_priority(recommendation)
        ai_review: Dict[str, Any] = default_ai_review(
            "Gemini disabled; deterministic train/validation guardrails approved candidate"
        )
        reviewed = rank <= REVIEW_TOP_N
        lo, hi = parameter_bounds(param)
        if reviewed and gemini_enabled():
            ai_review = gemini_review_candidate({
                "coin": coin,
                "parameter": param,
                "current_value": current,
                "candidate_value": recommendation.get("recommended"),
                "current_score_push": current if param == "score_push" else None,
                "candidate_score_push": recommendation.get("recommended") if param == "score_push" else None,
                "horizon": recommendation.get("horizon"),
                "mature_samples": recommendation.get("samples"),
                "train_gain": recommendation.get("train_gain"),
                "validation_gain": recommendation.get("validation_gain"),
                "candidate_rank": rank,
                "candidate_priority": priority,
                "review_top_n": REVIEW_TOP_N,
                "baseline_train": rounded_metric(((recommendation.get("baseline") or {}).get("train"))),
                "baseline_validation": rounded_metric(((recommendation.get("baseline") or {}).get("validation"))),
                "candidate_train": rounded_metric(((recommendation.get("candidate") or {}).get("train"))),
                "candidate_validation": rounded_metric(((recommendation.get("candidate") or {}).get("validation"))),
                "market_regimes": recommendation.get("regime_metrics", {}),
                "safety_note": "min_watch_score candidates are tightening-only; no automatic loosening is allowed" if param == "min_watch_score" else "bounded score_push threshold adjustment",
                "hard_limits": {
                    "step": STEP,
                    "minimum": lo,
                    "maximum": hi,
                    "minimum_samples": MIN_SAMPLES,
                },
            })
        elif not reviewed:
            ai_review = {
                "status": "not_reviewed",
                "approved": False,
                "decision": "HOLD",
                "confidence": 0.0,
                "risk_level": "medium",
                "reason": f"Skipped to preserve Gemini quota; candidate rank {rank} is outside review_top_n={REVIEW_TOP_N}",
                "warnings": [],
                "model": "quota_guard",
            }

        applied = False
        if (
            reviewed
            and recommendation.get("status") == "recommend_change"
            and ai_review.get("approved")
            and mode == "guarded"
            and shadows_started < MAX_CHANGES
        ):
            new_value = float(recommendation["recommended"])
            shadow_payload = {
                "status": "collecting",
                "parameter": param,
                "started_at": now_str(),
                "current_value": current,
                "candidate_value": new_value,
                "return_key": recommendation["return_key"],
                "hurdle": recommendation["hurdle"],
                "train_gain": recommendation["train_gain"],
                "validation_gain": recommendation["validation_gain"],
                "candidate_rank": rank,
                "priority": priority,
                "reason": "train_validation_and_ai_approved" if gemini_enabled() else "train_and_validation_improved",
                "ai_model": ai_review.get("model"),
                "ai_confidence": ai_review.get("confidence"),
            }
            if param == "score_push":
                shadow_payload["current_score_push"] = current
                shadow_payload["candidate_score_push"] = new_value
            auto["shadows"][coin] = shadow_payload
            shadows_started += 1
            applied = True
            append_history(history_path, {
                "time": now_str(), "coin": coin, "parameter": param, "action": "shadow_started",
                "from": current, "to": new_value, "horizon": recommendation.get("horizon"),
                "rank": rank, "priority": priority,
                "train_gain": recommendation.get("train_gain"),
                "validation_gain": recommendation.get("validation_gain"),
                "ai_model": ai_review.get("model"),
                "ai_decision": ai_review.get("decision"),
                "ai_confidence": ai_review.get("confidence"),
            })

        baseline_validation = ((recommendation.get("baseline") or {}).get("validation"))
        candidate_validation = ((recommendation.get("candidate") or {}).get("validation"))
        action_name = "shadow_started" if applied else (
            "candidate_not_reviewed" if not reviewed else (
                "ai_hold" if recommendation.get("status") == "recommend_change" and not ai_review.get("approved") else recommendation.get("status", "unknown")
            )
        )
        results.append({
            "coin": coin,
            "parameter": param,
            "action": action_name,
            "current": current,
            "recommended": recommendation.get("recommended", current),
            "horizon": recommendation.get("horizon", ""),
            "samples": recommendation.get("samples", len(item.get("rows") or [])),
            "train_gain": recommendation.get("train_gain"),
            "validation_gain": recommendation.get("validation_gain"),
            "priority_rank": rank,
            "priority_score": priority[0],
            "reviewed": reviewed,
            "baseline_validation": rounded_metric(baseline_validation),
            "candidate_validation": rounded_metric(candidate_validation),
            "ai_status": ai_review.get("status"),
            "ai_decision": ai_review.get("decision"),
            "ai_confidence": ai_review.get("confidence"),
            "ai_risk": ai_review.get("risk_level"),
            "ai_reason": ai_review.get("reason"),
            "ai_warnings": ai_review.get("warnings", []),
            "ai_model": ai_review.get("model"),
        })

    if mode == "guarded":
        auto["updated_at"] = now_str()
        auto["mode"] = mode
        auto["review_top_n"] = REVIEW_TOP_N
        write_json_atomic(auto_path, auto)

    mature_counts = {
        label: sum(as_float(r.get(key)) is not None for r in events)
        for label, key, _hurdle in HORIZONS
    }
    snapshot = {
        "generated_at": now_str(),
        "mode": mode,
        "raw_events": len(raw_events),
        "events": len(events),
        "sample_store": archive_stats,
        "coins": len(by_coin),
        "mature_counts": mature_counts,
        "changes_applied": changes,
        "shadows_started": shadows_started,
        "candidate_recommendations": len(pending_recommendations),
        "candidate_reviews_limit": REVIEW_TOP_N,
        "gemini_review_enabled": gemini_enabled(),
        "guardrails": {
            "min_samples": MIN_SAMPLES,
            "validation_samples": VALIDATION_SAMPLES,
            "step": STEP,
            "min_gain": MIN_GAIN,
            "cooldown_hours": COOLDOWN_HOURS,
            "post_samples": POST_SAMPLES,
            "max_changes_per_run": MAX_CHANGES,
            "review_top_n": REVIEW_TOP_N,
            "score_push_range": [MIN_THRESHOLD, MAX_THRESHOLD],
            "min_watch_score_range": [MIN_WATCH_MIN_THRESHOLD, MIN_WATCH_MAX_THRESHOLD],
            "min_watch_score_tighten_only": True,
            "data_quality_min_success_rate": DATA_QUALITY_MIN_SUCCESS_RATE,
            "event_gap_hours": EVENT_GAP_HOURS,
            "max_pending_days": MAX_PENDING_DAYS,
            "sample_retention_days": SAMPLE_RETENTION_DAYS,
            "history_max_lines": HISTORY_MAX_LINES,
        },
        "results": results,
    }
    write_json_atomic(details_dir / "auto_analysis_latest.json", snapshot)

    fields = [
        "coin", "parameter", "action", "current", "recommended", "horizon", "samples",
        "train_gain", "validation_gain", "quality_gap", "status", "priority_rank", "priority_score", "reviewed",
        "ai_status", "ai_decision", "ai_confidence", "ai_risk", "ai_reason", "ai_model",
        "ai_outcome_decision", "ai_outcome_confidence", "ai_outcome_diagnosis",
    ]
    with (details_dir / "auto_optimization_latest.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    with (report_dir / "auto_analysis_report.txt").open("w", encoding="utf-8") as handle:
        print("【自动优化分析层】", file=handle)
        print(f"更新时间 UTC：{snapshot['generated_at']} | 模式：{mode} | Gemini审核：{'开启' if gemini_enabled() else '关闭'}", file=handle)
        print("优化范围：score_push + min_watch_score（min_watch_score 只自动提高，不自动放宽）；不修改手工阈值、不触发交易。", file=handle)
        print(
            f"原始/长期轻量样本：{len(raw_events)}/{len(events)} | 币种：{len(by_coin)} | "
            f"24h/72h/7d成熟样本：{mature_counts['24h']}/{mature_counts['72h']}/{mature_counts['7d']} | "
            f"本轮应用：{changes} | 新影子实验：{shadows_started} | 候选：{len(pending_recommendations)}",
            file=handle,
        )
        print(
            f"轻量样本库：保留={SAMPLE_RETENTION_DAYS}天 | 本轮新增={archive_stats.get('inserted', 0)} "
            f"更新={archive_stats.get('updated', 0)} 总计={archive_stats.get('total', 0)}",
            file=handle,
        )
        print(
            f"护栏：最少样本={MIN_SAMPLES}，验证集={VALIDATION_SAMPLES}，单次步长={STEP}，"
            f"最小双段提升={MIN_GAIN}，每轮最多修改={MAX_CHANGES}，Gemini最多审核Top {REVIEW_TOP_N}，"
            f"数据质量门槛={DATA_QUALITY_MIN_SUCCESS_RATE:.0%}",
            file=handle,
        )
        print("", file=handle)
        if not results:
            print("暂无可分析的成熟信号样本。", file=handle)
        for row in results:
            rank = f"#{row.get('priority_rank')} " if row.get("priority_rank") else ""
            print(
                f"{rank}{row['coin']} | {row.get('parameter', 'score_push')} | {row['action']} | 当前={row.get('current')} | "
                f"建议={row.get('recommended', '-')} | 周期={row.get('horizon', '-')} | "
                f"样本={row.get('samples', 0)} | 验证提升={row.get('validation_gain', '-')} | "
                f"AI={row.get('ai_decision', '-')}({row.get('ai_confidence', '-')}) {row.get('ai_reason', '')}",
                file=handle,
            )
            if row.get("ai_outcome_decision"):
                print(
                    f"  调整后AI复盘={row.get('ai_outcome_decision')}"
                    f"({row.get('ai_outcome_confidence', '-')}) {row.get('ai_outcome_diagnosis', '')}",
                    file=handle,
                )
    return snapshot

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="HL Monitor guarded auto-analysis layer")
    parser.add_argument("--db", default=DB_FILE)
    parser.add_argument("--manual-config", default=THRESHOLD_FILE)
    parser.add_argument("--auto-config", default=AUTO_THRESHOLD_FILE)
    parser.add_argument("--report-dir", default=REPORT_DIR)
    parser.add_argument("--details-dir", default=DETAILS_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    snapshot = run_analysis(
        Path(args.db), Path(args.manual_config), Path(args.auto_config),
        Path(args.report_dir), Path(args.details_dir),
    )
    print(
        f"[auto-analysis] mode={snapshot['mode']} events={snapshot['events']} "
        f"coins={snapshot['coins']} changes={snapshot['changes_applied']}",
        flush=True,
    )


if __name__ == "__main__":
    main()

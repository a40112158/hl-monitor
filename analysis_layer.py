#!/usr/bin/env python3
"""Guarded, auditable self-optimization for HL Monitor.

The layer reads evaluated alert events and tunes only ``score_push``. It never
places orders and never edits the manual threshold file. Changes are written to
``coin_thresholds_auto.json`` and consumed on the next monitor run.
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
MIN_THRESHOLD = float(os.getenv("AUTO_OPTIMIZE_MIN_THRESHOLD", "4"))
MAX_THRESHOLD = float(os.getenv("AUTO_OPTIMIZE_MAX_THRESHOLD", "12"))
ROLLBACK_GAP = max(MIN_GAIN, float(os.getenv("AUTO_OPTIMIZE_ROLLBACK_GAP", "0.20")))
EVENT_GAP_HOURS = max(1.0, float(os.getenv("AUTO_OPTIMIZE_EVENT_GAP_HOURS", "24")))
MAX_PENDING_DAYS = max(7.0, float(os.getenv("AUTO_OPTIMIZE_MAX_PENDING_DAYS", "30")))

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


def current_threshold(manual: Dict[str, Any], auto: Dict[str, Any], coin: str) -> float:
    default = as_float((manual.get("DEFAULT") or {}).get("score_push")) or 8.0
    manual_value = as_float((manual.get(coin) or {}).get("score_push"))
    auto_value = as_float(((auto.get("overrides") or {}).get(coin) or {}).get("score_push"))
    return auto_value if auto_value is not None else (manual_value if manual_value is not None else default)


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
    }


def rounded_metric(value: Optional[Dict[str, float]]) -> Dict[str, Any]:
    if not value:
        return {}
    return {k: (int(v) if k == "n" else round(v, 6)) for k, v in value.items()}


def cooldown_active(meta: Dict[str, Any]) -> bool:
    applied = parse_time(meta.get("applied_at"))
    return bool(applied and (utc_now() - applied).total_seconds() < COOLDOWN_HOURS * 3600)


def post_change_check(
    rows: List[Dict[str, Any]], current: float, meta: Dict[str, Any]
) -> Dict[str, Any]:
    previous = as_float(meta.get("previous_score_push"))
    applied_at = parse_time(meta.get("applied_at"))
    return_key = str(meta.get("return_key") or "ret_72h")
    hurdle = as_float(meta.get("hurdle")) or 2.0
    if previous is None or applied_at is None:
        return {"status": "invalid_pending_state"}
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
                "previous": previous,
                "samples": len(post_rows),
            }
        return {"status": "awaiting_post_samples", "samples": len(post_rows)}
    gap = current_metric["quality"] - previous_metric["quality"]
    return {
        "status": "rollback" if gap <= -ROLLBACK_GAP else "validated",
        "quality_gap": gap,
        "current_metric": current_metric,
        "previous_metric": previous_metric,
        "previous": previous,
        "samples": len(post_rows),
    }


def append_history(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def run_analysis(
    db_path: Path,
    manual_path: Path,
    auto_path: Path,
    report_dir: Path,
    details_dir: Path,
) -> Dict[str, Any]:
    report_dir.mkdir(parents=True, exist_ok=True)
    details_dir.mkdir(parents=True, exist_ok=True)
    manual = load_json(manual_path, {"DEFAULT": {"score_push": 8.0}})
    auto = load_json(auto_path, {"version": 1, "overrides": {}, "meta": {}})
    auto.setdefault("version", 1)
    auto.setdefault("overrides", {})
    auto.setdefault("meta", {})
    raw_events = load_events(db_path)
    events = deduplicate_events(raw_events)
    by_coin: Dict[str, List[Dict[str, Any]]] = {}
    for row in events:
        by_coin.setdefault(row["coin"], []).append(row)

    mode = MODE if MODE in {"off", "shadow", "guarded"} else "shadow"
    changes = 0
    results: List[Dict[str, Any]] = []
    history_path = details_dir / "auto_optimization_history.jsonl"

    for coin in sorted(by_coin):
        rows = by_coin[coin]
        current = current_threshold(manual, auto, coin)
        coin_meta = dict((auto.get("meta") or {}).get(coin) or {})

        if coin_meta.get("status") == "pending":
            check = post_change_check(rows, current, coin_meta)
            if check["status"] in {"awaiting_post_samples", "invalid_pending_state"}:
                results.append({"coin": coin, "action": "hold_pending", "current": current, **check})
                continue
            if check["status"] == "rollback":
                previous = float(check["previous"])
                applied = mode == "guarded" and changes < MAX_CHANGES
                if applied:
                    auto["overrides"].setdefault(coin, {})["score_push"] = previous
                    auto["meta"][coin] = {
                        "status": "pending",
                        "applied_at": now_str(),
                        "previous_score_push": current,
                        "return_key": coin_meta.get("return_key", "ret_72h"),
                        "hurdle": coin_meta.get("hurdle", 2.0),
                        "reason": "automatic_rollback",
                    }
                    changes += 1
                    append_history(history_path, {
                        "time": now_str(), "coin": coin, "action": "rollback",
                        "from": current, "to": previous, "quality_gap": check.get("quality_gap"),
                    })
                results.append({
                    "coin": coin, "action": "rollback_applied" if applied else "rollback_recommended",
                    "current": current, "recommended": previous, "samples": check.get("samples"),
                    "quality_gap": check.get("quality_gap"),
                })
                continue
            auto["meta"][coin]["status"] = "validated"
            auto["meta"][coin]["validated_at"] = now_str()
            results.append({
                "coin": coin, "action": "validated", "current": current,
                "samples": check.get("samples"), "quality_gap": check.get("quality_gap"),
            })
            continue

        if coin_meta and cooldown_active(coin_meta):
            results.append({"coin": coin, "action": "cooldown", "current": current, "samples": len(rows)})
            continue

        recommendation = recommend_threshold(rows, current)
        action = recommendation.get("status", "unknown")
        applied = False
        if action == "recommend_change" and mode == "guarded" and changes < MAX_CHANGES:
            new_value = float(recommendation["recommended"])
            auto["overrides"].setdefault(coin, {})["score_push"] = new_value
            auto["meta"][coin] = {
                "status": "pending",
                "applied_at": now_str(),
                "previous_score_push": current,
                "return_key": recommendation["return_key"],
                "hurdle": recommendation["hurdle"],
                "train_gain": recommendation["train_gain"],
                "validation_gain": recommendation["validation_gain"],
                "reason": "train_and_validation_improved",
            }
            changes += 1
            applied = True
            append_history(history_path, {
                "time": now_str(), "coin": coin, "action": "apply",
                "from": current, "to": new_value, "horizon": recommendation.get("horizon"),
                "train_gain": recommendation.get("train_gain"),
                "validation_gain": recommendation.get("validation_gain"),
            })

        baseline_validation = ((recommendation.get("baseline") or {}).get("validation"))
        candidate_validation = ((recommendation.get("candidate") or {}).get("validation"))
        results.append({
            "coin": coin,
            "action": "applied" if applied else action,
            "current": current,
            "recommended": recommendation.get("recommended", current),
            "horizon": recommendation.get("horizon", ""),
            "samples": recommendation.get("samples", len(rows)),
            "train_gain": recommendation.get("train_gain"),
            "validation_gain": recommendation.get("validation_gain"),
            "baseline_validation": rounded_metric(baseline_validation),
            "candidate_validation": rounded_metric(candidate_validation),
        })

    if mode == "guarded":
        auto["updated_at"] = now_str()
        auto["mode"] = mode
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
        "coins": len(by_coin),
        "mature_counts": mature_counts,
        "changes_applied": changes,
        "guardrails": {
            "min_samples": MIN_SAMPLES,
            "validation_samples": VALIDATION_SAMPLES,
            "step": STEP,
            "min_gain": MIN_GAIN,
            "cooldown_hours": COOLDOWN_HOURS,
            "post_samples": POST_SAMPLES,
            "max_changes_per_run": MAX_CHANGES,
            "threshold_range": [MIN_THRESHOLD, MAX_THRESHOLD],
            "event_gap_hours": EVENT_GAP_HOURS,
            "max_pending_days": MAX_PENDING_DAYS,
        },
        "results": results,
    }
    write_json_atomic(details_dir / "auto_analysis_latest.json", snapshot)

    fields = [
        "coin", "action", "current", "recommended", "horizon", "samples",
        "train_gain", "validation_gain", "quality_gap", "status",
    ]
    with (details_dir / "auto_optimization_latest.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    with (report_dir / "auto_analysis_report.txt").open("w", encoding="utf-8") as handle:
        print("【自动优化分析层】", file=handle)
        print(f"更新时间 UTC：{snapshot['generated_at']} | 模式：{mode}", file=handle)
        print("只优化监控 score_push，不修改手工阈值、不触发交易。", file=handle)
        print(
            f"原始/去重评估事件：{len(raw_events)}/{len(events)} | 币种：{len(by_coin)} | "
            f"24h/72h/7d成熟样本：{mature_counts['24h']}/{mature_counts['72h']}/{mature_counts['7d']} | "
            f"本轮应用：{changes}",
            file=handle,
        )
        print(
            f"护栏：最少样本={MIN_SAMPLES}，验证集={VALIDATION_SAMPLES}，单次步长={STEP}，"
            f"最小双段提升={MIN_GAIN}，每轮最多修改={MAX_CHANGES}",
            file=handle,
        )
        print("", file=handle)
        if not results:
            print("暂无可分析的成熟信号样本。", file=handle)
        for row in results:
            print(
                f"{row['coin']} | {row['action']} | 当前={row.get('current')} | "
                f"建议={row.get('recommended', '-')} | 周期={row.get('horizon', '-')} | "
                f"样本={row.get('samples', 0)} | 验证提升={row.get('validation_gain', '-')}",
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

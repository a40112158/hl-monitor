#!/usr/bin/env python3
"""Generate Gemini commentary after a completed monitor scan.

This file is intentionally read-only: it only reads aggregated reports / CSVs,
asks Gemini for research commentary, optionally pushes a short Telegram summary,
and never edits thresholds or places orders.
"""

import argparse
import csv
import datetime as dt
import json
import os
import re
import sqlite3
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Tuple

from gemini_optimizer import analyze_scan


REPORT_DIR = Path(os.getenv("REPORT_DIR", "reports"))
DETAILS_DIR = Path(os.getenv("DETAILS_DIR", str(REPORT_DIR / "details")))
DB_FILE = Path(os.getenv("HL_DB_FILE", "hl_monitor.db"))
MAX_CHARS = max(4000, min(50000, int(os.getenv("GEMINI_SCAN_MAX_CHARS", "24000"))))
INTERVAL_MINUTES = max(30, int(os.getenv("GEMINI_SCAN_INTERVAL_MINUTES", "120")))
MAX_CONTEXT_ROWS = max(5, min(50, int(os.getenv("GEMINI_SCAN_CONTEXT_TOP_N", "20"))))
HISTORY_RUNS = max(2, min(12, int(os.getenv("GEMINI_SCAN_HISTORY_RUNS", "4"))))
STRONG_TRIGGER_ENABLED = os.getenv("GEMINI_SCAN_STRONG_TRIGGER", "1") == "1"
TG_ENABLED = os.getenv("GEMINI_SCAN_TG_ENABLED", "1") == "1"
TG_MIN_URGENCY = os.getenv("GEMINI_SCAN_TG_MIN_URGENCY", "watch").strip().lower()
GEMINI_ERROR_TG_ENABLED = os.getenv("GEMINI_ERROR_TG_ENABLED", "1") == "1"
GEMINI_ERROR_TG_ONCE_PER_DAY = os.getenv("GEMINI_ERROR_TG_ONCE_PER_DAY", "1") == "1"
REQUIRE_DATA_QUALITY = os.getenv("GEMINI_SCAN_REQUIRE_DATA_QUALITY", "1") == "1"
DATA_QUALITY_MIN_SUCCESS_RATE = max(0.0, min(1.0, float(os.getenv("DATA_QUALITY_MIN_SUCCESS_RATE", os.getenv("MIN_OK_RATE", "0.85")))))
ADDRESS_RE = re.compile(r"0x[a-fA-F0-9]{40}")

REPORT_FILES = [
    "last_run_status.json",
    "final_latest_report.txt",
    "long_term_plan.txt",
    "long_short_state_report.txt",
    "data_quality_report.txt",
    "research_dashboard.txt",
    "auto_analysis_report.txt",
    "signal_explain_report.txt",
]

SIGNAL_FIELDS = [
    "run_id", "created_at", "created_at_cn",
    "coin", "direction", "signal_category", "watchlist", "candidate_state", "candidate_gate",
    "candidate_block_reasons", "candidate_side", "alert_score", "long_score",
    "long_candidate_score", "short_candidate_score", "final_score", "threshold_score",
    "perp_active", "spot_active", "weighted_flow", "pct_1h", "pct_4h", "pct_24h",
    "avg_leverage", "avg_liq_distance", "longterm_leverage_ratio", "highrisk_leverage_ratio",
    "confidence", "conclusion", "risk", "reason",
]
RISK_FIELDS = [
    "coin", "funding_rate_pct", "funding_risk", "day_volume_usd", "liquidity_risk",
    "open_interest", "open_interest_usd", "price",
]
ACTION_FIELDS = [
    "groups", "coin", "market", "direction", "action_type", "side", "active_delta",
    "price_effect", "qty_delta", "entry_px", "leverage", "margin_mode", "liq_distance_pct",
    "leverage_style", "position_value", "spot_increases", "spot_decreases", "spot_net_changes",
    "spot_operations", "perp_operations",
]

LONG_CANDIDATE_CATEGORIES = {"低杠杆长期候选", "长期多单候选", "长期空单候选"}
LONG_OBSERVE_CATEGORIES = {"多单建仓观察", "空单建仓观察", "滚动建仓观察", "长期资格未通过"}
STRONG_SIGNAL_CATEGORIES = {"短线突发异动", "高杠杆短线异动"}


def sanitize(text: str) -> str:
    return ADDRESS_RE.sub("[wallet-redacted]", text).replace("\x00", "")


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None, microsecond=0)


def load_state(path: Path) -> Dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def analysis_due(state: Dict[str, Any], interval_minutes: int = INTERVAL_MINUTES) -> Tuple[bool, float]:
    text = str(state.get("last_success_at") or "")
    try:
        previous = dt.datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return True, 0.0
    elapsed = max(0.0, (utc_now() - previous).total_seconds() / 60)
    return elapsed >= interval_minutes, elapsed


def collect_reports(report_dir: Path, max_chars: int = MAX_CHARS) -> Dict[str, str]:
    collected: Dict[str, str] = {}
    remaining = max_chars
    for name in REPORT_FILES:
        path = report_dir / name
        if not path.exists() or remaining <= 0:
            continue
        try:
            text = sanitize(path.read_text(encoding="utf-8", errors="replace"))
        except OSError:
            continue
        text = text[: min(6000, remaining)]
        if text.strip():
            collected[name] = text
            remaining -= len(text)
    return collected


def as_float(value: Any) -> float:
    try:
        number = float(value)
        if number == number and abs(number) != float("inf"):
            return number
    except (TypeError, ValueError):
        pass
    return 0.0


def load_csv_rows(path: Path, limit: int = 500) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                return []
            rows = []
            for row in reader:
                rows.append({str(k): sanitize(str(v)) if isinstance(v, str) else v for k, v in row.items()})
                if len(rows) >= limit:
                    break
            return rows
    except OSError:
        return []


def compact_row(row: Dict[str, Any], fields: List[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for field in fields:
        if field not in row:
            continue
        value = row.get(field)
        if value is None or value == "":
            continue
        if isinstance(value, str) and len(value) > 600:
            value = value[:600]
        out[field] = value
    return out


def signal_rank(row: Dict[str, Any]) -> float:
    scores = [
        abs(as_float(row.get("alert_score"))),
        abs(as_float(row.get("long_score"))),
        abs(as_float(row.get("long_candidate_score"))),
        abs(as_float(row.get("short_candidate_score"))),
        abs(as_float(row.get("final_score"))),
    ]
    category = str(row.get("signal_category") or "")
    category_bonus = 2.0 if category in STRONG_SIGNAL_CATEGORIES | LONG_CANDIDATE_CATEGORIES else 0.0
    gate_bonus = 1.0 if str(row.get("candidate_state") or "").upper() in {"CANDIDATE", "CONFIRMED"} else 0.0
    return max(scores) + category_bonus + gate_bonus



def latest_run_id_from_status(report_dir: Path) -> int | None:
    try:
        status = json.loads((report_dir / "last_run_status.json").read_text(encoding="utf-8"))
        value = status.get("run_id") if isinstance(status, dict) else None
        return int(value) if value is not None else None
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None


def data_quality_status(report_dir: Path) -> Tuple[bool, str, Dict[str, Any]]:
    """Return whether latest scan quality is good enough for Gemini analysis.

    Missing status is treated as non-blocking so local dry-runs and tests without
    a completed monitor report still work. When status exists, a low ok rate
    blocks Gemini calls and prevents bad data from entering the AI report.
    """
    path = report_dir / "last_run_status.json"
    if not path.exists():
        return True, "last_run_status_missing_non_blocking", {}
    try:
        status = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(status, dict):
            return True, "last_run_status_not_object_non_blocking", {}
    except (OSError, json.JSONDecodeError) as exc:
        return True, f"last_run_status_unreadable_non_blocking:{type(exc).__name__}", {}
    raw_rate = status.get("success_rate")
    try:
        ok_rate = float(raw_rate)
    except (TypeError, ValueError):
        return True, "success_rate_missing_non_blocking", status
    if ok_rate > 1.0:
        ok_rate /= 100.0
    if REQUIRE_DATA_QUALITY and ok_rate < DATA_QUALITY_MIN_SUCCESS_RATE:
        return False, f"success_rate {ok_rate:.2%} below required {DATA_QUALITY_MIN_SUCCESS_RATE:.2%}", status
    return True, f"success_rate {ok_rate:.2%}", status


def query_recent_signal_history(
    db_path: Path,
    candidate_coins: List[str],
    latest_run_id: int | None,
    run_count: int = HISTORY_RUNS,
) -> Dict[str, Any]:
    if not db_path.exists() or not candidate_coins:
        return {"runs": [], "by_coin": {}}
    coins = [str(c or "").upper() for c in candidate_coins if c]
    coins = list(dict.fromkeys(coins))[:MAX_CONTEXT_ROWS]
    if not coins:
        return {"runs": [], "by_coin": {}}
    conn = sqlite3.connect(str(db_path), timeout=20)
    conn.row_factory = sqlite3.Row
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='coin_signals'"
        ).fetchone()
        if not exists:
            return {"runs": [], "by_coin": {}}
        if latest_run_id is None:
            row = conn.execute("SELECT MAX(run_id) AS run_id FROM coin_signals").fetchone()
            latest_run_id = int(row["run_id"]) if row and row["run_id"] is not None else None
        if latest_run_id is None:
            return {"runs": [], "by_coin": {}}
        run_rows = conn.execute(
            """
            SELECT DISTINCT run_id
            FROM coin_signals
            WHERE run_id <= ?
            ORDER BY run_id DESC
            LIMIT ?
            """,
            (latest_run_id, run_count),
        ).fetchall()
        run_ids = sorted(int(r["run_id"]) for r in run_rows if r["run_id"] is not None)
        if not run_ids:
            return {"runs": [], "by_coin": {}}
        placeholders_runs = ",".join("?" for _ in run_ids)
        placeholders_coins = ",".join("?" for _ in coins)
        rows = conn.execute(
            f"""
            SELECT run_id, created_at, created_at_cn, coin, direction, signal_category,
                   candidate_state, candidate_gate, candidate_side, alert_score, long_score,
                   long_candidate_score, short_candidate_score, final_score, threshold_score,
                   perp_active, spot_active, weighted_flow, pct_1h, pct_4h, pct_24h,
                   avg_leverage, avg_liq_distance, longterm_leverage_ratio, highrisk_leverage_ratio,
                   confidence, conclusion, risk
            FROM coin_signals
            WHERE run_id IN ({placeholders_runs}) AND UPPER(coin) IN ({placeholders_coins})
            ORDER BY coin, run_id
            """,
            tuple(run_ids + coins),
        ).fetchall()
    except Exception as exc:
        return {"runs": [], "by_coin": {}, "error": f"{type(exc).__name__}: {exc}"[:300]}
    finally:
        conn.close()

    by_coin: Dict[str, List[Dict[str, Any]]] = {}
    for raw in rows:
        row = dict(raw)
        coin = str(row.get("coin") or "").upper()
        by_coin.setdefault(coin, []).append(compact_row(row, SIGNAL_FIELDS))

    summarized: Dict[str, Dict[str, Any]] = {}
    for coin, items in by_coin.items():
        ordered = sorted(items, key=lambda x: int(as_float(x.get("run_id")) or 0))
        first = ordered[0] if ordered else {}
        last = ordered[-1] if ordered else {}
        summarized[coin] = {
            "last_n_scans": ordered,
            "change": {
                "final_score_delta": round(as_float(last.get("final_score")) - as_float(first.get("final_score")), 4) if as_float(last.get("final_score")) is not None and as_float(first.get("final_score")) is not None else None,
                "alert_score_delta": round(as_float(last.get("alert_score")) - as_float(first.get("alert_score")), 4) if as_float(last.get("alert_score")) is not None and as_float(first.get("alert_score")) is not None else None,
                "weighted_flow_delta": round(as_float(last.get("weighted_flow")) - as_float(first.get("weighted_flow")), 4) if as_float(last.get("weighted_flow")) is not None and as_float(first.get("weighted_flow")) is not None else None,
                "candidate_state_from_to": [first.get("candidate_state"), last.get("candidate_state")],
                "candidate_gate_from_to": [first.get("candidate_gate"), last.get("candidate_gate")],
            },
        }
    return {"runs": run_ids, "by_coin": summarized}


def collect_structured_context(details_dir: Path, report_dir: Path) -> Dict[str, Any]:
    signals = load_csv_rows(details_dir / "coin_signals_latest.csv")
    risks = load_csv_rows(details_dir / "coin_risk_latest.csv")
    actions = load_csv_rows(details_dir / "active_changes_all_latest.csv")
    flows = load_csv_rows(details_dir / "fund_flow_lite_all_latest.csv")

    risk_by_coin = {str(r.get("coin") or "").upper(): compact_row(r, RISK_FIELDS) for r in risks if r.get("coin")}
    ranked_signals = sorted(signals, key=signal_rank, reverse=True)[:MAX_CONTEXT_ROWS]
    compact_signals = []
    candidate_coins: List[str] = []
    for row in ranked_signals:
        coin = str(row.get("coin") or "").upper()
        if coin:
            candidate_coins.append(coin)
        item = compact_row(row, SIGNAL_FIELDS)
        if coin and coin in risk_by_coin:
            item["risk_metrics"] = risk_by_coin[coin]
        compact_signals.append(item)

    ranked_actions = sorted(actions, key=lambda r: abs(as_float(r.get("active_delta"))), reverse=True)[:MAX_CONTEXT_ROWS]
    compact_actions = [compact_row(row, ACTION_FIELDS) for row in ranked_actions]

    compact_flows = []
    for row in flows[: min(MAX_CONTEXT_ROWS, len(flows))]:
        cleaned = {k: v for k, v in row.items() if k != "address"}
        compact_flows.append(compact_row(cleaned, list(cleaned.keys())[:20]))

    latest_run_id = latest_run_id_from_status(report_dir)
    recent_history = query_recent_signal_history(DB_FILE, candidate_coins, latest_run_id, HISTORY_RUNS)
    context = {
        "generated_at": utc_now().isoformat() + "Z",
        "signals_top_n": compact_signals,
        "recent_signal_history": recent_history,
        "risk_by_coin_top_n": [risk_by_coin[k] for k in list(risk_by_coin)[:MAX_CONTEXT_ROWS]],
        "wallet_actions_top_n": compact_actions,
        "fund_flow_samples": compact_flows,
        "limits": {
            "max_context_rows": MAX_CONTEXT_ROWS,
            "recent_history_runs": HISTORY_RUNS,
            "wallet_addresses_removed": True,
        },
    }
    try:
        details_dir.mkdir(parents=True, exist_ok=True)
        (details_dir / "ai_signal_context_latest.json").write_text(
            json.dumps(context, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        (details_dir / "ai_signal_context_2h.json").write_text(
            json.dumps({
                "generated_at": context["generated_at"],
                "recent_signal_history": recent_history,
                "signals_top_n": compact_signals,
                "limits": context["limits"],
            }, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    except OSError:
        pass
    return context

def detect_strong_trigger(reports: Dict[str, str], structured_context: Dict[str, Any]) -> Tuple[bool, str]:
    if not STRONG_TRIGGER_ENABLED:
        return False, "disabled"
    final_report = reports.get("final_latest_report.txt", "")
    if "🚨" in final_report:
        return True, "final_report_contains_alert"
    if "暂无达到币种专属阈值的短线强异动" not in final_report and "【短线强信号 / 异动雷达】" in final_report:
        section = final_report.split("【短线强信号 / 异动雷达】", 1)[-1].split("【做多观察】", 1)[0]
        if section.strip() and "暂无" not in section[:80]:
            return True, "short_signal_section_non_empty"
    for item in structured_context.get("signals_top_n") or []:
        category = str(item.get("signal_category") or "")
        alert_score = abs(as_float(item.get("alert_score")))
        long_score = abs(as_float(item.get("long_score")))
        threshold = as_float(item.get("threshold_score"))
        if category in STRONG_SIGNAL_CATEGORIES | LONG_CANDIDATE_CATEGORIES:
            return True, f"structured_category:{category}"
        if threshold > 0 and max(alert_score, long_score) >= threshold:
            return True, "structured_score_reached_threshold"
    return False, "none"


def write_outputs(result: Dict[str, Any], report_dir: Path, details_dir: Path) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    details_dir.mkdir(parents=True, exist_ok=True)
    (details_dir / "gemini_scan_analysis_latest.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    # New no-JSON scan mode: Gemini returns a complete Markdown report.
    # Write it directly so the human-facing report never depends on parsing
    # Gemini output as JSON. The JSON file above is only local metadata/state.
    markdown_report = str(result.get("markdown_report") or "").strip()
    if markdown_report:
        (report_dir / "gemini_scan_analysis.txt").write_text(markdown_report + "\n", encoding="utf-8")
        return

    with (report_dir / "gemini_scan_analysis.txt").open("w", encoding="utf-8") as handle:
        print("【Gemini 本轮扫描分析】", file=handle)
        print(f"状态：{result.get('status')} | 模型：{result.get('model', '-')} | 紧急度：{result.get('urgency', '-')}", file=handle)
        if result.get("force_reason"):
            print(f"触发：{result.get('force_reason')}", file=handle)
        print("说明：AI 只分析已生成的聚合报告，不控制交易或绕过本地风控。", file=handle)
        print("", file=handle)
        print("【执行摘要】", file=handle)
        print(result.get("executive_summary") or "暂无分析。", file=handle)
        print("", file=handle)
        print("【数据质量】", file=handle)
        print(result.get("data_quality") or "暂无。", file=handle)
        print("", file=handle)
        print("【市场与资金结构】", file=handle)
        print(result.get("market_structure") or "暂无。", file=handle)
        print("", file=handle)
        print("【相比上次 AI 分析】", file=handle)
        for value in result.get("changes_since_previous") or ["暂无可比较的上次分析。"]:
            print("- " + value, file=handle)
        print("", file=handle)
        print("【重点观察】", file=handle)
        for item in result.get("focus_items") or []:
            print(
                f"{item.get('coin') or '-'} {item.get('side') or 'observe'} | "
                f"置信度={float(item.get('confidence') or 0):.0%} | {item.get('observation')}",
                file=handle,
            )
            if item.get("evidence"):
                print("  依据：" + "；".join(item["evidence"]), file=handle)
            if item.get("risks"):
                print("  风险：" + "；".join(item["risks"]), file=handle)
        print("", file=handle)
        print("【风险提示】", file=handle)
        for value in result.get("risk_warnings") or ["暂无新增风险提示。"]:
            print("- " + value, file=handle)
        print("", file=handle)
        print("【参数观察】", file=handle)
        for value in result.get("parameter_notes") or ["暂无参数异常。"]:
            print("- " + value, file=handle)
        print("", file=handle)
        print("【下一轮重点检查】", file=handle)
        for value in result.get("next_checks") or ["继续等待下一轮数据。"]:
            print("- " + value, file=handle)


def urgency_rank(value: str) -> int:
    return {"normal": 0, "watch": 1, "high": 2}.get(str(value or "normal").lower(), 0)


def should_push_tg(result: Dict[str, Any], force_due: bool) -> bool:
    if not TG_ENABLED or result.get("status") != "completed":
        return False
    markdown_report = str(result.get("markdown_report") or "").strip()
    urgency_ok = urgency_rank(str(result.get("urgency") or "normal")) >= urgency_rank(TG_MIN_URGENCY)
    if markdown_report:
        return bool(force_due or urgency_ok)
    if force_due and result.get("focus_items"):
        return True
    return urgency_ok and bool(result.get("focus_items") or result.get("risk_warnings"))


def format_tg_message(result: Dict[str, Any]) -> str:
    lines = [
        "🤖 Gemini 扫描分析",
        f"紧急度：{result.get('urgency', 'normal')} | 模型：{result.get('model', '-')}",
    ]
    if result.get("force_reason"):
        lines.append(f"触发：{result.get('force_reason')}")
    summary = str(result.get("executive_summary") or result.get("markdown_report") or "")[:900]
    if summary:
        lines.extend(["", "【摘要】", summary])
    focus = result.get("focus_items") or []
    if focus:
        lines.extend(["", "【重点观察】"])
        for item in focus[:6]:
            lines.append(
                f"- {item.get('coin') or '-'} {item.get('side') or 'observe'} "
                f"置信度={float(item.get('confidence') or 0):.0%}：{str(item.get('observation') or '')[:260]}"
            )
    warnings = result.get("risk_warnings") or []
    if warnings:
        lines.extend(["", "【风险】"])
        for warning in warnings[:5]:
            lines.append("- " + str(warning)[:260])
    lines.append("\n说明：AI 只做研究总结，不是下单建议。")
    return "\n".join(lines)



def record_ai_signal_reviews(result: Dict[str, Any], db_path: Path, report_dir: Path) -> None:
    """Persist Gemini focus-item decisions for later backtesting.

    This is append-only and research-only. Future returns are intentionally left
    NULL here; a later evaluator can fill them once enough time has passed.
    """
    if result.get("status") != "completed" or not db_path.exists():
        return
    focus_items = result.get("focus_items") if isinstance(result.get("focus_items"), list) else []
    if not focus_items:
        return
    latest_run_id = latest_run_id_from_status(report_dir)
    try:
        conn = sqlite3.connect(str(db_path), timeout=20)
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_signal_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER,
            created_at TEXT,
            model TEXT,
            urgency TEXT,
            coin TEXT,
            side TEXT,
            ai_confidence REAL,
            observation TEXT,
            evidence_json TEXT,
            risks_json TEXT,
            executive_summary TEXT,
            future_ret_24h REAL,
            future_ret_72h REAL,
            future_ret_7d REAL
        )
        """)
        created_at = result.get("generated_at") or utc_now().isoformat() + "Z"
        for item in focus_items[:20]:
            if not isinstance(item, dict):
                continue
            cur.execute(
                """
                INSERT INTO ai_signal_reviews(
                    run_id, created_at, model, urgency, coin, side, ai_confidence,
                    observation, evidence_json, risks_json, executive_summary
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    latest_run_id,
                    created_at,
                    result.get("model"),
                    result.get("urgency"),
                    str(item.get("coin") or "").upper()[:40],
                    str(item.get("side") or "observe")[:30],
                    float(item.get("confidence") or 0.0),
                    str(item.get("observation") or "")[:1500],
                    json.dumps(item.get("evidence") or [], ensure_ascii=False),
                    json.dumps(item.get("risks") or [], ensure_ascii=False),
                    str(result.get("executive_summary") or "")[:2000],
                ),
            )
        conn.commit()
    except Exception as exc:
        print(f"[gemini-scan] ai review persistence skipped: {type(exc).__name__}: {exc}", flush=True)
    finally:
        try:
            conn.close()  # type: ignore[name-defined]
        except Exception:
            pass


def send_telegram(text: str) -> bool:
    token = os.getenv("TG_BOT_TOKEN") or ""
    chat_id = os.getenv("TG_CHAT_ID") or ""
    if not token or not chat_id:
        print("[gemini-scan] TG_BOT_TOKEN or TG_CHAT_ID missing; skip TG", flush=True)
        return False
    ok = True
    for start in range(0, len(text), 3500):
        chunk = text[start:start + 3500]
        data = urllib.parse.urlencode({"chat_id": chat_id, "text": chunk}).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=data,
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                ok = ok and (200 <= resp.status < 300)
        except Exception as exc:
            print(f"[gemini-scan] TG push failed: {type(exc).__name__}: {exc}", flush=True)
            ok = False
    return ok


QUOTA_ERROR_PATTERNS = (
    "resourceexhausted",
    "quota",
    "429",
    "insufficient credits",
    "insufficient_credit",
    "credit exhausted",
    "credits exhausted",
    "billing",
    "daily_budget_exhausted",
    "scan_budget_exhausted",
    "budget_exhausted",
    "rate limit",
    "ratelimit",
)

SERVICE_ERROR_PATTERNS = (
    "503",
    "unavailable",
    "overloaded",
    "servererror",
    "internal",
    "deadline",
    "timeout",
    "502",
    "504",
)


def classify_gemini_error(result: Dict[str, Any]) -> Tuple[str, str]:
    """Classify Gemini failures that should be surfaced to Telegram.

    Returns (kind, reason). Empty kind means no alert is needed.
    This intentionally catches both provider-side quota errors and the script's
    own daily budget guard, because both mean the AI layer did not run.
    """
    status = str(result.get("status") or "").lower()
    combined = " ".join(
        str(result.get(key) or "")
        for key in ("status", "executive_summary", "data_quality", "market_structure")
    )
    lowered = combined.lower()
    if status == "missing_key" or "missing_key" in lowered or "api_key" in lowered:
        return "missing_key", combined[:1200] or "GEMINI_API_KEY missing"
    if status == "api_error" and any(pattern in lowered for pattern in QUOTA_ERROR_PATTERNS):
        return "quota_or_budget", combined[:1200]
    if any(pattern in lowered for pattern in ("daily_budget_exhausted", "scan_budget_exhausted", "budget_exhausted")):
        return "quota_or_budget", combined[:1200]
    if status == "api_error" and any(pattern in lowered for pattern in SERVICE_ERROR_PATTERNS):
        return "service_unavailable", combined[:1200]
    return "", ""


def format_gemini_error_tg(kind: str, reason: str, result: Dict[str, Any]) -> str:
    if kind == "missing_key":
        title = "⚠️ Gemini API Key / Vertex AI 凭证未生效"
        suggestion = "检查 GitHub Secrets：GEMINI_API_KEY；如果启用 Vertex AI，还要检查 GCP_SA_KEY_JSON、GOOGLE_CLOUD_PROJECT、GOOGLE_CLOUD_LOCATION。"
    elif kind == "service_unavailable":
        title = "⚠️ Gemini 服务端暂时不可用"
        suggestion = "这通常是 503/overloaded/timeout。脚本已启用重试、退避和 Vertex AI fallback；如果持续出现，检查 Vertex AI 凭证或等下一轮。"
    else:
        title = "⚠️ Gemini API 额度/配额异常"
        suggestion = "检查 Google AI Studio / Google Cloud Billing 的 credits、quota 和 API usage；也可以临时提高脚本预算或降低分析频率。"
    reason = sanitize(str(reason or "unknown"))[:1500]
    lines = [
        title,
        "",
        f"状态：{result.get('status', '-')}",
        f"模型：{result.get('model', '-')}",
        "",
        "【原因】",
        reason,
        "",
        "【影响】",
        "钱包扫描和本地报告仍会继续；本轮 Gemini AI 分析/自动解读可能为空或跳过。",
        "",
        "【建议】",
        suggestion,
    ]
    return "\n".join(lines)


def maybe_send_gemini_error_alert(result: Dict[str, Any], details_dir: Path) -> bool:
    if not GEMINI_ERROR_TG_ENABLED:
        return False
    kind, reason = classify_gemini_error(result)
    if not kind:
        return False
    today = utc_now().strftime("%Y-%m-%d")
    state_path = details_dir / "gemini_error_alert_state.json"
    state = load_state(state_path)
    if GEMINI_ERROR_TG_ONCE_PER_DAY and state.get(f"last_{kind}_date") == today:
        print(f"[gemini-scan] Gemini error alert already sent today kind={kind}; skip TG", flush=True)
        return False
    sent = send_telegram(format_gemini_error_tg(kind, reason, result))
    if sent:
        state[f"last_{kind}_date"] = today
        state[f"last_{kind}_at"] = utc_now().isoformat() + "Z"
        state[f"last_{kind}_reason"] = reason[:800]
        try:
            details_dir.mkdir(parents=True, exist_ok=True)
            state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        except OSError as exc:
            print(f"[gemini-scan] failed to write Gemini error alert state: {type(exc).__name__}: {exc}", flush=True)
    return sent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze the latest HL Monitor scan with Gemini")
    parser.add_argument("--report-dir", default=str(REPORT_DIR))
    parser.add_argument("--details-dir", default=str(DETAILS_DIR))
    parser.add_argument("--force", action="store_true", help="Ignore the regular interval once")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report_dir = Path(args.report_dir)
    details_dir = Path(args.details_dir)
    state_path = details_dir / "gemini_scan_state.json"
    state = load_state(state_path)
    reports = collect_reports(report_dir)
    structured_context = collect_structured_context(details_dir, report_dir)
    quality_ok, quality_reason, quality_payload = data_quality_status(report_dir)
    if not quality_ok:
        result = {
            "status": "skipped_data_quality",
            "urgency": "normal",
            "model": "none",
            "executive_summary": f"本轮数据质量不足，跳过 Gemini 分析：{quality_reason}",
            "data_quality": quality_reason,
            "market_structure": "未分析",
            "changes_since_previous": [],
            "focus_items": [],
            "risk_warnings": ["数据质量不足时不调用 AI、不推送 AI 结论，避免误判。"],
            "parameter_notes": [],
            "next_checks": ["等待下一轮成功率恢复后再分析。"],
            "generated_at": utc_now().isoformat() + "Z",
            "quality_payload": quality_payload,
        }
        write_outputs(result, report_dir, details_dir)
        print(f"[gemini-scan] skipped due to data quality: {quality_reason}", flush=True)
        return
    strong_due, strong_reason = detect_strong_trigger(reports, structured_context)
    force_due = args.force or strong_due
    due, elapsed = analysis_due(state)
    if not due and not force_due:
        remaining = max(0, INTERVAL_MINUTES - int(elapsed))
        print(
            f"[gemini-scan] skipped interval={INTERVAL_MINUTES}m "
            f"elapsed={elapsed:.1f}m remaining={remaining}m strong_trigger={strong_reason}",
            flush=True,
        )
        return

    previous = load_state(details_dir / "gemini_scan_analysis_latest.json")
    previous_summary = {
        "generated_at": previous.get("generated_at"),
        "urgency": previous.get("urgency"),
        "executive_summary": previous.get("executive_summary"),
        "market_structure": previous.get("market_structure"),
        "focus_items": [
            {
                "coin": item.get("coin"), "side": item.get("side"),
                "confidence": item.get("confidence"), "observation": item.get("observation"),
            }
            for item in (previous.get("focus_items") or [])[:8]
            if isinstance(item, dict)
        ],
        "risk_warnings": (previous.get("risk_warnings") or [])[:8],
    } if previous else None
    payload = {
        "source": "HL Monitor completed scan reports",
        "report_count": len(reports),
        "reports": reports,
        "structured_signal_context": structured_context,
        "previous_ai_analysis": previous_summary,
        "force_analysis": bool(force_due),
        "force_reason": "manual" if args.force else strong_reason,
        "data_quality_gate": {"ok": quality_ok, "reason": quality_reason},
        "constraints": {
            "analysis_only": True,
            "no_order_execution": True,
            "wallet_addresses_redacted": True,
            "prefer_structured_context_over_report_prose": True,
        },
    }
    result = analyze_scan(payload)
    result["generated_at"] = utc_now().isoformat() + "Z"
    result["interval_minutes"] = INTERVAL_MINUTES
    result["input_reports"] = list(reports)
    result["force_analysis"] = bool(force_due)
    result["force_reason"] = "manual" if args.force else strong_reason
    write_outputs(result, report_dir, details_dir)
    error_tg_sent = maybe_send_gemini_error_alert(result, details_dir)
    tg_sent = False
    if should_push_tg(result, force_due):
        tg_sent = send_telegram(format_tg_message(result))
    result["tg_sent"] = tg_sent
    result["error_tg_sent"] = error_tg_sent
    # Re-write with tg_sent/error_tg_sent included.
    write_outputs(result, report_dir, details_dir)
    record_ai_signal_reviews(result, DB_FILE, report_dir)
    if result.get("status") == "completed":
        details_dir.mkdir(parents=True, exist_ok=True)
        state_path.write_text(
            json.dumps(
                {
                    "last_success_at": result["generated_at"],
                    "model": result.get("model"),
                    "interval_minutes": INTERVAL_MINUTES,
                    "force_analysis": bool(force_due),
                    "force_reason": result.get("force_reason"),
                    "tg_sent": tg_sent,
                },
                ensure_ascii=False,
                indent=2,
            ) + "\n",
            encoding="utf-8",
        )
    print(
        f"[gemini-scan] status={result.get('status')} urgency={result.get('urgency')} "
        f"reports={len(reports)} focus={len(result.get('focus_items') or [])} "
        f"force={force_due}:{strong_reason} tg={tg_sent}",
        flush=True,
    )


if __name__ == "__main__":
    main()

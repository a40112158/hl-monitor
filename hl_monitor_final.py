# -*- coding: utf-8 -*-
"""
Hyperliquid Wallet Monitor FINAL

功能：
- 读取 Money Printer / Smart Money 钱包地址
- 查询 Hyperliquid 合约和现货账户状态
- 合约 / 现货分开统计
- 剥离价格影响，识别主动加仓/减仓
- 币种专属阈值
- 市场环境和价格位置
- 信号历史 1h/4h/24h/72h/7d/15d/30d 表现追踪
- 合约杠杆质量过滤：杠杆倍数 / cross-isolated / 强平距离 / 钱包杠杆风格
- 仓位生命周期追踪：开仓 / 加仓 / 减仓 / 平仓 / 真实仓位收益
- 观察列表和 Telegram 推送
- 适配 GitHub Actions 定时运行

注意：
- 本脚本不会自动下单，只做监控和提醒。
- 资金流 Lite 只是根据钱包内 USDC 与现货变化推断，不是交易所充值提现路径标签。
"""

import os
import re
import csv
import json
import time
import math
import sqlite3
import asyncio
import argparse
import datetime as dt
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import aiohttp


HL_INFO_URL = "https://api.hyperliquid.xyz/info"
DB_FILE = os.getenv("HL_DB_FILE", "hl_monitor.db")
REPORT_DIR = os.getenv("REPORT_DIR", "reports")
THRESHOLD_FILE = os.getenv("THRESHOLD_FILE", "coin_thresholds.json")

ADDRESS_SOURCES = {
    "money_printer": "money_printer_all_addresses.txt",
    "smart_money": "smart_money_all_addresses.txt",
}

DEFAULT_RPM = int(os.getenv("HL_RPM", "200"))
DEFAULT_CONCURRENCY = int(os.getenv("HL_CONCURRENCY", "5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
TOP_N = int(os.getenv("TOP_N", "15"))

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()

PUSH_EVERY_RUN = os.getenv("PUSH_EVERY_RUN", "0") == "1"
DAILY_PUSH_HOUR_UTC = int(os.getenv("DAILY_PUSH_HOUR_UTC", "0"))
MIN_OK_RATE = float(os.getenv("MIN_OK_RATE", "0.85"))
MIN_WALLET_COUNT = int(os.getenv("MIN_WALLET_COUNT", "100"))

# 低杠杆长期单模式：把信号再过滤成“长期观察计划”，避免被短线噪音带偏
LONG_TERM_MODE = os.getenv("LONG_TERM_MODE", "1") == "1"
LONG_TERM_MIN_SCORE = float(os.getenv("LONG_TERM_MIN_SCORE", "7"))
LONG_TERM_MIN_STREAK = int(os.getenv("LONG_TERM_MIN_STREAK", "2"))
LONG_TERM_RISK_PCT = float(os.getenv("LONG_TERM_RISK_PCT", "2"))
LONG_TERM_MAX_LEVERAGE = float(os.getenv("LONG_TERM_MAX_LEVERAGE", "3"))

# 每日归档：latest 文件仍然覆盖；daily 目录每天保留一份长期复盘快照
DAILY_ARCHIVE = os.getenv("DAILY_ARCHIVE", "1") == "1"
DAILY_ARCHIVE_KEEP_DAYS = int(os.getenv("DAILY_ARCHIVE_KEEP_DAYS", "30"))

# 钱包质量系统：按最近 N 天的动作后续收益，对所有监控钱包分级并导出
WALLET_QUALITY_MODE = os.getenv("WALLET_QUALITY_MODE", "1") == "1"
WALLET_QUALITY_WINDOW_DAYS = int(os.getenv("WALLET_QUALITY_WINDOW_DAYS", "30"))
WALLET_QUALITY_MIN_SAMPLES = int(os.getenv("WALLET_QUALITY_MIN_SAMPLES", "10"))
WALLET_QUALITY_EXPORT = os.getenv("WALLET_QUALITY_EXPORT", "1") == "1"

# 合约杠杆质量过滤：更适合低杠杆长期单，避免被高杠杆短线钱包带偏
LEVERAGE_QUALITY_MODE = os.getenv("LEVERAGE_QUALITY_MODE", "1") == "1"
LEVERAGE_LOW_MAX = float(os.getenv("LEVERAGE_LOW_MAX", "3"))
LEVERAGE_MID_MAX = float(os.getenv("LEVERAGE_MID_MAX", "5"))
LEVERAGE_HIGH_MIN = float(os.getenv("LEVERAGE_HIGH_MIN", "10"))
LIQ_SAFE_DISTANCE_PCT = float(os.getenv("LIQ_SAFE_DISTANCE_PCT", "40"))
LIQ_DANGER_DISTANCE_PCT = float(os.getenv("LIQ_DANGER_DISTANCE_PCT", "10"))

# 仓位生命周期追踪：不看账户权益 ROI，只按每个钱包的真实仓位开/加/减/平来记录收益
POSITION_TRADE_MODE = os.getenv("POSITION_TRADE_MODE", "1") == "1"
POSITION_PERF_WINDOW_DAYS = int(os.getenv("POSITION_PERF_WINDOW_DAYS", "30"))
POSITION_MIN_QTY_CHANGE_RATIO = float(os.getenv("POSITION_MIN_QTY_CHANGE_RATIO", "0.05"))
POSITION_MIN_QTY_CHANGE_USD = float(os.getenv("POSITION_MIN_QTY_CHANGE_USD", "1000"))

# 现货增减明细：资金流 Lite 里明确显示本轮增持/减持了哪些现货币
SPOT_DETAIL_MIN_USD = float(os.getenv("SPOT_DETAIL_MIN_USD", "1000"))

# 报告底部复盘窗口：默认看过去30天，而不是过去24h
REPORT_REVIEW_WINDOW_DAYS = int(os.getenv("REPORT_REVIEW_WINDOW_DAYS", "30"))

# 默认阈值，可被 coin_thresholds.json 覆盖
DEFAULT_THRESHOLDS = {
    "score_push": 8.0,
    "min_watch_score": 5.0,
    "perp": 1_000_000.0,
    "spot": 500_000.0,
}

ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


def utc_now() -> dt.datetime:
    return dt.datetime.utcnow().replace(microsecond=0)


def now_str() -> str:
    return utc_now().strftime("%Y-%m-%d %H:%M:%S")


def utc_today() -> str:
    return utc_now().strftime("%Y-%m-%d")


def ms_now() -> int:
    return int(time.time() * 1000)


def parse_time(s: Optional[str]) -> Optional[dt.datetime]:
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return dt.datetime.strptime(s[:19], fmt)
        except Exception:
            continue
    return None


def safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        if isinstance(x, float) and math.isnan(x):
            return None
        return float(x)
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        mult = 1.0
        up = s.upper()
        if up.endswith("K"):
            mult = 1_000.0
            s = s[:-1]
        elif up.endswith("M"):
            mult = 1_000_000.0
            s = s[:-1]
        elif up.endswith("B"):
            mult = 1_000_000_000.0
            s = s[:-1]
        s = s.replace("$", "").replace(",", "").replace("%", "").replace("x", "").replace("X", "").strip()
        try:
            return float(s) * mult
        except Exception:
            return None
    return None


def fmt_money(x: Optional[float]) -> str:
    if x is None:
        return "N/A"
    sign = "-" if x < 0 else ""
    x = abs(float(x))
    if x >= 1_000_000_000:
        return f"{sign}${x/1_000_000_000:.2f}B"
    if x >= 1_000_000:
        return f"{sign}${x/1_000_000:.2f}M"
    if x >= 1_000:
        return f"{sign}${x/1_000:.2f}K"
    return f"{sign}${x:.2f}"


def fmt_num(x: Optional[float]) -> str:
    if x is None:
        return "N/A"
    x = float(x)
    if abs(x) >= 1_000_000:
        return f"{x/1_000_000:.2f}M"
    if abs(x) >= 1_000:
        return f"{x/1_000:.2f}K"
    return f"{x:.6g}"


def fmt_pct(x: Optional[float]) -> str:
    if x is None:
        return "N/A"
    return f"{float(x):+.2f}%"


def short_addr(addr: str) -> str:
    return addr[:6] + "..." + addr[-4:] if addr else ""


def dir_cn(direction: str) -> str:
    if direction == "bullish":
        return "偏多"
    if direction == "bearish":
        return "偏空"
    return "中性"



def action_type_cn(action_type: str, side: str = "") -> str:
    a = action_type or ""
    if a == "new_long":
        return "新开多"
    if a == "new_short":
        return "新开空"
    if a == "close_long":
        return "平多"
    if a == "close_short":
        return "平空"
    if a.startswith("flip_"):
        return "方向反转"
    if a == "buy_spot":
        return "现货买入"
    if a == "sell_spot":
        return "现货卖出"
    if a == "perp_change":
        if side == "long":
            return "合约多单变化"
        if side == "short":
            return "合约空单变化"
        return "合约变化"
    return a or "变化"


def compact_join(items: List[str], limit: int = 3) -> str:
    if not items:
        return "-"
    if len(items) <= limit:
        return "；".join(items)
    return "；".join(items[:limit]) + f"；另{len(items)-limit}项"


def side_cn(side: str) -> str:
    if side == "long":
        return "多"
    if side == "short":
        return "空"
    return side or "-"

def sign_num(x: float, threshold: float = 0.0) -> int:
    if x > threshold:
        return 1
    if x < -threshold:
        return -1
    return 0


def calc_liq_distance_pct(side: str, mark_px: Optional[float], liq_px: Optional[float]) -> Optional[float]:
    """当前价格距离强平价的百分比。

    做多：mark 越高于 liq 越安全；做空：liq 越高于 mark 越安全。
    返回正数代表仍有安全距离；负数代表价格已经越过/接近异常区域。
    """
    mark = safe_float(mark_px)
    liq = safe_float(liq_px)
    if mark is None or liq is None or mark <= 0 or liq <= 0:
        return None
    if side == "long":
        return (mark - liq) / mark * 100.0
    if side == "short":
        return (liq - mark) / mark * 100.0
    return None


def leverage_style_and_weight(leverage: Optional[float], liq_distance_pct: Optional[float], margin_mode: str = "") -> Tuple[str, float, float, str]:
    """返回：杠杆风格、权重、风险分、说明。

    权重用于合约主动变化加权：低杠杆长期型略加权，高杠杆/爆仓边缘降权。
    risk_score 0-100，越高越适合低杠杆长期参考。
    """
    lev = safe_float(leverage)
    dist = safe_float(liq_distance_pct)
    mode = (margin_mode or "unknown").lower()

    style = "杠杆未知"
    weight = 1.0
    risk_score = 50.0
    notes: List[str] = []

    if lev is not None:
        if lev <= LEVERAGE_LOW_MAX:
            style = "低杠杆长期型"
            weight *= 1.18
            risk_score += 22
            notes.append(f"杠杆{lev:.1f}x，偏长期")
        elif lev <= LEVERAGE_MID_MAX:
            style = "中杠杆趋势型"
            weight *= 1.00
            risk_score += 8
            notes.append(f"杠杆{lev:.1f}x，趋势参考")
        elif lev < LEVERAGE_HIGH_MIN:
            style = "中高杠杆型"
            weight *= 0.72
            risk_score -= 6
            notes.append(f"杠杆{lev:.1f}x，长期降权")
        elif lev < 20:
            style = "高杠杆短线型"
            weight *= 0.42
            risk_score -= 22
            notes.append(f"杠杆{lev:.1f}x，偏短线")
        else:
            style = "极高杠杆短线型"
            weight *= 0.25
            risk_score -= 35
            notes.append(f"杠杆{lev:.1f}x，强降权")

    if dist is not None:
        if dist < 0:
            style = "强平异常/极危"
            weight *= 0.10
            risk_score -= 45
            notes.append(f"强平距离{dist:.1f}%异常")
        elif dist < LIQ_DANGER_DISTANCE_PCT:
            style = "爆仓边缘型"
            weight *= 0.18
            risk_score -= 35
            notes.append(f"强平距离仅{dist:.1f}%")
        elif dist < 20:
            weight *= 0.55
            risk_score -= 12
            notes.append(f"强平距离{dist:.1f}%偏近")
        elif dist >= LIQ_SAFE_DISTANCE_PCT:
            weight *= 1.08
            risk_score += 14
            notes.append(f"强平距离{dist:.1f}%较安全")
        else:
            risk_score += 4
            notes.append(f"强平距离{dist:.1f}%正常")
    else:
        notes.append("强平距离未知")

    if mode == "isolated":
        risk_score += 3
        notes.append("isolated 风险隔离")
    elif mode == "cross":
        notes.append("cross 共享保证金")

    weight = max(0.08, min(1.35, weight))
    risk_score = max(0.0, min(100.0, risk_score))
    return style, weight, risk_score, "；".join(notes)


def ensure_dirs() -> None:
    os.makedirs(REPORT_DIR, exist_ok=True)


def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def load_thresholds() -> Dict[str, Dict[str, float]]:
    if not os.path.exists(THRESHOLD_FILE):
        return {"DEFAULT": DEFAULT_THRESHOLDS.copy()}
    try:
        with open(THRESHOLD_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if "DEFAULT" not in data:
            data["DEFAULT"] = DEFAULT_THRESHOLDS.copy()
        return data
    except Exception as e:
        print("读取 coin_thresholds.json 失败，使用默认阈值：", e)
        return {"DEFAULT": DEFAULT_THRESHOLDS.copy()}


def threshold(ths: Dict[str, Dict[str, float]], coin: str, key: str) -> float:
    base = ths.get("DEFAULT", DEFAULT_THRESHOLDS).get(key, DEFAULT_THRESHOLDS.get(key, 0.0))
    return float(ths.get(coin, {}).get(key, base))


def init_db() -> None:
    conn = db_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS runs (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at TEXT,
        finished_at TEXT,
        note TEXT,
        total_wallets INTEGER,
        ok_wallets INTEGER,
        partial_wallets INTEGER,
        failed_wallets INTEGER,
        perp_rows INTEGER,
        spot_rows INTEGER,
        ok_rate REAL,
        pushed INTEGER DEFAULT 0
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS wallet_states (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER,
        address TEXT,
        groups TEXT,
        status TEXT,
        error TEXT,
        perp_account_value REAL,
        perp_total_ntl_pos REAL,
        perp_withdrawable REAL,
        perp_account_leverage REAL,
        perp_position_count INTEGER,
        spot_total_value REAL,
        spot_usdc_value REAL,
        spot_token_count INTEGER
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS perp_positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER,
        address TEXT,
        groups TEXT,
        coin TEXT,
        side TEXT,
        szi REAL,
        abs_szi REAL,
        mark_px REAL,
        position_value REAL,
        entry_px REAL,
        unrealized_pnl REAL,
        roe REAL,
        leverage REAL,
        liquidation_px REAL,
        margin_mode TEXT,
        margin_used REAL,
        liq_distance_pct REAL,
        account_leverage REAL,
        leverage_style TEXT,
        leverage_weight REAL,
        leverage_risk_score REAL,
        leverage_note TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS spot_balances (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER,
        address TEXT,
        groups TEXT,
        coin TEXT,
        token INTEGER,
        total REAL,
        hold REAL,
        free REAL,
        entry_ntl REAL,
        mark_px REAL,
        current_value REAL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS wallet_actions (
        action_id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER,
        created_at TEXT,
        address TEXT,
        groups TEXT,
        coin TEXT,
        market TEXT,
        direction TEXT,
        action_type TEXT,
        active_delta REAL,
        price_effect REAL,
        qty_delta REAL,
        entry_px REAL,
        ret_1h REAL,
        ret_4h REAL,
        ret_24h REAL,
        ret_72h REAL,
        ret_7d REAL,
        ret_15d REAL,
        ret_30d REAL,
        evaluated_at TEXT,
        UNIQUE(run_id, address, coin, market, direction)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS signal_events (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER,
        created_at TEXT,
        coin TEXT,
        direction TEXT,
        score REAL,
        entry_px REAL,
        reason TEXT,
        ret_1h REAL,
        ret_4h REAL,
        ret_24h REAL,
        ret_72h REAL,
        ret_7d REAL,
        ret_15d REAL,
        ret_30d REAL,
        evaluated_at TEXT,
        UNIQUE(run_id, coin, direction)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS coin_signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER,
        coin TEXT,
        direction TEXT,
        score REAL,
        confidence TEXT,
        signal_type TEXT,
        signal_state TEXT,
        watchlist TEXT,
        perp_active REAL,
        spot_active REAL,
        weighted_flow REAL,
        price_position REAL,
        pct_1h REAL,
        pct_4h REAL,
        pct_24h REAL,
        final_score REAL,
        threshold_score REAL,
        avg_leverage REAL,
        avg_liq_distance REAL,
        longterm_leverage_ratio REAL,
        highrisk_leverage_ratio REAL,
        leverage_note TEXT,
        conclusion TEXT,
        risk TEXT,
        reason TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS market_context (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER,
        coin TEXT,
        px REAL,
        pct_1h REAL,
        pct_4h REAL,
        pct_24h REAL,
        pos_24h REAL,
        dist_high_24h REAL,
        dist_low_24h REAL,
        rel_btc_24h REAL,
        regime TEXT,
        created_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS wallet_quality (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER,
        calculated_at TEXT,
        window_days INTEGER,
        address TEXT,
        groups TEXT,
        grade TEXT,
        quality_score REAL,
        quality_weight REAL,
        sample_total INTEGER,
        eval_24h INTEGER,
        win_24h REAL,
        avg_24h REAL,
        eval_72h INTEGER,
        win_72h REAL,
        avg_72h REAL,
        eval_7d INTEGER,
        win_7d REAL,
        avg_7d REAL,
        eval_15d INTEGER,
        win_15d REAL,
        avg_15d REAL,
        eval_30d INTEGER,
        win_30d REAL,
        avg_30d REAL,
        expectancy_72h REAL,
        expectancy_30d REAL,
        reverse_score REAL,
        last_action_at TEXT,
        dominant_coins TEXT,
        note TEXT,
        UNIQUE(run_id, address)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS position_trades (
        trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
        address TEXT,
        groups TEXT,
        coin TEXT,
        side TEXT,
        status TEXT,
        open_time TEXT,
        close_time TEXT,
        last_seen TEXT,
        entry_px REAL,
        exit_px REAL,
        current_px REAL,
        initial_qty REAL,
        current_qty REAL,
        max_qty REAL,
        closed_qty REAL,
        closed_notional_usd REAL,
        max_position_value REAL,
        current_position_value REAL,
        avg_leverage REAL,
        max_leverage REAL,
        min_liq_distance_pct REAL,
        realized_return_pct REAL,
        realized_pnl_usd REAL,
        unrealized_return_pct REAL,
        estimated_roe_pct REAL,
        final_return_pct REAL,
        max_favorable_pct REAL,
        max_adverse_pct REAL,
        holding_hours REAL,
        add_count INTEGER DEFAULT 0,
        reduce_count INTEGER DEFAULT 0,
        close_reason TEXT,
        note TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS position_trade_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER,
        trade_id INTEGER,
        created_at TEXT,
        event_type TEXT,
        address TEXT,
        groups TEXT,
        coin TEXT,
        side TEXT,
        qty_delta REAL,
        px REAL,
        return_pct REAL,
        position_value REAL,
        leverage REAL,
        liq_distance_pct REAL,
        note TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS wallet_position_performance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER,
        calculated_at TEXT,
        window_days INTEGER,
        address TEXT,
        groups TEXT,
        position_grade TEXT,
        position_score REAL,
        position_weight_multiplier REAL,
        sample_trades INTEGER,
        closed_trades INTEGER,
        open_trades INTEGER,
        closed_win_rate REAL,
        avg_realized_return REAL,
        avg_unrealized_return REAL,
        avg_final_return REAL,
        avg_holding_hours REAL,
        avg_leverage REAL,
        max_leverage REAL,
        avg_max_favorable_pct REAL,
        avg_max_adverse_pct REAL,
        low_leverage_ratio REAL,
        high_leverage_ratio REAL,
        dominant_coins TEXT,
        note TEXT,
        UNIQUE(run_id, address)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS final_reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER,
        created_at TEXT,
        strong_count INTEGER,
        long_count INTEGER,
        short_count INTEGER,
        report TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS push_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        push_type TEXT,
        push_date TEXT,
        pushed_at TEXT,
        UNIQUE(push_type, push_date)
    )
    """)

    # 兼容旧数据库：给 wallet_actions / signal_events / wallet_quality 补充新增评估字段
    def add_col_if_missing(table: str, col: str, decl: str) -> None:
        cur.execute(f"PRAGMA table_info({table})")
        cols = {r[1] for r in cur.fetchall()}
        if col not in cols:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")

    for t in ("wallet_actions", "signal_events"):
        add_col_if_missing(t, "ret_72h", "REAL")
        add_col_if_missing(t, "ret_7d", "REAL")
        add_col_if_missing(t, "ret_15d", "REAL")
        add_col_if_missing(t, "ret_30d", "REAL")

    for col in ("eval_15d", "eval_30d"):
        add_col_if_missing("wallet_quality", col, "INTEGER")
    for col in ("win_15d", "avg_15d", "win_30d", "avg_30d", "expectancy_30d"):
        add_col_if_missing("wallet_quality", col, "REAL")

    add_col_if_missing("wallet_states", "perp_account_leverage", "REAL")
    for col in ("margin_mode", "leverage_style", "leverage_note"):
        add_col_if_missing("perp_positions", col, "TEXT")
    for col in ("margin_used", "liq_distance_pct", "account_leverage", "leverage_weight", "leverage_risk_score"):
        add_col_if_missing("perp_positions", col, "REAL")
    for col in ("avg_leverage", "avg_liq_distance", "longterm_leverage_ratio", "highrisk_leverage_ratio"):
        add_col_if_missing("coin_signals", col, "REAL")
    add_col_if_missing("coin_signals", "leverage_note", "TEXT")

    cur.execute("CREATE INDEX IF NOT EXISTS idx_perp_run_addr_coin ON perp_positions(run_id, address, coin)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_spot_run_addr_coin ON spot_balances(run_id, address, coin)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_actions_addr ON wallet_actions(address)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_actions_created ON wallet_actions(created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_quality_run ON wallet_quality(run_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_quality_addr ON wallet_quality(address)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_signal_coin ON signal_events(coin)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_coin_signal_run ON coin_signals(run_id)")

    conn.commit()
    conn.close()


def create_run(note: str) -> int:
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO runs(started_at, note, pushed) VALUES (?, ?, 0)", (now_str(), note))
    run_id = cur.lastrowid
    conn.commit()
    conn.close()
    return int(run_id)


def finish_run(run_id: int, wallet_rows: List[Dict[str, Any]], perp_rows: List[Dict[str, Any]], spot_rows: List[Dict[str, Any]], pushed: bool) -> None:
    total = len(wallet_rows)
    ok = sum(1 for w in wallet_rows if w.get("status") == "ok")
    partial = sum(1 for w in wallet_rows if w.get("status") == "partial")
    failed = sum(1 for w in wallet_rows if w.get("status") == "failed")
    ok_rate = (ok + partial * 0.5) / total if total else 0.0

    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
    UPDATE runs
    SET finished_at=?, total_wallets=?, ok_wallets=?, partial_wallets=?, failed_wallets=?,
        perp_rows=?, spot_rows=?, ok_rate=?, pushed=?
    WHERE run_id=?
    """, (now_str(), total, ok, partial, failed, len(perp_rows), len(spot_rows), ok_rate, 1 if pushed else 0, run_id))
    conn.commit()
    conn.close()


def get_previous_run_id(run_id: int) -> Optional[int]:
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT run_id FROM runs WHERE run_id < ? ORDER BY run_id DESC LIMIT 1", (run_id,))
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else None


def load_rows(table: str, run_id: int) -> List[Dict[str, Any]]:
    allowed = {"wallet_states", "perp_positions", "spot_balances", "wallet_actions", "coin_signals", "market_context", "wallet_quality", "position_trades", "position_trade_events", "wallet_position_performance"}
    if table not in allowed:
        raise ValueError("bad table")
    conn = db_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table} WHERE run_id=?", (run_id,))
    rows = [dict(x) for x in cur.fetchall()]
    conn.close()
    return rows


def run_wallet_stats(run_id: int) -> Dict[str, Any]:
    rows = load_rows("wallet_states", run_id)
    total = len(rows)
    ok = sum(1 for w in rows if w.get("status") == "ok")
    partial = sum(1 for w in rows if w.get("status") == "partial")
    failed = sum(1 for w in rows if w.get("status") == "failed")
    ok_rate = (ok + partial * 0.5) / total if total else 0.0
    return {
        "total": total,
        "ok": ok,
        "partial": partial,
        "failed": failed,
        "ok_rate": ok_rate,
    }


def load_wallet_addresses() -> Dict[str, List[str]]:
    address_groups: Dict[str, List[str]] = defaultdict(list)
    for group, filename in ADDRESS_SOURCES.items():
        if not os.path.exists(filename):
            print(f"地址文件不存在，跳过：{filename}")
            continue
        count = 0
        with open(filename, "r", encoding="utf-8") as f:
            for line in f:
                addr = line.strip().lower()
                if not addr or addr.startswith("#"):
                    continue
                if not ADDRESS_RE.match(addr):
                    continue
                if group not in address_groups[addr]:
                    address_groups[addr].append(group)
                    count += 1
        print(f"{group} 读取地址数：{count}", flush=True)
    if not address_groups:
        raise RuntimeError("没有读取到钱包地址：请检查 money_printer_all_addresses.txt 和 smart_money_all_addresses.txt")
    print(f"去重后总地址数：{len(address_groups)}", flush=True)
    return dict(address_groups)


class RateLimiter:
    def __init__(self, rpm: int):
        self.interval = 60.0 / max(1, rpm)
        self.lock = asyncio.Lock()
        self.next_time = 0.0

    async def wait(self) -> None:
        async with self.lock:
            now = time.monotonic()
            if now < self.next_time:
                await asyncio.sleep(self.next_time - now)
            self.next_time = time.monotonic() + self.interval


async def post_info(session: aiohttp.ClientSession, limiter: Optional[RateLimiter], payload: Dict[str, Any]) -> Tuple[bool, Any]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if limiter:
                await limiter.wait()
            async with session.post(HL_INFO_URL, json=payload) as resp:
                text = await resp.text()
                if resp.status == 200:
                    try:
                        return True, json.loads(text)
                    except Exception:
                        return False, f"不是JSON: {text[:200]}"
                if resp.status in (429, 500, 502, 503, 504):
                    await asyncio.sleep(attempt * 3)
                    continue
                return False, f"HTTP {resp.status}: {text[:200]}"
        except Exception as e:
            await asyncio.sleep(attempt * 3)
            if attempt == MAX_RETRIES:
                return False, str(e)
    return False, "重试失败"


async def fetch_all_mids(session: aiohttp.ClientSession, limiter: Optional[RateLimiter]) -> Dict[str, float]:
    ok, data = await post_info(session, limiter, {"type": "allMids"})
    out: Dict[str, float] = {}
    if ok and isinstance(data, dict):
        for coin, px in data.items():
            f = safe_float(px)
            if f is not None:
                out[str(coin)] = f
    return out


async def fetch_spot_prices(session: aiohttp.ClientSession, limiter: Optional[RateLimiter]) -> Tuple[Dict[int, float], Dict[str, float]]:
    ok, data = await post_info(session, limiter, {"type": "spotMetaAndAssetCtxs"})
    token_price: Dict[int, float] = {0: 1.0}
    coin_price: Dict[str, float] = {"USDC": 1.0}
    if not ok:
        print("获取 spotMetaAndAssetCtxs 失败：", data)
        return token_price, coin_price
    try:
        meta, ctxs = data
        tokens = meta.get("tokens") or []
        universe = meta.get("universe") or []
        token_name = {int(t["index"]): t.get("name") for t in tokens if "index" in t}
        for u, ctx in zip(universe, ctxs):
            pair_tokens = u.get("tokens") or []
            if len(pair_tokens) < 2:
                continue
            base_token = int(pair_tokens[0])
            quote_token = int(pair_tokens[1])
            if quote_token != 0:
                continue
            px = safe_float(ctx.get("markPx")) or safe_float(ctx.get("midPx"))
            name = token_name.get(base_token)
            if px is not None:
                token_price[base_token] = px
                if name:
                    coin_price[str(name)] = px
    except Exception as e:
        print("解析 spotMetaAndAssetCtxs 失败：", e)
    return token_price, coin_price


async def fetch_candles(session: aiohttp.ClientSession, limiter: Optional[RateLimiter], coin: str, hours: int = 30) -> List[Dict[str, Any]]:
    end_ms = ms_now()
    start_ms = end_ms - hours * 3600 * 1000
    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": coin,
            "interval": "1h",
            "startTime": start_ms,
            "endTime": end_ms,
        },
    }
    ok, data = await post_info(session, limiter, payload)
    if ok and isinstance(data, list):
        return data
    return []


def candle_val(c: Dict[str, Any], *keys: str) -> Optional[float]:
    for k in keys:
        v = safe_float(c.get(k))
        if v is not None:
            return v
    return None


def calc_context(coin: str, px: Optional[float], candles: List[Dict[str, Any]], btc_24h: Optional[float]) -> Dict[str, Any]:
    closes = [candle_val(c, "c", "close") for c in candles]
    highs = [candle_val(c, "h", "high") for c in candles]
    lows = [candle_val(c, "l", "low") for c in candles]
    closes = [x for x in closes if x is not None and x > 0]
    highs = [x for x in highs if x is not None and x > 0]
    lows = [x for x in lows if x is not None and x > 0]
    if px is None and closes:
        px = closes[-1]

    def pct_from(idx_back: int) -> Optional[float]:
        if px is not None and len(closes) > idx_back and closes[-1 - idx_back] > 0:
            base = closes[-1 - idx_back]
            return (px - base) / base * 100
        return None

    pct_1h = pct_from(1)
    pct_4h = pct_from(4)
    pct_24h = pct_from(24)
    h24 = max(highs[-24:]) if highs else None
    l24 = min(lows[-24:]) if lows else None
    pos = None
    dist_h = None
    dist_l = None
    if px is not None and h24 and l24 and h24 > l24:
        pos = (px - l24) / (h24 - l24)
        dist_h = (px - h24) / h24 * 100
        dist_l = (px - l24) / l24 * 100
    rel_btc = pct_24h - btc_24h if pct_24h is not None and btc_24h is not None else None
    regime = "neutral"
    if pct_4h is not None:
        if pct_4h >= 3:
            regime = "strong_up"
        elif pct_4h <= -3:
            regime = "strong_down"
        elif pos is not None and pos >= 0.75:
            regime = "near_high"
        elif pos is not None and pos <= 0.25:
            regime = "near_low"
    return {
        "coin": coin,
        "px": px,
        "pct_1h": pct_1h,
        "pct_4h": pct_4h,
        "pct_24h": pct_24h,
        "pos_24h": pos,
        "dist_high_24h": dist_h,
        "dist_low_24h": dist_l,
        "rel_btc_24h": rel_btc,
        "regime": regime,
    }


def parse_perp_state(address: str, groups: str, data: Dict[str, Any], mid_prices: Dict[str, float]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    margin = data.get("marginSummary") or {}
    cross = data.get("crossMarginSummary") or {}
    account_value = safe_float(margin.get("accountValue")) or safe_float(cross.get("accountValue"))
    total_ntl = safe_float(margin.get("totalNtlPos")) or safe_float(cross.get("totalNtlPos"))
    withdrawable = safe_float(data.get("withdrawable"))
    rows: List[Dict[str, Any]] = []
    for item in data.get("assetPositions") or []:
        p = item.get("position") or {}
        coin = p.get("coin")
        szi = safe_float(p.get("szi"))
        if not coin or szi is None or abs(szi) <= 0:
            continue
        value = safe_float(p.get("positionValue")) or 0.0
        mark = mid_prices.get(str(coin))
        if mark is None and abs(szi) > 0:
            mark = value / abs(szi)
        lev_raw = p.get("leverage")
        margin_mode = "unknown"
        if isinstance(lev_raw, dict):
            leverage = safe_float(lev_raw.get("value"))
            margin_mode = str(lev_raw.get("type") or lev_raw.get("mode") or "unknown")
            margin_used = safe_float(p.get("marginUsed")) or safe_float(p.get("positionMargin")) or safe_float(lev_raw.get("rawUsd"))
        else:
            leverage = safe_float(lev_raw)
            margin_used = safe_float(p.get("marginUsed")) or safe_float(p.get("positionMargin"))
        if margin_used is None and leverage and leverage > 0:
            margin_used = abs(value) / leverage
        side = "long" if szi > 0 else "short"
        liq_px = safe_float(p.get("liquidationPx"))
        liq_dist = calc_liq_distance_pct(side, mark, liq_px)
        lev_style, lev_weight, lev_risk, lev_note = leverage_style_and_weight(leverage, liq_dist, margin_mode)
        account_leverage = (total_ntl / account_value) if account_value and account_value > 0 and total_ntl is not None else None
        rows.append({
            "address": address,
            "groups": groups,
            "coin": str(coin),
            "side": side,
            "szi": szi,
            "abs_szi": abs(szi),
            "mark_px": mark,
            "position_value": abs(value),
            "entry_px": safe_float(p.get("entryPx")),
            "unrealized_pnl": safe_float(p.get("unrealizedPnl")),
            "roe": safe_float(p.get("returnOnEquity")),
            "leverage": leverage,
            "liquidation_px": liq_px,
            "margin_mode": margin_mode,
            "margin_used": margin_used,
            "liq_distance_pct": liq_dist,
            "account_leverage": account_leverage,
            "leverage_style": lev_style,
            "leverage_weight": lev_weight,
            "leverage_risk_score": lev_risk,
            "leverage_note": lev_note,
        })
    account_leverage = (total_ntl / account_value) if account_value and account_value > 0 and total_ntl is not None else None
    wallet_part = {
        "perp_account_value": account_value,
        "perp_total_ntl_pos": total_ntl,
        "perp_withdrawable": withdrawable,
        "perp_account_leverage": account_leverage,
        "perp_position_count": len(rows),
    }
    return wallet_part, rows


def parse_spot_state(address: str, groups: str, data: Dict[str, Any], token_price: Dict[int, float], coin_price: Dict[str, float]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    spot_value = 0.0
    usdc_value = 0.0
    for b in data.get("balances") or []:
        coin = str(b.get("coin"))
        try:
            token = int(b.get("token"))
        except Exception:
            token = None
        total = safe_float(b.get("total")) or 0.0
        hold = safe_float(b.get("hold")) or 0.0
        entry_ntl = safe_float(b.get("entryNtl")) or 0.0
        if abs(total) <= 0:
            continue
        mark = token_price.get(token) if token is not None else None
        if mark is None:
            mark = coin_price.get(coin)
        if coin.upper() == "USDC":
            mark = 1.0
        current_value = total * mark if mark is not None else entry_ntl
        if coin.upper() == "USDC":
            usdc_value += current_value
        else:
            spot_value += current_value
        rows.append({
            "address": address,
            "groups": groups,
            "coin": coin,
            "token": token,
            "total": total,
            "hold": hold,
            "free": total - hold,
            "entry_ntl": entry_ntl,
            "mark_px": mark,
            "current_value": current_value,
        })
    wallet_part = {
        "spot_total_value": spot_value,
        "spot_usdc_value": usdc_value,
        "spot_token_count": len(rows),
    }
    return wallet_part, rows


async def fetch_wallet(session: aiohttp.ClientSession, limiter: RateLimiter, address: str, groups_list: List[str], mid_prices: Dict[str, float], token_price: Dict[int, float], coin_price: Dict[str, float]) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    groups = ",".join(groups_list)
    perp_ok, perp_data = await post_info(session, limiter, {"type": "clearinghouseState", "user": address})
    spot_ok, spot_data = await post_info(session, limiter, {"type": "spotClearinghouseState", "user": address})
    errors = []
    wallet = {
        "address": address,
        "groups": groups,
        "status": "ok",
        "error": "",
        "perp_account_value": None,
        "perp_total_ntl_pos": None,
        "perp_withdrawable": None,
        "perp_position_count": 0,
        "spot_total_value": 0.0,
        "spot_usdc_value": 0.0,
        "spot_token_count": 0,
    }
    perp_rows: List[Dict[str, Any]] = []
    spot_rows: List[Dict[str, Any]] = []
    if perp_ok:
        part, perp_rows = parse_perp_state(address, groups, perp_data, mid_prices)
        wallet.update(part)
    else:
        errors.append(f"perp={perp_data}")
    if spot_ok:
        part, spot_rows = parse_spot_state(address, groups, spot_data, token_price, coin_price)
        wallet.update(part)
    else:
        errors.append(f"spot={spot_data}")
    if perp_ok and spot_ok:
        wallet["status"] = "ok"
    elif perp_ok or spot_ok:
        wallet["status"] = "partial"
    else:
        wallet["status"] = "failed"
    wallet["error"] = "; ".join(errors)
    return wallet, perp_rows, spot_rows


async def fetch_all(addresses: Dict[str, List[str]], rpm: int, concurrency: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, float], Dict[int, float], Dict[str, float]]:
    timeout = aiohttp.ClientTimeout(total=100)
    connector = aiohttp.TCPConnector(limit=concurrency)
    limiter = RateLimiter(rpm)
    wallet_rows: List[Dict[str, Any]] = []
    perp_rows: List[Dict[str, Any]] = []
    spot_rows: List[Dict[str, Any]] = []
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        mid_prices = await fetch_all_mids(session, limiter)
        token_price, coin_price = await fetch_spot_prices(session, limiter)
        print(f"合约价格数：{len(mid_prices)} | 现货价格数：token={len(token_price)} coin={len(coin_price)}")
        sem = asyncio.Semaphore(concurrency)
        async def wrapped(addr: str, grps: List[str]):
            async with sem:
                return await fetch_wallet(session, limiter, addr, grps, mid_prices, token_price, coin_price)
        tasks = [wrapped(a, g) for a, g in addresses.items()]
        done = 0
        for coro in asyncio.as_completed(tasks):
            w, p, s = await coro
            wallet_rows.append(w)
            perp_rows.extend(p)
            spot_rows.extend(s)
            done += 1
            if done % 50 == 0 or done == len(tasks):
                ok = sum(1 for x in wallet_rows if x["status"] == "ok")
                part = sum(1 for x in wallet_rows if x["status"] == "partial")
                fail = sum(1 for x in wallet_rows if x["status"] == "failed")
                print(f"进度 {done}/{len(tasks)} | ok={ok} partial={part} failed={fail} | perp={len(perp_rows)} spot={len(spot_rows)}")
    return wallet_rows, perp_rows, spot_rows, mid_prices, token_price, coin_price


def save_snapshot(run_id: int, wallet_rows: List[Dict[str, Any]], perp_rows: List[Dict[str, Any]], spot_rows: List[Dict[str, Any]]) -> None:
    conn = db_conn()
    cur = conn.cursor()
    cur.executemany("""
    INSERT INTO wallet_states (
        run_id, address, groups, status, error,
        perp_account_value, perp_total_ntl_pos, perp_withdrawable, perp_account_leverage, perp_position_count,
        spot_total_value, spot_usdc_value, spot_token_count
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [(
        run_id, w.get("address"), w.get("groups"), w.get("status"), w.get("error"),
        w.get("perp_account_value"), w.get("perp_total_ntl_pos"), w.get("perp_withdrawable"), w.get("perp_account_leverage"), w.get("perp_position_count"),
        w.get("spot_total_value"), w.get("spot_usdc_value"), w.get("spot_token_count")
    ) for w in wallet_rows])
    cur.executemany("""
    INSERT INTO perp_positions (
        run_id, address, groups, coin, side, szi, abs_szi, mark_px, position_value,
        entry_px, unrealized_pnl, roe, leverage, liquidation_px,
        margin_mode, margin_used, liq_distance_pct, account_leverage, leverage_style, leverage_weight, leverage_risk_score, leverage_note
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [(
        run_id, p.get("address"), p.get("groups"), p.get("coin"), p.get("side"), p.get("szi"), p.get("abs_szi"), p.get("mark_px"), p.get("position_value"),
        p.get("entry_px"), p.get("unrealized_pnl"), p.get("roe"), p.get("leverage"), p.get("liquidation_px"),
        p.get("margin_mode"), p.get("margin_used"), p.get("liq_distance_pct"), p.get("account_leverage"), p.get("leverage_style"), p.get("leverage_weight"), p.get("leverage_risk_score"), p.get("leverage_note")
    ) for p in perp_rows])
    cur.executemany("""
    INSERT INTO spot_balances (
        run_id, address, groups, coin, token, total, hold, free, entry_ntl, mark_px, current_value
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [(
        run_id, s.get("address"), s.get("groups"), s.get("coin"), s.get("token"), s.get("total"), s.get("hold"), s.get("free"), s.get("entry_ntl"), s.get("mark_px"), s.get("current_value")
    ) for s in spot_rows])
    conn.commit()
    conn.close()


def export_latest_csv(run_id: int) -> None:
    ensure_dirs()
    for table, filename in [
        ("wallet_states", "wallet_states_latest.csv"),
        ("perp_positions", "perp_positions_latest.csv"),
        ("spot_balances", "spot_balances_latest.csv"),
        ("coin_signals", "coin_signals_latest.csv"),
        ("wallet_quality", "wallet_quality_latest.csv"),
        ("wallet_position_performance", "wallet_position_performance_latest.csv"),
    ]:
        rows = load_rows(table, run_id)
        if not rows:
            continue
        path = os.path.join(REPORT_DIR, filename)
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)



def export_operation_detail_files(actions: List[Dict[str, Any]], cashflows: List[Dict[str, Any]]) -> None:
    """导出全量单钱包主动变化和资金流 Lite 明细。
    报告里只显示 Top；这两个 CSV 保留所有触发阈值的地址，并标出：
    - 操作的现货币
    - 合约币种、方向、杠杆、强平距离
    """
    ensure_dirs()

    def write_csv(filename: str, rows: List[Dict[str, Any]], preferred: List[str]) -> None:
        path = os.path.join(REPORT_DIR, filename)
        if not rows:
            with open(path, "w", encoding="utf-8-sig", newline="") as f:
                f.write("empty\n")
            return
        keys = []
        for k in preferred:
            if any(k in r for r in rows):
                keys.append(k)
        for r in rows:
            for k in r.keys():
                if k not in keys:
                    keys.append(k)
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(rows)

    write_csv("active_changes_all_latest.csv", actions, [
        "address", "groups", "coin", "market", "direction", "action_type", "side",
        "active_delta", "price_effect", "qty_delta", "entry_px", "leverage", "margin_mode",
        "liq_distance_pct", "leverage_style", "position_value",
        "spot_increases", "spot_decreases", "spot_net_changes",
        "spot_operations", "perp_operations", "current_perp_positions", "current_spot_holdings",
    ])
    write_csv("fund_flow_lite_all_latest.csv", cashflows, [
        "address", "groups", "usdc_delta", "spot_delta", "flow_type",
        "spot_increases", "spot_decreases", "spot_net_changes",
        "spot_operations", "perp_operations", "current_perp_positions", "current_spot_holdings",
    ])

def signed_perp_value(row: Optional[Dict[str, Any]]) -> float:
    if not row:
        return 0.0
    value = safe_float(row.get("position_value")) or 0.0
    if row.get("side") == "long":
        return value
    if row.get("side") == "short":
        return -value
    return 0.0


def map_addr_coin(rows: List[Dict[str, Any]]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    return {(r["address"], r["coin"]): r for r in rows}


def group_base_weight(groups: str) -> float:
    g = groups or ""
    if "smart_money" in g and "money_printer" in g:
        return 1.8
    if "smart_money" in g:
        return 1.4
    if "money_printer" in g:
        return 1.2
    return 1.0


def wallet_quality_weight(address: str, groups: str, quality_map: Optional[Dict[str, Dict[str, Any]]] = None) -> float:
    """钱包动作权重：基础名单权重 + 最近30天真实表现动态权重。

    R级钱包会反向使用权重；S/A 级会放大；C/N 级降权。
    """
    base = group_base_weight(groups)
    if not quality_map:
        return base
    q = quality_map.get((address or "").lower())
    if not q:
        return base
    grade = q.get("grade") or "N"
    dyn = safe_float(q.get("quality_weight")) or base
    pos_grade = q.get("position_grade") or ""
    pos_mult = safe_float(q.get("position_weight_multiplier"))
    if pos_mult is None:
        pos_mult = 1.0
    # 仓位生命周期表现优先修正权重：P-G 赌徒型降权，P-R 反向仓位钱包反向参考。
    if pos_grade == "P-R":
        return -abs(dyn) * abs(pos_mult)
    if grade == "R":
        return -abs(dyn) * abs(pos_mult)
    return dyn * pos_mult


# 兼容旧函数名：没有质量图时仍按来源分组加权
def group_weight(groups: str) -> float:
    return group_base_weight(groups)




def build_wallet_operation_maps(
    wallet_actions: List[Dict[str, Any]],
    cur_perp_rows: List[Dict[str, Any]],
    cur_spot_rows: List[Dict[str, Any]],
    pre_spot_rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Dict[str, str]]:
    """给报告里的单钱包主动变化/资金流 Lite 补充：
    - 本轮具体操作了哪些现货币
    - 当前开了哪些合约、方向、杠杆、强平距离
    """
    # 精确现货增减明细：直接比较本轮和上一轮的现货数量，
    # 用来回答“这个钱包本轮增加的现货到底是什么币”。
    spot_increases: Dict[str, List[str]] = defaultdict(list)
    spot_decreases: Dict[str, List[str]] = defaultdict(list)
    spot_net_changes: Dict[str, List[str]] = defaultdict(list)
    if pre_spot_rows is not None:
        cur_map = {(str(r.get("address") or "").lower(), str(r.get("coin") or "")): r for r in cur_spot_rows}
        pre_map = {(str(r.get("address") or "").lower(), str(r.get("coin") or "")): r for r in pre_spot_rows}
        for key in set(cur_map.keys()) | set(pre_map.keys()):
            addr, coin = key
            if not addr or not coin or coin.upper() == "USDC":
                continue
            cur = cur_map.get(key)
            pre = pre_map.get(key)
            cur_qty = safe_float(cur.get("total")) if cur else 0.0
            pre_qty = safe_float(pre.get("total")) if pre else 0.0
            qty_delta = cur_qty - pre_qty
            cur_px = safe_float(cur.get("mark_px")) if cur else None
            pre_px = safe_float(pre.get("mark_px")) if pre else None
            ref_px = cur_px or pre_px or 0.0
            usd_delta = qty_delta * ref_px
            if abs(usd_delta) < SPOT_DETAIL_MIN_USD:
                continue
            txt = f"{coin} {fmt_money(usd_delta)} 数量Δ={fmt_num(qty_delta)} @ {fmt_num(ref_px)}"
            if usd_delta > 0:
                spot_increases[addr].append(txt)
            else:
                spot_decreases[addr].append(txt)
            spot_net_changes[addr].append(txt)

    spot_ops: Dict[str, List[str]] = defaultdict(list)
    perp_ops: Dict[str, List[str]] = defaultdict(list)
    for a in wallet_actions:
        addr = (a.get("address") or "").lower()
        if not addr:
            continue
        coin = a.get("coin") or ""
        if a.get("market") == "spot":
            op = action_type_cn(a.get("action_type") or "")
            spot_ops[addr].append(
                f"{op}{coin} {fmt_money(a.get('active_delta'))} 数量Δ={fmt_num(a.get('qty_delta'))} @ {fmt_num(a.get('entry_px'))}"
            )
        elif a.get("market") == "perp":
            op = action_type_cn(a.get("action_type") or "", a.get("side") or "")
            lev = a.get("leverage")
            lev_txt = f" {fmt_num(lev)}x" if lev is not None else ""
            liq_txt = f" 强平距={fmt_pct(a.get('liq_distance_pct'))}" if a.get("liq_distance_pct") is not None else ""
            perp_ops[addr].append(
                f"{op}{coin}{lev_txt} {fmt_money(a.get('active_delta'))}{liq_txt}"
            )

    current_perps: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in cur_perp_rows:
        addr = (r.get("address") or "").lower()
        if not addr:
            continue
        current_perps[addr].append(r)

    current_spots: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in cur_spot_rows:
        addr = (r.get("address") or "").lower()
        coin = str(r.get("coin") or "")
        if not addr or coin.upper() == "USDC":
            continue
        if (safe_float(r.get("current_value")) or 0.0) <= 0:
            continue
        current_spots[addr].append(r)

    all_addrs = (
        set(spot_ops.keys()) | set(perp_ops.keys()) | set(current_perps.keys()) | set(current_spots.keys()) |
        set(spot_increases.keys()) | set(spot_decreases.keys()) | set(spot_net_changes.keys())
    )
    result: Dict[str, Dict[str, str]] = {}
    for addr in all_addrs:
        perps = sorted(current_perps.get(addr, []), key=lambda x: abs(safe_float(x.get("position_value")) or 0.0), reverse=True)
        spots = sorted(current_spots.get(addr, []), key=lambda x: abs(safe_float(x.get("current_value")) or 0.0), reverse=True)
        perp_pos_txts = []
        for r in perps[:3]:
            lev = r.get("leverage")
            lev_txt = f" {fmt_num(lev)}x" if lev is not None else ""
            mm = r.get("margin_mode") or ""
            liq = r.get("liq_distance_pct")
            liq_txt = f" 强平距={fmt_pct(liq)}" if liq is not None else ""
            style = r.get("leverage_style") or ""
            style_txt = f" {style}" if style else ""
            perp_pos_txts.append(
                f"{r.get('coin')}{side_cn(r.get('side'))}{lev_txt} {fmt_money(r.get('position_value'))} {mm}{liq_txt}{style_txt}".strip()
            )
        spot_hold_txts = []
        for r in spots[:3]:
            spot_hold_txts.append(
                f"{r.get('coin')} {fmt_money(r.get('current_value'))} 数量={fmt_num(r.get('total'))}"
            )
        result[addr] = {
            "spot_increases": compact_join(spot_increases.get(addr, []), 8),
            "spot_decreases": compact_join(spot_decreases.get(addr, []), 8),
            "spot_net_changes": compact_join(spot_net_changes.get(addr, []), 10),
            "spot_operations": compact_join(spot_ops.get(addr, []), 4),
            "perp_operations": compact_join(perp_ops.get(addr, []), 4),
            "current_perp_positions": compact_join(perp_pos_txts, 4),
            "current_spot_holdings": compact_join(spot_hold_txts, 4),
        }
    return result


def enrich_actions_and_cashflows(
    actions: List[Dict[str, Any]],
    cashflows: List[Dict[str, Any]],
    cur_perp_rows: List[Dict[str, Any]],
    cur_spot_rows: List[Dict[str, Any]],
    pre_spot_rows: Optional[List[Dict[str, Any]]] = None,
) -> None:
    maps = build_wallet_operation_maps(actions, cur_perp_rows, cur_spot_rows, pre_spot_rows)
    for item in actions:
        addr = (item.get("address") or "").lower()
        extra = maps.get(addr, {})
        item.update({
            "spot_increases": extra.get("spot_increases", "-"),
            "spot_decreases": extra.get("spot_decreases", "-"),
            "spot_net_changes": extra.get("spot_net_changes", "-"),
            "spot_operations": extra.get("spot_operations", "-"),
            "perp_operations": extra.get("perp_operations", "-"),
            "current_perp_positions": extra.get("current_perp_positions", "-"),
            "current_spot_holdings": extra.get("current_spot_holdings", "-"),
        })
    for item in cashflows:
        addr = (item.get("address") or "").lower()
        extra = maps.get(addr, {})
        item.update({
            "spot_increases": extra.get("spot_increases", "-"),
            "spot_decreases": extra.get("spot_decreases", "-"),
            "spot_net_changes": extra.get("spot_net_changes", "-"),
            "spot_operations": extra.get("spot_operations", "-"),
            "perp_operations": extra.get("perp_operations", "-"),
            "current_perp_positions": extra.get("current_perp_positions", "-"),
            "current_spot_holdings": extra.get("current_spot_holdings", "-"),
        })
def compute_preliminary(run_id: int, prev_run_id: Optional[int], thresholds: Dict[str, Dict[str, float]], quality_map: Optional[Dict[str, Dict[str, Any]]] = None) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    if prev_run_id is None:
        return {}, [], []
    cur_perp = load_rows("perp_positions", run_id)
    pre_perp = load_rows("perp_positions", prev_run_id)
    cur_spot = load_rows("spot_balances", run_id)
    pre_spot = load_rows("spot_balances", prev_run_id)
    cur_wallet = {w["address"]: w for w in load_rows("wallet_states", run_id)}
    pre_wallet = {w["address"]: w for w in load_rows("wallet_states", prev_run_id)}

    curp = map_addr_coin(cur_perp)
    prep = map_addr_coin(pre_perp)
    curs = map_addr_coin(cur_spot)
    pres = map_addr_coin(pre_spot)

    coins: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "coin": "",
        "perp_active": 0.0,
        "perp_price_effect": 0.0,
        "perp_total_delta": 0.0,
        "spot_active": 0.0,
        "spot_price_effect": 0.0,
        "spot_total_delta": 0.0,
        "weighted_flow": 0.0,
        "wallet_count": 0,
    })
    wallet_actions: List[Dict[str, Any]] = []

    for key in set(curp.keys()) | set(prep.keys()):
        cur = curp.get(key)
        pre = prep.get(key)
        addr, coin = key
        cur_szi = safe_float(cur.get("szi")) if cur else 0.0
        pre_szi = safe_float(pre.get("szi")) if pre else 0.0
        cur_px = safe_float(cur.get("mark_px")) if cur else None
        pre_px = safe_float(pre.get("mark_px")) if pre else None
        ref_px = cur_px or pre_px or 0.0
        qty_delta = cur_szi - pre_szi
        active = qty_delta * ref_px
        signed_delta = signed_perp_value(cur) - signed_perp_value(pre)
        price_effect = pre_szi * (cur_px - pre_px) if cur_px is not None and pre_px is not None else signed_delta - active
        cm = coins[coin]
        cm["coin"] = coin
        cm["perp_active"] += active
        cm["perp_price_effect"] += price_effect
        cm["perp_total_delta"] += signed_delta
        ref = cur or pre or {}
        w = wallet_quality_weight(addr, ref.get("groups", ""), quality_map)
        lw = safe_float(ref.get("leverage_weight")) or 1.0
        if not LEVERAGE_QUALITY_MODE:
            lw = 1.0
        cm["weighted_flow"] += active * w * lw
        if abs(active) >= threshold(thresholds, coin, "perp") * 0.5 and ref_px > 0:
            direction = "bullish" if active > 0 else "bearish"
            action_type = "perp_change"
            if pre is None and cur is not None:
                action_type = "new_long" if cur.get("side") == "long" else "new_short"
            elif cur is None and pre is not None:
                action_type = "close_long" if pre.get("side") == "long" else "close_short"
            elif cur and pre and cur.get("side") != pre.get("side"):
                action_type = f"flip_{pre.get('side')}_to_{cur.get('side')}"
            wallet_actions.append({
                "address": addr,
                "groups": ref.get("groups", ""),
                "coin": coin,
                "market": "perp",
                "direction": direction,
                "action_type": action_type,
                "side": ref.get("side"),
                "position_value": ref.get("position_value"),
                "active_delta": active,
                "price_effect": price_effect,
                "qty_delta": qty_delta,
                "entry_px": ref_px,
                "leverage": ref.get("leverage"),
                "margin_mode": ref.get("margin_mode"),
                "liq_distance_pct": ref.get("liq_distance_pct"),
                "leverage_style": ref.get("leverage_style"),
                "leverage_weight": ref.get("leverage_weight"),
            })

    for key in set(curs.keys()) | set(pres.keys()):
        cur = curs.get(key)
        pre = pres.get(key)
        addr, coin = key
        if coin.upper() == "USDC":
            continue
        cur_qty = safe_float(cur.get("total")) if cur else 0.0
        pre_qty = safe_float(pre.get("total")) if pre else 0.0
        cur_px = safe_float(cur.get("mark_px")) if cur else None
        pre_px = safe_float(pre.get("mark_px")) if pre else None
        cur_val = safe_float(cur.get("current_value")) if cur else 0.0
        pre_val = safe_float(pre.get("current_value")) if pre else 0.0
        ref_px = cur_px or pre_px or 0.0
        qty_delta = cur_qty - pre_qty
        active = qty_delta * ref_px
        value_delta = cur_val - pre_val
        price_effect = pre_qty * (cur_px - pre_px) if cur_px is not None and pre_px is not None else value_delta - active
        cm = coins[coin]
        cm["coin"] = coin
        cm["spot_active"] += active
        cm["spot_price_effect"] += price_effect
        cm["spot_total_delta"] += value_delta
        ref = cur or pre or {}
        w = wallet_quality_weight(addr, ref.get("groups", ""), quality_map)
        cm["weighted_flow"] += active * w
        if abs(active) >= threshold(thresholds, coin, "spot") * 0.5 and ref_px > 0:
            wallet_actions.append({
                "address": addr,
                "groups": ref.get("groups", ""),
                "coin": coin,
                "market": "spot",
                "direction": "bullish" if active > 0 else "bearish",
                "action_type": "buy_spot" if active > 0 else "sell_spot",
                "active_delta": active,
                "price_effect": price_effect,
                "qty_delta": qty_delta,
                "entry_px": ref_px,
            })

    # 资金流 Lite：基于 USDC 与非 USDC 现货余额变化
    cashflows: List[Dict[str, Any]] = []
    for addr, cw in cur_wallet.items():
        pw = pre_wallet.get(addr)
        if not pw:
            continue
        usdc_delta = (safe_float(cw.get("spot_usdc_value")) or 0.0) - (safe_float(pw.get("spot_usdc_value")) or 0.0)
        spot_delta = (safe_float(cw.get("spot_total_value")) or 0.0) - (safe_float(pw.get("spot_total_value")) or 0.0)
        if abs(usdc_delta) < 500_000 and abs(spot_delta) < 500_000:
            continue
        if usdc_delta < 0 and spot_delta > 0:
            flow_type = "USDC减少 + 现货增加，疑似买入"
        elif usdc_delta > 0 and spot_delta < 0:
            flow_type = "USDC增加 + 现货减少，疑似卖出/转现金"
        elif usdc_delta > 0:
            flow_type = "USDC增加，疑似资金回流"
        elif usdc_delta < 0:
            flow_type = "USDC减少，疑似资金流出/买入"
        else:
            flow_type = "现货变化"
        cashflows.append({"address": addr, "groups": cw.get("groups", ""), "usdc_delta": usdc_delta, "spot_delta": spot_delta, "flow_type": flow_type})

    enrich_actions_and_cashflows(wallet_actions, cashflows, cur_perp, cur_spot, pre_spot)
    wallet_actions.sort(key=lambda x: abs(x["active_delta"]), reverse=True)
    cashflows.sort(key=lambda x: abs(x["usdc_delta"]) + abs(x["spot_delta"]), reverse=True)
    return dict(coins), wallet_actions, cashflows


def save_wallet_actions(run_id: int, actions: List[Dict[str, Any]]) -> int:
    if not actions:
        return 0
    conn = db_conn()
    cur = conn.cursor()
    inserted = 0
    for a in actions:
        try:
            cur.execute("""
            INSERT INTO wallet_actions (
                run_id, created_at, address, groups, coin, market, direction, action_type,
                active_delta, price_effect, qty_delta, entry_px
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (run_id, now_str(), a["address"], a.get("groups", ""), a["coin"], a["market"], a["direction"], a["action_type"],
                  a["active_delta"], a["price_effect"], a["qty_delta"], a["entry_px"]))
            inserted += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    conn.close()
    return inserted


def evaluate_events(prices: Dict[str, float]) -> Tuple[int, int]:
    conn = db_conn()
    cur = conn.cursor()
    now_dt = utc_now()
    updated_actions = 0
    updated_signals = 0

    def eval_table(table: str, id_col: str) -> int:
        cur.execute(f"SELECT * FROM {table} WHERE ret_1h IS NULL OR ret_4h IS NULL OR ret_24h IS NULL OR ret_72h IS NULL OR ret_7d IS NULL OR ret_15d IS NULL OR ret_30d IS NULL")
        rows = [dict(x) for x in cur.fetchall()]
        n = 0
        for r in rows:
            coin = r.get("coin")
            current_px = prices.get(coin)
            entry_px = safe_float(r.get("entry_px"))
            created = parse_time(r.get("created_at"))
            if current_px is None or entry_px is None or entry_px <= 0 or not created:
                continue
            elapsed = (now_dt - created).total_seconds() / 3600
            raw_ret = (current_px - entry_px) / entry_px * 100
            dir_ret = raw_ret if r.get("direction") == "bullish" else -raw_ret
            updates: Dict[str, Any] = {}
            if elapsed >= 1 and r.get("ret_1h") is None:
                updates["ret_1h"] = dir_ret
            if elapsed >= 4 and r.get("ret_4h") is None:
                updates["ret_4h"] = dir_ret
            if elapsed >= 24 and r.get("ret_24h") is None:
                updates["ret_24h"] = dir_ret
            if elapsed >= 72 and r.get("ret_72h") is None:
                updates["ret_72h"] = dir_ret
            if elapsed >= 168 and r.get("ret_7d") is None:
                updates["ret_7d"] = dir_ret
            if elapsed >= 360 and r.get("ret_15d") is None:
                updates["ret_15d"] = dir_ret
            if elapsed >= 720 and r.get("ret_30d") is None:
                updates["ret_30d"] = dir_ret
            if not updates:
                continue
            updates["evaluated_at"] = now_str()
            set_sql = ", ".join([f"{k}=?" for k in updates])
            values = list(updates.values()) + [r[id_col]]
            cur.execute(f"UPDATE {table} SET {set_sql} WHERE {id_col}=?", values)
            n += 1
        return n

    updated_actions = eval_table("wallet_actions", "action_id")
    updated_signals = eval_table("signal_events", "event_id")
    conn.commit()
    conn.close()
    return updated_actions, updated_signals


def get_signal_perf(coin: str, direction: str) -> Dict[str, Any]:
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT COUNT(*) AS n,
           AVG(ret_1h) AS avg_1h,
           AVG(CASE WHEN ret_1h > 0 THEN 1.0 WHEN ret_1h <= 0 THEN 0.0 ELSE NULL END) AS win_1h,
           AVG(ret_4h) AS avg_4h,
           AVG(CASE WHEN ret_4h > 0 THEN 1.0 WHEN ret_4h <= 0 THEN 0.0 ELSE NULL END) AS win_4h,
           AVG(ret_24h) AS avg_24h,
           AVG(CASE WHEN ret_24h > 0 THEN 1.0 WHEN ret_24h <= 0 THEN 0.0 ELSE NULL END) AS win_24h,
           AVG(ret_72h) AS avg_72h,
           AVG(CASE WHEN ret_72h > 0 THEN 1.0 WHEN ret_72h <= 0 THEN 0.0 ELSE NULL END) AS win_72h,
           AVG(ret_7d) AS avg_7d,
           AVG(CASE WHEN ret_7d > 0 THEN 1.0 WHEN ret_7d <= 0 THEN 0.0 ELSE NULL END) AS win_7d,
           AVG(ret_15d) AS avg_15d,
           AVG(CASE WHEN ret_15d > 0 THEN 1.0 WHEN ret_15d <= 0 THEN 0.0 ELSE NULL END) AS win_15d,
           AVG(ret_30d) AS avg_30d,
           AVG(CASE WHEN ret_30d > 0 THEN 1.0 WHEN ret_30d <= 0 THEN 0.0 ELSE NULL END) AS win_30d
    FROM signal_events WHERE coin=? AND direction=?
    """, (coin, direction))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else {}


def confidence_for(coin: str, direction: str) -> Tuple[str, str, float]:
    perf = get_signal_perf(coin, direction)
    n = int(perf.get("n") or 0)
    win4 = safe_float(perf.get("win_4h"))
    avg4 = safe_float(perf.get("avg_4h"))
    if n >= 5 and win4 is not None and win4 >= 0.65 and (avg4 or 0) > 0:
        return "高", f"历史样本{n}，4h胜率{win4*100:.1f}%", 2.0
    if n >= 3 and win4 is not None and win4 >= 0.55:
        return "中", f"历史样本{n}，4h胜率{win4*100:.1f}%", 0.8
    if n >= 3 and win4 is not None and win4 <= 0.45:
        return "低", f"历史样本{n}，4h胜率{win4*100:.1f}%偏低", -0.8
    return "低", "历史样本不足", 0.0


async def build_market_context(run_id: int, candidate_coins: List[str], prices: Dict[str, float]) -> Dict[str, Dict[str, Any]]:
    ensure_dirs()
    coins = sorted(set([c for c in candidate_coins if c] + ["BTC", "ETH"]))[:30]
    timeout = aiohttp.ClientTimeout(total=60)
    limiter = RateLimiter(120)
    ctx_map: Dict[str, Dict[str, Any]] = {}
    async with aiohttp.ClientSession(timeout=timeout) as session:
        btc_candles = await fetch_candles(session, limiter, "BTC", 30)
        btc_ctx = calc_context("BTC", prices.get("BTC"), btc_candles, None)
        btc_24h = btc_ctx.get("pct_24h")
        ctx_map["BTC"] = btc_ctx
        for coin in coins:
            if coin == "BTC":
                continue
            candles = await fetch_candles(session, limiter, coin, 30)
            ctx_map[coin] = calc_context(coin, prices.get(coin), candles, btc_24h)
            await asyncio.sleep(0.05)
    save_market_context(run_id, ctx_map)
    return ctx_map


def save_market_context(run_id: int, ctx_map: Dict[str, Dict[str, Any]]) -> None:
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM market_context WHERE run_id=?", (run_id,))
    cur.executemany("""
    INSERT INTO market_context (
        run_id, coin, px, pct_1h, pct_4h, pct_24h, pos_24h, dist_high_24h, dist_low_24h, rel_btc_24h, regime, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [(
        run_id, c.get("coin"), c.get("px"), c.get("pct_1h"), c.get("pct_4h"), c.get("pct_24h"), c.get("pos_24h"), c.get("dist_high_24h"), c.get("dist_low_24h"), c.get("rel_btc_24h"), c.get("regime"), now_str()
    ) for c in ctx_map.values()])
    conn.commit()
    conn.close()


def market_adjust(direction: str, btc_ctx: Dict[str, Any], coin_ctx: Dict[str, Any]) -> Tuple[float, List[str]]:
    adj = 0.0
    reasons: List[str] = []
    btc4 = btc_ctx.get("pct_4h")
    rel = coin_ctx.get("rel_btc_24h")
    if direction == "bullish":
        if btc4 is not None and btc4 >= 1.5:
            adj += 1.0; reasons.append("BTC 4h偏强，顺势多头加分")
        elif btc4 is not None and btc4 <= -1.5:
            adj -= 1.0; reasons.append("BTC 4h偏弱，多头降权")
        if rel is not None and rel > 2:
            adj += 0.8; reasons.append("该币24h强于BTC")
        elif rel is not None and rel < -2:
            adj -= 0.5; reasons.append("该币24h弱于BTC")
    elif direction == "bearish":
        if btc4 is not None and btc4 <= -1.5:
            adj += 1.0; reasons.append("BTC 4h偏弱，顺势空头加分")
        elif btc4 is not None and btc4 >= 1.5:
            adj -= 1.0; reasons.append("BTC 4h偏强，空头降权")
        if rel is not None and rel < -2:
            adj += 0.8; reasons.append("该币24h弱于BTC")
        elif rel is not None and rel > 2:
            adj -= 0.5; reasons.append("该币24h强于BTC，空头谨慎")
    return adj, reasons


def position_adjust(direction: str, ctx: Dict[str, Any]) -> Tuple[float, List[str], str]:
    pos = ctx.get("pos_24h")
    pct4 = ctx.get("pct_4h")
    adj = 0.0
    reasons: List[str] = []
    stype = "普通趋势"
    if pos is None:
        return adj, reasons, stype
    if direction == "bullish":
        if pos <= 0.25:
            adj += 1.2; stype = "低位吸筹"; reasons.append("价格靠近24h低位，多头加分")
        elif pos >= 0.75:
            if pct4 is not None and pct4 > 5:
                adj -= 1.0; stype = "高位追多"; reasons.append("高位且短线涨幅大，追多风险")
            else:
                adj += 0.3; stype = "高位突破"; reasons.append("靠近24h高位，可能突破延续")
    elif direction == "bearish":
        if pos >= 0.75:
            adj += 1.2; stype = "高位加空"; reasons.append("价格靠近24h高位，空头加分")
        elif pos <= 0.25:
            if pct4 is not None and pct4 < -5:
                adj -= 1.0; stype = "低位追空"; reasons.append("低位且短线跌幅大，追空风险")
            else:
                adj += 0.3; stype = "低位破位"; reasons.append("靠近24h低位，可能破位延续")
    return adj, reasons, stype


def classify_state(direction: str, perp_active: float, spot_active: float, coin: str, ths: Dict[str, Dict[str, float]], stype: str) -> str:
    dsign = 1 if direction == "bullish" else -1
    p = sign_num(perp_active, threshold(ths, coin, "perp") * 0.5)
    s = sign_num(spot_active, threshold(ths, coin, "spot") * 0.5)
    if p == dsign and s == dsign:
        return "清晰同向"
    if p == dsign and s == 0:
        return "合约主导"
    if s == dsign and p == 0:
        return "现货主导"
    if direction == "bullish" and s == 1 and p == -1:
        return "可能对冲"
    if direction == "bearish" and s == 1 and p == -1:
        return "现货持有+合约做空，对冲偏空"
    if direction == "bullish" and s == -1 and p == 1:
        return "现货流出+合约做多，换杠杆/冲突"
    if "高位追多" in stype or "低位追空" in stype:
        return "追涨杀跌风险"
    return "不明确"


def build_leverage_signal_map(run_id: int) -> Dict[str, Dict[str, Any]]:
    """按币种/方向汇总当前合约仓位的杠杆质量，用于信号加减分。"""
    rows = load_rows("perp_positions", run_id)
    by_coin: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "coin": "",
        "long_value": 0.0,
        "short_value": 0.0,
        "long_lev_num": 0.0,
        "short_lev_num": 0.0,
        "long_liq_num": 0.0,
        "short_liq_num": 0.0,
        "long_liq_den": 0.0,
        "short_liq_den": 0.0,
        "long_longterm_value": 0.0,
        "short_longterm_value": 0.0,
        "long_highrisk_value": 0.0,
        "short_highrisk_value": 0.0,
        "style_counts": defaultdict(float),
    })
    for r in rows:
        coin = r.get("coin")
        side = r.get("side")
        if not coin or side not in ("long", "short"):
            continue
        val = abs(safe_float(r.get("position_value")) or 0.0)
        if val <= 0:
            continue
        d = by_coin[coin]
        d["coin"] = coin
        d[f"{side}_value"] += val
        lev = safe_float(r.get("leverage"))
        if lev is not None:
            d[f"{side}_lev_num"] += lev * val
        dist = safe_float(r.get("liq_distance_pct"))
        if dist is not None:
            d[f"{side}_liq_num"] += dist * val
            d[f"{side}_liq_den"] += val
        style = r.get("leverage_style") or "杠杆未知"
        d["style_counts"][style] += val
        if style in ("低杠杆长期型", "中杠杆趋势型"):
            d[f"{side}_longterm_value"] += val
        if style in ("中高杠杆型", "高杠杆短线型", "极高杠杆短线型", "爆仓边缘型", "强平异常/极危"):
            d[f"{side}_highrisk_value"] += val

    out: Dict[str, Dict[str, Any]] = {}
    for coin, d in by_coin.items():
        for side in ("long", "short"):
            val = d[f"{side}_value"]
            d[f"{side}_avg_leverage"] = d[f"{side}_lev_num"] / val if val else None
            d[f"{side}_avg_liq_distance"] = d[f"{side}_liq_num"] / d[f"{side}_liq_den"] if d[f"{side}_liq_den"] else None
            d[f"{side}_longterm_ratio"] = d[f"{side}_longterm_value"] / val if val else 0.0
            d[f"{side}_highrisk_ratio"] = d[f"{side}_highrisk_value"] / val if val else 0.0
        styles = sorted(d["style_counts"].items(), key=lambda x: x[1], reverse=True)
        d["dominant_leverage_style"] = styles[0][0] if styles else "无持仓"
        d["style_mix"] = ",".join([f"{k}:{fmt_money(v)}" for k, v in styles[:5]])
        out[coin] = dict(d)
    return out


def leverage_signal_adjust(direction: str, lev: Dict[str, Any]) -> Tuple[float, List[str], List[str], Dict[str, Any]]:
    """合约杠杆质量对币种信号的加减分。"""
    if not LEVERAGE_QUALITY_MODE or not lev:
        return 0.0, [], [], {}
    side = "long" if direction == "bullish" else "short"
    val = safe_float(lev.get(f"{side}_value")) or 0.0
    avg_lev = safe_float(lev.get(f"{side}_avg_leverage"))
    avg_liq = safe_float(lev.get(f"{side}_avg_liq_distance"))
    long_ratio = safe_float(lev.get(f"{side}_longterm_ratio")) or 0.0
    high_ratio = safe_float(lev.get(f"{side}_highrisk_ratio")) or 0.0
    if val <= 0:
        return 0.0, [], ["没有同方向合约仓位杠杆数据"], {
            "avg_leverage": None,
            "avg_liq_distance": None,
            "longterm_leverage_ratio": 0.0,
            "highrisk_leverage_ratio": 0.0,
            "leverage_note": "无同方向杠杆样本",
        }

    adj = 0.0
    reasons: List[str] = []
    risks: List[str] = []
    if long_ratio >= 0.60 and (avg_lev is None or avg_lev <= LEVERAGE_MID_MAX) and (avg_liq is None or avg_liq >= 20):
        adj += 1.0
        reasons.append(f"同方向低/中杠杆仓位占比{long_ratio*100:.0f}%")
    elif long_ratio >= 0.40 and (avg_lev is None or avg_lev <= LEVERAGE_MID_MAX):
        adj += 0.4
        reasons.append(f"同方向低/中杠杆仓位占比{long_ratio*100:.0f}%")

    if high_ratio >= 0.60:
        adj -= 1.2
        risks.append(f"同方向高杠杆/爆仓边缘仓位占比{high_ratio*100:.0f}%")
    elif high_ratio >= 0.35:
        adj -= 0.6
        risks.append(f"同方向高杠杆仓位偏多{high_ratio*100:.0f}%")

    if avg_lev is not None:
        if avg_lev <= LEVERAGE_LOW_MAX:
            adj += 0.4
            reasons.append(f"同方向平均杠杆{avg_lev:.1f}x，适合长期参考")
        elif avg_lev >= 20:
            adj -= 1.0
            risks.append(f"同方向平均杠杆{avg_lev:.1f}x，极高杠杆短线风险")
        elif avg_lev >= LEVERAGE_HIGH_MIN:
            adj -= 0.6
            risks.append(f"同方向平均杠杆{avg_lev:.1f}x，偏短线")

    if avg_liq is not None:
        if avg_liq < LIQ_DANGER_DISTANCE_PCT:
            adj -= 1.4
            risks.append(f"同方向平均强平距离仅{avg_liq:.1f}%")
        elif avg_liq >= LIQ_SAFE_DISTANCE_PCT:
            adj += 0.4
            reasons.append(f"同方向平均强平距离{avg_liq:.1f}%较安全")

    note = f"同向仓位{fmt_money(val)}，均杠杆={avg_lev:.1f}x" if avg_lev is not None else f"同向仓位{fmt_money(val)}，均杠杆=N/A"
    if avg_liq is not None:
        note += f"，均强平距离={avg_liq:.1f}%"
    note += f"，长期型占比={long_ratio*100:.0f}%，高风险占比={high_ratio*100:.0f}%"
    return adj, reasons, risks, {
        "avg_leverage": avg_lev,
        "avg_liq_distance": avg_liq,
        "longterm_leverage_ratio": long_ratio,
        "highrisk_leverage_ratio": high_ratio,
        "leverage_note": note,
    }


def direction_return_pct(side: str, entry_px: Optional[float], exit_px: Optional[float]) -> Optional[float]:
    """按仓位方向计算收益率，不看账户权益，避免充值/提现误判。"""
    entry = safe_float(entry_px)
    px = safe_float(exit_px)
    if entry is None or px is None or entry <= 0 or px <= 0:
        return None
    if side == "long":
        return (px - entry) / entry * 100.0
    if side == "short":
        return (entry - px) / entry * 100.0
    return None


def _hours_between(start: Optional[str], end: Optional[str] = None) -> Optional[float]:
    st = parse_time(start)
    ed = parse_time(end) or utc_now()
    if not st:
        return None
    return max(0.0, (ed - st).total_seconds() / 3600.0)


def _current_perp_map(run_id: int) -> Dict[Tuple[str, str], Dict[str, Any]]:
    rows = load_rows("perp_positions", run_id)
    return {((r.get("address") or "").lower(), str(r.get("coin") or "")): r for r in rows if r.get("address") and r.get("coin")}


def _wallet_perp_ok_map(run_id: int) -> Dict[str, bool]:
    out: Dict[str, bool] = {}
    for w in load_rows("wallet_states", run_id):
        addr = (w.get("address") or "").lower()
        status = w.get("status") or ""
        err = w.get("error") or ""
        # failed 或 perp 请求失败时，不把“当前没有仓位”误判成平仓。
        out[addr] = bool(status == "ok" or (status == "partial" and "perp=" not in err))
    return out


def _insert_position_event(cur: sqlite3.Cursor, run_id: int, trade_id: int, event_type: str, row: Dict[str, Any], qty_delta: float, px: Optional[float], ret_pct: Optional[float], note: str = "") -> None:
    cur.execute("""
    INSERT INTO position_trade_events (
        run_id, trade_id, created_at, event_type, address, groups, coin, side,
        qty_delta, px, return_pct, position_value, leverage, liq_distance_pct, note
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        run_id, trade_id, now_str(), event_type, row.get("address"), row.get("groups"), row.get("coin"), row.get("side"),
        qty_delta, px, ret_pct, row.get("position_value"), row.get("leverage"), row.get("liq_distance_pct"), note
    ))


def _open_position_trade(cur: sqlite3.Cursor, run_id: int, row: Dict[str, Any], reason: str = "open") -> int:
    px = safe_float(row.get("entry_px")) or safe_float(row.get("mark_px"))
    cur_px = safe_float(row.get("mark_px")) or px
    qty = abs(safe_float(row.get("abs_szi")) or safe_float(row.get("szi")) or 0.0)
    val = abs(safe_float(row.get("position_value")) or ((qty or 0.0) * (cur_px or 0.0)))
    lev = safe_float(row.get("leverage"))
    dist = safe_float(row.get("liq_distance_pct"))
    ret = direction_return_pct(row.get("side"), px, cur_px)
    roe = ret * lev if ret is not None and lev is not None else safe_float(row.get("roe"))
    cur.execute("""
    INSERT INTO position_trades (
        address, groups, coin, side, status, open_time, last_seen,
        entry_px, current_px, initial_qty, current_qty, max_qty, closed_qty, closed_notional_usd,
        max_position_value, current_position_value, avg_leverage, max_leverage, min_liq_distance_pct,
        realized_return_pct, realized_pnl_usd, unrealized_return_pct, estimated_roe_pct, final_return_pct,
        max_favorable_pct, max_adverse_pct, holding_hours, add_count, reduce_count, close_reason, note
    ) VALUES (?, ?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, 0, 0, 0, '', ?)
    """, (
        row.get("address"), row.get("groups"), row.get("coin"), row.get("side"), now_str(), now_str(),
        px, cur_px, qty, qty, qty, val, val, lev, lev, dist,
        ret, roe, ret, max(0.0, ret or 0.0), min(0.0, ret or 0.0), reason
    ))
    trade_id = int(cur.lastrowid)
    _insert_position_event(cur, run_id, trade_id, reason, row, qty, cur_px, ret, "新建仓位生命周期记录")
    return trade_id


def _close_position_trade(cur: sqlite3.Cursor, run_id: int, trade: Dict[str, Any], px: Optional[float], reason: str = "close") -> None:
    entry = safe_float(trade.get("entry_px"))
    side = trade.get("side")
    qty = abs(safe_float(trade.get("current_qty")) or 0.0)
    exit_px = safe_float(px) or safe_float(trade.get("current_px")) or entry
    ret = direction_return_pct(side, entry, exit_px)
    lev = safe_float(trade.get("avg_leverage"))
    roe = ret * lev if ret is not None and lev is not None else None
    closed_notional = (safe_float(trade.get("closed_notional_usd")) or 0.0) + (qty * (entry or exit_px or 0.0))
    realized_pnl = (safe_float(trade.get("realized_pnl_usd")) or 0.0)
    if ret is not None and entry:
        realized_pnl += qty * entry * ret / 100.0
    realized_return = (realized_pnl / closed_notional * 100.0) if closed_notional else ret
    mfe = max(safe_float(trade.get("max_favorable_pct")) or 0.0, ret or 0.0)
    mae = min(safe_float(trade.get("max_adverse_pct")) or 0.0, ret or 0.0)
    hold = _hours_between(trade.get("open_time"))
    cur.execute("""
    UPDATE position_trades
    SET status='closed', close_time=?, last_seen=?, exit_px=?, current_px=?, current_qty=0,
        closed_qty=COALESCE(closed_qty,0)+?, closed_notional_usd=?, realized_pnl_usd=?, realized_return_pct=?,
        unrealized_return_pct=NULL, estimated_roe_pct=?, final_return_pct=?, max_favorable_pct=?, max_adverse_pct=?,
        holding_hours=?, close_reason=?
    WHERE trade_id=?
    """, (now_str(), now_str(), exit_px, exit_px, qty, closed_notional, realized_pnl, realized_return,
          roe, realized_return, mfe, mae, hold, reason, trade.get("trade_id")))
    event_row = {"address": trade.get("address"), "groups": trade.get("groups"), "coin": trade.get("coin"), "side": trade.get("side"), "position_value": qty * (exit_px or 0.0), "leverage": lev, "liq_distance_pct": trade.get("min_liq_distance_pct")}
    _insert_position_event(cur, run_id, int(trade.get("trade_id")), reason, event_row, -qty, exit_px, ret, "仓位结束，记录已实现收益")


def update_position_trades(run_id: int, prices: Dict[str, float]) -> None:
    """仓位生命周期追踪：只看仓位开/加/减/平，不用账户权益 ROI。"""
    if not POSITION_TRADE_MODE:
        return
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM position_trades WHERE status='open'")
    active = {((r["address"] or "").lower(), str(r["coin"] or "")): dict(r) for r in cur.fetchall()}
    current = _current_perp_map(run_id)
    perp_ok = _wallet_perp_ok_map(run_id)
    processed = set()

    for key, row in current.items():
        addr, coin = key
        processed.add(key)
        cur_px = safe_float(row.get("mark_px")) or safe_float(prices.get(coin)) or safe_float(row.get("entry_px"))
        qty = abs(safe_float(row.get("abs_szi")) or safe_float(row.get("szi")) or 0.0)
        if qty <= 0 or cur_px is None:
            continue
        trade = active.get(key)
        if not trade:
            _open_position_trade(cur, run_id, row, "open_detected")
            continue
        if trade.get("side") != row.get("side"):
            _close_position_trade(cur, run_id, trade, cur_px, "reverse_close")
            _open_position_trade(cur, run_id, row, "reverse_open")
            continue

        prev_qty = abs(safe_float(trade.get("current_qty")) or 0.0)
        delta = qty - prev_qty
        entry = safe_float(row.get("entry_px")) or safe_float(trade.get("entry_px")) or cur_px
        ret = direction_return_pct(row.get("side"), entry, cur_px)
        lev = safe_float(row.get("leverage")) or safe_float(trade.get("avg_leverage"))
        roe = ret * lev if ret is not None and lev is not None else safe_float(row.get("roe"))
        val = abs(safe_float(row.get("position_value")) or qty * cur_px)
        max_qty = max(safe_float(trade.get("max_qty")) or 0.0, qty)
        max_val = max(safe_float(trade.get("max_position_value")) or 0.0, val)
        max_lev = max(safe_float(trade.get("max_leverage")) or 0.0, lev or 0.0) if lev is not None else safe_float(trade.get("max_leverage"))
        dist = safe_float(row.get("liq_distance_pct"))
        old_min_dist = safe_float(trade.get("min_liq_distance_pct"))
        min_dist = dist if old_min_dist is None else (min(old_min_dist, dist) if dist is not None else old_min_dist)
        mfe = max(safe_float(trade.get("max_favorable_pct")) or 0.0, ret or 0.0)
        mae = min(safe_float(trade.get("max_adverse_pct")) or 0.0, ret or 0.0)
        closed_qty = safe_float(trade.get("closed_qty")) or 0.0
        closed_notional = safe_float(trade.get("closed_notional_usd")) or 0.0
        realized_pnl = safe_float(trade.get("realized_pnl_usd")) or 0.0
        realized_return = safe_float(trade.get("realized_return_pct")) or 0.0
        add_count = int(trade.get("add_count") or 0)
        reduce_count = int(trade.get("reduce_count") or 0)
        event_type = "hold"
        note = "仓位继续持有"
        # 只有数量变化超过比例或最小美元价值，才记录为加/减仓，避免浮点噪音。
        delta_value = abs(delta) * cur_px
        change_trigger = abs(delta) >= max(prev_qty * POSITION_MIN_QTY_CHANGE_RATIO, 0.0) and delta_value >= POSITION_MIN_QTY_CHANGE_USD
        if change_trigger and delta > 0:
            add_count += 1
            event_type = "add"
            note = "加仓，更新平均入场价/当前浮盈"
            _insert_position_event(cur, run_id, int(trade.get("trade_id")), event_type, row, delta, cur_px, ret, note)
        elif change_trigger and delta < 0:
            reduce_count += 1
            event_type = "reduce"
            reduce_qty = abs(delta)
            part_ret = direction_return_pct(row.get("side"), safe_float(trade.get("entry_px")) or entry, cur_px)
            base_entry = safe_float(trade.get("entry_px")) or entry
            closed_qty += reduce_qty
            if base_entry:
                closed_notional += reduce_qty * base_entry
                if part_ret is not None:
                    realized_pnl += reduce_qty * base_entry * part_ret / 100.0
            realized_return = realized_pnl / closed_notional * 100.0 if closed_notional else realized_return
            note = "部分平仓，记录已实现收益，剩余仓位继续跟踪"
            _insert_position_event(cur, run_id, int(trade.get("trade_id")), event_type, row, delta, cur_px, part_ret, note)
        else:
            # 不写入 hold 事件，避免数据库无限膨胀；只更新 trade 状态。
            pass
        hold = _hours_between(trade.get("open_time"))
        final_ret = realized_return if qty <= 0 else ret
        cur.execute("""
        UPDATE position_trades
        SET last_seen=?, entry_px=?, current_px=?, current_qty=?, max_qty=?, closed_qty=?, closed_notional_usd=?,
            max_position_value=?, current_position_value=?, avg_leverage=?, max_leverage=?, min_liq_distance_pct=?,
            realized_return_pct=?, realized_pnl_usd=?, unrealized_return_pct=?, estimated_roe_pct=?, final_return_pct=?,
            max_favorable_pct=?, max_adverse_pct=?, holding_hours=?, add_count=?, reduce_count=?, note=?
        WHERE trade_id=?
        """, (now_str(), entry, cur_px, qty, max_qty, closed_qty, closed_notional, max_val, val, lev, max_lev, min_dist,
              realized_return, realized_pnl, ret, roe, final_ret, mfe, mae, hold, add_count, reduce_count, note, trade.get("trade_id")))

    # 当前已经没有该仓位：只有在钱包 perp 查询成功时才认定为平仓，避免 API 失败导致误判。
    for key, trade in active.items():
        if key in processed:
            continue
        addr, coin = key
        if not perp_ok.get(addr, False):
            continue
        px = safe_float(prices.get(coin)) or safe_float(trade.get("current_px")) or safe_float(trade.get("entry_px"))
        _close_position_trade(cur, run_id, trade, px, "close_detected")

    conn.commit()
    conn.close()
    export_position_trade_files(run_id)


def _grade_position_wallet(sample: int, closed: int, win: Optional[float], avg_ret: Optional[float], avg_hold: Optional[float], avg_lev: Optional[float], high_ratio: float) -> Tuple[str, float, float, str]:
    winv = win if win is not None else 0.0
    avgv = avg_ret if avg_ret is not None else 0.0
    holdv = avg_hold if avg_hold is not None else 0.0
    levv = avg_lev if avg_lev is not None else 0.0
    if sample < 3:
        return "P-N", 50.0, 1.0, "仓位样本不足，暂不影响权重"
    # G = gambler：收益可能好，但高杠杆短线特征明显，不适合低杠杆长期单参考。
    if sample >= 3 and high_ratio >= 0.55 and (levv >= LEVERAGE_HIGH_MIN or holdv < 6):
        score = 45 + max(-10, min(20, avgv))
        return "P-G", score, 0.65, "高杠杆/短线赌徒型，长期单降权"
    if sample >= 5 and winv <= 0.38 and avgv < 0:
        return "P-R", 25.0, -0.80, "仓位真实收益偏反向，可作为反向参考"
    score = 50.0
    score += min(15.0, math.log10(max(1, sample)) * 10)
    score += (winv - 0.5) * 70
    score += max(-18.0, min(18.0, avgv * 2.0))
    if holdv >= 24:
        score += 6
    if holdv >= 72:
        score += 6
    if avg_lev is not None and avg_lev <= LEVERAGE_LOW_MAX:
        score += 8
    elif avg_lev is not None and avg_lev >= LEVERAGE_HIGH_MIN:
        score -= 12
    score -= high_ratio * 20
    score = max(0.0, min(100.0, score))
    if sample >= 8 and winv >= 0.60 and avgv >= 2.0 and (avg_lev is None or avg_lev <= LEVERAGE_MID_MAX) and holdv >= 24:
        return "P-S", score, 1.25, "仓位收益、持仓周期、杠杆结构适合长期参考"
    if sample >= 5 and winv >= 0.55 and avgv >= 1.0 and (avg_lev is None or avg_lev <= 8):
        return "P-A", score, 1.15, "仓位收益稳定，适合加权参考"
    if winv >= 0.48 and avgv >= -0.5:
        return "P-B", score, 1.0, "仓位表现普通有效"
    return "P-C", score, 0.75, "仓位收益偏弱，降权参考"


def export_position_trade_files(run_id: int) -> None:
    if not POSITION_TRADE_MODE:
        return
    ensure_dirs()
    conn = db_conn()
    cur = conn.cursor()
    since = (utc_now() - dt.timedelta(days=POSITION_PERF_WINDOW_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    cur.execute("""
    SELECT * FROM position_trades
    WHERE status='open' OR COALESCE(close_time, open_time) >= ?
    ORDER BY COALESCE(close_time, last_seen, open_time) DESC
    """, (since,))
    trade_rows = [dict(x) for x in cur.fetchall()]

    if trade_rows:
        with open(os.path.join(REPORT_DIR, "wallet_position_trades_latest.csv"), "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(trade_rows[0].keys()))
            writer.writeheader()
            writer.writerows(trade_rows)

    by_addr: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in trade_rows:
        by_addr[(r.get("address") or "").lower()].append(r)

    perf_rows: List[Dict[str, Any]] = []
    for addr, arr in by_addr.items():
        groups = arr[0].get("groups", "") if arr else ""
        closed_rows = [r for r in arr if r.get("status") == "closed"]
        open_rows = [r for r in arr if r.get("status") == "open"]
        closed_rets = [safe_float(r.get("final_return_pct")) for r in closed_rows if safe_float(r.get("final_return_pct")) is not None]
        open_rets = [safe_float(r.get("unrealized_return_pct")) for r in open_rows if safe_float(r.get("unrealized_return_pct")) is not None]
        all_rets = [safe_float(r.get("final_return_pct")) for r in arr if safe_float(r.get("final_return_pct")) is not None]
        win_rate = (sum(1 for x in closed_rets if x > 0) / len(closed_rets)) if closed_rets else None
        avg_real = _avg(closed_rets)
        avg_unreal = _avg(open_rets)
        avg_final = _avg(all_rets)
        holds = [safe_float(r.get("holding_hours")) for r in arr if safe_float(r.get("holding_hours")) is not None]
        levs = [safe_float(r.get("avg_leverage")) for r in arr if safe_float(r.get("avg_leverage")) is not None]
        mfes = [safe_float(r.get("max_favorable_pct")) for r in arr if safe_float(r.get("max_favorable_pct")) is not None]
        maes = [safe_float(r.get("max_adverse_pct")) for r in arr if safe_float(r.get("max_adverse_pct")) is not None]
        low_count = sum(1 for r in arr if (safe_float(r.get("avg_leverage")) or 999) <= LEVERAGE_LOW_MAX)
        high_count = sum(1 for r in arr if (safe_float(r.get("avg_leverage")) or 0) >= LEVERAGE_HIGH_MIN)
        sample = len(arr)
        low_ratio = low_count / sample if sample else 0.0
        high_ratio = high_count / sample if sample else 0.0
        coins_count: Dict[str, int] = defaultdict(int)
        for r in arr:
            if r.get("coin"):
                coins_count[str(r.get("coin"))] += 1
        dominant = ",".join([c for c, _ in sorted(coins_count.items(), key=lambda x: x[1], reverse=True)[:5]])
        grade, score, mult, note = _grade_position_wallet(sample, len(closed_rows), win_rate, avg_final, _avg(holds), _avg(levs), high_ratio)
        perf_rows.append({
            "run_id": run_id,
            "calculated_at": now_str(),
            "window_days": POSITION_PERF_WINDOW_DAYS,
            "address": addr,
            "groups": groups,
            "position_grade": grade,
            "position_score": score,
            "position_weight_multiplier": mult,
            "sample_trades": sample,
            "closed_trades": len(closed_rows),
            "open_trades": len(open_rows),
            "closed_win_rate": win_rate,
            "avg_realized_return": avg_real,
            "avg_unrealized_return": avg_unreal,
            "avg_final_return": avg_final,
            "avg_holding_hours": _avg(holds),
            "avg_leverage": _avg(levs),
            "max_leverage": max(levs) if levs else None,
            "avg_max_favorable_pct": _avg(mfes),
            "avg_max_adverse_pct": _avg(maes),
            "low_leverage_ratio": low_ratio,
            "high_leverage_ratio": high_ratio,
            "dominant_coins": dominant,
            "note": note,
        })

    cur.execute("DELETE FROM wallet_position_performance WHERE run_id=?", (run_id,))
    if perf_rows:
        cur.executemany("""
        INSERT INTO wallet_position_performance (
            run_id, calculated_at, window_days, address, groups, position_grade, position_score, position_weight_multiplier,
            sample_trades, closed_trades, open_trades, closed_win_rate, avg_realized_return, avg_unrealized_return,
            avg_final_return, avg_holding_hours, avg_leverage, max_leverage, avg_max_favorable_pct, avg_max_adverse_pct,
            low_leverage_ratio, high_leverage_ratio, dominant_coins, note
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [(
            r["run_id"], r["calculated_at"], r["window_days"], r["address"], r["groups"], r["position_grade"], r["position_score"], r["position_weight_multiplier"],
            r["sample_trades"], r["closed_trades"], r["open_trades"], r["closed_win_rate"], r["avg_realized_return"], r["avg_unrealized_return"],
            r["avg_final_return"], r["avg_holding_hours"], r["avg_leverage"], r["max_leverage"], r["avg_max_favorable_pct"], r["avg_max_adverse_pct"],
            r["low_leverage_ratio"], r["high_leverage_ratio"], r["dominant_coins"], r["note"]
        ) for r in perf_rows])
    conn.commit()
    conn.close()

    if perf_rows:
        perf_rows.sort(key=lambda x: safe_float(x.get("position_score")) or 0.0, reverse=True)
        with open(os.path.join(REPORT_DIR, "wallet_position_performance_latest.csv"), "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(perf_rows[0].keys()))
            writer.writeheader()
            writer.writerows(perf_rows)

    with open(os.path.join(REPORT_DIR, "wallet_position_report.txt"), "w", encoding="utf-8") as f:
        f.write("【仓位生命周期收益报告】\n")
        f.write(f"更新时间 UTC：{now_str()}\n")
        f.write(f"统计窗口：最近 {POSITION_PERF_WINDOW_DAYS} 天\n")
        f.write("说明：本报告只按仓位开仓/加仓/减仓/平仓计算收益，不使用账户权益 ROI，避免充值/提现误判。\n\n")
        counts: Dict[str, int] = defaultdict(int)
        for r in perf_rows:
            counts[r.get("position_grade") or "P-N"] += 1
        f.write("等级数量：" + " | ".join([f"{g}:{counts.get(g,0)}" for g in ["P-S","P-A","P-B","P-C","P-R","P-G","P-N"]]) + "\n\n")
        f.write("【适合长期参考的钱包 Top】\n")
        good = [r for r in perf_rows if r.get("position_grade") in ("P-S", "P-A")]
        if not good:
            f.write("暂无。刚开始运行时仓位样本不足很正常。\n")
        for r in good[:TOP_N]:
            f.write(
                f"{short_addr(r['address'])} [{r.get('groups','')}] {r['position_grade']} | "
                f"分={safe_float(r.get('position_score')) or 0:.1f} | 仓位样本={r.get('sample_trades')} | "
                f"平仓胜率={(safe_float(r.get('closed_win_rate')) or 0)*100:.1f}% | "
                f"均收益={fmt_pct(r.get('avg_final_return'))} | 均持仓={fmt_num(r.get('avg_holding_hours'))}h | "
                f"均杠杆={fmt_num(r.get('avg_leverage'))}x | 主币={r.get('dominant_coins') or '-'}\n"
            )
        f.write("\n【反向 / 高杠杆赌徒型钱包 Top】\n")
        bad = [r for r in perf_rows if r.get("position_grade") in ("P-R", "P-G", "P-C")]
        bad.sort(key=lambda x: (x.get("position_grade") == "P-G", safe_float(x.get("high_leverage_ratio")) or 0.0, -(safe_float(x.get("position_score")) or 0.0)), reverse=True)
        if not bad:
            f.write("暂无。\n")
        for r in bad[:TOP_N]:
            f.write(
                f"{short_addr(r['address'])} [{r.get('groups','')}] {r['position_grade']} | "
                f"分={safe_float(r.get('position_score')) or 0:.1f} | 均收益={fmt_pct(r.get('avg_final_return'))} | "
                f"高杠杆占比={(safe_float(r.get('high_leverage_ratio')) or 0)*100:.0f}% | "
                f"均持仓={fmt_num(r.get('avg_holding_hours'))}h | {r.get('note') or ''}\n"
            )
    print(f"仓位生命周期收益已更新：trades={len(trade_rows)} perf_wallets={len(perf_rows)}", flush=True)


def get_position_performance_map(run_id: int) -> Dict[str, Dict[str, Any]]:
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM wallet_position_performance WHERE run_id=?", (run_id,))
    rows = {str(r["address"]).lower(): dict(r) for r in cur.fetchall()}
    conn.close()
    return rows


def export_leverage_quality_files(run_id: int) -> None:
    """导出当前合约持仓的杠杆质量表，以及按钱包汇总的杠杆风格。"""
    if not LEVERAGE_QUALITY_MODE:
        return
    ensure_dirs()
    rows = load_rows("perp_positions", run_id)
    if not rows:
        return

    pos_fields = [
        "run_id", "address", "groups", "coin", "side", "position_value", "mark_px", "entry_px",
        "leverage", "margin_mode", "margin_used", "liquidation_px", "liq_distance_pct",
        "account_leverage", "leverage_style", "leverage_weight", "leverage_risk_score", "leverage_note",
    ]
    with open(os.path.join(REPORT_DIR, "leverage_quality_latest.csv"), "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=pos_fields)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k) for k in pos_fields})

    by_addr: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        addr = r.get("address")
        if not addr:
            continue
        val = abs(safe_float(r.get("position_value")) or 0.0)
        d = by_addr.setdefault(addr, {
            "run_id": run_id,
            "address": addr,
            "groups": r.get("groups", ""),
            "position_count": 0,
            "total_position_value": 0.0,
            "avg_leverage_num": 0.0,
            "min_liq_distance_pct": None,
            "max_account_leverage": None,
            "low_mid_value": 0.0,
            "high_risk_value": 0.0,
            "dominant_style": "",
            "style_mix": defaultdict(float),
        })
        d["position_count"] += 1
        d["total_position_value"] += val
        lev = safe_float(r.get("leverage"))
        if lev is not None:
            d["avg_leverage_num"] += lev * val
        dist = safe_float(r.get("liq_distance_pct"))
        if dist is not None:
            d["min_liq_distance_pct"] = dist if d["min_liq_distance_pct"] is None else min(d["min_liq_distance_pct"], dist)
        acc_lev = safe_float(r.get("account_leverage"))
        if acc_lev is not None:
            d["max_account_leverage"] = acc_lev if d["max_account_leverage"] is None else max(d["max_account_leverage"], acc_lev)
        style = r.get("leverage_style") or "杠杆未知"
        d["style_mix"][style] += val
        if style in ("低杠杆长期型", "中杠杆趋势型"):
            d["low_mid_value"] += val
        if style in ("中高杠杆型", "高杠杆短线型", "极高杠杆短线型", "爆仓边缘型", "强平异常/极危"):
            d["high_risk_value"] += val

    wallet_rows: List[Dict[str, Any]] = []
    for d in by_addr.values():
        total = d["total_position_value"] or 0.0
        avg_lev = d["avg_leverage_num"] / total if total else None
        low_ratio = d["low_mid_value"] / total if total else 0.0
        high_ratio = d["high_risk_value"] / total if total else 0.0
        styles = sorted(d["style_mix"].items(), key=lambda x: x[1], reverse=True)
        dominant_style = styles[0][0] if styles else "无"
        if high_ratio >= 0.6 or (d["min_liq_distance_pct"] is not None and d["min_liq_distance_pct"] < LIQ_DANGER_DISTANCE_PCT):
            wallet_style = "高风险短线钱包"
        elif low_ratio >= 0.6 and (avg_lev is None or avg_lev <= LEVERAGE_MID_MAX):
            wallet_style = "低杠杆长期钱包"
        elif avg_lev is not None and avg_lev >= LEVERAGE_HIGH_MIN:
            wallet_style = "高杠杆短线钱包"
        else:
            wallet_style = "中性趋势钱包"
        wallet_rows.append({
            "run_id": d["run_id"],
            "address": d["address"],
            "groups": d["groups"],
            "position_count": d["position_count"],
            "total_position_value": total,
            "avg_leverage": avg_lev,
            "min_liq_distance_pct": d["min_liq_distance_pct"],
            "max_account_leverage": d["max_account_leverage"],
            "low_mid_ratio": low_ratio,
            "high_risk_ratio": high_ratio,
            "wallet_leverage_style": wallet_style,
            "dominant_position_style": dominant_style,
            "style_mix": ",".join([f"{k}:{fmt_money(v)}" for k, v in styles[:5]]),
        })
    wallet_rows.sort(key=lambda x: (safe_float(x.get("high_risk_ratio")) or 0.0, safe_float(x.get("total_position_value")) or 0.0), reverse=True)
    if wallet_rows:
        with open(os.path.join(REPORT_DIR, "wallet_leverage_profile_latest.csv"), "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(wallet_rows[0].keys()))
            writer.writeheader()
            writer.writerows(wallet_rows)

    lev_map = build_leverage_signal_map(run_id)
    coin_rows: List[Dict[str, Any]] = []
    for coin, d in lev_map.items():
        coin_rows.append({
            "run_id": run_id,
            "coin": coin,
            "long_value": d.get("long_value"),
            "short_value": d.get("short_value"),
            "long_avg_leverage": d.get("long_avg_leverage"),
            "short_avg_leverage": d.get("short_avg_leverage"),
            "long_avg_liq_distance": d.get("long_avg_liq_distance"),
            "short_avg_liq_distance": d.get("short_avg_liq_distance"),
            "long_longterm_ratio": d.get("long_longterm_ratio"),
            "short_longterm_ratio": d.get("short_longterm_ratio"),
            "long_highrisk_ratio": d.get("long_highrisk_ratio"),
            "short_highrisk_ratio": d.get("short_highrisk_ratio"),
            "dominant_leverage_style": d.get("dominant_leverage_style"),
            "style_mix": d.get("style_mix"),
        })
    coin_rows.sort(key=lambda x: (abs(safe_float(x.get("long_value")) or 0.0) + abs(safe_float(x.get("short_value")) or 0.0)), reverse=True)
    if coin_rows:
        with open(os.path.join(REPORT_DIR, "coin_leverage_summary_latest.csv"), "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(coin_rows[0].keys()))
            writer.writeheader()
            writer.writerows(coin_rows)

    with open(os.path.join(REPORT_DIR, "leverage_quality_report.txt"), "w", encoding="utf-8") as f:
        f.write("【合约杠杆质量报告】\n")
        f.write(f"更新时间 UTC：{now_str()}\n")
        f.write("说明：低杠杆长期型会略加权；高杠杆短线/爆仓边缘会降权，避免影响低杠杆长期单判断。\n\n")
        f.write("【高风险钱包 Top】\n")
        high = [w for w in wallet_rows if (safe_float(w.get("high_risk_ratio")) or 0.0) >= 0.35]
        if not high:
            f.write("暂无明显高杠杆风险钱包。\n")
        for w in high[:TOP_N]:
            f.write(
                f"{short_addr(w['address'])} [{w.get('groups','')}] {w['wallet_leverage_style']} | "
                f"仓位={fmt_money(w['total_position_value'])} | 均杠杆={fmt_num(w.get('avg_leverage'))}x | "
                f"最近强平距离={fmt_pct(w.get('min_liq_distance_pct'))} | 高风险占比={(safe_float(w.get('high_risk_ratio')) or 0)*100:.0f}%\n"
            )
        f.write("\n【币种杠杆结构 Top】\n")
        for c in coin_rows[:TOP_N]:
            f.write(
                f"{c['coin']} | 多={fmt_money(c.get('long_value'))} 均杠杆={fmt_num(c.get('long_avg_leverage'))}x 强平距={fmt_pct(c.get('long_avg_liq_distance'))} 长期占比={(safe_float(c.get('long_longterm_ratio')) or 0)*100:.0f}% | "
                f"空={fmt_money(c.get('short_value'))} 均杠杆={fmt_num(c.get('short_avg_leverage'))}x 强平距={fmt_pct(c.get('short_avg_liq_distance'))} 长期占比={(safe_float(c.get('short_longterm_ratio')) or 0)*100:.0f}%\n"
            )
    print(f"杠杆质量报告已导出：{len(rows)} 个合约仓位", flush=True)


def build_signals(run_id: int, preliminary: Dict[str, Dict[str, Any]], ctx_map: Dict[str, Dict[str, Any]], thresholds: Dict[str, Dict[str, float]]) -> List[Dict[str, Any]]:
    btc_ctx = ctx_map.get("BTC", {})
    lev_map = build_leverage_signal_map(run_id) if LEVERAGE_QUALITY_MODE else {}
    rows: List[Dict[str, Any]] = []
    for coin, d in preliminary.items():
        perp_active = float(d.get("perp_active") or 0.0)
        spot_active = float(d.get("spot_active") or 0.0)
        weighted_flow = float(d.get("weighted_flow") or 0.0)
        score = 0.0
        reasons: List[str] = []
        pth = threshold(thresholds, coin, "perp")
        sth = threshold(thresholds, coin, "spot")
        if abs(perp_active) >= pth:
            score += 3.0 if perp_active > 0 else -3.0
            reasons.append(f"合约主动变化{fmt_money(perp_active)}")
        if abs(spot_active) >= sth:
            score += 2.0 if spot_active > 0 else -2.0
            reasons.append(f"现货主动变化{fmt_money(spot_active)}")
        if abs(weighted_flow) >= pth * 3:
            score += 2.0 if weighted_flow > 0 else -2.0
            reasons.append(f"钱包质量加权资金流{fmt_money(weighted_flow)}")
        elif abs(weighted_flow) >= pth:
            score += 1.0 if weighted_flow > 0 else -1.0
            reasons.append(f"钱包质量加权资金流{fmt_money(weighted_flow)}")
        if score == 0:
            continue
        direction = "bullish" if score > 0 else "bearish"
        confidence, conf_reason, conf_adj = confidence_for(coin, direction)
        m_adj, m_reasons = market_adjust(direction, btc_ctx, ctx_map.get(coin, {}))
        p_adj, p_reasons, stype = position_adjust(direction, ctx_map.get(coin, {}))
        lev_adj, lev_reasons, lev_risks, lev_fields = leverage_signal_adjust(direction, lev_map.get(coin, {}))
        final_score = score + conf_adj + m_adj + p_adj + lev_adj
        state = classify_state(direction, perp_active, spot_active, coin, thresholds, stype)
        th_score = threshold(thresholds, coin, "score_push")
        min_watch = threshold(thresholds, coin, "min_watch_score")
        watchlist = "observe"
        if abs(final_score) >= th_score:
            watchlist = "long" if direction == "bullish" else "short"
        elif abs(final_score) >= min_watch:
            watchlist = "observe"
        conclusion = ("做多观察" if watchlist == "long" else "做空观察" if watchlist == "short" else "只观察") + f" / {stype}"
        risk_parts: List[str] = []
        if confidence == "低":
            risk_parts.append("历史样本/胜率不足")
        if state in ("可能对冲", "现货流出+合约做多，换杠杆/冲突", "不明确"):
            risk_parts.append(f"信号状态：{state}")
        if stype in ("高位追多", "低位追空"):
            risk_parts.append("价格位置有追涨杀跌风险")
        risk_parts.extend(lev_risks)
        risk = "；".join(risk_parts) if risk_parts else "无明显额外风险"
        reason = "；".join(reasons + [conf_reason] + m_reasons + p_reasons + lev_reasons)
        ctx = ctx_map.get(coin, {})
        rows.append({
            "run_id": run_id,
            "coin": coin,
            "direction": direction,
            "score": score,
            "confidence": confidence,
            "signal_type": stype,
            "signal_state": state,
            "watchlist": watchlist,
            "perp_active": perp_active,
            "spot_active": spot_active,
            "weighted_flow": weighted_flow,
            "price_position": ctx.get("pos_24h"),
            "pct_1h": ctx.get("pct_1h"),
            "pct_4h": ctx.get("pct_4h"),
            "pct_24h": ctx.get("pct_24h"),
            "final_score": final_score,
            "threshold_score": th_score,
            "avg_leverage": lev_fields.get("avg_leverage"),
            "avg_liq_distance": lev_fields.get("avg_liq_distance"),
            "longterm_leverage_ratio": lev_fields.get("longterm_leverage_ratio"),
            "highrisk_leverage_ratio": lev_fields.get("highrisk_leverage_ratio"),
            "leverage_note": lev_fields.get("leverage_note"),
            "conclusion": conclusion,
            "risk": risk,
            "reason": reason,
        })
    rows.sort(key=lambda x: abs(x["final_score"]), reverse=True)
    save_coin_signals(run_id, rows)
    return rows


def save_coin_signals(run_id: int, rows: List[Dict[str, Any]]) -> None:
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM coin_signals WHERE run_id=?", (run_id,))
    cur.executemany("""
    INSERT INTO coin_signals (
        run_id, coin, direction, score, confidence, signal_type, signal_state, watchlist,
        perp_active, spot_active, weighted_flow, price_position, pct_1h, pct_4h, pct_24h,
        final_score, threshold_score, avg_leverage, avg_liq_distance, longterm_leverage_ratio, highrisk_leverage_ratio, leverage_note,
        conclusion, risk, reason
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [(
        run_id, r["coin"], r["direction"], r["score"], r["confidence"], r["signal_type"], r["signal_state"], r["watchlist"],
        r["perp_active"], r["spot_active"], r["weighted_flow"], r["price_position"], r["pct_1h"], r["pct_4h"], r["pct_24h"],
        r["final_score"], r["threshold_score"], r.get("avg_leverage"), r.get("avg_liq_distance"), r.get("longterm_leverage_ratio"), r.get("highrisk_leverage_ratio"), r.get("leverage_note"),
        r["conclusion"], r["risk"], r["reason"]
    ) for r in rows])
    conn.commit()
    conn.close()


def create_signal_events(run_id: int, signals: List[Dict[str, Any]], prices: Dict[str, float], thresholds: Dict[str, Dict[str, float]]) -> int:
    conn = db_conn()
    cur = conn.cursor()
    created = 0
    for s in signals:
        coin = s["coin"]
        px = prices.get(coin)
        if px is None or px <= 0:
            continue
        if abs(s["final_score"]) < threshold(thresholds, coin, "min_watch_score"):
            continue
        try:
            cur.execute("""
            INSERT INTO signal_events(run_id, created_at, coin, direction, score, entry_px, reason)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (run_id, now_str(), coin, s["direction"], s["final_score"], px, s["reason"]))
            created += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    conn.close()
    return created




def _win_rate(values: List[float], hurdle: float) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return sum(1 for v in vals if v >= hurdle) / len(vals)


def _avg(values: List[float]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


def _expectancy(values: List[float], hurdle: float) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    wins = [v for v in vals if v >= hurdle]
    losses = [v for v in vals if v < hurdle]
    win_rate = len(wins) / len(vals)
    avg_win = sum(wins) / len(wins) if wins else 0.0
    avg_loss = sum(losses) / len(losses) if losses else 0.0
    return win_rate * avg_win + (1 - win_rate) * avg_loss


def grade_wallet(sample_total: int,
                 eval24: int, win24: Optional[float], avg24: Optional[float],
                 eval72: int, win72: Optional[float], avg72: Optional[float],
                 eval7: int, win7: Optional[float], avg7: Optional[float],
                 eval15: int, win15: Optional[float], avg15: Optional[float],
                 eval30: int, win30: Optional[float], avg30: Optional[float],
                 groups: str) -> Tuple[str, float, float, float, str]:
    """返回 grade, quality_score, quality_weight, reverse_score, note。

    短线参考 24h/72h，中长期参考 7d/15d/30d。
    对低杠杆长期单来说，15d/30d 样本一旦成熟，会给钱包质量更高权重。
    """
    base = group_base_weight(groups)
    win24v = win24 if win24 is not None else 0.0
    win72v = win72 if win72 is not None else 0.0
    win7v = win7 if win7 is not None else 0.0
    win15v = win15 if win15 is not None else 0.0
    win30v = win30 if win30 is not None else 0.0
    avg24v = avg24 if avg24 is not None else 0.0
    avg72v = avg72 if avg72 is not None else 0.0
    avg7v = avg7 if avg7 is not None else 0.0
    avg15v = avg15 if avg15 is not None else 0.0
    avg30v = avg30 if avg30 is not None else 0.0

    # 样本不足：先按来源分组，不盲目给 S/A
    if eval24 < max(3, min(WALLET_QUALITY_MIN_SAMPLES, 10)):
        return "N", 50.0, base, 0.0, "样本不足，暂按来源分组权重"

    # 长期单优先级：30d > 15d > 7d > 72h > 24h。
    # 但如果长周期样本还没成熟，就自动降级用短周期，不会硬等30天。
    if eval30 >= WALLET_QUALITY_MIN_SAMPLES:
        main_n, main_win, main_avg, main_label = eval30, win30v, avg30v, "30d"
    elif eval15 >= WALLET_QUALITY_MIN_SAMPLES:
        main_n, main_win, main_avg, main_label = eval15, win15v, avg15v, "15d"
    elif eval7 >= WALLET_QUALITY_MIN_SAMPLES:
        main_n, main_win, main_avg, main_label = eval7, win7v, avg7v, "7d"
    elif eval72 >= WALLET_QUALITY_MIN_SAMPLES:
        main_n, main_win, main_avg, main_label = eval72, win72v, avg72v, "72h"
    else:
        main_n, main_win, main_avg, main_label = eval24, win24v, avg24v, "24h"

    reverse_score = 0.0
    if main_n >= WALLET_QUALITY_MIN_SAMPLES and main_win <= 0.38 and main_avg < 0:
        reverse_score = min(100.0, (0.45 - main_win) * 160 + min(abs(main_avg) * 8, 35))
        return "R", 20.0 - min(10.0, abs(main_avg)), -max(0.8, min(1.5, base)), reverse_score, f"反向钱包：{main_label}方向胜率和平均收益偏差"

    score = 50.0
    score += min(20.0, math.log10(max(1, sample_total)) * 12)
    score += (main_win - 0.5) * 80
    score += max(-20.0, min(20.0, main_avg * 4))
    if eval72 >= 20:
        score += 3
    if eval7 >= 8 and win7 is not None and avg7 is not None:
        score += (win7 - 0.5) * 18 + max(-6.0, min(6.0, avg7 * 1.2))
    if eval15 >= 5 and win15 is not None and avg15 is not None:
        score += (win15 - 0.5) * 22 + max(-7.0, min(7.0, avg15 * 0.9))
    if eval30 >= 3 and win30 is not None and avg30 is not None:
        score += (win30 - 0.5) * 25 + max(-8.0, min(8.0, avg30 * 0.7))
    score = max(0.0, min(100.0, score))

    # S/A 先看成熟长周期，其次看72h。长周期成熟后更适合低杠杆长期单。
    if eval30 >= 10 and win30v >= 0.58 and avg30v >= 3.0 and avg15v >= 1.0:
        grade, weight, note = "S", min(2.35, base + 0.65), "S级：30d胜率和长期平均收益较好"
    elif eval15 >= 12 and win15v >= 0.60 and avg15v >= 2.0:
        grade, weight, note = "S", min(2.25, base + 0.60), "S级：15d胜率和中期收益较好"
    elif eval72 >= 30 and win72v >= 0.62 and avg72v >= 1.5 and avg24v > 0:
        grade, weight, note = "S", min(2.2, base + 0.55), "S级：样本多，72h胜率和期望较好"
    elif eval30 >= 6 and win30v >= 0.54 and avg30v > 1.0:
        grade, weight, note = "A", min(2.05, base + 0.45), "A级：30d长期表现偏好"
    elif eval15 >= 8 and win15v >= 0.56 and avg15v > 1.0:
        grade, weight, note = "A", min(2.0, base + 0.42), "A级：15d中期表现偏好"
    elif eval72 >= 20 and win72v >= 0.56 and avg72v > 0.5:
        grade, weight, note = "A", min(1.9, base + 0.35), "A级：72h表现稳定偏好"
    elif main_n >= WALLET_QUALITY_MIN_SAMPLES and main_win >= 0.50 and main_avg >= -0.2:
        grade, weight, note = "B", base, f"B级：普通有效参考，主周期={main_label}"
    elif main_n >= WALLET_QUALITY_MIN_SAMPLES:
        grade, weight, note = "C", max(0.55, base * 0.55), f"C级：噪音偏多，降权参考，主周期={main_label}"
    else:
        grade, weight, note = "N", base, "样本不足，暂按来源分组权重"

    return grade, score, weight, reverse_score, note

def refresh_wallet_quality(run_id: int, address_groups: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    """按最近 WALLET_QUALITY_WINDOW_DAYS 天给所有监控钱包分级并导出。"""
    if not WALLET_QUALITY_MODE:
        return []
    # 为了统计 15d / 30d 胜率，需要回看更早的动作；否则刚好30天窗口里大多动作还没有30d结果。
    # 例如 window=30 时，实际动作回看 60 天，但每个 horizon 只统计已经成熟的样本。
    action_lookback_days = max(WALLET_QUALITY_WINDOW_DAYS + 30, 60)
    since = (utc_now() - dt.timedelta(days=action_lookback_days)).strftime("%Y-%m-%d %H:%M:%S")
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT address, groups, coin, created_at, ret_24h, ret_72h, ret_7d, ret_15d, ret_30d
    FROM wallet_actions
    WHERE created_at >= ?
    """, (since,))
    acts = [dict(x) for x in cur.fetchall()]

    by_addr: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for a in acts:
        by_addr[(a.get("address") or "").lower()].append(a)

    rows: List[Dict[str, Any]] = []
    for addr, groups_list in sorted(address_groups.items()):
        groups = ",".join(groups_list)
        arr = by_addr.get(addr.lower(), [])
        ret24 = [safe_float(a.get("ret_24h")) for a in arr if safe_float(a.get("ret_24h")) is not None]
        ret72 = [safe_float(a.get("ret_72h")) for a in arr if safe_float(a.get("ret_72h")) is not None]
        ret7 = [safe_float(a.get("ret_7d")) for a in arr if safe_float(a.get("ret_7d")) is not None]
        ret15 = [safe_float(a.get("ret_15d")) for a in arr if safe_float(a.get("ret_15d")) is not None]
        ret30 = [safe_float(a.get("ret_30d")) for a in arr if safe_float(a.get("ret_30d")) is not None]
        coins_count: Dict[str, int] = defaultdict(int)
        last_action = None
        for a in arr:
            if a.get("coin"):
                coins_count[a["coin"]] += 1
            ca = a.get("created_at")
            if ca and (last_action is None or ca > last_action):
                last_action = ca
        dominant = ",".join([c for c, _ in sorted(coins_count.items(), key=lambda x: x[1], reverse=True)[:5]])

        win24 = _win_rate(ret24, 1.0)
        win72 = _win_rate(ret72, 2.0)
        win7 = _win_rate(ret7, 4.0)
        win15 = _win_rate(ret15, 6.0)
        win30 = _win_rate(ret30, 8.0)
        avg24 = _avg(ret24)
        avg72 = _avg(ret72)
        avg7 = _avg(ret7)
        avg15 = _avg(ret15)
        avg30 = _avg(ret30)
        exp72 = _expectancy(ret72, 2.0)
        exp30 = _expectancy(ret30, 8.0)
        grade, qscore, qweight, reverse, note = grade_wallet(
            len(arr), len(ret24), win24, avg24, len(ret72), win72, avg72, len(ret7), win7, avg7,
            len(ret15), win15, avg15, len(ret30), win30, avg30, groups
        )
        rows.append({
            "run_id": run_id,
            "calculated_at": now_str(),
            "window_days": WALLET_QUALITY_WINDOW_DAYS,
            "address": addr,
            "groups": groups,
            "grade": grade,
            "quality_score": qscore,
            "quality_weight": qweight,
            "sample_total": len(arr),
            "eval_24h": len(ret24),
            "win_24h": win24,
            "avg_24h": avg24,
            "eval_72h": len(ret72),
            "win_72h": win72,
            "avg_72h": avg72,
            "eval_7d": len(ret7),
            "win_7d": win7,
            "avg_7d": avg7,
            "eval_15d": len(ret15),
            "win_15d": win15,
            "avg_15d": avg15,
            "eval_30d": len(ret30),
            "win_30d": win30,
            "avg_30d": avg30,
            "expectancy_72h": exp72,
            "expectancy_30d": exp30,
            "reverse_score": reverse,
            "last_action_at": last_action,
            "dominant_coins": dominant,
            "note": note,
        })

    cur.execute("DELETE FROM wallet_quality WHERE run_id=?", (run_id,))
    cur.executemany("""
    INSERT INTO wallet_quality (
        run_id, calculated_at, window_days, address, groups, grade, quality_score, quality_weight,
        sample_total, eval_24h, win_24h, avg_24h, eval_72h, win_72h, avg_72h,
        eval_7d, win_7d, avg_7d, eval_15d, win_15d, avg_15d, eval_30d, win_30d, avg_30d,
        expectancy_72h, expectancy_30d, reverse_score, last_action_at, dominant_coins, note
     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [(
        r["run_id"], r["calculated_at"], r["window_days"], r["address"], r["groups"], r["grade"], r["quality_score"], r["quality_weight"],
        r["sample_total"], r["eval_24h"], r["win_24h"], r["avg_24h"], r["eval_72h"], r["win_72h"], r["avg_72h"],
        r["eval_7d"], r["win_7d"], r["avg_7d"], r["eval_15d"], r["win_15d"], r["avg_15d"], r["eval_30d"], r["win_30d"], r["avg_30d"],
        r["expectancy_72h"], r["expectancy_30d"], r["reverse_score"], r["last_action_at"], r["dominant_coins"], r["note"]
    ) for r in rows])
    conn.commit()
    conn.close()

    if WALLET_QUALITY_EXPORT:
        export_wallet_quality_files(rows)
    print(f"钱包质量分类已更新：{len(rows)} 个钱包，窗口={WALLET_QUALITY_WINDOW_DAYS}天", flush=True)
    return rows


def get_wallet_quality_map(run_id: int) -> Dict[str, Dict[str, Any]]:
    """返回用于信号加权的钱包质量图。

    这里会把两套评分合并到同一个 address：
    1) wallet_quality：动作后方向胜率 / 24h / 72h / 7d / 15d / 30d。
    2) wallet_position_performance：仓位生命周期真实表现 / 平仓收益 / 持仓时长 / 杠杆风格。

    注意：早期版本只读取 wallet_quality，导致 P-S/P-A/P-G/P-R
    只导出报告但没有真正参与加权。这里已修复。
    """
    qmap: Dict[str, Dict[str, Any]] = {}
    try:
        qrows = load_rows("wallet_quality", run_id)
    except Exception:
        qrows = []
    for r in qrows:
        addr = str(r.get("address", "")).lower()
        if addr:
            qmap[addr] = dict(r)

    try:
        prows = load_rows("wallet_position_performance", run_id)
    except Exception:
        prows = []
    for r in prows:
        addr = str(r.get("address", "")).lower()
        if not addr:
            continue
        base = qmap.setdefault(addr, {"address": addr, "groups": r.get("groups", ""), "grade": "N", "quality_weight": group_base_weight(r.get("groups", ""))})
        # 合并仓位生命周期评分，让 wallet_quality_weight() 真正能读到 P-S/P-A/P-G/P-R。
        base["position_grade"] = r.get("position_grade")
        base["position_score"] = r.get("position_score")
        base["position_weight_multiplier"] = r.get("position_weight_multiplier")
        base["position_sample_trades"] = r.get("sample_trades")
        base["position_closed_win_rate"] = r.get("closed_win_rate")
        base["position_avg_final_return"] = r.get("avg_final_return")
        base["position_avg_holding_hours"] = r.get("avg_holding_hours")
        base["position_avg_leverage"] = r.get("avg_leverage")
        base["position_high_leverage_ratio"] = r.get("high_leverage_ratio")
        base["position_note"] = r.get("note")

    return qmap


def wallet_quality_summary(run_id: int) -> Dict[str, Any]:
    rows = load_rows("wallet_quality", run_id)
    counts: Dict[str, int] = defaultdict(int)
    for r in rows:
        counts[r.get("grade") or "N"] += 1
    return {"total": len(rows), "counts": dict(counts)}


def top_wallet_quality(run_id: int, grade_filter: Optional[List[str]] = None, limit: int = 10) -> List[Dict[str, Any]]:
    rows = load_rows("wallet_quality", run_id)
    if grade_filter:
        rows = [r for r in rows if r.get("grade") in grade_filter]
    rows.sort(key=lambda r: (safe_float(r.get("quality_score")) or 0.0, safe_float(r.get("sample_total")) or 0.0), reverse=True)
    return rows[:limit]


def export_wallet_quality_files(rows: List[Dict[str, Any]]) -> None:
    ensure_dirs()
    csv_path = os.path.join(REPORT_DIR, "wallet_quality_latest.csv")
    if rows:
        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    txt_path = os.path.join(REPORT_DIR, "wallet_quality_report.txt")
    counts: Dict[str, int] = defaultdict(int)
    for r in rows:
        counts[r.get("grade") or "N"] += 1
    top_good = [r for r in rows if r.get("grade") in ("S", "A")]
    top_good.sort(key=lambda r: safe_float(r.get("quality_score")) or 0.0, reverse=True)
    reverse = [r for r in rows if r.get("grade") == "R"]
    reverse.sort(key=lambda r: safe_float(r.get("reverse_score")) or 0.0, reverse=True)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("【钱包质量分类】\n")
        f.write(f"更新时间 UTC：{now_str()}\n")
        f.write(f"统计窗口：最近 {WALLET_QUALITY_WINDOW_DAYS} 天\n")
        f.write("胜率定义：24h>=+1%算赢；72h>=+2%算赢；7d>=+4%算赢。做空会按方向收益计算。\n\n")
        f.write("等级数量：" + " | ".join([f"{g}:{counts.get(g,0)}" for g in ["S","A","B","C","R","N"]]) + "\n\n")
        f.write("【S/A 钱包 Top】\n")
        if not top_good:
            f.write("暂无。\n")
        for r in top_good[:TOP_N]:
            f.write(
                f"{short_addr(r['address'])} [{r.get('groups','')}] {r['grade']} | "
                f"分={safe_float(r.get('quality_score')) or 0:.1f} | 权重={safe_float(r.get('quality_weight')) or 0:.2f} | "
                f"样本={r.get('sample_total')} | 72h胜率={(safe_float(r.get('win_72h')) or 0)*100:.1f}% | "
                f"72h均值={fmt_pct(r.get('avg_72h'))} | 主币={r.get('dominant_coins') or '-'}\n"
            )
        f.write("\n【R级反向钱包 Top】\n")
        if not reverse:
            f.write("暂无。\n")
        for r in reverse[:TOP_N]:
            f.write(
                f"{short_addr(r['address'])} [{r.get('groups','')}] R | "
                f"反向分={safe_float(r.get('reverse_score')) or 0:.1f} | 权重={safe_float(r.get('quality_weight')) or 0:.2f} | "
                f"样本={r.get('sample_total')} | 72h胜率={(safe_float(r.get('win_72h')) or 0)*100:.1f}% | "
                f"72h均值={fmt_pct(r.get('avg_72h'))} | 主币={r.get('dominant_coins') or '-'}\n"
            )

def recent_signal_summary(days: int = REPORT_REVIEW_WINDOW_DAYS) -> List[Dict[str, Any]]:
    """报告底部信号复盘。默认统计过去30天。"""
    conn = db_conn()
    cur = conn.cursor()
    since = (utc_now() - dt.timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    cur.execute("""
    SELECT coin, direction, COUNT(*) AS n, AVG(score) AS avg_score, MAX(ABS(score)) AS max_abs_score
    FROM signal_events
    WHERE created_at >= ?
    GROUP BY coin, direction
    ORDER BY max_abs_score DESC
    LIMIT 50
    """, (since,))
    rows = [dict(x) for x in cur.fetchall()]
    conn.close()
    return rows


def recent_wallet_flow(days: int = REPORT_REVIEW_WINDOW_DAYS) -> List[Dict[str, Any]]:
    """报告底部钱包主动资金流。默认统计过去30天。"""
    conn = db_conn()
    cur = conn.cursor()
    since = (utc_now() - dt.timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    cur.execute("""
    SELECT coin, market, direction, COUNT(*) AS n, SUM(active_delta) AS active_sum
    FROM wallet_actions
    WHERE created_at >= ?
    GROUP BY coin, market, direction
    ORDER BY ABS(active_sum) DESC
    LIMIT 50
    """, (since,))
    rows = [dict(x) for x in cur.fetchall()]
    conn.close()
    return rows


def already_pushed_today(push_type: str) -> bool:
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM push_log WHERE push_type=? AND push_date=?", (push_type, utc_today()))
    row = cur.fetchone()
    conn.close()
    return row is not None


def mark_pushed(push_type: str) -> None:
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO push_log(push_type, push_date, pushed_at) VALUES (?, ?, ?)", (push_type, utc_today(), now_str()))
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    conn.close()


def should_push_daily() -> bool:
    """每日复盘推送判断。

    旧逻辑是必须刚好在 DAILY_PUSH_HOUR_UTC 这个小时运行才推送。
    如果 GitHub Actions 那一小时失败/排队/手动运行错过，就会出现：
    今天报告已经生成，但 TG 没有收到。

    新逻辑：到达设定小时之后，只要今天还没成功推送过，
    第一轮成功生成报告就会补推一次。
    """
    now = utc_now()
    return now.hour >= DAILY_PUSH_HOUR_UTC and not already_pushed_today("daily")



def get_coin_recent_rows(coin: str, run_id: int, limit: int = 6) -> List[Dict[str, Any]]:
    """读取某个币最近几轮信号，用于判断连续性。"""
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
        SELECT run_id, direction, final_score, confidence, signal_state, signal_type
        FROM coin_signals
        WHERE coin = ? AND run_id <= ?
        ORDER BY run_id DESC
        LIMIT ?
        """, (coin, run_id, limit))
        rows = [dict(x) for x in cur.fetchall()]
    except Exception:
        rows = []
    conn.close()
    return rows


def signal_streak(coin: str, direction: str, run_id: int, min_abs_score: float = 0.0) -> int:
    """统计最近连续同方向轮数。只统计分数达到 min_abs_score 的轮次。"""
    rows = get_coin_recent_rows(coin, run_id, limit=8)
    streak = 0
    for r in rows:
        fs = abs(safe_float(r.get("final_score")) or 0.0)
        if r.get("direction") == direction and fs >= min_abs_score:
            streak += 1
        else:
            break
    return streak


def long_term_leverage_hint(sig: Dict[str, Any], ctx: Dict[str, Any]) -> str:
    """按波动和信号质量给低杠杆建议，不做自动下单。"""
    pct4 = abs(safe_float(ctx.get("pct_4h")) or 0.0)
    pct24 = abs(safe_float(ctx.get("pct_24h")) or 0.0)
    stype = sig.get("signal_type") or ""
    confidence = sig.get("confidence") or "低"
    score = abs(safe_float(sig.get("final_score")) or 0.0)

    cap = LONG_TERM_MAX_LEVERAGE
    avg_lev = safe_float(sig.get("avg_leverage"))
    avg_liq = safe_float(sig.get("avg_liq_distance"))
    high_ratio = safe_float(sig.get("highrisk_leverage_ratio")) or 0.0
    long_ratio = safe_float(sig.get("longterm_leverage_ratio")) or 0.0
    if high_ratio >= 0.5 or (avg_liq is not None and avg_liq < LIQ_DANGER_DISTANCE_PCT) or (avg_lev is not None and avg_lev >= LEVERAGE_HIGH_MIN):
        cap = min(cap, 1.5)
    elif long_ratio >= 0.6 and (avg_lev is None or avg_lev <= LEVERAGE_MID_MAX) and (avg_liq is None or avg_liq >= 20):
        cap = min(cap, 3.0)
    elif pct4 >= 5 or pct24 >= 12 or stype in ("高位追多", "低位追空"):
        cap = min(cap, 2.0)
    elif confidence == "高" and score >= 9 and stype in ("低位吸筹", "高位加空", "普通趋势", "高位突破", "低位破位"):
        cap = min(cap, 3.0)
    else:
        cap = min(cap, 2.5)

    if cap <= 1.5:
        return "1x-1.5x"
    if cap <= 2:
        return "1x-2x"
    if cap <= 2.5:
        return "1x-2.5x"
    return "1x-3x"


def long_term_entry_plan(sig: Dict[str, Any], ctx: Dict[str, Any], streak: int) -> Dict[str, str]:
    direction = sig.get("direction")
    stype = sig.get("signal_type") or ""
    confidence = sig.get("confidence") or "低"
    state = sig.get("signal_state") or ""
    score = abs(safe_float(sig.get("final_score")) or 0.0)
    pct4 = safe_float(ctx.get("pct_4h"))
    pos = safe_float(ctx.get("pos_24h"))

    avoid_states = {"可能对冲", "现货流出+合约做多，换杠杆/冲突", "不明确", "追涨杀跌风险"}
    good_state = state in {"清晰同向", "合约主导", "现货主导", "现货持有+合约做空，对冲偏空"}
    bad_position = stype in {"高位追多", "低位追空"}

    avg_liq = safe_float(sig.get("avg_liq_distance"))
    avg_lev = safe_float(sig.get("avg_leverage"))
    high_ratio = safe_float(sig.get("highrisk_leverage_ratio")) or 0.0
    leverage_bad = high_ratio >= 0.6 or (avg_liq is not None and avg_liq < LIQ_DANGER_DISTANCE_PCT) or (avg_lev is not None and avg_lev >= 20)

    if confidence == "低" or state in avoid_states or bad_position or leverage_bad:
        action = "只观察，不适合直接做长期单"
        entry = "等待下一轮确认；不要因为单次异动直接开仓。"
        if leverage_bad:
            entry += " 当前同方向杠杆结构偏短线/强平距离偏近，长期单降权。"
    elif score >= LONG_TERM_MIN_SCORE and streak >= LONG_TERM_MIN_STREAK and good_state:
        action = "可进入低杠杆长期观察"
        entry = "分3批：30%试仓，30%确认加仓，40%回踩/反抽后再加；不要一次打满。"
    elif score >= LONG_TERM_MIN_SCORE and good_state:
        action = "等待连续性确认"
        entry = f"当前只有连续{streak}轮，建议等到连续{LONG_TERM_MIN_STREAK}轮同方向后再考虑。"
    else:
        action = "只观察"
        entry = "分数或状态不足，先看下一轮。"

    if direction == "bullish":
        invalid = "失效条件：final_score跌破4；跌出做多观察；现货转流出且合约净多下降；BTC 4h明显转弱。"
        price_note = "做多更适合低位吸筹、普通趋势或突破回踩；高位追多要降仓位。"
    else:
        invalid = "失效条件：final_score回到-4以内；跌出做空观察；空头明显平仓；现货重新流入；BTC 4h明显转强。"
        price_note = "做空更适合高位加空、普通趋势或破位反抽；低位追空要降仓位。"

    if sig.get("leverage_note"):
        price_note += f" 杠杆结构：{sig.get('leverage_note')}。"
    if pct4 is not None and abs(pct4) >= 5:
        price_note += f" 当前4h波动 {fmt_pct(pct4)}，不适合重仓追。"
    if pos is not None:
        price_note += f" 24h价格位置约 {pos:.2f}。"

    return {
        "action": action,
        "entry": entry,
        "invalid": invalid,
        "price_note": price_note,
        "leverage": long_term_leverage_hint(sig, ctx),
    }


def build_long_term_candidates(run_id: int, signals: List[Dict[str, Any]], ctx_map: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """把短线/小时级信号过滤成适合低杠杆长期单观察的候选。"""
    if not LONG_TERM_MODE:
        return []

    out: List[Dict[str, Any]] = []
    for s in signals:
        score = abs(safe_float(s.get("final_score")) or 0.0)
        if score < threshold(load_thresholds(), s["coin"], "min_watch_score"):
            continue
        coin = s["coin"]
        direction = s["direction"]
        ctx = ctx_map.get(coin, {})
        streak = signal_streak(coin, direction, run_id, min_abs_score=threshold(load_thresholds(), coin, "min_watch_score"))
        plan = long_term_entry_plan(s, ctx, streak)

        # 长期评分：final_score + 连续性 + 可信度 + 清晰状态 - 风险位置
        lt_score = score
        if streak >= LONG_TERM_MIN_STREAK:
            lt_score += 1.0
        if s.get("confidence") == "高":
            lt_score += 1.0
        elif s.get("confidence") == "低":
            lt_score -= 1.0
        if s.get("signal_state") in {"清晰同向", "合约主导", "现货主导", "现货持有+合约做空，对冲偏空"}:
            lt_score += 0.8
        if s.get("signal_type") in {"高位追多", "低位追空"}:
            lt_score -= 1.5
        lt_score += min(1.0, (safe_float(s.get("longterm_leverage_ratio")) or 0.0))
        lt_score -= min(1.5, (safe_float(s.get("highrisk_leverage_ratio")) or 0.0) * 1.5)

        out.append({
            "coin": coin,
            "direction": direction,
            "direction_cn": dir_cn(direction),
            "final_score": safe_float(s.get("final_score")) or 0.0,
            "long_term_score": lt_score,
            "streak": streak,
            "confidence": s.get("confidence"),
            "signal_state": s.get("signal_state"),
            "signal_type": s.get("signal_type"),
            "leverage": plan["leverage"],
            "avg_leverage": s.get("avg_leverage"),
            "avg_liq_distance": s.get("avg_liq_distance"),
            "longterm_leverage_ratio": s.get("longterm_leverage_ratio"),
            "highrisk_leverage_ratio": s.get("highrisk_leverage_ratio"),
            "leverage_note": s.get("leverage_note"),
            "action": plan["action"],
            "entry": plan["entry"],
            "invalid": plan["invalid"],
            "price_note": plan["price_note"],
            "risk_pct": LONG_TERM_RISK_PCT,
        })

    out.sort(key=lambda x: x["long_term_score"], reverse=True)
    return out


def write_long_term_plan(candidates: List[Dict[str, Any]]) -> None:
    ensure_dirs()
    path = os.path.join(REPORT_DIR, "long_term_plan.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("【低杠杆长期单观察计划】\n")
        f.write(f"更新时间 UTC：{now_str()}\n")
        f.write(f"风控默认：单币最大亏损控制在账户 {LONG_TERM_RISK_PCT:.1f}% 左右；建议低杠杆，不一次打满。\n\n")
        if not candidates:
            f.write("暂无适合低杠杆长期单的候选。\n")
            return
        for c in candidates[:TOP_N]:
            f.write(f"{c['coin']} {c['direction_cn']} | 长期分={c['long_term_score']:.1f} | final={c['final_score']:+.1f} | 连续={c['streak']}轮 | 可信度={c['confidence']}\n")
            f.write(f"状态：{c['signal_state']} | 类型：{c['signal_type']} | 建议杠杆：{c['leverage']}\n")
            f.write(f"动作：{c['action']}\n")
            f.write(f"入场：{c['entry']}\n")
            f.write(f"价格：{c['price_note']}\n")
            f.write(f"失效：{c['invalid']}\n\n")

    csv_path = os.path.join(REPORT_DIR, "long_term_candidates.csv")
    if candidates:
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=list(candidates[0].keys()))
            writer.writeheader()
            writer.writerows(candidates)

def write_watchlists(signals: List[Dict[str, Any]]) -> None:
    ensure_dirs()
    mapping = {"long": "watchlist_long.txt", "short": "watchlist_short.txt", "observe": "watchlist_observe.txt"}
    titles = {"long": "做多观察", "short": "做空观察", "observe": "只观察"}
    for key, fname in mapping.items():
        path = os.path.join(REPORT_DIR, fname)
        rows = [s for s in signals if s["watchlist"] == key]
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"【{titles[key]}】\n更新时间 UTC：{now_str()}\n\n")
            if not rows:
                f.write("暂无。\n")
            for s in rows[:TOP_N]:
                f.write(f"{s['coin']} {dir_cn(s['direction'])} score={s['final_score']:+.1f}/阈值{s['threshold_score']:.1f} 状态={s['signal_state']}\n")
                f.write(f"结论：{s['conclusion']}\n")
                f.write(f"风险：{s['risk']}\n")
                f.write(f"原因：{s['reason']}\n\n")


def build_report(run_id: int, signals: List[Dict[str, Any]], ctx_map: Dict[str, Dict[str, Any]], actions: List[Dict[str, Any]], cashflows: List[Dict[str, Any]], ok_rate: float, new_signal_events: int, updated_actions: int, updated_signals: int) -> str:
    strong = [s for s in signals if abs(s["final_score"]) >= s["threshold_score"]]
    longs = [s for s in signals if s["watchlist"] == "long"]
    shorts = [s for s in signals if s["watchlist"] == "short"]
    observes = [s for s in signals if s["watchlist"] == "observe"]
    btc = ctx_map.get("BTC", {})
    eth = ctx_map.get("ETH", {})
    lines: List[str] = []
    lines.append("🧠 Hyperliquid 钱包监控 FINAL")
    lines.append("币种阈值 + 钱包主动变化 + 市场环境 + 观察列表 + TG 推送")
    lines.append(f"UTC时间：{now_str()}")
    lines.append(f"run_id：{run_id}")
    stats = run_wallet_stats(run_id)
    lines.append("【扫描健康】")
    lines.append(
        f"监控钱包：{stats['total']} | 成功：{stats['ok']} | "
        f"partial：{stats['partial']} | failed：{stats['failed']} | "
        f"成功率：{stats['ok_rate']*100:.2f}%"
    )
    lines.append(f"新信号追踪：{new_signal_events} | 更新动作收益：{updated_actions} | 更新信号收益：{updated_signals}")
    if WALLET_QUALITY_MODE:
        qs = wallet_quality_summary(run_id)
        qc = qs.get("counts", {})
        lines.append("【钱包质量】")
        lines.append(f"统计窗口：最近{WALLET_QUALITY_WINDOW_DAYS}天 | 总钱包：{qs.get('total', 0)} | S:{qc.get('S',0)} A:{qc.get('A',0)} B:{qc.get('B',0)} C:{qc.get('C',0)} R:{qc.get('R',0)} N:{qc.get('N',0)}")
        topq = top_wallet_quality(run_id, ["S", "A"], limit=5)
        if topq:
            lines.append("优质钱包Top：" + "；".join([f"{short_addr(r['address'])} {r['grade']} 分{(safe_float(r.get('quality_score')) or 0):.0f}" for r in topq]))
        revq = top_wallet_quality(run_id, ["R"], limit=3)
        if revq:
            lines.append("反向钱包提醒：" + "；".join([f"{short_addr(r['address'])} R 反向{(safe_float(r.get('reverse_score')) or 0):.0f}" for r in revq]))
    if POSITION_TRADE_MODE:
        try:
            perf = load_rows("wallet_position_performance", run_id)
            pc: Dict[str, int] = defaultdict(int)
            for r in perf:
                pc[r.get("position_grade") or "P-N"] += 1
            lines.append("【仓位生命周期收益】")
            lines.append(
                f"统计窗口：最近{POSITION_PERF_WINDOW_DAYS}天 | "
                f"P-S:{pc.get('P-S',0)} P-A:{pc.get('P-A',0)} P-B:{pc.get('P-B',0)} "
                f"P-C:{pc.get('P-C',0)} P-R:{pc.get('P-R',0)} P-G:{pc.get('P-G',0)} P-N:{pc.get('P-N',0)}"
            )
            goodp = [r for r in perf if r.get("position_grade") in ("P-S", "P-A")]
            goodp.sort(key=lambda r: safe_float(r.get("position_score")) or 0.0, reverse=True)
            if goodp:
                lines.append("仓位收益优质Top：" + "；".join([f"{short_addr(r['address'])} {r['position_grade']} 收益{fmt_pct(r.get('avg_final_return'))}" for r in goodp[:5]]))
        except Exception:
            pass
    lines.append("")
    lines.append("【大盘环境】")
    lines.append(f"BTC: 1h {fmt_pct(btc.get('pct_1h'))} | 4h {fmt_pct(btc.get('pct_4h'))} | 24h {fmt_pct(btc.get('pct_24h'))} | regime={btc.get('regime')}")
    lines.append(f"ETH: 1h {fmt_pct(eth.get('pct_1h'))} | 4h {fmt_pct(eth.get('pct_4h'))} | 24h {fmt_pct(eth.get('pct_24h'))} | regime={eth.get('regime')}")
    lines.append("")
    lines.append("【最终强信号】")
    if not strong:
        lines.append("暂无达到币种专属阈值的强信号。")
    else:
        for s in strong[:TOP_N]:
            lines.append(f"🚨 {s['coin']} {dir_cn(s['direction'])} | score={s['final_score']:+.1f}/阈值{s['threshold_score']:.1f} | {s['signal_state']} | 可信度={s['confidence']} | 类型={s['signal_type']}")
            lines.append(f"  结论：{s['conclusion']}")
            lines.append(f"  风险：{s['risk']}")
            lines.append(f"  原因：{s['reason']}")
    lines.append("")
    lines.append("【做多观察】")
    if not longs:
        lines.append("暂无。")
    else:
        for s in longs[:TOP_N]:
            lines.append(f"{s['coin']} score={s['final_score']:+.1f} | {s['signal_state']} | {s['conclusion']}")
    lines.append("")
    lines.append("【做空观察】")
    if not shorts:
        lines.append("暂无。")
    else:
        for s in shorts[:TOP_N]:
            lines.append(f"{s['coin']} score={s['final_score']:+.1f} | {s['signal_state']} | {s['conclusion']}")
    lines.append("")
    lines.append("【只观察 / 信号不足】")
    if not observes:
        lines.append("暂无。")
    else:
        for s in observes[:TOP_N]:
            lines.append(f"{s['coin']} {dir_cn(s['direction'])} score={s['final_score']:+.1f} | {s['signal_state']} | 风险：{s['risk']}")
    lines.append("")
    if LONG_TERM_MODE:
        lt_candidates = build_long_term_candidates(run_id, signals, ctx_map)
        lines.append("【低杠杆长期单过滤】")
        if not lt_candidates:
            lines.append("暂无适合低杠杆长期单的候选。")
        else:
            for c in lt_candidates[:TOP_N]:
                lines.append(
                    f"{c['coin']} {c['direction_cn']} | 长期分={c['long_term_score']:.1f} | "
                    f"final={c['final_score']:+.1f} | 连续={c['streak']}轮 | "
                    f"可信度={c['confidence']} | 建议杠杆={c['leverage']}"
                )
                lines.append(f"  动作：{c['action']}")
                lines.append(f"  入场：{c['entry']}")
                lines.append(f"  失效：{c['invalid']}")
        lines.append("")
    lines.append("【单钱包主动变化 Top】")
    if not actions:
        lines.append("暂无超过阈值的钱包主动变化。")
    else:
        for a in actions[:TOP_N]:
            lev_txt = ""
            if a.get("market") == "perp":
                lev_txt = f" | 杠杆={fmt_num(a.get('leverage'))}x | {a.get('leverage_style') or ''} | 强平距={fmt_pct(a.get('liq_distance_pct'))}"
            if a.get("market") == "spot":
                if a.get("active_delta", 0) >= 0:
                    op_txt = f" | 增持现货={a.get('spot_increases','-')}"
                else:
                    op_txt = f" | 减持现货={a.get('spot_decreases','-')}"
            else:
                op_txt = f" | 合约操作={a.get('perp_operations','-')}"
            pos_txt = f" | 当前合约={a.get('current_perp_positions','-')} | 当前现货Top={a.get('current_spot_holdings','-')}"
            lines.append(f"{a['coin']} {a['market']} {dir_cn(a['direction'])} {short_addr(a['address'])} [{a.get('groups','')}] | 主动={fmt_money(a['active_delta'])} | 价格影响={fmt_money(a['price_effect'])}{lev_txt}{op_txt}{pos_txt}")
    lines.append("")
    lines.append("【资金流 Lite】")
    lines.append("说明：基于钱包 USDC 和现货余额变化推断，不是外部链上充值提现标签。")
    if not cashflows:
        lines.append("暂无明显 USDC/现货资金流变化。")
    else:
        for c in cashflows[:TOP_N]:
            lines.append(f"{short_addr(c['address'])} [{c['groups']}] | USDC={fmt_money(c['usdc_delta'])} | 现货={fmt_money(c['spot_delta'])} | {c['flow_type']} | 增持现货={c.get('spot_increases','-')} | 减持现货={c.get('spot_decreases','-')} | 当前合约={c.get('current_perp_positions','-')}")
    lines.append("")
    lines.append(f"【过去{REPORT_REVIEW_WINDOW_DAYS}天 信号复盘】")
    summary = recent_signal_summary(REPORT_REVIEW_WINDOW_DAYS)
    if not summary:
        lines.append(f"暂无{REPORT_REVIEW_WINDOW_DAYS}天信号数据。")
    else:
        for r in summary[:TOP_N]:
            lines.append(f"{r['coin']} {dir_cn(r['direction'])} | 次数={r['n']} | 均分={r['avg_score']:+.2f} | 最高={r['max_abs_score']:.2f}")
    lines.append("")
    lines.append(f"【过去{REPORT_REVIEW_WINDOW_DAYS}天 钱包主动资金流】")
    flow = recent_wallet_flow(REPORT_REVIEW_WINDOW_DAYS)
    if not flow:
        lines.append(f"暂无{REPORT_REVIEW_WINDOW_DAYS}天钱包动作数据。")
    else:
        for r in flow[:TOP_N]:
            lines.append(f"{r['coin']} {r['market']} {dir_cn(r['direction'])} | 次数={r['n']} | 主动变化={fmt_money(r['active_sum'])}")
    lines.append("")
    lines.append("【说明】")
    lines.append("做多/做空观察不是自动下单建议，只是监控信号方向。")
    lines.append("第一次运行只建立快照，第二次开始才有趋势对比。")
    lines.append("如果 TG 太少，降低 coin_thresholds.json 的 score_push；如果太多，提高 score_push。")
    return "\n".join(lines)


def save_report(run_id: int, signals: List[Dict[str, Any]], report: str) -> None:
    ensure_dirs()
    strong_count = sum(1 for s in signals if abs(s["final_score"]) >= s["threshold_score"])
    long_count = sum(1 for s in signals if s["watchlist"] == "long")
    short_count = sum(1 for s in signals if s["watchlist"] == "short")
    with open(os.path.join(REPORT_DIR, "final_latest_report.txt"), "w", encoding="utf-8") as f:
        f.write(report)
    # 不再生成 final_report_run_x.txt：
    # - final_latest_report.txt 每轮覆盖，用来看最新状态
    # - reports/daily/YYYY-MM-DD/ 每天保留一份快照
    # - hl_monitor.db 长期累积历史
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO final_reports(run_id, created_at, strong_count, long_count, short_count, report) VALUES (?, ?, ?, ?, ?, ?)",
                (run_id, now_str(), strong_count, long_count, short_count, report))
    conn.commit()
    conn.close()


async def send_tg(text: str) -> bool:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        print("未配置 TG_BOT_TOKEN / TG_CHAT_ID，不推送。")
        return False
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    chunks: List[str] = []
    max_len = 3500
    while len(text) > max_len:
        cut = text[:max_len]
        pos = cut.rfind("\n")
        if pos == -1:
            pos = max_len
        chunks.append(text[:pos])
        text = text[pos:]
    if text.strip():
        chunks.append(text)
    ok_all = True
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for chunk in chunks:
            try:
                async with session.post(url, data={"chat_id": TG_CHAT_ID, "text": chunk, "disable_web_page_preview": True}) as resp:
                    print("TG 推送状态：", resp.status)
                    if resp.status != 200:
                        ok_all = False
            except Exception as e:
                print("TG 推送失败：", e)
                ok_all = False
            await asyncio.sleep(1)
    return ok_all


def prune_reports(keep: int = 0) -> None:
    """清理旧版小时级 run 报告。

    当前版本不再生成 final_report_run_x.txt，避免 reports 目录无限变乱。
    只保留：
    - final_latest_report.txt：每轮覆盖的最新报告
    - reports/daily/YYYY-MM-DD/：每日归档
    - hl_monitor.db：长期历史数据库
    """
    ensure_dirs()
    try:
        for name in os.listdir(REPORT_DIR):
            if name.startswith("final_report_run_") and name.endswith(".txt"):
                os.remove(os.path.join(REPORT_DIR, name))
    except Exception:
        pass


def save_daily_archive(run_id: int, report: str) -> None:
    """每天保留一份长期复盘快照。

    逻辑：
    - reports/*_latest.* 继续每轮覆盖，用来看最新状态。
    - reports/daily/YYYY-MM-DD/ 每天一个目录，会在当天每次运行时更新；
      到第二天后就固定为前一天最后一次运行的快照。
    - 默认只保留最近 DAILY_ARCHIVE_KEEP_DAYS 天，防止仓库越来越大。
    """
    if not DAILY_ARCHIVE:
        return
    ensure_dirs()
    today = utc_today()
    daily_root = os.path.join(REPORT_DIR, "daily")
    day_dir = os.path.join(daily_root, today)
    os.makedirs(day_dir, exist_ok=True)

    # 1) 总报告每日归档
    with open(os.path.join(day_dir, "final_report.txt"), "w", encoding="utf-8") as f:
        f.write(report)

    # 2) 把长期单计划 / 观察列表 / 最新 CSV 同步一份到当天目录
    copy_files = [
        "long_term_plan.txt",
        "watchlist_long.txt",
        "watchlist_short.txt",
        "watchlist_observe.txt",
        "wallet_states_latest.csv",
        "perp_positions_latest.csv",
        "spot_balances_latest.csv",
        "coin_signals_latest.csv",
        "long_term_candidates.csv",
        "wallet_quality_latest.csv",
        "wallet_quality_report.txt",
        "leverage_quality_latest.csv",
        "wallet_leverage_profile_latest.csv",
        "coin_leverage_summary_latest.csv",
        "leverage_quality_report.txt",
        "wallet_position_trades_latest.csv",
        "wallet_position_performance_latest.csv",
        "wallet_position_report.txt",
        "active_changes_all_latest.csv",
        "fund_flow_lite_all_latest.csv",
    ]
    for name in copy_files:
        src = os.path.join(REPORT_DIR, name)
        if not os.path.exists(src):
            continue
        dst_name = name.replace("_latest", "")
        dst = os.path.join(day_dir, dst_name)
        try:
            with open(src, "rb") as rf, open(dst, "wb") as wf:
                wf.write(rf.read())
        except Exception as e:
            print(f"每日归档复制失败：{name} -> {e}", flush=True)

    # 3) 写一个索引，方便打开目录时先看这个
    index = (
        f"Hyperliquid Monitor Daily Archive\n"
        f"date_utc: {today}\n"
        f"run_id: {run_id}\n"
        f"updated_at_utc: {now_str()}\n\n"
        f"主要看：final_report.txt 和 long_term_plan.txt\n"
        f"CSV 用来复盘当天最后一次扫描的钱包、合约、现货和币种信号。\n"
    )
    with open(os.path.join(day_dir, "README.txt"), "w", encoding="utf-8") as f:
        f.write(index)

    prune_daily_archives(DAILY_ARCHIVE_KEEP_DAYS)
    print(f"每日归档已更新：{day_dir}", flush=True)


def prune_daily_archives(keep_days: int) -> None:
    if keep_days <= 0:
        return
    daily_root = os.path.join(REPORT_DIR, "daily")
    if not os.path.isdir(daily_root):
        return
    dirs = []
    for name in os.listdir(daily_root):
        path = os.path.join(daily_root, name)
        if os.path.isdir(path) and re.match(r"^\d{4}-\d{2}-\d{2}$", name):
            dirs.append((name, path))
    dirs.sort(reverse=True)
    for _, path in dirs[keep_days:]:
        try:
            for root, subdirs, files in os.walk(path, topdown=False):
                for fn in files:
                    os.remove(os.path.join(root, fn))
                for sd in subdirs:
                    os.rmdir(os.path.join(root, sd))
            os.rmdir(path)
            print(f"删除旧每日归档：{path}", flush=True)
        except Exception as e:
            print(f"删除旧每日归档失败：{path} -> {e}", flush=True)


async def run_once(args: argparse.Namespace) -> None:
    ensure_dirs()
    init_db()
    thresholds = load_thresholds()
    addresses = load_wallet_addresses()
    if len(addresses) < MIN_WALLET_COUNT:
        msg = (
            "⚠️ 地址数量异常，可能文件没上传完整。\n\n"
            f"读取到的钱包数：{len(addresses)}\n"
            f"最低要求：{MIN_WALLET_COUNT}\n\n"
            "请检查 money_printer_all_addresses.txt 和 smart_money_all_addresses.txt，"
            "确保一行一个 0x 钱包地址。"
        )
        print(msg, flush=True)
        await send_tg(msg)
        raise RuntimeError(msg)
    run_id = create_run(args.note)
    print(f"开始 run_id={run_id}", flush=True)

    wallet_rows, perp_rows, spot_rows, mid_prices, _token_price, spot_coin_price = await fetch_all(addresses, args.rpm, args.concurrency)
    save_snapshot(run_id, wallet_rows, perp_rows, spot_rows)
    update_position_trades(run_id, {**spot_coin_price, **mid_prices})
    export_leverage_quality_files(run_id)
    prev_id = get_previous_run_id(run_id)

    total = len(wallet_rows)
    ok = sum(1 for w in wallet_rows if w.get("status") == "ok")
    partial = sum(1 for w in wallet_rows if w.get("status") == "partial")
    ok_rate = (ok + partial * 0.5) / total if total else 0.0

    updated_actions, updated_signals = evaluate_events({**spot_coin_price, **mid_prices})
    quality_rows = refresh_wallet_quality(run_id, addresses) if WALLET_QUALITY_MODE else []
    quality_map = get_wallet_quality_map(run_id) if quality_rows else {}

    if prev_id is None:
        stats = run_wallet_stats(run_id)
        report = (
            f"🧠 Hyperliquid 钱包监控 FINAL\n"
            f"UTC时间：{now_str()}\n"
            f"run_id：{run_id}\n\n"
            f"【扫描健康】\n"
            f"监控钱包：{stats['total']} | 成功：{stats['ok']} | "
            f"partial：{stats['partial']} | failed：{stats['failed']} | "
            f"成功率：{stats['ok_rate']*100:.2f}%\n\n"
            f"钱包质量分类已导出：reports/wallet_quality_latest.csv / wallet_quality_report.txt\n"
            f"仓位生命周期追踪已导出：reports/wallet_position_trades_latest.csv / wallet_position_report.txt\n\n"
            f"第一次运行，已建立快照。第二次开始才有趋势对比。"
        )
        with open(os.path.join(REPORT_DIR, "final_latest_report.txt"), "w", encoding="utf-8") as f:
            f.write(report)
        with open(os.path.join(REPORT_DIR, "long_term_plan.txt"), "w", encoding="utf-8") as f:
            f.write("第一次运行，已建立快照。第二次开始生成低杠杆长期单观察计划。\n")
        export_latest_csv(run_id)
        save_daily_archive(run_id, report)
        daily_due = should_push_daily()
        pushed = False
        if PUSH_EVERY_RUN or daily_due:
            pushed = await send_tg(report)
            if pushed and daily_due:
                mark_pushed("daily")
        finish_run(run_id, wallet_rows, perp_rows, spot_rows, pushed)
        print(report, flush=True)
        return

    preliminary, actions, cashflows = compute_preliminary(run_id, prev_id, thresholds, quality_map)
    export_operation_detail_files(actions, cashflows)
    inserted_actions = 0
    if ok_rate >= MIN_OK_RATE:
        inserted_actions = save_wallet_actions(run_id, actions)
    else:
        print(f"成功率 {ok_rate*100:.2f}% 低于阈值 {MIN_OK_RATE*100:.2f}%，不记录本轮钱包动作。")

    candidate_coins = sorted(preliminary.keys(), key=lambda c: abs(preliminary[c].get("weighted_flow") or 0), reverse=True)[:25]
    ctx_map = await build_market_context(run_id, candidate_coins, {**spot_coin_price, **mid_prices})
    signals = build_signals(run_id, preliminary, ctx_map, thresholds) if ok_rate >= MIN_OK_RATE else []
    new_signal_events = create_signal_events(run_id, signals, {**spot_coin_price, **mid_prices}, thresholds) if ok_rate >= MIN_OK_RATE else 0

    write_watchlists(signals)
    if LONG_TERM_MODE:
        write_long_term_plan(build_long_term_candidates(run_id, signals, ctx_map))
    export_latest_csv(run_id)

    report = build_report(run_id, signals, ctx_map, actions, cashflows, ok_rate, new_signal_events, updated_actions, updated_signals)
    save_report(run_id, signals, report)
    prune_reports()
    save_daily_archive(run_id, report)

    strong = [s for s in signals if abs(s["final_score"]) >= s["threshold_score"]]
    daily_due = should_push_daily()
    should_push = PUSH_EVERY_RUN or bool(strong) or daily_due
    pushed = False
    if should_push:
        pushed = await send_tg(report)
        if pushed and daily_due:
            mark_pushed("daily")
    else:
        print("无强信号，也不是每日推送时间，不推送 TG。")

    finish_run(run_id, wallet_rows, perp_rows, spot_rows, pushed)
    print(f"新增钱包动作：{inserted_actions} | 强信号：{len(strong)}", flush=True)
    print(report, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hyperliquid Wallet Monitor FINAL")
    parser.add_argument("--rpm", type=int, default=DEFAULT_RPM)
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--note", type=str, default="manual")
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run_once(parse_args()))

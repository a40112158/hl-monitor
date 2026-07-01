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
- 信号生命周期追踪：强信号/长期单从出现到消失/反转的真实表现
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
DB_BACKEND = os.getenv("DB_BACKEND", "sqlite").strip().lower()
TURSO_DATABASE_URL = os.getenv("TURSO_DATABASE_URL", "").strip()
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "").strip()
USE_TURSO = (DB_BACKEND == "turso") or (bool(TURSO_DATABASE_URL) and bool(TURSO_AUTH_TOKEN))
DB_INSERT_CHUNK = int(os.getenv("DB_INSERT_CHUNK", "500"))
RUN_STEP_LOG = os.getenv("RUN_STEP_LOG", "1") == "1"
REPORT_DIR = os.getenv("REPORT_DIR", "reports")
# 精简报告结构：根目录只放每天要看的核心报告；全量 CSV 和辅助报告放 reports/details/
DETAILS_DIR = os.getenv("DETAILS_DIR", os.path.join(REPORT_DIR, "details"))
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
# 显示时间统一用北京时间（UTC+8）。数据库内部仍保存 UTC，便于窗口计算和回测不混乱。
DISPLAY_TZ_NAME = os.getenv("DISPLAY_TZ_NAME", "北京时间")
DISPLAY_TZ_OFFSET_HOURS = int(os.getenv("DISPLAY_TZ_OFFSET_HOURS", "8"))
DAILY_PUSH_HOUR_CN = int(os.getenv("DAILY_PUSH_HOUR_CN", str((DAILY_PUSH_HOUR_UTC + DISPLAY_TZ_OFFSET_HOURS) % 24)))
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

# 可解释性、回测、资金费率/流动性过滤、运行健康状态
SIGNAL_EXPLAIN_MODE = os.getenv("SIGNAL_EXPLAIN_MODE", "1") == "1"
BACKTEST_MODE = os.getenv("BACKTEST_MODE", "1") == "1"
RISK_FILTER_MODE = os.getenv("RISK_FILTER_MODE", "1") == "1"
SIGNAL_BACKTEST_WINDOW_DAYS = int(os.getenv("SIGNAL_BACKTEST_WINDOW_DAYS", "30"))
# 信号/长期单回测的门槛胜率。单位是方向收益百分比。
# 普通胜率 = 方向收益 > 0；门槛胜率 = 方向收益达到下面门槛，长期单更应该看门槛胜率。
BACKTEST_HURDLE_24H = float(os.getenv("BACKTEST_HURDLE_24H", "1"))
BACKTEST_HURDLE_72H = float(os.getenv("BACKTEST_HURDLE_72H", "2"))
BACKTEST_HURDLE_7D = float(os.getenv("BACKTEST_HURDLE_7D", "4"))
BACKTEST_HURDLE_15D = float(os.getenv("BACKTEST_HURDLE_15D", "6"))
BACKTEST_HURDLE_30D = float(os.getenv("BACKTEST_HURDLE_30D", "8"))

# 信号生命周期：按提示从出现到消失/反转结算，补充固定周期回测。
# 强信号默认连续 1 轮消失就结算；长期单默认连续 2 轮消失才失效，避免单轮抖动。
SIGNAL_LIFECYCLE_MODE = os.getenv("SIGNAL_LIFECYCLE_MODE", "1") == "1"
STRONG_SIGNAL_MISSING_ROUNDS = int(os.getenv("STRONG_SIGNAL_MISSING_ROUNDS", "1"))
LONGTERM_SIGNAL_MISSING_ROUNDS = int(os.getenv("LONGTERM_SIGNAL_MISSING_ROUNDS", "2"))

# funding 用百分比表达。例如 0.03 表示 0.03%，超过后长期单会降权。
FUNDING_WARN_ABS_PCT = float(os.getenv("FUNDING_WARN_ABS_PCT", "0.03"))
FUNDING_DANGER_ABS_PCT = float(os.getenv("FUNDING_DANGER_ABS_PCT", "0.08"))
# 24h成交额低于这些阈值时，长期单降权。
LIQUIDITY_LOW_DAY_VOLUME = float(os.getenv("LIQUIDITY_LOW_DAY_VOLUME", "20000000"))
LIQUIDITY_MIN_DAY_VOLUME = float(os.getenv("LIQUIDITY_MIN_DAY_VOLUME", "5000000"))
HEALTH_STALE_HOURS = float(os.getenv("HEALTH_STALE_HOURS", "2"))

# 数据异常保护：API 成功率低时不更新信号生命周期，避免把 API 抽风误判成信号消失/平仓。
DATA_ANOMALY_PROTECT_MODE = os.getenv("DATA_ANOMALY_PROTECT_MODE", "1") == "1"

# 主导钱包名单：每个强信号/长期单标出主要推动的钱包，方便判断信号质量。
DOMINANT_WALLETS_MODE = os.getenv("DOMINANT_WALLETS_MODE", "1") == "1"
DOMINANT_WALLET_TOP_N = int(os.getenv("DOMINANT_WALLET_TOP_N", "5"))

# 滚动建仓信号：不再只看“当前快照 vs 上一快照”。
# 短线异动仍看上一轮；中长期建仓看 2h/6h/24h/72h/15d/30d 累计净流入。
ROLLING_FLOW_MODE = os.getenv("ROLLING_FLOW_MODE", "1") == "1"
# 滚动杠杆质量：长期单不只看资金流，还看这些资金流对应的杠杆结构是否健康。
ROLLING_LEVERAGE_MODE = os.getenv("ROLLING_LEVERAGE_MODE", "1") == "1"
EXPECTED_INTERVAL_MINUTES = float(os.getenv("EXPECTED_INTERVAL_MINUTES", "30"))
MAX_SHORT_SIGNAL_GAP_MINUTES = float(os.getenv("MAX_SHORT_SIGNAL_GAP_MINUTES", "90"))
ROLLING_FLOW_WINDOWS_HOURS = [
    float(x.strip()) for x in os.getenv("ROLLING_FLOW_WINDOWS_HOURS", "2,6,24,72,360,720").split(",")
    if x.strip()
]

# 滚动建仓质量过滤：避免同一笔大额变动被 2h/6h/24h/72h/15d/30d 重复加分。
# 核心原则：嵌套窗口分组取最高，长期窗口必须看持续性、钱包广度、杠杆确认。
ROLLING_DEDUP_SCORE_MODE = os.getenv("ROLLING_DEDUP_SCORE_MODE", "1") == "1"
ROLLING_MIN_WALLETS_MID = int(os.getenv("ROLLING_MIN_WALLETS_MID", "2"))
ROLLING_MIN_WALLETS_LONG = int(os.getenv("ROLLING_MIN_WALLETS_LONG", "3"))
ROLLING_TOP1_MAX_SHARE = float(os.getenv("ROLLING_TOP1_MAX_SHARE", "0.70"))
ROLLING_TOP3_MAX_SHARE = float(os.getenv("ROLLING_TOP3_MAX_SHARE", "0.90"))
ROLLING_SPOT_ONLY_SHARE = float(os.getenv("ROLLING_SPOT_ONLY_SHARE", "0.80"))
ROLLING_SPOT_ONLY_MULT = float(os.getenv("ROLLING_SPOT_ONLY_MULT", "0.40"))
ROLLING_CONCENTRATION_MULT = float(os.getenv("ROLLING_CONCENTRATION_MULT", "0.45"))
ROLLING_PERSISTENCE_MULT = float(os.getenv("ROLLING_PERSISTENCE_MULT", "0.45"))
ROLLING_SUSPECT_CAP_SCORE = float(os.getenv("ROLLING_SUSPECT_CAP_SCORE", "6.5"))

# 滚动窗口防“时间越久分越高”：
# 1) 15d/30d 等长窗口必须真的有足够历史覆盖，不能刚跑几天就拿 30d 分。
# 2) 嵌套窗口不再把短/中/长三组满额相加，默认只取一个主窗口，其他窗口只做小幅确认。
ROLLING_REQUIRE_WINDOW_MATURITY = os.getenv("ROLLING_REQUIRE_WINDOW_MATURITY", "1") == "1"
ROLLING_SCORE_USE_BEST_HORIZON = os.getenv("ROLLING_SCORE_USE_BEST_HORIZON", "1") == "1"
ROLLING_MIN_COVERAGE_SHORT = float(os.getenv("ROLLING_MIN_COVERAGE_SHORT", "0.25"))
ROLLING_MIN_COVERAGE_MID = float(os.getenv("ROLLING_MIN_COVERAGE_MID", "0.50"))
ROLLING_MIN_COVERAGE_LONG = float(os.getenv("ROLLING_MIN_COVERAGE_LONG", "0.70"))
ROLLING_CONTINUITY_BONUS_MAX = float(os.getenv("ROLLING_CONTINUITY_BONUS_MAX", "0.80"))

# 双分数模型：把“短线报警”和“低杠杆长期资格”彻底分开。
# alert_score：本轮/短线异动雷达，负责提醒你看盘。
# long_score：滚动建仓 + 持续性 + 钱包广度 + 低杠杆质量，负责长期单候选。
DUAL_SCORE_MODE = os.getenv("DUAL_SCORE_MODE", "1") == "1"
LONG_SCORE_REQUIRE_PERP_CONFIRM = os.getenv("LONG_SCORE_REQUIRE_PERP_CONFIRM", "1") == "1"
LONG_SCORE_MIN_LEVERAGE_CONFIRM = float(os.getenv("LONG_SCORE_MIN_LEVERAGE_CONFIRM", "0.20"))
LONG_SCORE_SPOT_ONLY_CAP = float(os.getenv("LONG_SCORE_SPOT_ONLY_CAP", "4.0"))
LONG_SCORE_CONCENTRATION_CAP = float(os.getenv("LONG_SCORE_CONCENTRATION_CAP", "4.5"))
LONG_SCORE_PERSISTENCE_CAP = float(os.getenv("LONG_SCORE_PERSISTENCE_CAP", "5.0"))
ALERT_SCORE_ROLLING_CONFIRM_CAP = float(os.getenv("ALERT_SCORE_ROLLING_CONFIRM_CAP", "1.5"))
# 多空分离 + 长期状态机：不要用一个 long_score 同时代表长期多/空。
# alert_score 仍然只做短线雷达；长期候选拆成 long_candidate_score / short_candidate_score。
LONG_SHORT_STATE_MODE = os.getenv("LONG_SHORT_STATE_MODE", "1") == "1"
LONG_SHORT_MIN_STREAK_FORMING = int(os.getenv("LONG_SHORT_MIN_STREAK_FORMING", "2"))
LONG_SHORT_MIN_STREAK_CANDIDATE = int(os.getenv("LONG_SHORT_MIN_STREAK_CANDIDATE", "3"))
LONG_SHORT_BLOCK_TOP1_SHARE = float(os.getenv("LONG_SHORT_BLOCK_TOP1_SHARE", "0.70"))
LONG_SHORT_BLOCK_HIGH_LEV_RATIO = float(os.getenv("LONG_SHORT_BLOCK_HIGH_LEV_RATIO", "0.50"))
LONG_SHORT_BLOCK_AVG_LEVERAGE = float(os.getenv("LONG_SHORT_BLOCK_AVG_LEVERAGE", "10"))
LONG_SHORT_BLOCK_LIQ_DISTANCE = float(os.getenv("LONG_SHORT_BLOCK_LIQ_DISTANCE", "10"))
LONG_SHORT_PRICE_24H_EXTREME = float(os.getenv("LONG_SHORT_PRICE_24H_EXTREME", "18"))
LONG_SHORT_REQUIRE_CANDIDATE_STATE = os.getenv("LONG_SHORT_REQUIRE_CANDIDATE_STATE", "0") == "1"



# 数据库体积控制：GitHub 单文件硬限制 100MB，必须定期裁剪原始快照并 VACUUM。
# 只裁剪最占空间的逐轮原始表；钱包动作、信号、仓位生命周期保留足够窗口用于 30d 回测。
DB_PRUNE_MODE = os.getenv("DB_PRUNE_MODE", "1") == "1"
DB_RAW_KEEP_RUNS = int(os.getenv("DB_RAW_KEEP_RUNS", "24"))
DB_HISTORY_KEEP_DAYS = int(os.getenv("DB_HISTORY_KEEP_DAYS", "35"))
DB_MAX_MB = float(os.getenv("DB_MAX_MB", "85"))

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


def display_now() -> dt.datetime:
    return utc_now() + dt.timedelta(hours=DISPLAY_TZ_OFFSET_HOURS)


def display_now_str() -> str:
    return display_now().strftime("%Y-%m-%d %H:%M:%S")


def display_today() -> str:
    return display_now().strftime("%Y-%m-%d")


def display_time_from_utc(value: Optional[Any]) -> str:
    """把数据库里的 UTC 时间转换成北京时间显示；输入为空时返回当前北京时间。"""
    if value is None:
        return display_now_str()
    if isinstance(value, dt.datetime):
        base = value
    else:
        base = parse_time(str(value))
    if base is None:
        return str(value)
    return (base + dt.timedelta(hours=DISPLAY_TZ_OFFSET_HOURS)).strftime("%Y-%m-%d %H:%M:%S")


def signal_time_cn(run_id: Optional[int] = None) -> str:
    """本轮信号显示时间：优先使用 runs.started_at，避免报告生成时间和扫描开始时间混淆。"""
    try:
        if run_id is not None:
            started = get_run_started_at(run_id)
            if started is not None:
                return display_time_from_utc(started)
    except Exception:
        pass
    return display_now_str()


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

def safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(float(x))
    except Exception:
        return default


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
    os.makedirs(DETAILS_DIR, exist_ok=True)


def step_log(msg: str) -> None:
    if RUN_STEP_LOG:
        print(f"[STEP] {now_str()} | {msg}", flush=True)


def db_conn():
    """Return a DB-API connection.

    Default: local SQLite file.
    Turso mode: set DB_BACKEND=turso plus TURSO_DATABASE_URL and TURSO_AUTH_TOKEN.
    The rest of the script keeps using SQLite-style SQL/placeholders.
    """
    if USE_TURSO:
        try:
            import libsql  # pip install libsql
        except Exception as e:
            raise RuntimeError(
                "DB_BACKEND=turso but Python package 'libsql' is not installed. "
                "Add libsql to requirements.txt and rerun GitHub Actions."
            ) from e
        if not TURSO_DATABASE_URL or not TURSO_AUTH_TOKEN:
            raise RuntimeError("Turso mode requires TURSO_DATABASE_URL and TURSO_AUTH_TOKEN secrets.")
        conn = libsql.connect(TURSO_DATABASE_URL, auth_token=TURSO_AUTH_TOKEN)
        # libsql Python SDK is SQLite-compatible. Some versions support row_factory; set it when available.
        try:
            conn.row_factory = sqlite3.Row
        except Exception:
            pass
        return conn

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
        created_at TEXT,
        created_at_cn TEXT,
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
    CREATE TABLE IF NOT EXISTS coin_flow_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER,
        created_at TEXT,
        coin TEXT,
        perp_active REAL,
        spot_active REAL,
        weighted_flow REAL,
        active_total REAL,
        source_gap_minutes REAL,
        is_gap INTEGER DEFAULT 0,
        flow_direction TEXT,
        avg_leverage REAL,
        avg_liq_distance REAL,
        longterm_leverage_ratio REAL,
        highrisk_leverage_ratio REAL,
        leverage_health_score REAL,
        leverage_note TEXT,
        UNIQUE(run_id, coin)
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
    CREATE TABLE IF NOT EXISTS coin_risk_metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER,
        coin TEXT,
        funding_rate REAL,
        funding_rate_pct REAL,
        day_volume_usd REAL,
        open_interest_usd REAL,
        liquidity_risk TEXT,
        funding_risk TEXT,
        funding_note TEXT,
        liquidity_note TEXT,
        created_at TEXT,
        UNIQUE(run_id, coin)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS longterm_events (
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
    CREATE TABLE IF NOT EXISTS signal_lifecycles (
        lifecycle_id INTEGER PRIMARY KEY AUTOINCREMENT,
        lifecycle_type TEXT,
        coin TEXT,
        direction TEXT,
        status TEXT,
        entry_run_id INTEGER,
        entry_time TEXT,
        entry_px REAL,
        entry_score REAL,
        max_score REAL,
        last_seen_run_id INTEGER,
        last_seen_at TEXT,
        last_score REAL,
        missing_count INTEGER DEFAULT 0,
        exit_run_id INTEGER,
        exit_time TEXT,
        exit_px REAL,
        exit_reason TEXT,
        lifecycle_return_pct REAL,
        holding_hours REAL,
        note TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS signal_lifecycle_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lifecycle_id INTEGER,
        run_id INTEGER,
        created_at TEXT,
        lifecycle_type TEXT,
        event_type TEXT,
        coin TEXT,
        direction TEXT,
        px REAL,
        score REAL,
        missing_count INTEGER,
        return_pct REAL,
        reason TEXT
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

    for t in ("wallet_actions", "signal_events", "longterm_events"):
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
    add_col_if_missing("coin_signals", "created_at", "TEXT")
    add_col_if_missing("coin_signals", "created_at_cn", "TEXT")
    for col in ("avg_leverage", "avg_liq_distance", "longterm_leverage_ratio", "highrisk_leverage_ratio"):
        add_col_if_missing("coin_signals", col, "REAL")
    add_col_if_missing("coin_signals", "leverage_note", "TEXT")
    for col in ("alert_score", "long_score", "long_candidate_score", "short_candidate_score"):
        add_col_if_missing("coin_signals", col, "REAL")
    for col in ("signal_category", "candidate_state", "candidate_gate", "candidate_block_reasons", "candidate_side"):
        add_col_if_missing("coin_signals", col, "TEXT")


    # 滚动资金流快照补充杠杆质量字段，兼容旧 db。
    add_col_if_missing("coin_flow_snapshots", "flow_direction", "TEXT")
    for col in ("avg_leverage", "avg_liq_distance", "longterm_leverage_ratio", "highrisk_leverage_ratio", "leverage_health_score"):
        add_col_if_missing("coin_flow_snapshots", col, "REAL")
    add_col_if_missing("coin_flow_snapshots", "leverage_note", "TEXT")

    # wallet_actions 也保留动作级杠杆字段，便于以后复盘。
    for col in ("side", "margin_mode", "leverage_style"):
        add_col_if_missing("wallet_actions", col, "TEXT")
    for col in ("position_value", "leverage", "liq_distance_pct", "leverage_weight"):
        add_col_if_missing("wallet_actions", col, "REAL")

    cur.execute("CREATE INDEX IF NOT EXISTS idx_perp_run_addr_coin ON perp_positions(run_id, address, coin)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_spot_run_addr_coin ON spot_balances(run_id, address, coin)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_actions_addr ON wallet_actions(address)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_actions_created ON wallet_actions(created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_quality_run ON wallet_quality(run_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_quality_addr ON wallet_quality(address)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_signal_coin ON signal_events(coin)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_coin_signal_run ON coin_signals(run_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_coin_flow_run ON coin_flow_snapshots(run_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_coin_flow_created_coin ON coin_flow_snapshots(created_at, coin)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_risk_run_coin ON coin_risk_metrics(run_id, coin)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_longterm_coin ON longterm_events(coin)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_signal_lifecycles_status ON signal_lifecycles(lifecycle_type, status, coin, direction)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_signal_lifecycles_exit_run ON signal_lifecycles(exit_run_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_signal_lifecycle_events_run ON signal_lifecycle_events(run_id)")

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
    allowed = {"wallet_states", "perp_positions", "spot_balances", "wallet_actions", "coin_signals", "coin_flow_snapshots", "market_context", "wallet_quality", "position_trades", "position_trade_events", "wallet_position_performance", "coin_risk_metrics"}
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


def _chunks(seq: List[Any], size: int):
    size = max(1, int(size or 500))
    for i in range(0, len(seq), size):
        yield i, seq[i:i + size]


def save_snapshot(run_id: int, wallet_rows: List[Dict[str, Any]], perp_rows: List[Dict[str, Any]], spot_rows: List[Dict[str, Any]]) -> None:
    """保存本轮快照。

    Turso 是远程数据库，单次大事务在 GitHub Actions 里可能长时间无输出，
    也可能在 commit 前 SQL Console 看不到变化。这里改成分表/分块提交，
    并打印进度，方便确认到底卡在哪一步。
    """
    step_log(
        f"开始写入快照 run_id={run_id} | backend={'turso' if USE_TURSO else 'sqlite'} | "
        f"wallet={len(wallet_rows)} perp={len(perp_rows)} spot={len(spot_rows)} | chunk={DB_INSERT_CHUNK}"
    )
    t0 = time.time()
    conn = db_conn()
    cur = conn.cursor()
    try:
        wallet_payload = [(
            run_id, w.get("address"), w.get("groups"), w.get("status"), w.get("error"),
            w.get("perp_account_value"), w.get("perp_total_ntl_pos"), w.get("perp_withdrawable"), w.get("perp_account_leverage"), w.get("perp_position_count"),
            w.get("spot_total_value"), w.get("spot_usdc_value"), w.get("spot_token_count")
        ) for w in wallet_rows]
        cur.executemany("""
        INSERT INTO wallet_states (
            run_id, address, groups, status, error,
            perp_account_value, perp_total_ntl_pos, perp_withdrawable, perp_account_leverage, perp_position_count,
            spot_total_value, spot_usdc_value, spot_token_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, wallet_payload)
        conn.commit()
        step_log(f"wallet_states 写入完成：{len(wallet_payload)} 行 | elapsed={time.time()-t0:.1f}s")

        perp_payload = [(
            run_id, p.get("address"), p.get("groups"), p.get("coin"), p.get("side"), p.get("szi"), p.get("abs_szi"), p.get("mark_px"), p.get("position_value"),
            p.get("entry_px"), p.get("unrealized_pnl"), p.get("roe"), p.get("leverage"), p.get("liquidation_px"),
            p.get("margin_mode"), p.get("margin_used"), p.get("liq_distance_pct"), p.get("account_leverage"), p.get("leverage_style"), p.get("leverage_weight"), p.get("leverage_risk_score"), p.get("leverage_note")
        ) for p in perp_rows]
        inserted = 0
        for _, chunk in _chunks(perp_payload, DB_INSERT_CHUNK):
            cur.executemany("""
            INSERT INTO perp_positions (
                run_id, address, groups, coin, side, szi, abs_szi, mark_px, position_value,
                entry_px, unrealized_pnl, roe, leverage, liquidation_px,
                margin_mode, margin_used, liq_distance_pct, account_leverage, leverage_style, leverage_weight, leverage_risk_score, leverage_note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, chunk)
            conn.commit()
            inserted += len(chunk)
            step_log(f"perp_positions 写入进度：{inserted}/{len(perp_payload)} 行 | elapsed={time.time()-t0:.1f}s")

        spot_payload = [(
            run_id, srow.get("address"), srow.get("groups"), srow.get("coin"), srow.get("token"), srow.get("total"), srow.get("hold"), srow.get("free"), srow.get("entry_ntl"), srow.get("mark_px"), srow.get("current_value")
        ) for srow in spot_rows]
        inserted = 0
        for _, chunk in _chunks(spot_payload, DB_INSERT_CHUNK):
            cur.executemany("""
            INSERT INTO spot_balances (
                run_id, address, groups, coin, token, total, hold, free, entry_ntl, mark_px, current_value
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, chunk)
            conn.commit()
            inserted += len(chunk)
            step_log(f"spot_balances 写入进度：{inserted}/{len(spot_payload)} 行 | elapsed={time.time()-t0:.1f}s")

        step_log(f"本轮快照写入完成 run_id={run_id} | total_elapsed={time.time()-t0:.1f}s")
    finally:
        try:
            conn.close()
        except Exception:
            pass

def export_latest_csv(run_id: int) -> None:
    ensure_dirs()
    for table, filename in [
        ("wallet_states", "wallet_states_latest.csv"),
        ("perp_positions", "perp_positions_latest.csv"),
        ("spot_balances", "spot_balances_latest.csv"),
        ("coin_signals", "coin_signals_latest.csv"),
        ("wallet_quality", "wallet_quality_latest.csv"),
        ("wallet_position_performance", "wallet_position_performance_latest.csv"),
        ("coin_risk_metrics", "coin_risk_latest.csv"),
    ]:
        rows = load_rows(table, run_id)
        if not rows:
            continue
        path = os.path.join(DETAILS_DIR, filename)
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
        path = os.path.join(DETAILS_DIR, filename)
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

def rolling_window_label(hours: float) -> str:
    if abs(hours - 720) < 0.01:
        return "30d"
    if abs(hours - 360) < 0.01:
        return "15d"
    if abs(hours - 168) < 0.01:
        return "7d"
    if abs(hours - 72) < 0.01:
        return "72h"
    if abs(hours - 24) < 0.01:
        return "24h"
    if hours >= 24 and abs(hours % 24) < 0.01:
        return f"{int(hours/24)}d"
    if abs(hours - int(hours)) < 0.01:
        return f"{int(hours)}h"
    return f"{hours:g}h"


def get_run_started_at(run_id: Optional[int]) -> Optional[dt.datetime]:
    if not run_id:
        return None
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT started_at FROM runs WHERE run_id=?", (run_id,))
    row = cur.fetchone()
    conn.close()
    return parse_time(row[0]) if row else None


def get_run_gap_minutes(run_id: int, prev_run_id: Optional[int]) -> Optional[float]:
    if not prev_run_id:
        return None
    cur_ts = get_run_started_at(run_id)
    pre_ts = get_run_started_at(prev_run_id)
    if not cur_ts or not pre_ts:
        return None
    return max(0.0, (cur_ts - pre_ts).total_seconds() / 60.0)


def rolling_leverage_health_for_flow(direction: str, lev: Dict[str, Any]) -> Dict[str, Any]:
    """给某个币某个方向的滚动资金流打杠杆健康分。

    direction: bullish 用当前多头仓位的杠杆结构；bearish 用当前空头仓位的杠杆结构。
    这个分数只作为长期建仓的“质量过滤”，不是单独开信号。
    """
    if not ROLLING_LEVERAGE_MODE or not LEVERAGE_QUALITY_MODE or not lev:
        return {
            "avg_leverage": None,
            "avg_liq_distance": None,
            "longterm_leverage_ratio": 0.0,
            "highrisk_leverage_ratio": 0.0,
            "leverage_health_score": 0.0,
            "leverage_note": "无滚动杠杆样本",
        }
    side = "long" if direction == "bullish" else "short" if direction == "bearish" else ""
    if not side:
        return {
            "avg_leverage": None,
            "avg_liq_distance": None,
            "longterm_leverage_ratio": 0.0,
            "highrisk_leverage_ratio": 0.0,
            "leverage_health_score": 0.0,
            "leverage_note": "无方向",
        }
    val = safe_float(lev.get(f"{side}_value")) or 0.0
    avg_lev = safe_float(lev.get(f"{side}_avg_leverage"))
    avg_liq = safe_float(lev.get(f"{side}_avg_liq_distance"))
    long_ratio = safe_float(lev.get(f"{side}_longterm_ratio")) or 0.0
    high_ratio = safe_float(lev.get(f"{side}_highrisk_ratio")) or 0.0
    if val <= 0:
        return {
            "avg_leverage": avg_lev,
            "avg_liq_distance": avg_liq,
            "longterm_leverage_ratio": long_ratio,
            "highrisk_leverage_ratio": high_ratio,
            "leverage_health_score": -0.15,
            "leverage_note": "当前无同方向合约仓位杠杆样本",
        }

    score = 0.0
    notes: List[str] = []
    if long_ratio >= 0.60 and (avg_lev is None or avg_lev <= LEVERAGE_MID_MAX) and (avg_liq is None or avg_liq >= 20):
        score += 1.0
        notes.append(f"低/中杠杆占比{long_ratio*100:.0f}%")
    elif long_ratio >= 0.40 and (avg_lev is None or avg_lev <= LEVERAGE_MID_MAX):
        score += 0.45
        notes.append(f"低/中杠杆占比{long_ratio*100:.0f}%")

    if high_ratio >= 0.60:
        score -= 1.25
        notes.append(f"高杠杆/爆仓边缘占比{high_ratio*100:.0f}%")
    elif high_ratio >= 0.35:
        score -= 0.65
        notes.append(f"高杠杆占比{high_ratio*100:.0f}%")

    if avg_lev is not None:
        if avg_lev <= LEVERAGE_LOW_MAX:
            score += 0.45
            notes.append(f"均杠杆{avg_lev:.1f}x")
        elif avg_lev >= 20:
            score -= 1.05
            notes.append(f"均杠杆{avg_lev:.1f}x 极高")
        elif avg_lev >= LEVERAGE_HIGH_MIN:
            score -= 0.55
            notes.append(f"均杠杆{avg_lev:.1f}x 偏高")

    if avg_liq is not None:
        if avg_liq < LIQ_DANGER_DISTANCE_PCT:
            score -= 0.95
            notes.append(f"均强平距离{avg_liq:.1f}% 过近")
        elif avg_liq >= LIQ_SAFE_DISTANCE_PCT:
            score += 0.35
            notes.append(f"均强平距离{avg_liq:.1f}% 安全")

    score = max(-2.2, min(1.8, score))
    return {
        "avg_leverage": avg_lev,
        "avg_liq_distance": avg_liq,
        "longterm_leverage_ratio": long_ratio,
        "highrisk_leverage_ratio": high_ratio,
        "leverage_health_score": score,
        "leverage_note": "，".join(notes) if notes else "杠杆结构中性",
    }


def save_coin_flow_snapshots(run_id: int, prev_run_id: Optional[int], preliminary: Dict[str, Dict[str, Any]]) -> None:
    """保存每轮币种级净流入快照，用于 2h/6h/24h/72h/15d/30d 滚动建仓判断。"""
    if not ROLLING_FLOW_MODE:
        return
    created_at = (get_run_started_at(run_id) or utc_now()).strftime("%Y-%m-%d %H:%M:%S")
    gap_min = get_run_gap_minutes(run_id, prev_run_id)
    is_gap = 1 if (gap_min is not None and gap_min > MAX_SHORT_SIGNAL_GAP_MINUTES) else 0
    rows = []
    lev_map = build_leverage_signal_map(run_id) if (ROLLING_LEVERAGE_MODE and LEVERAGE_QUALITY_MODE) else {}
    for coin, d in preliminary.items():
        perp_active = float(d.get("perp_active") or 0.0)
        spot_active = float(d.get("spot_active") or 0.0)
        weighted_flow = float(d.get("weighted_flow") or 0.0)
        active_total = perp_active + spot_active
        # 太小的噪音不入滚动表，避免数据库膨胀；阈值很低，只过滤浮点灰尘。
        if abs(perp_active) < 1 and abs(spot_active) < 1 and abs(weighted_flow) < 1:
            continue
        flow_ref = weighted_flow if abs(weighted_flow) >= 1 else active_total
        flow_direction = "bullish" if flow_ref > 0 else "bearish" if flow_ref < 0 else "neutral"
        lev_health = rolling_leverage_health_for_flow(flow_direction, lev_map.get(coin, {}))
        rows.append((
            run_id, created_at, coin, perp_active, spot_active, weighted_flow, active_total, gap_min, is_gap,
            flow_direction, lev_health.get("avg_leverage"), lev_health.get("avg_liq_distance"),
            lev_health.get("longterm_leverage_ratio"), lev_health.get("highrisk_leverage_ratio"),
            lev_health.get("leverage_health_score"), lev_health.get("leverage_note"),
        ))
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM coin_flow_snapshots WHERE run_id=?", (run_id,))
    cur.executemany("""
        INSERT OR REPLACE INTO coin_flow_snapshots
        (run_id, created_at, coin, perp_active, spot_active, weighted_flow, active_total, source_gap_minutes, is_gap,
         flow_direction, avg_leverage, avg_liq_distance, longterm_leverage_ratio, highrisk_leverage_ratio,
         leverage_health_score, leverage_note)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    conn.close()
    step_log(f"滚动资金流快照完成 | rows={len(rows)} gap_min={gap_min if gap_min is not None else 'N/A'} is_gap={is_gap}")


def build_rolling_flow_metrics(run_id: int) -> Dict[str, Dict[str, Any]]:
    """按多个窗口汇总币种级净流入。输出给信号评分和 rolling_flow_report。

    重点修正：滚动窗口是嵌套窗口，评分时不能把同一笔流入在 2h/6h/24h/72h/15d/30d 里重复加满分。
    这里先把每个窗口的金额、方向持续性、钱包广度、集中度、杠杆健康度全部算出来；后续评分再做去重和过滤。
    """
    if not ROLLING_FLOW_MODE:
        return {}
    now_dt = get_run_started_at(run_id) or utc_now()
    result: Dict[str, Dict[str, Any]] = defaultdict(dict)
    conn = db_conn()
    cur = conn.cursor()
    for hours in ROLLING_FLOW_WINDOWS_HOURS:
        label = rolling_window_label(hours)
        cutoff = (now_dt - dt.timedelta(hours=float(hours))).strftime("%Y-%m-%d %H:%M:%S")

        # 1) 币种级窗口累计。注意：这只是原始窗口值，最终评分不会把所有嵌套窗口无脑相加。
        cur.execute("""
            SELECT coin,
                   SUM(perp_active) AS perp_active,
                   SUM(spot_active) AS spot_active,
                   SUM(weighted_flow) AS weighted_flow,
                   SUM(active_total) AS active_total,
                   COUNT(DISTINCT run_id) AS run_count,
                   SUM(CASE WHEN is_gap=1 THEN 1 ELSE 0 END) AS gap_count,
                   SUM(CASE WHEN weighted_flow > 0 THEN weighted_flow ELSE 0 END) AS bullish_weighted,
                   SUM(CASE WHEN weighted_flow < 0 THEN weighted_flow ELSE 0 END) AS bearish_weighted,
                   SUM(CASE WHEN weighted_flow > 0 THEN 1 ELSE 0 END) AS bullish_runs,
                   SUM(CASE WHEN weighted_flow < 0 THEN 1 ELSE 0 END) AS bearish_runs,
                   COUNT(DISTINCT CASE WHEN weighted_flow > 0 THEN substr(created_at,1,10) END) AS bullish_days,
                   COUNT(DISTINCT CASE WHEN weighted_flow < 0 THEN substr(created_at,1,10) END) AS bearish_days,
                   MIN(created_at) AS first_seen,
                   MAX(created_at) AS last_seen
            FROM coin_flow_snapshots
            WHERE run_id <= ? AND created_at >= ?
            GROUP BY coin
        """, (run_id, cutoff))
        for row in cur.fetchall():
            coin = row[0]
            d = result[coin]
            d["coin"] = coin
            d[f"perp_{label}"] = float(row[1] or 0.0)
            d[f"spot_{label}"] = float(row[2] or 0.0)
            d[f"weighted_{label}"] = float(row[3] or 0.0)
            d[f"active_{label}"] = float(row[4] or 0.0)
            d[f"runs_{label}"] = int(row[5] or 0)
            d[f"gaps_{label}"] = int(row[6] or 0)
            d[f"bullish_weighted_{label}"] = float(row[7] or 0.0)
            d[f"bearish_weighted_{label}"] = float(row[8] or 0.0)
            d[f"bullish_runs_{label}"] = int(row[9] or 0)
            d[f"bearish_runs_{label}"] = int(row[10] or 0)
            d[f"bullish_days_{label}"] = int(row[11] or 0)
            d[f"bearish_days_{label}"] = int(row[12] or 0)
            first_ts = parse_time(row[13]) if len(row) > 13 else None
            last_ts = parse_time(row[14]) if len(row) > 14 else None
            span_hours = max(0.0, (last_ts - first_ts).total_seconds() / 3600.0) if first_ts and last_ts else 0.0
            d[f"span_hours_{label}"] = span_hours
            d[f"coverage_{label}"] = min(1.0, span_hours / max(1.0, float(hours)))

            # 用金额本身估算现货占比。这里看绝对值，避免现货卖出/合约买入互相抵消后误判。
            perp_abs = abs(float(row[1] or 0.0))
            spot_abs = abs(float(row[2] or 0.0))
            d[f"spot_share_{label}"] = spot_abs / (perp_abs + spot_abs) if (perp_abs + spot_abs) > 0 else 0.0

        # 2) 同一窗口内，按方向聚合滚动杠杆质量。
        # 这样 72h/15d/30d 的建仓，不仅看累计流入，还能看是不是低杠杆健康建仓。
        if ROLLING_LEVERAGE_MODE and LEVERAGE_QUALITY_MODE:
            cur.execute("""
                SELECT coin, flow_direction,
                       SUM(ABS(weighted_flow)) AS den,
                       SUM(CASE WHEN avg_leverage IS NOT NULL THEN avg_leverage * ABS(weighted_flow) ELSE 0 END) AS lev_num,
                       SUM(CASE WHEN avg_liq_distance IS NOT NULL THEN avg_liq_distance * ABS(weighted_flow) ELSE 0 END) AS liq_num,
                       SUM(longterm_leverage_ratio * ABS(weighted_flow)) AS long_num,
                       SUM(highrisk_leverage_ratio * ABS(weighted_flow)) AS high_num,
                       SUM(leverage_health_score * ABS(weighted_flow)) AS health_num
                FROM coin_flow_snapshots
                WHERE run_id <= ? AND created_at >= ? AND flow_direction IN ('bullish', 'bearish')
                GROUP BY coin, flow_direction
            """, (run_id, cutoff))
            for row in cur.fetchall():
                coin = row[0]
                direction = row[1] or "neutral"
                den = float(row[2] or 0.0)
                if den <= 0:
                    continue
                d = result[coin]
                d["coin"] = coin
                prefix = "bullish" if direction == "bullish" else "bearish"
                d[f"{prefix}_lev_den_{label}"] = den
                d[f"{prefix}_avg_leverage_{label}"] = float(row[3] or 0.0) / den
                d[f"{prefix}_avg_liq_distance_{label}"] = float(row[4] or 0.0) / den
                d[f"{prefix}_longterm_leverage_ratio_{label}"] = float(row[5] or 0.0) / den
                d[f"{prefix}_highrisk_leverage_ratio_{label}"] = float(row[6] or 0.0) / den
                d[f"{prefix}_leverage_health_{label}"] = float(row[7] or 0.0) / den

        # 3) 钱包广度/集中度：长期建仓不能只靠一个地址一笔大额转移。
        # wallet_actions 只保留达到阈值的动作，所以这里是“主要推动钱包”的广度，不是所有钱包数。
        cur.execute("""
            SELECT coin, direction, address,
                   SUM(ABS(active_delta)) AS abs_flow,
                   SUM(CASE WHEN market='perp' THEN ABS(active_delta) ELSE 0 END) AS perp_abs,
                   SUM(CASE WHEN market='spot' THEN ABS(active_delta) ELSE 0 END) AS spot_abs,
                   COUNT(*) AS action_count
            FROM wallet_actions
            WHERE run_id <= ? AND created_at >= ? AND direction IN ('bullish', 'bearish')
            GROUP BY coin, direction, address
        """, (run_id, cutoff))
        grouped: Dict[Tuple[str, str], List[Tuple[str, float, float, float, int]]] = defaultdict(list)
        for row in cur.fetchall():
            coin = row[0]
            direction = row[1] or "neutral"
            addr = row[2] or ""
            abs_flow = float(row[3] or 0.0)
            perp_abs = float(row[4] or 0.0)
            spot_abs = float(row[5] or 0.0)
            action_count = int(row[6] or 0)
            if coin and direction in ("bullish", "bearish") and abs_flow > 0:
                grouped[(coin, direction)].append((addr, abs_flow, perp_abs, spot_abs, action_count))
        for (coin, direction), items in grouped.items():
            prefix = "bullish" if direction == "bullish" else "bearish"
            d = result[coin]
            d["coin"] = coin
            total_abs = sum(x[1] for x in items)
            perp_abs = sum(x[2] for x in items)
            spot_abs = sum(x[3] for x in items)
            action_count = sum(x[4] for x in items)
            vals = sorted([x[1] for x in items], reverse=True)
            wallet_count = len(items)
            top1 = vals[0] if vals else 0.0
            top3 = sum(vals[:3]) if vals else 0.0
            d[f"{prefix}_wallets_{label}"] = wallet_count
            d[f"{prefix}_actions_{label}"] = action_count
            d[f"{prefix}_wallet_abs_flow_{label}"] = total_abs
            d[f"{prefix}_top1_share_{label}"] = top1 / total_abs if total_abs > 0 else 0.0
            d[f"{prefix}_top3_share_{label}"] = top3 / total_abs if total_abs > 0 else 0.0
            d[f"{prefix}_wallet_spot_share_{label}"] = spot_abs / (perp_abs + spot_abs) if (perp_abs + spot_abs) > 0 else 0.0
            d[f"{prefix}_wallet_perp_abs_{label}"] = perp_abs
            d[f"{prefix}_wallet_spot_abs_{label}"] = spot_abs

    conn.close()
    # 计算每个币最强滚动窗口，方便排序/报告。
    for coin, d in result.items():
        best_label = ""
        best_val = 0.0
        best_runs = 0
        for hours in ROLLING_FLOW_WINDOWS_HOURS:
            label = rolling_window_label(hours)
            val = float(d.get(f"weighted_{label}") or d.get(f"active_{label}") or 0.0)
            if abs(val) > abs(best_val):
                best_val = val
                best_label = label
                best_runs = int(d.get(f"runs_{label}") or 0)
        d["best_window"] = best_label
        d["best_flow"] = best_val
        d["best_direction"] = "bullish" if best_val > 0 else "bearish" if best_val < 0 else "neutral"
        d["best_runs"] = best_runs
    return dict(result)

def rolling_score_for_coin(coin: str, rolling: Dict[str, Any], thresholds: Dict[str, Dict[str, float]]) -> Tuple[float, List[str], Dict[str, Any]]:
    if not rolling:
        return 0.0, [], {}

    # 稳定版：滚动窗口用于判断“持续建仓”，不是让分数随着数据库历史自然变大。
    # 关键修复：
    # 1) 15d/30d 必须真正覆盖足够时间，否则不参与评分。
    # 2) 2h/6h/24h/72h/15d/30d 是嵌套窗口，默认只取一个主窗口；其他窗口只给小幅确认，不再满额相加。
    # 3) 长窗口必须有持续性、钱包广度、低杠杆确认；spot-only/单钱包/断跑会明显降权。
    cfg = {
        "2h":  {"mult": 1.0,  "pts": 0.45, "group": "short", "min_runs": 2, "min_days": 1, "min_wallets": 1, "min_coverage": ROLLING_MIN_COVERAGE_SHORT},
        "6h":  {"mult": 1.5,  "pts": 0.75, "group": "short", "min_runs": 3, "min_days": 1, "min_wallets": 1, "min_coverage": ROLLING_MIN_COVERAGE_SHORT},
        "24h": {"mult": 2.5,  "pts": 1.20, "group": "mid",   "min_runs": 4, "min_days": 1, "min_wallets": ROLLING_MIN_WALLETS_MID,  "min_coverage": ROLLING_MIN_COVERAGE_MID},
        "72h": {"mult": 4.0,  "pts": 1.60, "group": "mid",   "min_runs": 6, "min_days": 2, "min_wallets": ROLLING_MIN_WALLETS_MID,  "min_coverage": ROLLING_MIN_COVERAGE_MID},
        "15d": {"mult": 7.0,  "pts": 2.10, "group": "long",  "min_runs": 12, "min_days": 6, "min_wallets": ROLLING_MIN_WALLETS_LONG, "min_coverage": ROLLING_MIN_COVERAGE_LONG},
        "30d": {"mult": 10.0, "pts": 2.60, "group": "long",  "min_runs": 20, "min_days": 10, "min_wallets": ROLLING_MIN_WALLETS_LONG, "min_coverage": ROLLING_MIN_COVERAGE_LONG},
    }
    group_caps = {"short": 1.0, "mid": 2.0, "long": 2.6}
    group_names = {"short": "短窗口", "mid": "中窗口", "long": "长窗口"}
    pth = threshold(thresholds, coin, "perp")

    parts: Dict[str, Any] = {}
    reasons: List[str] = []
    candidates: Dict[str, Dict[str, Any]] = {}
    all_candidate_count = 0
    risk_flags = {
        "spot_only": False,
        "concentration": False,
        "persistence": False,
        "gap": False,
        "immature": False,
    }

    for hours in ROLLING_FLOW_WINDOWS_HOURS:
        label = rolling_window_label(hours)
        signed = float(rolling.get(f"weighted_{label}") or rolling.get(f"active_{label}") or 0.0)
        parts[f"rolling_{label}"] = signed
        parts[f"coverage_{label}"] = float(rolling.get(f"coverage_{label}") or 0.0)
        parts[f"span_hours_{label}"] = float(rolling.get(f"span_hours_{label}") or 0.0)

        spec = cfg.get(label, {"mult": 3.0, "pts": 1.0, "group": "mid", "min_runs": 3, "min_days": 1, "min_wallets": ROLLING_MIN_WALLETS_MID, "min_coverage": ROLLING_MIN_COVERAGE_MID})
        if abs(signed) < pth * float(spec["mult"]):
            continue

        sign = 1 if signed > 0 else -1
        prefix = "bullish" if sign > 0 else "bearish"
        group = str(spec["group"])
        all_candidate_count += 1

        same_runs = int(rolling.get(f"{prefix}_runs_{label}") or 0)
        same_days = int(rolling.get(f"{prefix}_days_{label}") or 0)
        total_runs = int(rolling.get(f"runs_{label}") or 0)
        gaps = int(rolling.get(f"gaps_{label}") or 0)
        coverage = float(rolling.get(f"coverage_{label}") or 0.0)
        span_hours = float(rolling.get(f"span_hours_{label}") or 0.0)
        wallet_count = int(rolling.get(f"{prefix}_wallets_{label}") or 0)
        top1_share = float(rolling.get(f"{prefix}_top1_share_{label}") or 0.0)
        top3_share = float(rolling.get(f"{prefix}_top3_share_{label}") or 0.0)
        spot_share_window = float(rolling.get(f"spot_share_{label}") or 0.0)
        spot_share_wallet = float(rolling.get(f"{prefix}_wallet_spot_share_{label}") or 0.0)
        spot_share = max(spot_share_window, spot_share_wallet)
        perp_abs = abs(float(rolling.get(f"perp_{label}") or 0.0))

        lev_health = safe_float(rolling.get(f"{prefix}_leverage_health_{label}"))
        avg_lev = safe_float(rolling.get(f"{prefix}_avg_leverage_{label}"))
        avg_liq = safe_float(rolling.get(f"{prefix}_avg_liq_distance_{label}"))
        long_ratio = safe_float(rolling.get(f"{prefix}_longterm_leverage_ratio_{label}")) or 0.0
        high_ratio = safe_float(rolling.get(f"{prefix}_highrisk_leverage_ratio_{label}")) or 0.0

        quality_mult = 1.0
        notes: List[str] = []
        risk_notes: List[str] = []

        # 窗口成熟度：数据库刚开始积累时，不能把 5 天历史当作 30d 信号。
        min_coverage = float(spec.get("min_coverage", 0.5))
        if ROLLING_REQUIRE_WINDOW_MATURITY and coverage < min_coverage:
            # 长窗口不成熟直接跳过；短/中窗口不成熟则严重降权。
            risk_flags["immature"] = True
            risk_notes.append(f"窗口未成熟：覆盖{coverage*100:.0f}%/{min_coverage*100:.0f}% span={span_hours:.1f}h")
            if group == "long":
                continue
            quality_mult *= 0.35

        # 持续性：长期窗口必须是多轮/多天同向，不允许“单次大额变化”因为落在 30d 窗口里就拿长期分。
        min_runs = int(spec["min_runs"])
        min_days = int(spec["min_days"])
        if same_runs < min_runs or same_days < min_days:
            if group == "short":
                quality_mult *= 0.65
            else:
                quality_mult *= ROLLING_PERSISTENCE_MULT
                risk_flags["persistence"] = True
            risk_notes.append(f"持续性不足：{same_runs}/{min_runs}轮 {same_days}/{min_days}天")

        # 如果窗口里有 gap，窗口累计信号降权，避免断跑后把多小时累计当成普通连续建仓。
        if gaps > 0:
            quality_mult *= 0.75
            risk_flags["gap"] = True
            risk_notes.append(f"含断跑gap={gaps}")

        # 钱包广度/集中度：一个钱包贡献绝大部分，不算“集体持续建仓”。
        min_wallets = int(spec["min_wallets"])
        if wallet_count > 0 and wallet_count < min_wallets and group != "short":
            quality_mult *= 0.60
            risk_flags["concentration"] = True
            risk_notes.append(f"参与钱包少：{wallet_count}/{min_wallets}个")
        if top1_share >= ROLLING_TOP1_MAX_SHARE and wallet_count > 0:
            quality_mult *= ROLLING_CONCENTRATION_MULT
            risk_flags["concentration"] = True
            risk_notes.append(f"Top1占比{top1_share*100:.0f}%")
        elif top3_share >= ROLLING_TOP3_MAX_SHARE and wallet_count >= 3:
            quality_mult *= 0.75
            risk_flags["concentration"] = True
            risk_notes.append(f"Top3占比{top3_share*100:.0f}%")

        # 现货-only：现货减少/增加不一定是卖出/买入，可能是转账/归集/划转。
        if spot_share >= ROLLING_SPOT_ONLY_SHARE and perp_abs < pth * 0.35 and (lev_health is None or lev_health <= 0.15):
            quality_mult *= ROLLING_SPOT_ONLY_MULT
            risk_flags["spot_only"] = True
            risk_notes.append(f"现货主导{spot_share*100:.0f}%且无合约低杠杆确认")

        # 异常大额 + 单钱包：多数是转账/归集/数据异常，不直接给满分。
        if abs(signed) >= pth * 20 and (wallet_count <= 1 or top1_share >= 0.85):
            quality_mult *= 0.30
            risk_flags["concentration"] = True
            risk_notes.append("单钱包异常大额，按观察处理")

        quality_mult = max(0.05, min(1.0, quality_mult))
        raw_pts = float(spec["pts"])
        flow_score = raw_pts * quality_mult * sign

        lev_score = 0.0
        if ROLLING_LEVERAGE_MODE and LEVERAGE_QUALITY_MODE and lev_health is not None:
            lev_score = max(-0.60, min(0.60, lev_health * 0.35)) * sign
            if lev_score > 0.12:
                notes.append(f"杠杆健康：低/中杠杆{long_ratio*100:.0f}%" + (f" 均{avg_lev:.1f}x" if avg_lev is not None else ""))
            elif lev_score < -0.12:
                notes.append(f"杠杆风险：高风险{high_ratio*100:.0f}%" + (f" 均{avg_lev:.1f}x" if avg_lev is not None else ""))

        notes.append(f"{label}{fmt_money(signed)} 质量={quality_mult:.2f} 覆盖={coverage*100:.0f}% 钱包={wallet_count} 持续={same_runs}轮/{same_days}天")
        if risk_notes:
            notes.extend(risk_notes[:4])

        cand = {
            "label": label,
            "group": group,
            "sign": sign,
            "signed": signed,
            "flow_score": flow_score,
            "lev_score": lev_score,
            "quality_mult": quality_mult,
            "same_runs": same_runs,
            "same_days": same_days,
            "total_runs": total_runs,
            "gap_count": gaps,
            "coverage": coverage,
            "span_hours": span_hours,
            "wallet_count": wallet_count,
            "top1_share": top1_share,
            "top3_share": top3_share,
            "spot_share": spot_share,
            "avg_leverage": avg_lev,
            "avg_liq_distance": avg_liq,
            "longterm_leverage_ratio": long_ratio,
            "highrisk_leverage_ratio": high_ratio,
            "leverage_health": lev_health,
            "notes": notes,
        }
        prev = candidates.get(group)
        if prev is None or abs(cand["flow_score"]) + abs(cand["lev_score"]) > abs(prev["flow_score"]) + abs(prev["lev_score"]):
            candidates[group] = cand

    rolling_flow_core_score = 0.0
    rolling_leverage_adj = 0.0
    selected: List[Dict[str, Any]] = [candidates[g] for g in ("short", "mid", "long") if g in candidates]
    signs_by_group = [int(c["sign"]) for c in selected]

    if ROLLING_SCORE_USE_BEST_HORIZON and selected:
        # 主窗口决定主要滚动分；其他窗口只证明“不是单一时间段噪音”，不再 full add。
        best = max(selected, key=lambda c: abs(float(c.get("flow_score") or 0.0)) + abs(float(c.get("lev_score") or 0.0)))
        cap = group_caps.get(str(best.get("group")), 2.0)
        base_flow = max(-cap, min(cap, float(best.get("flow_score") or 0.0)))
        rolling_flow_core_score = base_flow
        rolling_leverage_adj = float(best.get("lev_score") or 0.0)
        reasons.append(f"主滚动窗口取{best['label']}：{fmt_money(best['signed'])}，资金分{base_flow:+.1f}")
        for n in best.get("notes", [])[:5]:
            reasons.append(n)

        same_side_confirm = [c for c in selected if c is not best and int(c.get("sign") or 0) == int(best.get("sign") or 0) and float(c.get("quality_mult") or 0.0) >= 0.50]
        opposite = [c for c in selected if int(c.get("sign") or 0) != int(best.get("sign") or 0)]
        if same_side_confirm:
            bonus = min(ROLLING_CONTINUITY_BONUS_MAX, 0.25 * len(same_side_confirm)) * int(best.get("sign") or 0)
            rolling_flow_core_score += bonus
            reasons.append(f"其他成熟窗口同向确认{len(same_side_confirm)}组，确认分{bonus:+.1f}")
        if opposite:
            penalty = min(0.6, 0.25 * len(opposite)) * int(best.get("sign") or 0)
            rolling_flow_core_score -= penalty
            reasons.append(f"存在反向滚动窗口{len(opposite)}组，冲突扣分{-penalty:+.1f}")
    else:
        for group in ("short", "mid", "long"):
            cand = candidates.get(group)
            if not cand:
                continue
            cap = group_caps.get(group, 2.0)
            flow_score = max(-cap, min(cap, float(cand["flow_score"])))
            rolling_flow_core_score += flow_score
            rolling_leverage_adj += float(cand.get("lev_score") or 0.0)
            reasons.append(f"{group_names.get(group, group)}取{cand['label']}：{fmt_money(cand['signed'])}，资金分{flow_score:+.1f}")
            for n in cand.get("notes", [])[:4]:
                reasons.append(n)
        if len(selected) >= 2 and signs_by_group and all(x == signs_by_group[0] for x in signs_by_group):
            avg_quality = sum(float(c.get("quality_mult") or 0.0) for c in selected) / max(1, len(selected))
            if avg_quality >= 0.50:
                bonus = min(ROLLING_CONTINUITY_BONUS_MAX, 0.25 * (len(selected) - 1)) * signs_by_group[0]
                rolling_flow_core_score += bonus
                reasons.append(f"短中长分组同向({len(selected)}组)，连续性加分{bonus:+.1f}")

    rolling_leverage_adj = max(-0.8, min(0.8, rolling_leverage_adj))
    score = rolling_flow_core_score + rolling_leverage_adj

    suspect = risk_flags["spot_only"] or risk_flags["concentration"] or risk_flags["persistence"] or risk_flags["immature"]
    if suspect and abs(score) > ROLLING_SUSPECT_CAP_SCORE:
        score = (1 if score > 0 else -1) * ROLLING_SUSPECT_CAP_SCORE
        reasons.append(f"滚动信号含现货only/集中度/持续性/窗口未成熟风险，rolling封顶到{ROLLING_SUSPECT_CAP_SCORE:.1f}")

    parts["rolling_score"] = round(score, 4)
    parts["rolling_flow_core"] = round(rolling_flow_core_score, 4)
    parts["rolling_leverage"] = round(rolling_leverage_adj, 4)
    parts["rolling_selected_groups"] = ",".join([str(c.get("group")) for c in selected])
    parts["rolling_candidate_windows"] = all_candidate_count
    parts["rolling_spot_only_risk"] = 1 if risk_flags["spot_only"] else 0
    parts["rolling_concentration_risk"] = 1 if risk_flags["concentration"] else 0
    parts["rolling_persistence_risk"] = 1 if risk_flags["persistence"] else 0
    parts["rolling_gap_risk"] = 1 if risk_flags["gap"] else 0
    parts["rolling_immature_risk"] = 1 if risk_flags["immature"] else 0

    if selected:
        best = max(selected, key=lambda c: abs(float(c.get("flow_score") or 0.0)) + abs(float(c.get("lev_score") or 0.0)))
        parts.update({
            "best_window": best.get("label"),
            "best_flow": best.get("signed"),
            "best_same_runs": best.get("same_runs"),
            "best_same_days": best.get("same_days"),
            "best_wallet_count": best.get("wallet_count"),
            "best_top1_share": best.get("top1_share"),
            "best_top3_share": best.get("top3_share"),
            "best_spot_share": best.get("spot_share"),
            "best_quality_mult": best.get("quality_mult"),
            "best_coverage": best.get("coverage"),
            "best_span_hours": best.get("span_hours"),
            "best_avg_leverage": best.get("avg_leverage"),
            "best_avg_liq_distance": best.get("avg_liq_distance"),
            "best_longterm_leverage_ratio": best.get("longterm_leverage_ratio"),
            "best_highrisk_leverage_ratio": best.get("highrisk_leverage_ratio"),
            "best_leverage_health": best.get("leverage_health"),
        })
    else:
        parts["best_window"] = rolling.get("best_window")
        parts["best_flow"] = rolling.get("best_flow")

    return score, reasons, parts

def export_rolling_flow_files(run_id: int, rolling_map: Dict[str, Dict[str, Any]], thresholds: Dict[str, Dict[str, float]]) -> None:
    if not ROLLING_FLOW_MODE:
        return
    ensure_dirs()
    labels = [rolling_window_label(h) for h in ROLLING_FLOW_WINDOWS_HOURS]
    rows: List[Dict[str, Any]] = []
    for coin, d in rolling_map.items():
        score, reasons, parts = rolling_score_for_coin(coin, d, thresholds)
        if abs(d.get("best_flow") or 0.0) < 1 and abs(score) < 0.01:
            continue
        best_flow_for_score = safe_float(parts.get("best_flow"))
        best_dir = "bullish" if (best_flow_for_score or 0.0) > 0 else "bearish" if (best_flow_for_score or 0.0) < 0 else (d.get("best_direction") or "neutral")
        best_prefix = "bullish" if best_dir == "bullish" else "bearish" if best_dir == "bearish" else ""
        best_label = parts.get("best_window") or d.get("best_window")
        row = {
            "run_id": run_id,
            "coin": coin,
            "rolling_score": round(score, 4),
            "rolling_leverage_score": parts.get("rolling_leverage"),
            "rolling_flow_core": parts.get("rolling_flow_core"),
            "selected_groups": parts.get("rolling_selected_groups"),
            "candidate_windows": parts.get("rolling_candidate_windows"),
            "spot_only_risk": parts.get("rolling_spot_only_risk"),
            "concentration_risk": parts.get("rolling_concentration_risk"),
            "persistence_risk": parts.get("rolling_persistence_risk"),
            "gap_risk": parts.get("rolling_gap_risk"),
            "best_window": best_label,
            "best_direction": best_dir,
            "best_flow": parts.get("best_flow", d.get("best_flow")),
            "best_runs": parts.get("best_same_runs", d.get("best_runs")),
            "best_same_days": parts.get("best_same_days"),
            "best_wallet_count": parts.get("best_wallet_count"),
            "best_top1_share": parts.get("best_top1_share"),
            "best_top3_share": parts.get("best_top3_share"),
            "best_spot_share": parts.get("best_spot_share"),
            "best_quality_mult": parts.get("best_quality_mult"),
            "best_avg_leverage": parts.get("best_avg_leverage", d.get(f"{best_prefix}_avg_leverage_{best_label}") if best_prefix and best_label else None),
            "best_avg_liq_distance": parts.get("best_avg_liq_distance", d.get(f"{best_prefix}_avg_liq_distance_{best_label}") if best_prefix and best_label else None),
            "best_longterm_leverage_ratio": parts.get("best_longterm_leverage_ratio", d.get(f"{best_prefix}_longterm_leverage_ratio_{best_label}") if best_prefix and best_label else None),
            "best_highrisk_leverage_ratio": parts.get("best_highrisk_leverage_ratio", d.get(f"{best_prefix}_highrisk_leverage_ratio_{best_label}") if best_prefix and best_label else None),
            "best_leverage_health": parts.get("best_leverage_health", d.get(f"{best_prefix}_leverage_health_{best_label}") if best_prefix and best_label else None),
            "reason": "；".join(reasons),
        }
        for label in labels:
            row[f"weighted_{label}"] = d.get(f"weighted_{label}")
            row[f"active_{label}"] = d.get(f"active_{label}")
            row[f"perp_{label}"] = d.get(f"perp_{label}")
            row[f"spot_{label}"] = d.get(f"spot_{label}")
            row[f"runs_{label}"] = d.get(f"runs_{label}")
            row[f"gaps_{label}"] = d.get(f"gaps_{label}")
            row[f"spot_share_{label}"] = d.get(f"spot_share_{label}")
            for prefix in ("bullish", "bearish"):
                row[f"{prefix}_runs_{label}"] = d.get(f"{prefix}_runs_{label}")
                row[f"{prefix}_days_{label}"] = d.get(f"{prefix}_days_{label}")
                row[f"{prefix}_wallets_{label}"] = d.get(f"{prefix}_wallets_{label}")
                row[f"{prefix}_top1_share_{label}"] = d.get(f"{prefix}_top1_share_{label}")
                row[f"{prefix}_top3_share_{label}"] = d.get(f"{prefix}_top3_share_{label}")
                row[f"{prefix}_wallet_spot_share_{label}"] = d.get(f"{prefix}_wallet_spot_share_{label}")
                row[f"{prefix}_avg_leverage_{label}"] = d.get(f"{prefix}_avg_leverage_{label}")
                row[f"{prefix}_avg_liq_distance_{label}"] = d.get(f"{prefix}_avg_liq_distance_{label}")
                row[f"{prefix}_longterm_leverage_ratio_{label}"] = d.get(f"{prefix}_longterm_leverage_ratio_{label}")
                row[f"{prefix}_highrisk_leverage_ratio_{label}"] = d.get(f"{prefix}_highrisk_leverage_ratio_{label}")
                row[f"{prefix}_leverage_health_{label}"] = d.get(f"{prefix}_leverage_health_{label}")
        rows.append(row)
    rows.sort(key=lambda r: abs(float(r.get("rolling_score") or 0.0)) * 1_000_000 + abs(float(r.get("best_flow") or 0.0)), reverse=True)
    fieldnames = [
        "run_id", "coin", "rolling_score", "rolling_leverage_score", "rolling_flow_core",
        "selected_groups", "candidate_windows", "spot_only_risk", "concentration_risk", "persistence_risk", "gap_risk",
        "best_window", "best_direction", "best_flow", "best_runs", "best_same_days",
        "best_wallet_count", "best_top1_share", "best_top3_share", "best_spot_share", "best_quality_mult",
        "best_avg_leverage", "best_avg_liq_distance",
        "best_longterm_leverage_ratio", "best_highrisk_leverage_ratio", "best_leverage_health", "reason"
    ]
    for label in labels:
        fieldnames.extend([f"weighted_{label}", f"active_{label}", f"perp_{label}", f"spot_{label}", f"runs_{label}", f"gaps_{label}", f"spot_share_{label}"])
        for prefix in ("bullish", "bearish"):
            fieldnames.extend([
                f"{prefix}_runs_{label}", f"{prefix}_days_{label}", f"{prefix}_wallets_{label}",
                f"{prefix}_top1_share_{label}", f"{prefix}_top3_share_{label}", f"{prefix}_wallet_spot_share_{label}",
                f"{prefix}_avg_leverage_{label}", f"{prefix}_avg_liq_distance_{label}",
                f"{prefix}_longterm_leverage_ratio_{label}", f"{prefix}_highrisk_leverage_ratio_{label}",
                f"{prefix}_leverage_health_{label}"
            ])
    with open(os.path.join(DETAILS_DIR, "rolling_flow_latest.csv"), "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    lines = [
        "【滚动建仓资金流 + 杠杆质量】",
        "说明：短线异动看上一轮；长期建仓看滚动窗口，但已修正嵌套窗口重复加分。",
        "新逻辑：2h/6h、24h/72h、15d/30d 分组取最高；同时检查持续性、钱包广度、单钱包集中度、现货-only、杠杆质量。",
        f"run_id={run_id} | {DISPLAY_TZ_NAME}={signal_time_cn(run_id)} | UTC={now_str()}",
        "",
    ]
    top = [r for r in rows if abs(float(r.get("rolling_score") or 0.0)) > 0]
    if not top:
        lines.append("暂无达到滚动窗口阈值的持续建仓信号。")
    else:
        for r in top[:20]:
            lines.append(
                f"{r['coin']} {dir_cn(r['best_direction'])} | rolling_score={float(r.get('rolling_score') or 0):+.1f} "
                f"杠杆项={float(r.get('rolling_leverage_score') or 0):+.1f} "
                f"| 最强窗口={r.get('best_window')} {fmt_money(r.get('best_flow'))} "
                f"| 均杠杆={r.get('best_avg_leverage') if r.get('best_avg_leverage') is not None else 'N/A'} "
                f"高风险占比={(float(r.get('best_highrisk_leverage_ratio') or 0)*100):.0f}% | {r.get('reason') or '-'}"
            )
    with open(os.path.join(REPORT_DIR, "rolling_flow_report.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

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
                active_delta, price_effect, qty_delta, entry_px, side, position_value, leverage,
                margin_mode, liq_distance_pct, leverage_style, leverage_weight
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (run_id, now_str(), a["address"], a.get("groups", ""), a["coin"], a["market"], a["direction"], a["action_type"],
                  a["active_delta"], a["price_effect"], a["qty_delta"], a["entry_px"],
                  a.get("side"), a.get("position_value"), a.get("leverage"), a.get("margin_mode"),
                  a.get("liq_distance_pct"), a.get("leverage_style"), a.get("leverage_weight")))
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
    try:
        updated_signals += eval_table("longterm_events", "event_id")
    except Exception as e:
        print(f"更新长期单回测失败：{e}", flush=True)
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
        with open(os.path.join(DETAILS_DIR, "wallet_position_trades_latest.csv"), "w", encoding="utf-8-sig", newline="") as f:
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
        with open(os.path.join(DETAILS_DIR, "wallet_position_performance_latest.csv"), "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(perf_rows[0].keys()))
            writer.writeheader()
            writer.writerows(perf_rows)

    with open(os.path.join(DETAILS_DIR, "wallet_position_report.txt"), "w", encoding="utf-8") as f:
        f.write("【仓位生命周期收益报告】\n")
        f.write(f"更新时间{DISPLAY_TZ_NAME}：{display_now_str()} | UTC：{now_str()}\n")
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
    with open(os.path.join(DETAILS_DIR, "leverage_quality_latest.csv"), "w", encoding="utf-8-sig", newline="") as f:
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
        with open(os.path.join(DETAILS_DIR, "wallet_leverage_profile_latest.csv"), "w", encoding="utf-8-sig", newline="") as f:
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
        with open(os.path.join(DETAILS_DIR, "coin_leverage_summary_latest.csv"), "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(coin_rows[0].keys()))
            writer.writeheader()
            writer.writerows(coin_rows)

    with open(os.path.join(DETAILS_DIR, "leverage_quality_report.txt"), "w", encoding="utf-8") as f:
        f.write("【合约杠杆质量报告】\n")
        f.write(f"更新时间{DISPLAY_TZ_NAME}：{display_now_str()} | UTC：{now_str()}\n")
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



async def fetch_perp_asset_contexts(session: aiohttp.ClientSession, limiter: Optional[RateLimiter]) -> Dict[str, Dict[str, Any]]:
    """读取 Hyperliquid 合约资产上下文，主要用于资金费率和24h成交额过滤。"""
    ok, data = await post_info(session, limiter, {"type": "metaAndAssetCtxs"})
    out: Dict[str, Dict[str, Any]] = {}
    if not ok:
        print("获取 metaAndAssetCtxs 失败：", data, flush=True)
        return out
    try:
        meta, ctxs = data
        universe = meta.get("universe") or []
        for u, ctx in zip(universe, ctxs):
            coin = str(u.get("name") or ctx.get("coin") or "")
            if not coin:
                continue
            funding = safe_float(ctx.get("funding"))
            day_vlm = safe_float(ctx.get("dayNtlVlm")) or safe_float(ctx.get("dayBaseVlm"))
            oi = safe_float(ctx.get("openInterest"))
            mark = safe_float(ctx.get("markPx")) or safe_float(ctx.get("midPx"))
            # openInterest 有些场景是币数量，用 mark 粗略换成美元；如果已经是美元，乘法会偏大，所以只做辅助显示。
            oi_usd = oi * mark if oi is not None and mark is not None else oi
            out[coin] = {
                "coin": coin,
                "funding_rate": funding,
                "funding_rate_pct": funding * 100 if funding is not None else None,
                "day_volume_usd": day_vlm,
                "open_interest_usd": oi_usd,
                "mark_px": mark,
            }
    except Exception as e:
        print("解析 metaAndAssetCtxs 失败：", e, flush=True)
    return out


def classify_coin_risk(row: Dict[str, Any]) -> Dict[str, Any]:
    funding_pct = safe_float(row.get("funding_rate_pct"))
    day_vol = safe_float(row.get("day_volume_usd"))
    funding_risk = "未知"
    funding_note = "资金费率未知"
    if funding_pct is not None:
        af = abs(funding_pct)
        if af >= FUNDING_DANGER_ABS_PCT:
            funding_risk = "高"
        elif af >= FUNDING_WARN_ABS_PCT:
            funding_risk = "中"
        else:
            funding_risk = "低"
        funding_note = f"funding={funding_pct:+.4f}%"

    liquidity_risk = "未知"
    liquidity_note = "24h成交额未知"
    if day_vol is not None:
        if day_vol < LIQUIDITY_MIN_DAY_VOLUME:
            liquidity_risk = "高"
        elif day_vol < LIQUIDITY_LOW_DAY_VOLUME:
            liquidity_risk = "中"
        else:
            liquidity_risk = "低"
        liquidity_note = f"24h成交额={fmt_money(day_vol)}"
    out = dict(row)
    out.update({
        "funding_risk": funding_risk,
        "funding_note": funding_note,
        "liquidity_risk": liquidity_risk,
        "liquidity_note": liquidity_note,
    })
    return out


async def build_coin_risk_metrics(run_id: int, candidate_coins: List[str]) -> Dict[str, Dict[str, Any]]:
    """导出币种资金费率/流动性风险，用于长期单过滤。"""
    if not RISK_FILTER_MODE:
        return {}
    timeout = aiohttp.ClientTimeout(total=60)
    limiter = RateLimiter(120)
    risk_map: Dict[str, Dict[str, Any]] = {}
    async with aiohttp.ClientSession(timeout=timeout) as session:
        all_ctx = await fetch_perp_asset_contexts(session, limiter)
    wanted = set([c for c in candidate_coins if c]) | {"BTC", "ETH"}
    rows: List[Dict[str, Any]] = []
    for coin in sorted(wanted):
        if coin not in all_ctx:
            continue
        r = classify_coin_risk(all_ctx[coin])
        risk_map[coin] = r
        rows.append(r)
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM coin_risk_metrics WHERE run_id=?", (run_id,))
    cur.executemany("""
    INSERT OR REPLACE INTO coin_risk_metrics (
        run_id, coin, funding_rate, funding_rate_pct, day_volume_usd, open_interest_usd,
        liquidity_risk, funding_risk, funding_note, liquidity_note, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [(
        run_id, r.get("coin"), r.get("funding_rate"), r.get("funding_rate_pct"), r.get("day_volume_usd"), r.get("open_interest_usd"),
        r.get("liquidity_risk"), r.get("funding_risk"), r.get("funding_note"), r.get("liquidity_note"), now_str()
    ) for r in rows])
    conn.commit()
    conn.close()

    ensure_dirs()
    if rows:
        with open(os.path.join(DETAILS_DIR, "coin_risk_latest.csv"), "w", encoding="utf-8-sig", newline="") as f:
            fields = ["coin", "funding_rate_pct", "funding_risk", "day_volume_usd", "liquidity_risk", "open_interest_usd", "funding_note", "liquidity_note"]
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader(); writer.writerows([{k: r.get(k) for k in fields} for r in rows])
        sorted_rows = sorted(rows, key=lambda r: ((r.get("liquidity_risk") == "高"), abs(safe_float(r.get("funding_rate_pct")) or 0.0)), reverse=True)
        with open(os.path.join(REPORT_DIR, "coin_risk_report.txt"), "w", encoding="utf-8") as f:
            f.write("【资金费率 / 流动性风险】\n")
            f.write(f"run_id={run_id} | 更新时间{DISPLAY_TZ_NAME}={signal_time_cn(run_id)} | UTC={now_str()}\n\n")
            for r in sorted_rows[:50]:
                f.write(f"{r['coin']} | funding={fmt_pct(r.get('funding_rate_pct'))} 风险={r.get('funding_risk')} | 24h成交额={fmt_money(r.get('day_volume_usd'))} 流动性={r.get('liquidity_risk')} | OI={fmt_money(r.get('open_interest_usd'))}\n")
    return risk_map


def risk_filter_adjust(direction: str, risk: Dict[str, Any]) -> Tuple[float, List[str], List[str], Dict[str, Any]]:
    """长期单风险过滤：资金费率和流动性。"""
    if not RISK_FILTER_MODE or not risk:
        return 0.0, [], [], {}
    adj = 0.0
    reasons: List[str] = []
    risks: List[str] = []
    funding_pct = safe_float(risk.get("funding_rate_pct"))
    day_vol = safe_float(risk.get("day_volume_usd"))
    funding_risk = risk.get("funding_risk") or "未知"
    liquidity_risk = risk.get("liquidity_risk") or "未知"

    # 简化逻辑：正 funding 通常对做多不利、对做空有利；负 funding 反过来。
    if funding_pct is not None:
        direction_cost = funding_pct if direction == "bullish" else -funding_pct
        af = abs(funding_pct)
        if direction_cost > 0 and af >= FUNDING_DANGER_ABS_PCT:
            adj -= 1.2; risks.append(f"资金费率对{dir_cn(direction)}明显不利：{funding_pct:+.4f}%")
        elif direction_cost > 0 and af >= FUNDING_WARN_ABS_PCT:
            adj -= 0.5; risks.append(f"资金费率对{dir_cn(direction)}偏不利：{funding_pct:+.4f}%")
        elif direction_cost < 0 and af >= FUNDING_WARN_ABS_PCT:
            adj += 0.3; reasons.append(f"资金费率对{dir_cn(direction)}有利：{funding_pct:+.4f}%")
        else:
            reasons.append(f"资金费率中性：{funding_pct:+.4f}%")

    if day_vol is not None:
        if day_vol < LIQUIDITY_MIN_DAY_VOLUME:
            adj -= 1.5; risks.append(f"流动性高风险：24h成交额仅{fmt_money(day_vol)}")
        elif day_vol < LIQUIDITY_LOW_DAY_VOLUME:
            adj -= 0.7; risks.append(f"流动性偏低：24h成交额{fmt_money(day_vol)}")
        else:
            reasons.append(f"流动性可接受：24h成交额{fmt_money(day_vol)}")

    return adj, reasons, risks, {
        "funding_rate_pct": funding_pct,
        "funding_risk": funding_risk,
        "day_volume_usd": day_vol,
        "liquidity_risk": liquidity_risk,
    }


def _score_sign(v: float) -> int:
    return 1 if v > 0 else -1 if v < 0 else 0


def _same_sign(a: float, b: float) -> bool:
    return _score_sign(a) != 0 and _score_sign(a) == _score_sign(b)


def _signed_cap(v: float, cap: float) -> float:
    if cap <= 0:
        return v
    return max(-cap, min(cap, v))


def build_signals(run_id: int, preliminary: Dict[str, Dict[str, Any]], ctx_map: Dict[str, Dict[str, Any]], thresholds: Dict[str, Dict[str, float]], risk_map: Optional[Dict[str, Dict[str, Any]]] = None, rolling_map: Optional[Dict[str, Dict[str, Any]]] = None, gap_minutes: Optional[float] = None) -> List[Dict[str, Any]]:
    """构建币种信号。双分数版：

    alert_score：短线异动雷达。主要看本轮主动变化，少量参考短窗口滚动确认。
    long_score：低杠杆长期资格。主要看成熟滚动窗口、持续性、钱包广度、杠杆健康。

    这样避免一个 final_score 同时承担“短线报警”和“长期单”的职责。
    """
    btc_ctx = ctx_map.get("BTC", {})
    lev_map = build_leverage_signal_map(run_id) if LEVERAGE_QUALITY_MODE else {}
    rows: List[Dict[str, Any]] = []
    rolling_map = rolling_map or {}
    current_gap_ok = (gap_minutes is None) or (gap_minutes <= MAX_SHORT_SIGNAL_GAP_MINUTES)
    coin_universe = set(preliminary.keys()) | set(rolling_map.keys())

    for coin in coin_universe:
        d = preliminary.get(coin, {})
        perp_active = float(d.get("perp_active") or 0.0)
        spot_active = float(d.get("spot_active") or 0.0)
        weighted_flow = float(d.get("weighted_flow") or 0.0)
        pth = threshold(thresholds, coin, "perp")
        sth = threshold(thresholds, coin, "spot")
        th_score = threshold(thresholds, coin, "score_push")
        min_watch = threshold(thresholds, coin, "min_watch_score")

        # 1) 本轮/短线异动分，只用于 alert_score。
        base_score = 0.0
        base_reasons: List[str] = []
        if current_gap_ok:
            if abs(perp_active) >= pth:
                base_score += 3.0 if perp_active > 0 else -3.0
                base_reasons.append(f"本轮合约主动变化{fmt_money(perp_active)}")
            if abs(spot_active) >= sth:
                # 现货只给较低报警分，不直接等同长期方向。
                base_score += 1.6 if spot_active > 0 else -1.6
                base_reasons.append(f"本轮现货主动变化{fmt_money(spot_active)}")
            if abs(weighted_flow) >= pth * 3:
                base_score += 2.0 if weighted_flow > 0 else -2.0
                base_reasons.append(f"本轮钱包质量加权资金流{fmt_money(weighted_flow)}")
            elif abs(weighted_flow) >= pth:
                base_score += 1.0 if weighted_flow > 0 else -1.0
                base_reasons.append(f"本轮钱包质量加权资金流{fmt_money(weighted_flow)}")
        else:
            if abs(perp_active) >= pth or abs(spot_active) >= sth or abs(weighted_flow) >= pth:
                base_reasons.append(f"距离上一轮{gap_minutes:.0f}分钟，当前变化只进入滚动窗口，不当作30m短线强信号")

        # 2) 滚动建仓分，只作为长期资格主来源。
        rolling_score, rolling_reasons, rolling_parts = rolling_score_for_coin(coin, rolling_map.get(coin, {}), thresholds) if ROLLING_FLOW_MODE else (0.0, [], {})
        if base_score == 0 and rolling_score == 0:
            continue

        # 如果本轮和滚动方向冲突，用绝对值更大的方向作为这条记录方向，但默认只观察。
        primary_seed = rolling_score if abs(rolling_score) > abs(base_score) else base_score
        if primary_seed == 0:
            primary_seed = base_score + rolling_score
        if primary_seed == 0:
            continue
        direction = "bullish" if primary_seed > 0 else "bearish"
        sign = 1 if direction == "bullish" else -1
        conflict = (_score_sign(base_score) != 0 and _score_sign(rolling_score) != 0 and _score_sign(base_score) != _score_sign(rolling_score))

        confidence, conf_reason, conf_adj = confidence_for(coin, direction)
        m_adj, m_reasons = market_adjust(direction, btc_ctx, ctx_map.get(coin, {}))
        p_adj, p_reasons, stype = position_adjust(direction, ctx_map.get(coin, {}))
        lev_adj, lev_reasons, lev_risks, lev_fields = leverage_signal_adjust(direction, lev_map.get(coin, {}))
        risk_adj, risk_reasons, risk_risks, risk_fields = risk_filter_adjust(direction, (risk_map or {}).get(coin, {}))
        state = classify_state(direction, perp_active, spot_active, coin, thresholds, stype)
        if state == "不明确" and abs(rolling_score) > 0:
            state = "滚动建仓/减仓"

        # 3) alert_score：短线报警。滚动只允许少量同向确认，不能靠30d滚动分打爆强信号。
        alert_score = base_score
        if _same_sign(base_score, rolling_score):
            alert_score += _signed_cap(rolling_score * 0.25, ALERT_SCORE_ROLLING_CONFIRM_CAP)
        alert_score += conf_adj * 0.25 + m_adj * 0.25 + p_adj * 0.25 + lev_adj * 0.50 + risk_adj * 0.50

        # 4) long_score：长期资格。必须主要来自滚动建仓和杠杆健康，不靠单轮异动。
        long_score = rolling_score
        if _same_sign(rolling_score, base_score):
            long_score += _signed_cap(base_score * 0.15, 0.8)
        long_score += conf_adj * 0.60 + m_adj * 0.35 + p_adj * 0.50 + lev_adj * 0.90 + risk_adj * 0.70

        # 5) 长期资格门槛/风控：现货-only、单钱包集中、持续性不足、窗口不成熟都不能直接进长期单。
        spot_only_risk = bool(rolling_parts.get("rolling_spot_only_risk"))
        concentration_risk = bool(rolling_parts.get("rolling_concentration_risk"))
        persistence_risk = bool(rolling_parts.get("rolling_persistence_risk"))
        gap_risk = bool(rolling_parts.get("rolling_gap_risk"))
        immature_risk = bool(rolling_parts.get("rolling_immature_risk"))
        rolling_suspect = spot_only_risk or concentration_risk or persistence_risk or gap_risk or immature_risk

        best_long_ratio = safe_float(rolling_parts.get("best_longterm_leverage_ratio")) or 0.0
        best_high_ratio = safe_float(rolling_parts.get("best_highrisk_leverage_ratio")) or 0.0
        best_lev_health = safe_float(rolling_parts.get("best_leverage_health"))
        best_wallet_count = int(rolling_parts.get("best_wallet_count") or 0)
        best_spot_share = safe_float(rolling_parts.get("best_spot_share")) or 0.0
        best_window = str(rolling_parts.get("best_window") or "")
        selected_groups = str(rolling_parts.get("rolling_selected_groups") or "")

        leverage_confirm = (
            (best_lev_health is not None and best_lev_health >= LONG_SCORE_MIN_LEVERAGE_CONFIRM) or
            (best_long_ratio >= 0.55 and best_high_ratio <= 0.35) or
            ((safe_float(lev_fields.get("longterm_leverage_ratio")) or 0.0) >= 0.55 and (safe_float(lev_fields.get("highrisk_leverage_ratio")) or 0.0) <= 0.35) or
            (abs(lev_adj) >= 0.55 and _score_sign(lev_adj) == _score_sign(rolling_score))
        )
        has_perp_confirm = abs(perp_active) >= pth * 0.35 or abs(float(rolling_map.get(coin, {}).get(f"perp_{best_window}") or 0.0)) >= pth * 0.5 or leverage_confirm
        mature_window = bool(selected_groups) and not immature_risk
        long_direction_ok = _score_sign(long_score) == _score_sign(rolling_score) and _score_sign(rolling_score) != 0

        long_block_reasons: List[str] = []
        if not mature_window:
            long_block_reasons.append("滚动窗口未成熟")
        if persistence_risk:
            long_block_reasons.append("持续性不足")
        if concentration_risk:
            long_block_reasons.append("钱包集中度过高")
        if spot_only_risk:
            long_block_reasons.append("现货主导，可能是转账/归集/划转")
        if LONG_SCORE_REQUIRE_PERP_CONFIRM and not has_perp_confirm:
            long_block_reasons.append("缺少合约/低杠杆确认")
        if best_high_ratio >= 0.60:
            long_block_reasons.append("高杠杆占比过高")
        if conflict:
            long_block_reasons.append("本轮异动与滚动方向冲突")

        if spot_only_risk and not leverage_confirm:
            long_score = _signed_cap(long_score, LONG_SCORE_SPOT_ONLY_CAP)
        if concentration_risk:
            long_score = _signed_cap(long_score, LONG_SCORE_CONCENTRATION_CAP)
        if persistence_risk:
            long_score = _signed_cap(long_score, LONG_SCORE_PERSISTENCE_CAP)
        if immature_risk or gap_risk:
            long_score = _signed_cap(long_score, max(min_watch - 0.3, 3.5))
        if conflict:
            alert_score *= 0.65
            long_score *= 0.65

        long_qualified = (
            DUAL_SCORE_MODE and
            long_direction_ok and
            mature_window and
            not persistence_risk and
            not concentration_risk and
            not (spot_only_risk and not leverage_confirm) and
            (not LONG_SCORE_REQUIRE_PERP_CONFIRM or has_perp_confirm) and
            best_high_ratio < 0.60 and
            abs(long_score) >= min_watch
        )

        # 6) 输出分类：不再只看一个 final_score。
        category = "只观察"
        if abs(alert_score) >= th_score:
            if spot_only_risk and abs(perp_active) < pth * 0.35:
                category = "现货异常变化"
            elif best_high_ratio >= 0.60 or (safe_float(lev_fields.get("highrisk_leverage_ratio")) or 0.0) >= 0.60:
                category = "高杠杆短线异动"
            else:
                category = "短线突发异动"
        if long_qualified and abs(long_score) >= th_score:
            category = "低杠杆长期候选"
        elif long_qualified and abs(long_score) >= min_watch and category == "只观察":
            category = "滚动建仓观察"
        elif abs(alert_score) >= min_watch and category == "只观察":
            category = "短线异动观察"

        # final_score 只保留兼容字段：代表当前最应该展示的主分数。
        if category in ("低杠杆长期候选", "滚动建仓观察"):
            final_score = long_score
        else:
            final_score = alert_score if abs(alert_score) >= abs(long_score) or not long_qualified else long_score

        watchlist = "observe"
        if category == "低杠杆长期候选" and abs(long_score) >= th_score:
            watchlist = "long" if long_score > 0 else "short"
        else:
            watchlist = "observe"

        if category == "低杠杆长期候选":
            conclusion = ("做多观察" if long_score > 0 else "做空观察") + " / 低杠杆长期候选"
        elif category in ("短线突发异动", "高杠杆短线异动"):
            conclusion = ("偏多" if alert_score > 0 else "偏空") + "短线雷达 / 不等于长期单"
        elif category == "现货异常变化":
            conclusion = "现货异常观察 / 等待合约低杠杆确认"
        else:
            conclusion = "只观察 / 等待确认"

        risk_parts: List[str] = []
        if confidence == "低":
            risk_parts.append("历史样本/胜率不足")
        if state in ("可能对冲", "现货流出+合约做多，换杠杆/冲突", "不明确"):
            risk_parts.append(f"信号状态：{state}")
        if spot_only_risk:
            risk_parts.append("滚动信号现货主导，可能是转账/归集/划转，不等于合约方向")
        if concentration_risk:
            risk_parts.append("滚动信号钱包集中度过高，可能是单钱包异动")
        if persistence_risk:
            risk_parts.append("滚动信号持续性不足，不能按长期建仓处理")
        if immature_risk:
            risk_parts.append("长窗口未成熟，不能按15d/30d成熟信号处理")
        if gap_risk:
            risk_parts.append("窗口内含断跑gap")
        if conflict:
            risk_parts.append("本轮异动与滚动方向冲突")
        if long_block_reasons and category != "低杠杆长期候选":
            risk_parts.append("长期资格未通过：" + "、".join(long_block_reasons[:5]))
        if stype in ("高位追多", "低位追空"):
            risk_parts.append("价格位置有追涨杀跌风险")
        risk_parts.extend(lev_risks)
        risk_parts.extend(risk_risks)
        risk = "；".join(risk_parts) if risk_parts else "无明显额外风险"

        reasons: List[str] = []
        reasons.extend(base_reasons)
        reasons.extend(rolling_reasons)
        if long_block_reasons and category != "低杠杆长期候选":
            reasons.append("长期资格未通过：" + "、".join(long_block_reasons))
        reason = "；".join(reasons + [conf_reason] + m_reasons + p_reasons + lev_reasons + risk_reasons)

        score_parts = {
            "base_flow": round(base_score, 4),
            "rolling_flow": round(rolling_parts.get("rolling_flow_core", rolling_score - float(rolling_parts.get("rolling_leverage") or 0.0)), 4),
            "rolling_leverage": round(float(rolling_parts.get("rolling_leverage") or 0.0), 4),
            "confidence": round(conf_adj, 4),
            "market": round(m_adj, 4),
            "price_position": round(p_adj, 4),
            "leverage": round(lev_adj, 4),
            "funding_liquidity": round(risk_adj, 4),
            "alert_score": round(alert_score, 4),
            "long_score": round(long_score, 4),
            "final_score": round(final_score, 4),
            "long_qualified": 1 if long_qualified else 0,
            "signal_category": category,
            "leverage_confirm": 1 if leverage_confirm else 0,
            "has_perp_confirm": 1 if has_perp_confirm else 0,
        }
        score_parts.update(rolling_parts)
        ctx = ctx_map.get(coin, {})
        rows.append({
            "run_id": run_id,
            "coin": coin,
            "direction": direction,
            "score": base_score + rolling_score,
            "alert_score": alert_score,
            "long_score": long_score,
            "long_qualified": long_qualified,
            "signal_category": category,
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
            "funding_rate_pct": risk_fields.get("funding_rate_pct"),
            "funding_risk": risk_fields.get("funding_risk"),
            "day_volume_usd": risk_fields.get("day_volume_usd"),
            "liquidity_risk": risk_fields.get("liquidity_risk"),
            "score_parts": score_parts,
            "conclusion": conclusion,
            "risk": risk,
            "reason": reason,
        })
    # 先排长期候选，再排短线报警，再排普通观察。
    rows.sort(key=lambda x: (1 if x.get("signal_category") == "低杠杆长期候选" else 0, abs(x.get("alert_score") or 0.0), abs(x.get("long_score") or 0.0), abs(x["final_score"])), reverse=True)
    save_coin_signals(run_id, rows)
    return rows

def save_coin_signals(run_id: int, rows: List[Dict[str, Any]]) -> None:
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM coin_signals WHERE run_id=?", (run_id,))
    created_at = now_str()
    created_at_cn = display_time_from_utc(created_at)
    cur.executemany("""
    INSERT INTO coin_signals (
        run_id, created_at, created_at_cn, coin, direction, score, confidence, signal_type, signal_state, watchlist,
        perp_active, spot_active, weighted_flow, price_position, pct_1h, pct_4h, pct_24h,
        final_score, threshold_score, avg_leverage, avg_liq_distance, longterm_leverage_ratio, highrisk_leverage_ratio, leverage_note,
        conclusion, risk, reason
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [(
        run_id, created_at, created_at_cn, r["coin"], r["direction"], r["score"], r["confidence"], r["signal_type"], r["signal_state"], r["watchlist"],
        r["perp_active"], r["spot_active"], r["weighted_flow"], r["price_position"], r["pct_1h"], r["pct_4h"], r["pct_24h"],
        r["final_score"], r["threshold_score"], r.get("avg_leverage"), r.get("avg_liq_distance"), r.get("longterm_leverage_ratio"), r.get("highrisk_leverage_ratio"), r.get("leverage_note"),
        r["conclusion"], r["risk"], r["reason"]
    ) for r in rows])
    conn.commit()
    conn.close()



def export_signal_explain_files(run_id: int, signals: List[Dict[str, Any]]) -> None:
    """导出每个信号的加减分来源，解决“为什么出这个信号”的问题。"""
    if not SIGNAL_EXPLAIN_MODE:
        return
    ensure_dirs()
    rows: List[Dict[str, Any]] = []
    for s in signals:
        parts = s.get("score_parts") or {}
        row = {
            "run_id": run_id,
            "signal_time_cn": signal_time_cn(run_id),
            "signal_time_utc": now_str(),
            "coin": s.get("coin"),
            "direction": s.get("direction"),
            "direction_cn": dir_cn(s.get("direction")),
            "final_score": s.get("final_score"),
            "alert_score": s.get("alert_score"),
            "long_score": s.get("long_score"),
            "long_qualified": 1 if s.get("long_qualified") else 0,
            "signal_category": s.get("signal_category"),
            "threshold_score": s.get("threshold_score"),
            "watchlist": s.get("watchlist"),
            "confidence": s.get("confidence"),
            "signal_state": s.get("signal_state"),
            "signal_type": s.get("signal_type"),
            "base_flow_score": parts.get("base_flow"),
            "rolling_flow_score": parts.get("rolling_flow"),
            "rolling_leverage_score": parts.get("rolling_leverage"),
            "leverage_confirm": parts.get("leverage_confirm"),
            "has_perp_confirm": parts.get("has_perp_confirm"),
            "confidence_adj": parts.get("confidence"),
            "market_adj": parts.get("market"),
            "price_position_adj": parts.get("price_position"),
            "leverage_adj": parts.get("leverage"),
            "funding_liquidity_adj": parts.get("funding_liquidity"),
            "perp_active": s.get("perp_active"),
            "spot_active": s.get("spot_active"),
            "weighted_flow": s.get("weighted_flow"),
            "avg_leverage": s.get("avg_leverage"),
            "avg_liq_distance": s.get("avg_liq_distance"),
            "funding_rate_pct": s.get("funding_rate_pct"),
            "funding_risk": s.get("funding_risk"),
            "day_volume_usd": s.get("day_volume_usd"),
            "liquidity_risk": s.get("liquidity_risk"),
            "risk": s.get("risk"),
            "reason": s.get("reason"),
            "dominant_wallets": s.get("dominant_wallets", "-"),
        }
        rows.append(row)
    if rows:
        with open(os.path.join(DETAILS_DIR, "signal_explain_latest.csv"), "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader(); writer.writerows(rows)
    with open(os.path.join(REPORT_DIR, "signal_explain_report.txt"), "w", encoding="utf-8") as f:
        f.write("【信号解释】\n")
        f.write(f"run_id={run_id} | 更新时间{DISPLAY_TZ_NAME}={signal_time_cn(run_id)} | UTC={now_str()}\n\n")
        if not signals:
            f.write("本轮暂无信号。\n")
        for s in signals[:TOP_N]:
            parts = s.get("score_parts") or {}
            f.write(f"{s['coin']} {dir_cn(s['direction'])} 时间={signal_time_cn(run_id)} final={s['final_score']:+.2f}/阈值{s['threshold_score']:.1f}\n")
            f.write(
                f"  资金={parts.get('base_flow',0):+.2f} | 历史={parts.get('confidence',0):+.2f} | "
                f"市场={parts.get('market',0):+.2f} | 位置={parts.get('price_position',0):+.2f} | "
                f"杠杆={parts.get('leverage',0):+.2f} | 资金费率/流动性={parts.get('funding_liquidity',0):+.2f}\n"
            )
            f.write(f"  风险：{s.get('risk')}\n")
            f.write(f"  原因：{s.get('reason')}\n\n")



def _dominant_action_text(a: Dict[str, Any]) -> str:
    """把单个钱包动作格式化成信号主导钱包说明。"""
    addr = short_addr(a.get("address") or "")
    groups = a.get("groups") or ""
    market = a.get("market") or ""
    coin = a.get("coin") or ""
    active = fmt_money(a.get("active_delta"))
    action = action_type_cn(a.get("action_type") or "", a.get("side") or "")
    if market == "perp":
        lev = a.get("leverage")
        lev_txt = f" {fmt_num(lev)}x" if lev is not None else ""
        mm = a.get("margin_mode") or ""
        liq = a.get("liq_distance_pct")
        liq_txt = f" 强平距={fmt_pct(liq)}" if liq is not None else ""
        style = a.get("leverage_style") or ""
        return f"{addr}[{groups}] {coin}合约{side_cn(a.get('side'))}{lev_txt} {action} {active} {mm}{liq_txt} {style}".strip()
    if market == "spot":
        detail = a.get("spot_increases") if (safe_float(a.get("active_delta")) or 0) >= 0 else a.get("spot_decreases")
        if not detail or detail == "-":
            detail = f"{coin} {active}"
        return f"{addr}[{groups}] 现货{action} {detail}".strip()
    return f"{addr}[{groups}] {coin} {market} {action} {active}".strip()


def build_dominant_wallets_by_coin(actions: List[Dict[str, Any]], limit: int = DOMINANT_WALLET_TOP_N) -> Dict[str, str]:
    """按币种列出本轮对信号贡献最大的几个钱包动作。"""
    if not DOMINANT_WALLETS_MODE:
        return {}
    by_coin: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for a in actions or []:
        coin = a.get("coin")
        if not coin:
            continue
        active = safe_float(a.get("active_delta")) or 0.0
        if active == 0:
            continue
        by_coin[coin].append(a)
    out: Dict[str, str] = {}
    for coin, rows in by_coin.items():
        rows = sorted(rows, key=lambda x: abs(safe_float(x.get("active_delta")) or 0.0), reverse=True)
        out[coin] = compact_join([_dominant_action_text(r) for r in rows[:limit]], limit)
    return out


def attach_dominant_wallets(signals: List[Dict[str, Any]], actions: List[Dict[str, Any]]) -> None:
    """把主导钱包名单挂到每个信号上，不改变评分，只增强解释性。"""
    dom = build_dominant_wallets_by_coin(actions)
    for s in signals or []:
        s["dominant_wallets"] = dom.get(s.get("coin"), "-")


def write_data_quality_report(run_id: int, ok_rate: float, wallet_rows: List[Dict[str, Any]], note: str = "") -> None:
    """导出本轮数据质量报告。低成功率时用于提醒：本轮信号/生命周期不应被过度解读。"""
    ensure_dirs()
    total = len(wallet_rows or [])
    ok = sum(1 for w in wallet_rows if w.get("status") == "ok")
    partial = sum(1 for w in wallet_rows if w.get("status") == "partial")
    failed = sum(1 for w in wallet_rows if w.get("status") == "failed")
    bad_examples = [w for w in wallet_rows if w.get("status") != "ok"][:20]
    path = os.path.join(DETAILS_DIR, "data_quality_report.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("【数据质量 / API异常保护】\n")
        f.write(f"run_id={run_id} | {DISPLAY_TZ_NAME}={signal_time_cn(run_id)} | UTC={now_str()} | note={note}\n")
        f.write(f"监控钱包={total} | ok={ok} | partial={partial} | failed={failed} | 成功率={ok_rate*100:.2f}% | 最低要求={MIN_OK_RATE*100:.2f}%\n")
        if DATA_ANOMALY_PROTECT_MODE and ok_rate < MIN_OK_RATE:
            f.write("\n⚠️ 本轮成功率低于阈值：脚本会跳过新信号生成/生命周期结算，避免把 API 抽风误判成信号消失或钱包平仓。\n")
        else:
            f.write("\n本轮数据质量通过，可以正常参考信号。\n")
        if bad_examples:
            f.write("\n异常钱包样例：\n")
            for w in bad_examples:
                f.write(f"  {short_addr(w.get('address') or '')} status={w.get('status')} err={w.get('error') or '-'}\n")


def _signals_by_coin(signals: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    by: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for s in signals or []:
        if s.get("coin"):
            by[s.get("coin")].append(s)
    for coin in by:
        by[coin].sort(key=lambda x: abs(safe_float(x.get("final_score")) or 0.0), reverse=True)
    return by


def _lifecycle_missing_reason(row: Dict[str, Any], current_signals: List[Dict[str, Any]], current_longterms: List[Dict[str, Any]]) -> str:
    """给信号消失/长期单失效提供更细原因，而不是只写“消失”。"""
    typ = row.get("lifecycle_type")
    coin = row.get("coin")
    direction = row.get("direction")
    by_coin = _signals_by_coin(current_signals)
    sigs = by_coin.get(coin, [])
    same = [s for s in sigs if s.get("direction") == direction]
    opp = [s for s in sigs if s.get("direction") and s.get("direction") != direction]
    if typ == "strong":
        if same:
            s0 = same[0]
            return (
                f"同方向信号降级：final_score={safe_float(s0.get('final_score')) or 0:+.2f}，"
                f"强信号阈值={safe_float(s0.get('threshold_score')) or 0:.1f}；"
                f"当前状态={s0.get('signal_state')}；风险={s0.get('risk')}"
            )
        if opp:
            o = opp[0]
            return f"同币种出现反向结构：当前{o.get('coin')} {dir_cn(o.get('direction'))} final={safe_float(o.get('final_score')) or 0:+.2f}"
        return "本轮跌出强信号/观察列表：可能是资金流减弱、主导钱包撤退、价格/杠杆/资金费率条件变差。"
    # longterm
    matching_lt = [c for c in current_longterms or [] if c.get("coin") == coin and c.get("direction") == direction]
    if matching_lt:
        c = matching_lt[0]
        return f"长期单降级：当前动作={c.get('action')}；final={c.get('final_score')}；连续={c.get('streak')}；状态={c.get('signal_state')}"
    if same:
        s0 = same[0]
        return (
            f"仍有同方向普通信号，但不再满足长期单：final={safe_float(s0.get('final_score')) or 0:+.2f}；"
            f"状态={s0.get('signal_state')}；类型={s0.get('signal_type')}；风险={s0.get('risk')}"
        )
    if opp:
        o = opp[0]
        return f"长期方向被反向信号压制：当前{dir_cn(o.get('direction'))} final={safe_float(o.get('final_score')) or 0:+.2f}"
    return "长期单条件消失：连续性/钱包质量/杠杆结构/价格位置/费率流动性中至少一项不再满足。"


def _signal_dir_return_pct(direction: str, entry_px: Optional[float], exit_px: Optional[float]) -> Optional[float]:
    """信号生命周期方向收益，单位百分比。只看信号价格到退出价格，不看账户权益。"""
    entry = safe_float(entry_px)
    px = safe_float(exit_px)
    if entry is None or px is None or entry <= 0 or px <= 0:
        return None
    if direction == "bullish":
        return (px - entry) / entry * 100.0
    if direction == "bearish":
        return (entry - px) / entry * 100.0
    return None


def _active_lifecycle_signal_rows(signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """强信号生命周期：只跟踪真正达到强信号阈值的币。"""
    rows: List[Dict[str, Any]] = []
    for s in signals:
        try:
            if abs(safe_float(s.get("alert_score")) or 0.0) >= (safe_float(s.get("threshold_score")) or 0.0) and s.get("signal_category") != "低杠杆长期候选":
                rows.append({
                    "type": "strong",
                    "coin": s.get("coin"),
                    "direction": s.get("direction"),
                    "score": safe_float(s.get("alert_score")) or 0.0,
                    "reason": s.get("reason") or "短线强信号仍在",
                })
        except Exception:
            continue
    return [r for r in rows if r.get("coin") and r.get("direction")]


def _active_lifecycle_longterm_rows(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """长期单生命周期：只跟踪明确“可进入低杠杆长期观察”的候选。"""
    rows: List[Dict[str, Any]] = []
    for c in candidates:
        action = str(c.get("action") or "")
        if "可进入低杠杆长期观察" not in action:
            continue
        direction = c.get("direction") or ("bullish" if "多" in str(c.get("direction_cn")) else "bearish")
        rows.append({
            "type": "longterm",
            "coin": c.get("coin"),
            "direction": direction,
            "score": safe_float(c.get("long_term_score")) or safe_float(c.get("final_score")) or 0.0,
            "reason": f"{action} | long={c.get('long_score')} alert={c.get('alert_score')} | 连续={c.get('streak')} | {c.get('signal_state')}",
        })
    return [r for r in rows if r.get("coin") and r.get("direction")]


def update_signal_lifecycles(run_id: int, signals: List[Dict[str, Any]], longterm_candidates: List[Dict[str, Any]], prices: Dict[str, float], data_quality_ok: bool = True, skip_reason: str = "") -> List[Dict[str, Any]]:
    """按提示逻辑追踪信号生命周期。\n\n    固定周期回测回答“信号出现后未来 24h/7d/30d 怎么样”。\n    生命周期追踪回答“你按提示看，从信号出现到消失/反转，真实表现怎么样”。\n    """
    if not SIGNAL_LIFECYCLE_MODE:
        return []
    if DATA_ANOMALY_PROTECT_MODE and not data_quality_ok:
        print(f"数据质量异常，跳过信号生命周期更新：{skip_reason}", flush=True)
        export_signal_lifecycle_files()
        return []
    now = now_str()
    active_rows = _active_lifecycle_signal_rows(signals) + _active_lifecycle_longterm_rows(longterm_candidates)
    active_map: Dict[Tuple[str, str, str], Dict[str, Any]] = {(r["type"], r["coin"], r["direction"]): r for r in active_rows}
    active_coin_type_dirs: Dict[Tuple[str, str], set] = defaultdict(set)
    for typ, coin, direction in active_map:
        active_coin_type_dirs[(typ, coin)].add(direction)

    conn = db_conn()
    cur = conn.cursor()
    closed: List[Dict[str, Any]] = []

    def _get_px(coin: str) -> Optional[float]:
        return safe_float(prices.get(coin))

    def _log(lifecycle_id: int, typ: str, event_type: str, coin: str, direction: str, px: Optional[float], score: Optional[float], missing_count: int, ret: Optional[float], reason: str) -> None:
        cur.execute("""
        INSERT INTO signal_lifecycle_events(lifecycle_id, run_id, created_at, lifecycle_type, event_type, coin, direction, px, score, missing_count, return_pct, reason)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (lifecycle_id, run_id, now, typ, event_type, coin, direction, px, score, missing_count, ret, reason))

    def _close(row: Dict[str, Any], exit_px: Optional[float], reason: str) -> Optional[Dict[str, Any]]:
        if exit_px is None or exit_px <= 0:
            return None
        ret = _signal_dir_return_pct(row.get("direction"), row.get("entry_px"), exit_px)
        holding = _hours_between(row.get("entry_time"), now)
        cur.execute("""
        UPDATE signal_lifecycles
        SET status='closed', exit_run_id=?, exit_time=?, exit_px=?, exit_reason=?, lifecycle_return_pct=?, holding_hours=?
        WHERE lifecycle_id=? AND status='open'
        """, (run_id, now, exit_px, reason, ret, holding, row["lifecycle_id"]))
        _log(row["lifecycle_id"], row.get("lifecycle_type"), "close", row.get("coin"), row.get("direction"), exit_px, row.get("last_score"), safe_int(row.get("missing_count"), 0), ret, reason)
        item = dict(row)
        item.update({
            "exit_run_id": run_id,
            "exit_time": now,
            "exit_px": exit_px,
            "exit_reason": reason,
            "lifecycle_return_pct": ret,
            "holding_hours": holding,
        })
        closed.append(item)
        return item

    # 1) 先处理已有 open 生命周期：刷新、缺失计数、消失/反向结算。
    cur.execute("SELECT * FROM signal_lifecycles WHERE status='open'")
    open_rows = [dict(x) for x in cur.fetchall()]
    for row in open_rows:
        typ = row.get("lifecycle_type")
        coin = row.get("coin")
        direction = row.get("direction")
        key = (typ, coin, direction)
        px = _get_px(coin)
        if not typ or not coin or not direction:
            continue
        if key in active_map:
            sig = active_map[key]
            score = safe_float(sig.get("score")) or 0.0
            cur.execute("""
            UPDATE signal_lifecycles
            SET last_seen_run_id=?, last_seen_at=?, last_score=?, max_score=?, missing_count=0
            WHERE lifecycle_id=?
            """, (run_id, now, score, max(abs(safe_float(row.get("max_score")) or 0.0), abs(score)), row["lifecycle_id"]))
            _log(row["lifecycle_id"], typ, "refresh", coin, direction, px, score, 0, _signal_dir_return_pct(direction, row.get("entry_px"), px), sig.get("reason") or "信号持续")
            continue

        # 同币同类型出现反向信号，直接结算。
        dirs_now = active_coin_type_dirs.get((typ, coin), set())
        opposite = "bearish" if direction == "bullish" else "bullish"
        if opposite in dirs_now:
            _close(row, px, "出现反向信号")
            continue

        miss = safe_int(row.get("missing_count"), 0) + 1
        cur.execute("UPDATE signal_lifecycles SET missing_count=? WHERE lifecycle_id=?", (miss, row["lifecycle_id"]))
        miss_reason = _lifecycle_missing_reason(row, signals, longterm_candidates)
        _log(row["lifecycle_id"], typ, "missing", coin, direction, px, row.get("last_score"), miss, _signal_dir_return_pct(direction, row.get("entry_px"), px), miss_reason)
        max_miss = STRONG_SIGNAL_MISSING_ROUNDS if typ == "strong" else LONGTERM_SIGNAL_MISSING_ROUNDS
        if miss >= max_miss:
            _close(row, px, f"连续{miss}轮信号消失｜{miss_reason}")

    # 2) 再开新生命周期。若同币同类型反方向 open 还存在，会先关闭反方向。
    for key, sig in active_map.items():
        typ, coin, direction = key
        px = _get_px(coin)
        if px is None or px <= 0:
            continue
        score = safe_float(sig.get("score")) or 0.0
        cur.execute("SELECT * FROM signal_lifecycles WHERE lifecycle_type=? AND coin=? AND direction=? AND status='open' LIMIT 1", (typ, coin, direction))
        if cur.fetchone():
            continue
        opposite = "bearish" if direction == "bullish" else "bullish"
        cur.execute("SELECT * FROM signal_lifecycles WHERE lifecycle_type=? AND coin=? AND direction=? AND status='open'", (typ, coin, opposite))
        for old in [dict(x) for x in cur.fetchall()]:
            _close(old, px, "新反向信号出现")
        cur.execute("""
        INSERT INTO signal_lifecycles(lifecycle_type, coin, direction, status, entry_run_id, entry_time, entry_px, entry_score, max_score, last_seen_run_id, last_seen_at, last_score, missing_count, note)
        VALUES (?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
        """, (typ, coin, direction, run_id, now, px, score, abs(score), run_id, now, score, sig.get("reason") or "新信号出现"))
        lid = cur.lastrowid
        _log(lid, typ, "open", coin, direction, px, score, 0, 0.0, sig.get("reason") or "新信号出现")

    conn.commit()
    conn.close()
    export_signal_lifecycle_files()
    return closed


def export_signal_lifecycle_files(days: int = SIGNAL_BACKTEST_WINDOW_DAYS) -> None:
    if not SIGNAL_LIFECYCLE_MODE:
        return
    ensure_dirs()
    since = (utc_now() - dt.timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT lifecycle_id, lifecycle_type, coin, direction, status, entry_time, entry_px, entry_score, max_score,
           last_seen_at, last_score, missing_count, exit_time, exit_px, exit_reason, lifecycle_return_pct, holding_hours, note
    FROM signal_lifecycles
    WHERE entry_time >= ? OR status='open'
    ORDER BY status DESC, COALESCE(exit_time, last_seen_at, entry_time) DESC
    """, (since,))
    rows = [dict(x) for x in cur.fetchall()]
    conn.close()

    csv_path = os.path.join(DETAILS_DIR, "signal_lifecycle_latest.csv")
    fields = ["lifecycle_id", "lifecycle_type", "coin", "direction", "status", "entry_time", "entry_px", "entry_score", "max_score", "last_seen_at", "last_score", "missing_count", "exit_time", "exit_px", "exit_reason", "lifecycle_return_pct", "holding_hours", "note"]
    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader(); writer.writerows(rows)

    report_path = os.path.join(DETAILS_DIR, "signal_lifecycle_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("【信号生命周期追踪】\n")
        f.write(f"窗口：过去{days}天 + 当前未结束信号\n")
        f.write("说明：固定周期回测看未来 24h/7d/30d；生命周期回测看信号从出现到消失/反转的真实表现。\n")
        f.write(f"强信号消失结算：连续{STRONG_SIGNAL_MISSING_ROUNDS}轮消失；长期单失效结算：连续{LONGTERM_SIGNAL_MISSING_ROUNDS}轮消失。\n\n")
        for typ in ("strong", "longterm"):
            subset = [r for r in rows if r.get("lifecycle_type") == typ]
            closed = [r for r in subset if r.get("status") == "closed" and safe_float(r.get("lifecycle_return_pct")) is not None]
            open_rows = [r for r in subset if r.get("status") == "open"]
            f.write(f"【{'强信号' if typ=='strong' else '长期单'}】未结束={len(open_rows)} | 已结束样本={len(closed)}\n")
            if closed:
                vals = [safe_float(r.get("lifecycle_return_pct")) for r in closed if safe_float(r.get("lifecycle_return_pct")) is not None]
                win = sum(1 for v in vals if v and v > 0) / len(vals) * 100 if vals else 0.0
                avg = sum(vals) / len(vals) if vals else 0.0
                med = sorted(vals)[len(vals)//2] if vals else 0.0
                f.write(f"生命周期普通胜率={win:.1f}% | 平均收益={avg:+.2f}% | 中位={med:+.2f}%\n")
            if open_rows:
                f.write("未结束：\n")
                for r in open_rows[:10]:
                    cur_ret = _signal_dir_return_pct(r.get("direction"), r.get("entry_px"), r.get("exit_px") or r.get("entry_px"))
                    f.write(f"  {r.get('coin')} {dir_cn(r.get('direction'))} | entry={fmt_num(r.get('entry_px'))} | last_score={fmt_num(r.get('last_score'))} | miss={r.get('missing_count')}\n")
            if closed:
                f.write("最近结束：\n")
                for r in closed[:10]:
                    f.write(f"  {r.get('exit_time')} {r.get('coin')} {dir_cn(r.get('direction'))} | 收益={fmt_pct(r.get('lifecycle_return_pct'))} | 持续={fmt_num(r.get('holding_hours'))}h | 原因={r.get('exit_reason')}\n")
            f.write("\n")


def get_closed_lifecycle_events(run_id: int) -> List[Dict[str, Any]]:
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT lifecycle_id, lifecycle_type, coin, direction, entry_time, entry_px, exit_time, exit_px, exit_reason, lifecycle_return_pct, holding_hours
    FROM signal_lifecycles
    WHERE exit_run_id=?
    ORDER BY exit_time DESC
    """, (run_id,))
    rows = [dict(x) for x in cur.fetchall()]
    conn.close()
    return rows


def create_longterm_events(run_id: int, candidates: List[Dict[str, Any]], prices: Dict[str, float]) -> int:
    if not BACKTEST_MODE or not candidates:
        return 0
    conn = db_conn()
    cur = conn.cursor()
    created = 0
    for c in candidates:
        coin = c.get("coin")
        px = prices.get(coin)
        if px is None or px <= 0:
            continue
        direction = "bullish" if "多" in str(c.get("direction_cn")) else "bearish"
        reason = f"长期分={c.get('long_term_score')} long={c.get('long_score')} alert={c.get('alert_score')} 连续={c.get('streak')} 动作={c.get('action')}"
        try:
            cur.execute("""
            INSERT INTO longterm_events(run_id, created_at, coin, direction, score, entry_px, reason)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (run_id, now_str(), coin, direction, c.get("long_term_score"), px, reason))
            created += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit(); conn.close()
    return created


def _backtest_periods() -> List[Tuple[str, str, float]]:
    """回测周期和对应门槛。ret 字段单位是百分比，例如 +3.2 表示 +3.2%。"""
    return [
        ("24h", "ret_24h", BACKTEST_HURDLE_24H),
        ("72h", "ret_72h", BACKTEST_HURDLE_72H),
        ("7d", "ret_7d", BACKTEST_HURDLE_7D),
        ("15d", "ret_15d", BACKTEST_HURDLE_15D),
        ("30d", "ret_30d", BACKTEST_HURDLE_30D),
    ]


def _export_event_backtest_table(table: str, filename: str, report_title: str, days: int = SIGNAL_BACKTEST_WINDOW_DAYS) -> List[Dict[str, Any]]:
    """导出强信号/长期单回测。

    现在同时显示两套胜率：
    - 普通胜率：方向收益 > 0，只说明方向有没有走对。
    - 门槛胜率：方向收益达到 24h/72h/7d/15d/30d 门槛，更适合评估低杠杆长期单质量。
    """
    since = (utc_now() - dt.timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    conn = db_conn()
    cur = conn.cursor()
    cur.execute(f"""
    SELECT event_id, run_id, created_at, coin, direction, score, entry_px,
           ret_1h, ret_4h, ret_24h, ret_72h, ret_7d, ret_15d, ret_30d, reason
    FROM {table}
    WHERE created_at >= ?
    ORDER BY created_at DESC
    """, (since,))
    rows = [dict(x) for x in cur.fetchall()]
    conn.close()
    ensure_dirs()

    periods = _backtest_periods()
    out_rows: List[Dict[str, Any]] = []
    for r in rows:
        rr = dict(r)
        for label, col, hurdle in periods:
            v = safe_float(r.get(col))
            key = label.replace("h", "h").replace("d", "d")
            rr[f"direction_win_{key}"] = "" if v is None else int(v > 0)
            rr[f"hurdle_win_{key}"] = "" if v is None else int(v >= hurdle)
            rr[f"hurdle_{key}_pct"] = hurdle
        out_rows.append(rr)

    if out_rows:
        # 固定主要列顺序，避免每次字段顺序乱。
        base_fields = ["event_id", "run_id", "created_at", "coin", "direction", "score", "entry_px", "ret_1h", "ret_4h", "ret_24h", "ret_72h", "ret_7d", "ret_15d", "ret_30d", "reason"]
        extra_fields: List[str] = []
        for label, _, _ in periods:
            key = label.replace("h", "h").replace("d", "d")
            extra_fields += [f"direction_win_{key}", f"hurdle_win_{key}", f"hurdle_{key}_pct"]
        fields = base_fields + extra_fields
        with open(os.path.join(DETAILS_DIR, filename), "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader(); writer.writerows(out_rows)
    else:
        with open(os.path.join(DETAILS_DIR, filename), "w", encoding="utf-8-sig", newline="") as f:
            f.write("empty\n")

    txt_name = filename.replace(".csv", "_report.txt")
    with open(os.path.join(DETAILS_DIR, txt_name), "w", encoding="utf-8") as f:
        f.write(f"【{report_title}】\n")
        f.write(f"窗口：过去{days}天 | 事件样本：{len(rows)}\n")
        f.write("说明：普通胜率=方向收益>0；门槛胜率=达到最低收益门槛，更适合长期单。\n")
        f.write("门槛：24h≥{:.1f}% | 72h≥{:.1f}% | 7d≥{:.1f}% | 15d≥{:.1f}% | 30d≥{:.1f}%\n\n".format(
            BACKTEST_HURDLE_24H, BACKTEST_HURDLE_72H, BACKTEST_HURDLE_7D, BACKTEST_HURDLE_15D, BACKTEST_HURDLE_30D
        ))
        for label, col, hurdle in periods:
            vals = [safe_float(r.get(col)) for r in rows if safe_float(r.get(col)) is not None]
            if not vals:
                f.write(f"{label}: 暂无成熟样本\n")
                continue
            direction_win = sum(1 for v in vals if v > 0) / len(vals)
            hurdle_win = sum(1 for v in vals if v >= hurdle) / len(vals)
            avg = sum(vals) / len(vals)
            median = sorted(vals)[len(vals)//2]
            f.write(
                f"{label}: 样本={len(vals)} | 普通胜率={direction_win*100:.1f}% | "
                f"门槛胜率={hurdle_win*100:.1f}% | 门槛≥{hurdle:.1f}% | "
                f"平均方向收益={avg:+.2f}% | 中位={median:+.2f}%\n"
            )
        f.write("\n最近样本：\n")
        for r in rows[:20]:
            parts = []
            for label, col, hurdle in periods:
                v = safe_float(r.get(col))
                if v is None:
                    continue
                mark = "✅" if v >= hurdle else ("↗" if v > 0 else "❌")
                parts.append(f"{label}={v:+.2f}%{mark}")
            f.write(
                f"{r.get('created_at')} {r.get('coin')} {dir_cn(r.get('direction'))} "
                f"score={safe_float(r.get('score')):+.2f} | " + " | ".join(parts[:5]) + "\n"
            )
    return rows



# =========================
# 研究面板 / 信号验证摘要 v1
# =========================
def _safe_pct(v: Any) -> Optional[float]:
    return safe_float(v)


def _agg_ret(rows: List[Dict[str, Any]], col: str, hurdle: float) -> Dict[str, Any]:
    vals = [_safe_pct(r.get(col)) for r in rows if _safe_pct(r.get(col)) is not None]
    if not vals:
        return {"n": 0, "win": None, "hurdle_win": None, "avg": None, "median": None}
    vals_sorted = sorted(vals)
    return {
        "n": len(vals),
        "win": sum(1 for v in vals if v > 0) / len(vals),
        "hurdle_win": sum(1 for v in vals if v >= hurdle) / len(vals),
        "avg": sum(vals) / len(vals),
        "median": vals_sorted[len(vals_sorted)//2],
    }


def _direction_bucket(direction: str) -> str:
    d = (direction or "").lower()
    if d in {"bullish", "long", "多", "偏多"}:
        return "long"
    if d in {"bearish", "short", "空", "偏空"}:
        return "short"
    return d or "unknown"


def _load_event_rows_for_research(table: str, days: int = 90) -> List[Dict[str, Any]]:
    conn = db_conn(); cur = conn.cursor()
    since = (utc_now() - dt.timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    try:
        cur.execute(f"""
        SELECT event_id, run_id, created_at, coin, direction, score, entry_px, reason,
               ret_24h, ret_72h, ret_7d, ret_15d, ret_30d
        FROM {table}
        WHERE created_at >= ?
        ORDER BY created_at DESC
        """, (since,))
        rows = [dict(x) for x in cur.fetchall()]
    except Exception:
        rows = []
    conn.close(); return rows


def export_research_intelligence_files(run_id: int) -> None:
    """导出研究面板：把信号回测、长期多空、钱包画像、币种画像集中到一个摘要里。

    这个模块不改变信号，只用于回答：哪些信号/钱包/币在历史上更有效。
    """
    ensure_dirs()
    periods = [("24h", "ret_24h", BACKTEST_HURDLE_24H), ("72h", "ret_72h", BACKTEST_HURDLE_72H), ("7d", "ret_7d", BACKTEST_HURDLE_7D), ("15d", "ret_15d", BACKTEST_HURDLE_15D), ("30d", "ret_30d", BACKTEST_HURDLE_30D)]
    signal_rows = _load_event_rows_for_research("signal_events", 90)
    longterm_rows = _load_event_rows_for_research("longterm_events", 180)

    # 1) 信号类型摘要：alert vs longterm，long/short 分开。
    summary_rows: List[Dict[str, Any]] = []
    for label, arr in [("alert_signal", signal_rows), ("longterm_candidate", longterm_rows)]:
        for side in ["long", "short", "all"]:
            subset = arr if side == "all" else [r for r in arr if _direction_bucket(r.get("direction")) == side]
            if not subset:
                continue
            base = {"run_id": run_id, "source": label, "side": side, "sample_events": len(subset)}
            for p_label, col, hurdle in periods:
                a = _agg_ret(subset, col, hurdle)
                base[f"{p_label}_n"] = a["n"]
                base[f"{p_label}_win"] = None if a["win"] is None else round(a["win"] * 100, 2)
                base[f"{p_label}_hurdle_win"] = None if a["hurdle_win"] is None else round(a["hurdle_win"] * 100, 2)
                base[f"{p_label}_avg_ret"] = None if a["avg"] is None else round(a["avg"], 4)
                base[f"{p_label}_median_ret"] = None if a["median"] is None else round(a["median"], 4)
            summary_rows.append(base)

    summary_path = os.path.join(DETAILS_DIR, "research_signal_summary_latest.csv")
    if summary_rows:
        fields = list(summary_rows[0].keys())
        with open(summary_path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore"); w.writeheader(); w.writerows(summary_rows)
    else:
        with open(summary_path, "w", encoding="utf-8-sig") as f: f.write("empty\n")

    # 2) Coin profile：按币种统计长期/强信号表现，帮助识别哪些币更适合跟踪。
    coin_bucket: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
    for src, arr in [("alert", signal_rows), ("longterm", longterm_rows)]:
        for r in arr:
            coin = (r.get("coin") or "").upper()
            if not coin:
                continue
            side = _direction_bucket(r.get("direction"))
            coin_bucket[(coin, src, side)].append(r)
    coin_rows: List[Dict[str, Any]] = []
    for (coin, src, side), arr in coin_bucket.items():
        a72 = _agg_ret(arr, "ret_72h", BACKTEST_HURDLE_72H)
        a7 = _agg_ret(arr, "ret_7d", BACKTEST_HURDLE_7D)
        a30 = _agg_ret(arr, "ret_30d", BACKTEST_HURDLE_30D)
        coin_rows.append({
            "run_id": run_id, "coin": coin, "source": src, "side": side, "events": len(arr),
            "win_72h": None if a72["win"] is None else round(a72["win"]*100, 2),
            "hurdle_72h": None if a72["hurdle_win"] is None else round(a72["hurdle_win"]*100, 2),
            "avg_72h": None if a72["avg"] is None else round(a72["avg"], 4),
            "win_7d": None if a7["win"] is None else round(a7["win"]*100, 2),
            "hurdle_7d": None if a7["hurdle_win"] is None else round(a7["hurdle_win"]*100, 2),
            "avg_7d": None if a7["avg"] is None else round(a7["avg"], 4),
            "win_30d": None if a30["win"] is None else round(a30["win"]*100, 2),
            "hurdle_30d": None if a30["hurdle_win"] is None else round(a30["hurdle_win"]*100, 2),
            "avg_30d": None if a30["avg"] is None else round(a30["avg"], 4),
        })
    coin_rows.sort(key=lambda r: ((r.get("events") or 0), (r.get("avg_7d") or -999)), reverse=True)
    coin_path = os.path.join(DETAILS_DIR, "coin_profile_latest.csv")
    if coin_rows:
        with open(coin_path, "w", encoding="utf-8-sig", newline="") as f:
            w=csv.DictWriter(f, fieldnames=list(coin_rows[0].keys()), extrasaction="ignore"); w.writeheader(); w.writerows(coin_rows)
    else:
        with open(coin_path, "w", encoding="utf-8-sig") as f: f.write("empty\n")

    # 3) Wallet profile：把现有 wallet_quality 拆成更直观的研究视图。
    qrows = load_rows("wallet_quality", run_id)
    wallet_rows: List[Dict[str, Any]] = []
    for r in qrows:
        wallet_rows.append({
            "run_id": run_id,
            "address": r.get("address"),
            "groups": r.get("groups"),
            "grade": r.get("grade"),
            "quality_score": r.get("quality_score"),
            "quality_weight": r.get("quality_weight"),
            "sample_total": r.get("sample_total"),
            "win_24h": r.get("win_24h"),
            "win_72h": r.get("win_72h"),
            "win_7d": r.get("win_7d"),
            "win_15d": r.get("win_15d"),
            "win_30d": r.get("win_30d"),
            "expectancy_72h": r.get("expectancy_72h"),
            "expectancy_30d": r.get("expectancy_30d"),
            "dominant_coins": r.get("dominant_coins"),
            "last_action_at_cn": display_time_from_utc(r.get("last_action_at")) if r.get("last_action_at") else "",
        })
    wallet_rows.sort(key=lambda r: (safe_float(r.get("quality_score")) or -999, safe_float(r.get("sample_total")) or 0), reverse=True)
    wp = os.path.join(DETAILS_DIR, "wallet_profile_latest.csv")
    if wallet_rows:
        with open(wp, "w", encoding="utf-8-sig", newline="") as f:
            w=csv.DictWriter(f, fieldnames=list(wallet_rows[0].keys()), extrasaction="ignore"); w.writeheader(); w.writerows(wallet_rows)
    else:
        with open(wp, "w", encoding="utf-8-sig") as f: f.write("empty\n")

    # 4) 文本研究面板。
    with open(os.path.join(REPORT_DIR, "research_dashboard.txt"), "w", encoding="utf-8") as f:
        print("【研究面板 / 信号验证摘要】", file=f)
        print(f"更新时间{DISPLAY_TZ_NAME}：{display_now_str()} | UTC：{now_str()}", file=f)
        print("说明：本报告不改变交易信号，只用于验证：哪些信号、钱包、币种历史上更有效。样本少时不要过度解读。", file=f)
        print("", file=f)
        print("【信号回测摘要】", file=f)
        if not summary_rows:
            print("暂无成熟回测样本。", file=f)
        else:
            for r in summary_rows:
                print(f"{r['source']} / {r['side']} | 样本={r['sample_events']} | 72h胜率={r.get('72h_win')}% 门槛={r.get('72h_hurdle_win')}% 平均={r.get('72h_avg_ret')}% | 7d胜率={r.get('7d_win')}% 门槛={r.get('7d_hurdle_win')}% 平均={r.get('7d_avg_ret')}% | 30d样本={r.get('30d_n')}", file=f)
        print("", file=f)
        print("【Top 钱包画像】", file=f)
        for r in wallet_rows[:10]:
            print(f"{short_addr(r.get('address'))} [{r.get('groups')}] grade={r.get('grade')} score={safe_float(r.get('quality_score')):.1f} 样本={r.get('sample_total')} 72h胜率={r.get('win_72h')} 30d胜率={r.get('win_30d')} 主币={r.get('dominant_coins')}", file=f)
        print("", file=f)
        print("【Top 币种画像】", file=f)
        for r in coin_rows[:15]:
            print(f"{r['coin']} {r['source']}/{r['side']} | 事件={r['events']} | 72h胜率={r.get('win_72h')}% 平均={r.get('avg_72h')}% | 7d胜率={r.get('win_7d')}% 平均={r.get('avg_7d')}%", file=f)
        print("", file=f)
        print("输出文件：reports/details/research_signal_summary_latest.csv、coin_profile_latest.csv、wallet_profile_latest.csv", file=f)

def export_backtest_files() -> None:
    if not BACKTEST_MODE:
        return
    _export_event_backtest_table("signal_events", "signal_backtest_latest.csv", "强信号/观察信号回测")
    _export_event_backtest_table("longterm_events", "longterm_backtest_latest.csv", "长期单候选回测")


def write_last_run_status(run_id: int, wallet_rows: List[Dict[str, Any]], perp_rows: List[Dict[str, Any]], spot_rows: List[Dict[str, Any]], pushed: bool, note: str, started_at: Optional[str] = None) -> None:
    ensure_dirs()
    stats = run_wallet_stats(run_id)
    start_dt = parse_time(started_at) if started_at else None
    duration_minutes = None
    if start_dt:
        duration_minutes = (utc_now() - start_dt).total_seconds() / 60
    payload = {
        "last_run_cn": display_now_str(),
        "last_run_utc": now_str(),
        "run_id": run_id,
        "trigger_note": note,
        "wallet_count": len(wallet_rows),
        "ok": stats.get("ok"),
        "partial": stats.get("partial"),
        "failed": stats.get("failed"),
        "success_rate": stats.get("ok_rate"),
        "perp_rows": len(perp_rows),
        "spot_rows": len(spot_rows),
        "tg_sent": bool(pushed),
        "duration_minutes": duration_minutes,
        "health_stale_hours_threshold": HEALTH_STALE_HOURS,
    }
    with open(os.path.join(REPORT_DIR, "last_run_status.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


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
    csv_path = os.path.join(DETAILS_DIR, "wallet_quality_latest.csv")
    if rows:
        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    txt_path = os.path.join(DETAILS_DIR, "wallet_quality_report.txt")
    counts: Dict[str, int] = defaultdict(int)
    for r in rows:
        counts[r.get("grade") or "N"] += 1
    top_good = [r for r in rows if r.get("grade") in ("S", "A")]
    top_good.sort(key=lambda r: safe_float(r.get("quality_score")) or 0.0, reverse=True)
    reverse = [r for r in rows if r.get("grade") == "R"]
    reverse.sort(key=lambda r: safe_float(r.get("reverse_score")) or 0.0, reverse=True)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("【钱包质量分类】\n")
        f.write(f"更新时间{DISPLAY_TZ_NAME}：{display_now_str()} | UTC：{now_str()}\n")
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
    cur.execute("SELECT id FROM push_log WHERE push_type=? AND push_date=?", (push_type, display_today()))
    row = cur.fetchone()
    conn.close()
    return row is not None


def mark_pushed(push_type: str) -> None:
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO push_log(push_type, push_date, pushed_at) VALUES (?, ?, ?)", (push_type, display_today(), now_str()))
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
    now = display_now()
    return now.hour >= DAILY_PUSH_HOUR_CN and not already_pushed_today("daily")



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
    funding_bad = sig.get("funding_risk") == "高"
    liquidity_bad = sig.get("liquidity_risk") == "高"

    if confidence == "低" or state in avoid_states or bad_position or leverage_bad or funding_bad or liquidity_bad:
        action = "只观察，不适合直接做长期单"
        entry = "等待下一轮确认；不要因为单次异动直接开仓。"
        if leverage_bad:
            entry += " 当前同方向杠杆结构偏短线/强平距离偏近，长期单降权。"
        if funding_bad:
            entry += " 当前资金费率风险高，长期持仓成本需要谨慎。"
        if liquidity_bad:
            entry += " 当前24h成交额偏低，流动性风险高，不适合重仓长期单。"
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
    if sig.get("funding_rate_pct") is not None:
        price_note += f" 资金费率：{fmt_pct(sig.get('funding_rate_pct'))}，风险={sig.get('funding_risk') or '未知'}。"
    if sig.get("day_volume_usd") is not None:
        price_note += f" 24h成交额：{fmt_money(sig.get('day_volume_usd'))}，流动性={sig.get('liquidity_risk') or '未知'}。"
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
        # 双分数版：长期候选只看 long_score + long_qualified，不再用短线 alert/final_score 混进长期单。
        coin = s["coin"]
        direction = s["direction"]
        score = abs(safe_float(s.get("long_score")) or 0.0)
        if not s.get("long_qualified"):
            continue
        if score < threshold(load_thresholds(), coin, "min_watch_score"):
            continue
        ctx = ctx_map.get(coin, {})
        streak = signal_streak(coin, direction, run_id, min_abs_score=threshold(load_thresholds(), coin, "min_watch_score"))
        plan = long_term_entry_plan(s, ctx, streak)

        # 长期评分：long_score + 连续性 + 可信度 + 清晰状态 - 风险位置
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
            "alert_score": safe_float(s.get("alert_score")) or 0.0,
            "long_score": safe_float(s.get("long_score")) or 0.0,
            "signal_category": s.get("signal_category"),
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
            "funding_rate_pct": s.get("funding_rate_pct"),
            "funding_risk": s.get("funding_risk"),
            "day_volume_usd": s.get("day_volume_usd"),
            "liquidity_risk": s.get("liquidity_risk"),
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
        f.write(f"更新时间{DISPLAY_TZ_NAME}：{display_now_str()} | UTC：{now_str()}\n")
        f.write(f"风控默认：单币最大亏损控制在账户 {LONG_TERM_RISK_PCT:.1f}% 左右；建议低杠杆，不一次打满。\n\n")
        if not candidates:
            f.write("暂无适合低杠杆长期单的候选。\n")
            return
        for c in candidates[:TOP_N]:
            f.write(f"{c['coin']} {c['direction_cn']} | 长期分={c['long_term_score']:.1f} | long={c.get('long_score',0):+.1f} | alert={c.get('alert_score',0):+.1f} | 连续={c['streak']}轮 | 可信度={c['confidence']}\n")
            f.write(f"状态：{c['signal_state']} | 类型：{c['signal_type']} | 建议杠杆：{c['leverage']}\n")
            f.write(f"动作：{c['action']}\n")
            f.write(f"入场：{c['entry']}\n")
            f.write(f"价格：{c['price_note']}\n")
            f.write(f"失效：{c['invalid']}\n\n")

    csv_path = os.path.join(DETAILS_DIR, "long_term_candidates.csv")
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
        path = os.path.join(DETAILS_DIR, fname)
        rows = [s for s in signals if s["watchlist"] == key]
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"【{titles[key]}】\n更新时间 UTC：{now_str()}\n\n")
            if not rows:
                f.write("暂无。\n")
            for s in rows[:TOP_N]:
                f.write(f"{s['coin']} {dir_cn(s['direction'])} alert={float(s.get('alert_score') or 0):+.1f} long={float(s.get('long_score') or 0):+.1f}/阈值{s['threshold_score']:.1f} 分类={s.get('signal_category','-')} 状态={s['signal_state']}\n")
                f.write(f"结论：{s['conclusion']}\n")
                f.write(f"风险：{s['risk']}\n")
                f.write(f"原因：{s['reason']}\n\n")


def build_report(run_id: int, signals: List[Dict[str, Any]], ctx_map: Dict[str, Dict[str, Any]], actions: List[Dict[str, Any]], cashflows: List[Dict[str, Any]], ok_rate: float, new_signal_events: int, updated_actions: int, updated_signals: int) -> str:
    strong = [s for s in signals if abs(s.get("alert_score") or 0.0) >= s["threshold_score"] and s.get("signal_category") != "低杠杆长期候选"]
    longs = [s for s in signals if s["watchlist"] == "long"]
    shorts = [s for s in signals if s["watchlist"] == "short"]
    observes = [s for s in signals if s["watchlist"] == "observe"]
    btc = ctx_map.get("BTC", {})
    eth = ctx_map.get("ETH", {})
    lines: List[str] = []
    lines.append("🧠 Hyperliquid 钱包监控 FINAL")
    lines.append("币种阈值 + 钱包主动变化 + 主导钱包 + 信号解释 + 回测 + 生命周期 + API异常保护 + TG 推送")
    lines.append(f"{DISPLAY_TZ_NAME}：{signal_time_cn(run_id)}")
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
    if DATA_ANOMALY_PROTECT_MODE and ok_rate < MIN_OK_RATE:
        lines.append(f"⚠️ 数据质量异常：成功率低于 {MIN_OK_RATE*100:.1f}%，本轮不生成新信号/不结算生命周期，避免误判。")
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
    if RISK_FILTER_MODE:
        risk_rows = load_rows("coin_risk_metrics", run_id)
        if risk_rows:
            lines.append("")
            lines.append("【资金费率 / 流动性过滤】")
            risky = [r for r in risk_rows if r.get("funding_risk") in ("中", "高") or r.get("liquidity_risk") in ("中", "高")]
            if not risky:
                lines.append("本轮候选币资金费率和流动性暂无明显风险。")
            else:
                risky.sort(key=lambda r: (r.get("liquidity_risk") == "高", r.get("funding_risk") == "高", abs(safe_float(r.get("funding_rate_pct")) or 0.0)), reverse=True)
                for r in risky[:min(TOP_N, 8)]:
                    lines.append(f"{r['coin']} | funding={fmt_pct(r.get('funding_rate_pct'))} 风险={r.get('funding_risk')} | 24h成交额={fmt_money(r.get('day_volume_usd'))} 流动性={r.get('liquidity_risk')}")
    lines.append("")
    lines.append("【短线强信号 / 异动雷达】")
    if not strong:
        lines.append("暂无达到币种专属阈值的短线强异动。")
    else:
        for s in strong[:TOP_N]:
            lines.append(f"🚨 {s['coin']} {dir_cn(s['direction'])} | 时间={signal_time_cn(run_id)} | alert={float(s.get('alert_score') or 0):+.1f}/阈值{s['threshold_score']:.1f} | long={float(s.get('long_score') or 0):+.1f} | {s.get('signal_category','-')} | {s['signal_state']} | 可信度={s['confidence']}")
            lines.append(f"  结论：{s['conclusion']}")
            parts = s.get("score_parts") or {}
            lines.append(f"  分解：本轮资金{parts.get('base_flow',0):+.1f} / 滚动建仓{parts.get('rolling_flow',0):+.1f} / 滚动杠杆{parts.get('rolling_leverage',0):+.1f} / 历史{parts.get('confidence',0):+.1f} / 市场{parts.get('market',0):+.1f} / 位置{parts.get('price_position',0):+.1f} / 当前杠杆{parts.get('leverage',0):+.1f} / 费率流动性{parts.get('funding_liquidity',0):+.1f}")
            lines.append(f"  风险：{s['risk']}")
            lines.append(f"  主导钱包：{s.get('dominant_wallets', '-')}")
            lines.append(f"  原因：{s['reason']}")
    lines.append("")
    lines.append("【做多观察】")
    if not longs:
        lines.append("暂无。")
    else:
        for s in longs[:TOP_N]:
            lines.append(f"{s['coin']} 时间={signal_time_cn(run_id)} | long={float(s.get('long_score') or 0):+.1f} alert={float(s.get('alert_score') or 0):+.1f} | {s.get('signal_category','-')} | {s['signal_state']} | {s['conclusion']}")
    lines.append("")
    lines.append("【做空观察】")
    if not shorts:
        lines.append("暂无。")
    else:
        for s in shorts[:TOP_N]:
            lines.append(f"{s['coin']} 时间={signal_time_cn(run_id)} | long={float(s.get('long_score') or 0):+.1f} alert={float(s.get('alert_score') or 0):+.1f} | {s.get('signal_category','-')} | {s['signal_state']} | {s['conclusion']}")
    lines.append("")
    lines.append("【只观察 / 信号不足】")
    if not observes:
        lines.append("暂无。")
    else:
        for s in observes[:TOP_N]:
            lines.append(f"{s['coin']} {dir_cn(s['direction'])} 时间={signal_time_cn(run_id)} | alert={float(s.get('alert_score') or 0):+.1f} long={float(s.get('long_score') or 0):+.1f} | {s.get('signal_category','-')} | {s['signal_state']} | 风险：{s['risk']}")
    lines.append("")
    if LONG_TERM_MODE:
        lt_candidates = build_long_term_candidates(run_id, signals, ctx_map)
        lines.append("【低杠杆长期单过滤】")
        if not lt_candidates:
            lines.append("暂无适合低杠杆长期单的候选。")
        else:
            for c in lt_candidates[:TOP_N]:
                lines.append(
                    f"{c['coin']} {c['direction_cn']} | 时间={signal_time_cn(run_id)} | 长期分={c['long_term_score']:.1f} | "
                    f"long={c.get('long_score',0):+.1f} alert={c.get('alert_score',0):+.1f} | 连续={c['streak']}轮 | "
                    f"可信度={c['confidence']} | 建议杠杆={c['leverage']}"
                )
                lines.append(f"  动作：{c['action']}")
                lines.append(f"  入场：{c['entry']}")
                lines.append(f"  失效：{c['invalid']}")
        lines.append("")
    closed_lifecycle = get_closed_lifecycle_events(run_id) if SIGNAL_LIFECYCLE_MODE else []
    lines.append("【信号生命周期提醒】")
    if not closed_lifecycle:
        lines.append("本轮没有强信号/长期单失效或反转。")
    else:
        for e in closed_lifecycle[:8]:
            typ_cn = "强信号" if e.get("lifecycle_type") == "strong" else "长期单"
            lines.append(
                f"⚠️ {e.get('coin')} {dir_cn(e.get('direction'))} {typ_cn}结束 | "
                f"收益={fmt_pct(e.get('lifecycle_return_pct'))} | 持续={fmt_num(e.get('holding_hours'))}h | "
                f"原因={e.get('exit_reason')}"
            )
    lines.append("详见 reports/details/signal_lifecycle_report.txt")
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
    if BACKTEST_MODE:
        try:
            lines.append("【强信号 / 长期单回测摘要】")
            lines.append("说明：普胜=方向收益>0；门胜=达到门槛收益，长期单优先看门胜。")
            for title, table in (("信号", "signal_events"), ("长期单", "longterm_events")):
                since = (utc_now() - dt.timedelta(days=SIGNAL_BACKTEST_WINDOW_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
                conn = db_conn(); cur = conn.cursor()
                cur.execute(f"SELECT ret_24h, ret_72h, ret_7d, ret_15d, ret_30d FROM {table} WHERE created_at >= ?", (since,))
                brs = [dict(x) for x in cur.fetchall()]; conn.close()
                parts_sum = []
                for label, col, hurdle in _backtest_periods():
                    vals = [safe_float(r.get(col)) for r in brs if safe_float(r.get(col)) is not None]
                    if vals:
                        dir_win = sum(1 for v in vals if v > 0) / len(vals) * 100
                        hurdle_win = sum(1 for v in vals if v >= hurdle) / len(vals) * 100
                        avg = sum(vals) / len(vals)
                        parts_sum.append(f"{label} {len(vals)}次 普胜{dir_win:.0f}% 门胜{hurdle_win:.0f}% 均{avg:+.1f}%")
                lines.append(f"{title}：" + ("；".join(parts_sum) if parts_sum else "暂无成熟样本"))
            lines.append("")
        except Exception:
            pass
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
    strong_count = sum(1 for s in signals if abs(s.get("alert_score") or 0.0) >= s["threshold_score"] and s.get("signal_category") != "低杠杆长期候选")
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
    """清理旧版/旧布局报告。

    当前精简版报告结构：
    - reports/ 根目录：只放每天要看的核心报告
    - reports/details/：放全量 CSV 和辅助报告
    - reports/daily/YYYY-MM-DD/：每日归档
    - hl_monitor.db：长期历史数据库
    """
    ensure_dirs()
    legacy_root_files = {
        "wallet_states_latest.csv", "perp_positions_latest.csv", "spot_balances_latest.csv", "coin_signals_latest.csv",
        "active_changes_all_latest.csv", "fund_flow_lite_all_latest.csv", "wallet_quality_latest.csv", "wallet_quality_report.txt",
        "leverage_quality_latest.csv", "wallet_leverage_profile_latest.csv", "coin_leverage_summary_latest.csv", "leverage_quality_report.txt",
        "wallet_position_trades_latest.csv", "wallet_position_performance_latest.csv", "wallet_position_report.txt",
        "signal_explain_latest.csv", "coin_risk_latest.csv", "signal_backtest_latest.csv", "signal_backtest_latest_report.txt",
        "longterm_backtest_latest.csv", "longterm_backtest_latest_report.txt", "long_term_candidates.csv",
        "watchlist_long.txt", "watchlist_short.txt", "watchlist_observe.txt",
    }
    try:
        for name in os.listdir(REPORT_DIR):
            path = os.path.join(REPORT_DIR, name)
            if os.path.isdir(path):
                continue
            if (name.startswith("final_report_run_") and name.endswith(".txt")) or name in legacy_root_files:
                try:
                    os.remove(path)
                except FileNotFoundError:
                    pass
    except Exception as e:
        print(f"清理旧版报告失败：{e}", flush=True)


def save_daily_archive(run_id: int, report: str) -> None:
    """每天保留一份长期复盘快照。

    精简版归档逻辑：
    - 根目录 latest 文件仍每轮覆盖。
    - 每日目录只保存复盘必看的核心报告和少量关键 CSV。
    - 全量明细保存在 reports/details/ 和 hl_monitor.db，不再每天全部复制，避免仓库变乱。
    """
    if not DAILY_ARCHIVE:
        return
    ensure_dirs()
    today = display_today()
    daily_root = os.path.join(REPORT_DIR, "daily")
    day_dir = os.path.join(daily_root, today)
    os.makedirs(day_dir, exist_ok=True)

    # 1) 总报告每日归档
    with open(os.path.join(day_dir, "final_report.txt"), "w", encoding="utf-8") as f:
        f.write(report)

    # 2) 核心文本报告：每天保留
    root_files = [
        "long_term_plan.txt",
        "signal_explain_report.txt",
        "coin_risk_report.txt",
        "rolling_flow_report.txt",
        "long_short_state_report.txt",
        "last_run_status.json",
    ]
    # 3) 关键 CSV：每天保留少量，用于长期复盘
    detail_files = [
        "coin_signals_latest.csv",
        "rolling_flow_latest.csv",
        "long_short_state_latest.csv",
        "wallet_quality_latest.csv",
        "wallet_position_performance_latest.csv",
        "signal_backtest_latest.csv",
        "longterm_backtest_latest.csv",
        "signal_lifecycle_latest.csv",
        "data_quality_report.txt",
    ]

    def _copy(src_dir: str, name: str) -> None:
        src = os.path.join(src_dir, name)
        if not os.path.exists(src):
            return
        dst_name = name.replace("_latest", "")
        dst = os.path.join(day_dir, dst_name)
        try:
            with open(src, "rb") as rf, open(dst, "wb") as wf:
                wf.write(rf.read())
        except Exception as e:
            print(f"每日归档复制失败：{name} -> {e}", flush=True)

    for name in root_files:
        _copy(REPORT_DIR, name)
    for name in detail_files:
        _copy(DETAILS_DIR, name)

    # 4) 写一个索引，方便打开目录时先看这个
    index = (
        f"Hyperliquid Monitor Daily Archive\n"
        f"date_cn: {today}\n"
        f"run_id: {run_id}\n"
        f"updated_at_cn: {signal_time_cn(run_id)}\n"
        f"updated_at_utc: {now_str()}\n\n"
        f"主要看：final_report.txt、long_term_plan.txt、signal_explain_report.txt、coin_risk_report.txt、rolling_flow_report.txt\n"
        f"关键 CSV：coin_signals.csv、rolling_flow.csv、wallet_quality.csv、wallet_position_performance.csv、signal_backtest.csv、longterm_backtest.csv、signal_lifecycle.csv\n"
        f"全量明细请看仓库 reports/details/，长期历史保存在数据库（Turso 或本地 SQLite）。\n"
    )
    with open(os.path.join(day_dir, "README.txt"), "w", encoding="utf-8") as f:
        f.write(index)

    prune_daily_archives(DAILY_ARCHIVE_KEEP_DAYS)

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
    step_log(f"启动脚本 | backend={'turso' if USE_TURSO else 'sqlite'} | DB_BACKEND={DB_BACKEND} | USE_TURSO={USE_TURSO}")
    init_db()
    step_log("数据库初始化完成")
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
    step_log("钱包扫描完成，准备写入快照")
    save_snapshot(run_id, wallet_rows, perp_rows, spot_rows)
    step_log("快照写入完成，开始更新仓位生命周期")
    update_position_trades(run_id, {**spot_coin_price, **mid_prices})
    step_log("仓位生命周期完成，开始导出杠杆质量")
    export_leverage_quality_files(run_id)
    step_log("杠杆质量导出完成")
    prev_id = get_previous_run_id(run_id)

    total = len(wallet_rows)
    ok = sum(1 for w in wallet_rows if w.get("status") == "ok")
    partial = sum(1 for w in wallet_rows if w.get("status") == "partial")
    ok_rate = (ok + partial * 0.5) / total if total else 0.0
    data_quality_ok = ok_rate >= MIN_OK_RATE
    write_data_quality_report(run_id, ok_rate, wallet_rows, args.note)
    step_log(f"数据质量报告完成 | ok_rate={ok_rate*100:.2f}%")

    step_log("开始更新回测事件")
    updated_actions, updated_signals = evaluate_events({**spot_coin_price, **mid_prices})
    step_log("回测事件更新完成，开始刷新钱包质量")
    quality_rows = refresh_wallet_quality(run_id, addresses) if WALLET_QUALITY_MODE else []
    quality_map = get_wallet_quality_map(run_id) if quality_rows else {}

    if prev_id is None:
        stats = run_wallet_stats(run_id)
        report = (
            f"🧠 Hyperliquid 钱包监控 FINAL\n"
            f"{DISPLAY_TZ_NAME}：{display_now_str()} | UTC时间：{now_str()}\n"
            f"run_id：{run_id}\n\n"
            f"【扫描健康】\n"
            f"监控钱包：{stats['total']} | 成功：{stats['ok']} | "
            f"partial：{stats['partial']} | failed：{stats['failed']} | "
            f"成功率：{stats['ok_rate']*100:.2f}%\n\n"
            f"钱包质量分类已导出：reports/details/wallet_quality_latest.csv / wallet_quality_report.txt\n"
            f"仓位生命周期追踪已导出：reports/details/wallet_position_trades_latest.csv / wallet_position_report.txt\n\n"
            f"第一次运行，已建立快照。第二次开始才有趋势对比。"
        )
        with open(os.path.join(REPORT_DIR, "final_latest_report.txt"), "w", encoding="utf-8") as f:
            f.write(report)
        with open(os.path.join(REPORT_DIR, "long_term_plan.txt"), "w", encoding="utf-8") as f:
            f.write("第一次运行，已建立快照。第二次开始生成低杠杆长期单观察计划。\n")
        export_latest_csv(run_id)
        export_signal_lifecycle_files()
        prune_reports()
        save_daily_archive(run_id, report)
        daily_due = should_push_daily()
        pushed = False
        if PUSH_EVERY_RUN or daily_due:
            pushed = await send_tg(report)
            if pushed and daily_due:
                mark_pushed("daily")
        finish_run(run_id, wallet_rows, perp_rows, spot_rows, pushed)
        write_last_run_status(run_id, wallet_rows, perp_rows, spot_rows, pushed, args.note)
        prune_database_for_github(run_id)
        print(report, flush=True)
        return

    step_log("开始计算主动变化/资金流")
    preliminary, actions, cashflows = compute_preliminary(run_id, prev_id, thresholds, quality_map)
    gap_minutes = get_run_gap_minutes(run_id, prev_id)
    step_log(f"主动变化计算完成 | actions={len(actions)} cashflows={len(cashflows)} coins={len(preliminary)} gap_min={gap_minutes if gap_minutes is not None else 'N/A'}")
    save_coin_flow_snapshots(run_id, prev_id, preliminary)
    rolling_map = build_rolling_flow_metrics(run_id)
    export_rolling_flow_files(run_id, rolling_map, thresholds)
    export_operation_detail_files(actions, cashflows)
    step_log("主动变化明细/滚动建仓导出完成")
    inserted_actions = 0
    if data_quality_ok:
        inserted_actions = save_wallet_actions(run_id, actions)
    else:
        print(f"成功率 {ok_rate*100:.2f}% 低于阈值 {MIN_OK_RATE*100:.2f}%，不记录本轮钱包动作/信号/生命周期。")

    candidate_pool = set(preliminary.keys()) | set(rolling_map.keys())
    candidate_coins = sorted(
        candidate_pool,
        key=lambda c: max(abs(preliminary.get(c, {}).get("weighted_flow") or 0), abs(rolling_map.get(c, {}).get("best_flow") or 0)),
        reverse=True
    )[:35]
    step_log(f"开始构建市场上下文/风险指标 | candidate_coins={len(candidate_coins)}")
    ctx_map = await build_market_context(run_id, candidate_coins, {**spot_coin_price, **mid_prices})
    risk_map = await build_coin_risk_metrics(run_id, candidate_coins)
    step_log("市场上下文/风险指标完成，开始构建信号")
    signals = build_signals(run_id, preliminary, ctx_map, thresholds, risk_map, rolling_map=rolling_map, gap_minutes=gap_minutes) if data_quality_ok else []
    attach_dominant_wallets(signals, actions)
    export_signal_explain_files(run_id, signals)
    new_signal_events = create_signal_events(run_id, signals, {**spot_coin_price, **mid_prices}, thresholds) if data_quality_ok else 0

    write_watchlists(signals)
    lt_candidates: List[Dict[str, Any]] = []
    if LONG_TERM_MODE:
        lt_candidates = build_long_term_candidates(run_id, signals, ctx_map)
        write_long_term_plan(lt_candidates)
        create_longterm_events(run_id, lt_candidates, {**spot_coin_price, **mid_prices})
    closed_lifecycle_events = update_signal_lifecycles(
        run_id, signals, lt_candidates, {**spot_coin_price, **mid_prices},
        data_quality_ok=data_quality_ok,
        skip_reason=f"ok_rate={ok_rate*100:.2f}% < MIN_OK_RATE={MIN_OK_RATE*100:.2f}%"
    )
    step_log("开始导出回测文件")
    export_backtest_files()
    export_research_intelligence_files(run_id)
    step_log("回测/研究面板导出完成，开始导出 latest CSV")
    export_latest_csv(run_id)
    step_log("latest CSV 导出完成")

    report = build_report(run_id, signals, ctx_map, actions, cashflows, ok_rate, new_signal_events, updated_actions, updated_signals)
    save_report(run_id, signals, report)
    prune_reports()
    save_daily_archive(run_id, report)

    strong = [s for s in signals if (abs(s.get("alert_score") or 0.0) >= s["threshold_score"] and s.get("signal_category") != "低杠杆长期候选") or (s.get("signal_category") == "低杠杆长期候选" and abs(s.get("long_score") or 0.0) >= s["threshold_score"])]
    daily_due = should_push_daily()
    should_push = PUSH_EVERY_RUN or bool(strong) or bool(closed_lifecycle_events) or daily_due
    pushed = False
    if should_push:
        pushed = await send_tg(report)
        if pushed and daily_due:
            mark_pushed("daily")
    else:
        print("无强信号，也不是每日推送时间，不推送 TG。")

    step_log("开始 finish_run / last_run_status / prune")
    finish_run(run_id, wallet_rows, perp_rows, spot_rows, pushed)
    write_last_run_status(run_id, wallet_rows, perp_rows, spot_rows, pushed, args.note)
    prune_database_for_github(run_id)
    step_log("finish/prune 完成")
    print(f"新增钱包动作：{inserted_actions} | 强信号：{len(strong)}", flush=True)
    print(report, flush=True)



def db_file_size_mb() -> float:
    if USE_TURSO:
        return 0.0
    try:
        if not os.path.exists(DB_FILE):
            return 0.0
        return os.path.getsize(DB_FILE) / 1024 / 1024
    except Exception:
        return 0.0


def prune_database_for_github(current_run_id: Optional[int] = None, aggressive: bool = False, emergency: bool = False) -> None:
    """控制数据库体积。

    SQLite 模式：裁剪原始快照并 VACUUM，避免 GitHub 100MB 单文件限制。
    Turso 模式：只裁剪历史行，不执行本地文件大小检查 / VACUUM。

    设计原则：
    - wallet_states / perp_positions / spot_balances 这类逐轮原始快照最占空间，只保留最近 N 轮。
    - wallet_actions / signal_events / longterm_events 等保留 30d+ 缓冲，用于回测和钱包质量。
    - position_trades / signal_lifecycles 保留未结束记录和最近窗口内已结束记录。
    - 最后 VACUUM 真正收缩 sqlite 文件大小。
    """
    if not DB_PRUNE_MODE:
        return
    if (not USE_TURSO) and (not os.path.exists(DB_FILE)):
        return

    before = db_file_size_mb()
    raw_keep = max(6, int(DB_RAW_KEEP_RUNS))
    history_days = max(30, int(DB_HISTORY_KEEP_DAYS))
    if aggressive:
        raw_keep = max(6, min(raw_keep, 12))
        history_days = max(30, min(history_days, 31))
    if emergency:
        # GitHub 单文件硬限制是 100MB；紧急模式只保留最少原始快照，
        # 但仍保留 30 天核心信号/生命周期/仓位结果，保证后续分析不断档。
        raw_keep = 3
        history_days = 30

    cutoff_dt = utc_now() - dt.timedelta(days=history_days)
    cutoff_str = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")

    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT run_id FROM runs ORDER BY run_id DESC LIMIT ?", (raw_keep,))
        keep_ids = [int(r[0]) for r in cur.fetchall()]
        cutoff_run = min(keep_ids) if keep_ids else (int(current_run_id or 0) + 1)

        # 逐轮原始快照：只保留最近 raw_keep 轮，足够做上一轮对比和短期连续性。
        raw_tables = [
            "wallet_states",
            "perp_positions",
            "spot_balances",
            "coin_signals",
            "market_context",
            "coin_risk_metrics",
            "wallet_quality",
            "wallet_position_performance",
        ]
        for t in raw_tables:
            try:
                cur.execute(f"DELETE FROM {t} WHERE run_id < ?", (cutoff_run,))
            except sqlite3.OperationalError:
                pass

        # 事件类保留 30d 回测窗口 + 缓冲。
        for t in ("wallet_actions", "coin_flow_snapshots", "signal_events", "longterm_events", "position_trade_events", "signal_lifecycle_events", "final_reports"):
            try:
                cur.execute(f"DELETE FROM {t} WHERE created_at < ?", (cutoff_str,))
            except sqlite3.OperationalError:
                pass

        # 已关闭的仓位交易，保留最近窗口；open 交易不删。
        try:
            cur.execute("DELETE FROM position_trades WHERE status='closed' AND close_time IS NOT NULL AND close_time < ?", (cutoff_str,))
        except sqlite3.OperationalError:
            pass

        # 已结束的生命周期，保留最近窗口；open 记录不删。
        try:
            cur.execute("DELETE FROM signal_lifecycles WHERE status='closed' AND exit_time IS NOT NULL AND exit_time < ?", (cutoff_str,))
        except sqlite3.OperationalError:
            pass

        # 紧急模式：进一步清理最占空间的历史明细，只保留当前分析必需的 30 天窗口。
        if emergency:
            try:
                cur.execute("DELETE FROM final_reports WHERE created_at < ?", ((utc_now() - dt.timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S"),))
            except sqlite3.OperationalError:
                pass
            try:
                cur.execute("DELETE FROM push_log WHERE created_at < ?", ((utc_now() - dt.timedelta(days=14)).strftime("%Y-%m-%d %H:%M:%S"),))
            except sqlite3.OperationalError:
                pass
            try:
                # runs 表本身不大，但清掉久远 run 记录可避免外键外的历史索引继续膨胀。
                cur.execute("DELETE FROM runs WHERE run_id < ?", (cutoff_run,))
            except sqlite3.OperationalError:
                pass

        conn.commit()
    finally:
        conn.close()

    if USE_TURSO:
        mode = "emergency" if emergency else ("aggressive" if aggressive else "normal")
        print(f"Turso 数据库裁剪({mode})完成 | raw_keep={raw_keep} | history_days={history_days}", flush=True)
        return

    # VACUUM 必须在事务外执行，才能真正缩小 SQLite 文件。
    try:
        conn = db_conn()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("VACUUM")
        conn.close()
    except Exception as e:
        print(f"数据库 VACUUM 失败：{e}", flush=True)

    after = db_file_size_mb()
    mode = "emergency" if emergency else ("aggressive" if aggressive else "normal")
    print(f"数据库体积控制({mode})：{before:.2f}MB -> {after:.2f}MB | raw_keep={raw_keep} | history_days={history_days}", flush=True)

    # 如果仍接近 GitHub 单文件限制，逐级加大裁剪。
    if not aggressive and not emergency and after > DB_MAX_MB:
        prune_database_for_github(current_run_id=current_run_id, aggressive=True, emergency=False)
    elif aggressive and not emergency and after > DB_MAX_MB:
        prune_database_for_github(current_run_id=current_run_id, aggressive=True, emergency=True)



# ===== long-short-state-final overrides =====
# 多单/空单分离 + 长期状态机。短线 alert 与长期候选彻底分开。
_build_signals_dualscore_base = build_signals
_build_long_term_candidates_base = build_long_term_candidates


def _ensure_coin_signal_state_columns(conn) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(coin_signals)")
    cols = {r[1] for r in cur.fetchall()}
    for col in ("alert_score", "long_score", "long_candidate_score", "short_candidate_score"):
        if col not in cols:
            cur.execute(f"ALTER TABLE coin_signals ADD COLUMN {col} REAL")
    for col in ("signal_category", "candidate_state", "candidate_gate", "candidate_block_reasons", "candidate_side"):
        if col not in cols:
            cur.execute(f"ALTER TABLE coin_signals ADD COLUMN {col} TEXT")


def _bool_from_parts(parts: Dict[str, Any], key: str) -> bool:
    v = parts.get(key)
    if isinstance(v, str):
        return v not in ("", "0", "false", "False", "None", "none")
    return bool(v)


def _candidate_side_name(direction: str) -> str:
    return "long" if direction == "bullish" else "short"


def _candidate_side_cn(direction: str) -> str:
    return "长期多单" if direction == "bullish" else "长期空单"


def _long_short_gate(sig: Dict[str, Any], direction: str) -> Tuple[bool, List[str]]:
    parts = sig.get("score_parts") or {}
    reasons: List[str] = []
    if _bool_from_parts(parts, "rolling_spot_only_risk"):
        reasons.append("spot-only/现货主导")
    if _bool_from_parts(parts, "rolling_concentration_risk"):
        reasons.append("钱包集中度过高")
    if _bool_from_parts(parts, "rolling_persistence_risk"):
        reasons.append("持续性不足")
    if _bool_from_parts(parts, "rolling_immature_risk"):
        reasons.append("长窗口未成熟")
    if _bool_from_parts(parts, "rolling_gap_risk"):
        reasons.append("含断跑gap")
    top1 = safe_float(parts.get("best_top1_share"))
    if top1 is not None and top1 >= LONG_SHORT_BLOCK_TOP1_SHARE:
        reasons.append(f"Top1贡献过高({top1:.0%})")
    has_perp_confirm = bool(parts.get("has_perp_confirm"))
    leverage_confirm = bool(parts.get("leverage_confirm"))
    if LONG_SCORE_REQUIRE_PERP_CONFIRM and not (has_perp_confirm or leverage_confirm):
        reasons.append("缺少合约/低杠杆确认")
    high_ratio = safe_float(sig.get("highrisk_leverage_ratio")) or safe_float(parts.get("best_highrisk_leverage_ratio")) or 0.0
    if high_ratio >= LONG_SHORT_BLOCK_HIGH_LEV_RATIO:
        reasons.append(f"高杠杆占比过高({high_ratio:.0%})")
    avg_lev = safe_float(sig.get("avg_leverage"))
    if avg_lev is not None and avg_lev >= LONG_SHORT_BLOCK_AVG_LEVERAGE:
        reasons.append(f"平均杠杆过高({avg_lev:.1f}x)")
    avg_liq = safe_float(sig.get("avg_liq_distance"))
    if avg_liq is not None and avg_liq < LONG_SHORT_BLOCK_LIQ_DISTANCE:
        reasons.append(f"强平距离过近({avg_liq:.1f}%)")
    stype = sig.get("signal_type") or ""
    pct24 = safe_float(sig.get("pct_24h"))
    if direction == "bullish":
        if stype == "高位追多":
            reasons.append("高位追多")
        if pct24 is not None and pct24 >= LONG_SHORT_PRICE_24H_EXTREME:
            reasons.append(f"24h涨幅过大({pct24:.1f}%)")
    else:
        if stype == "低位追空":
            reasons.append("低位追空")
        if pct24 is not None and pct24 <= -LONG_SHORT_PRICE_24H_EXTREME:
            reasons.append(f"24h跌幅过大({pct24:.1f}%)")
        state = sig.get("signal_state") or ""
        if state in {"现货主导", "不明确"} and not has_perp_confirm:
            reasons.append("长期空单缺少明确short确认")
    if sig.get("funding_risk") == "高":
        reasons.append("资金费率风险高")
    if sig.get("liquidity_risk") == "高":
        reasons.append("流动性风险高")
    return (len(reasons) == 0), reasons


def _candidate_state_for(sig: Dict[str, Any], direction: str, run_id: int, score: float, gate_ok: bool, min_watch: float, th_score: float) -> str:
    if score <= 0 or abs(score) < min_watch:
        return "WATCH"
    if not gate_ok:
        return "BLOCKED"
    streak = signal_streak(sig.get("coin"), direction, run_id, min_abs_score=min_watch)
    if score >= th_score and streak >= LONG_SHORT_MIN_STREAK_CANDIDATE:
        return "CANDIDATE"
    if streak >= LONG_SHORT_MIN_STREAK_FORMING and score >= min_watch + 1.0:
        return "CONFIRMED"
    return "FORMING"


def enhance_long_short_state(run_id: int, signals: List[Dict[str, Any]], thresholds: Dict[str, Dict[str, float]]) -> List[Dict[str, Any]]:
    if not LONG_SHORT_STATE_MODE:
        return signals
    out: List[Dict[str, Any]] = []
    for s in signals:
        coin = s.get("coin")
        direction = s.get("direction")
        th_score = threshold(thresholds, coin, "score_push")
        min_watch = threshold(thresholds, coin, "min_watch_score")
        abs_long = abs(safe_float(s.get("long_score")) or 0.0)
        long_candidate_score = abs_long if direction == "bullish" else 0.0
        short_candidate_score = abs_long if direction == "bearish" else 0.0
        candidate_score = long_candidate_score if direction == "bullish" else short_candidate_score
        gate_ok, gate_reasons = _long_short_gate(s, direction)
        state = _candidate_state_for(s, direction, run_id, candidate_score, gate_ok, min_watch, th_score)
        side = _candidate_side_name(direction)
        old_category = s.get("signal_category") or "只观察"
        category = old_category
        if candidate_score >= min_watch:
            if gate_ok:
                if direction == "bullish":
                    category = "长期多单候选" if state == "CANDIDATE" and candidate_score >= th_score else "多单建仓观察"
                else:
                    category = "长期空单候选" if state == "CANDIDATE" and candidate_score >= th_score else "空单建仓观察"
            else:
                category = "长期资格未通过"
        alert_score_abs = abs(safe_float(s.get("alert_score")) or 0.0)
        if not gate_ok and alert_score_abs >= th_score and old_category in {"现货异常变化", "高杠杆短线异动", "短线突发异动"}:
            category = old_category
        watchlist = side if (gate_ok and state == "CANDIDATE" and candidate_score >= th_score) else "observe"
        if gate_ok and state == "CANDIDATE":
            conclusion = f"{_candidate_side_cn(direction)}候选 / 已通过硬门槛"
        elif gate_ok and state in {"FORMING", "CONFIRMED"}:
            conclusion = f"{_candidate_side_cn(direction)}形成中 / 等待连续确认"
        elif not gate_ok and candidate_score >= min_watch:
            conclusion = f"{_candidate_side_cn(direction)}未通过 / " + "、".join(gate_reasons[:3])
        else:
            conclusion = s.get("conclusion") or "只观察 / 等待确认"
        risk = s.get("risk") or ""
        if gate_reasons:
            extra = "长期多空硬门槛未通过：" + "、".join(gate_reasons)
            risk = (risk + "；" + extra).strip("；") if risk else extra
        parts = dict(s.get("score_parts") or {})
        parts.update({
            "long_candidate_score": round(long_candidate_score, 4),
            "short_candidate_score": round(short_candidate_score, 4),
            "candidate_state": state,
            "candidate_gate": "PASS" if gate_ok else "BLOCK",
            "candidate_side": side,
            "candidate_block_reasons": "、".join(gate_reasons),
        })
        s2 = dict(s)
        s2.update({
            "long_candidate_score": long_candidate_score,
            "short_candidate_score": short_candidate_score,
            "candidate_score": candidate_score,
            "candidate_state": state,
            "candidate_gate": "PASS" if gate_ok else "BLOCK",
            "candidate_side": side,
            "candidate_block_reasons": "、".join(gate_reasons),
            "signal_category": category,
            "watchlist": watchlist,
            "conclusion": conclusion,
            "risk": risk,
            "score_parts": parts,
        })
        out.append(s2)
    return out


def save_coin_signals(run_id: int, rows: List[Dict[str, Any]]) -> None:
    conn = db_conn()
    _ensure_coin_signal_state_columns(conn)
    cur = conn.cursor()
    cur.execute("DELETE FROM coin_signals WHERE run_id=?", (run_id,))
    created_at = now_str()
    created_at_cn = display_time_from_utc(created_at)
    cur.executemany("""
    INSERT INTO coin_signals (
        run_id, created_at, created_at_cn, coin, direction, score, confidence, signal_type, signal_state, watchlist,
        perp_active, spot_active, weighted_flow, price_position, pct_1h, pct_4h, pct_24h,
        final_score, threshold_score, avg_leverage, avg_liq_distance, longterm_leverage_ratio, highrisk_leverage_ratio, leverage_note,
        alert_score, long_score, long_candidate_score, short_candidate_score, signal_category, candidate_state, candidate_gate, candidate_block_reasons, candidate_side,
        conclusion, risk, reason
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [(
        run_id, created_at, created_at_cn, r.get("coin"), r.get("direction"), r.get("score"), r.get("confidence"), r.get("signal_type"), r.get("signal_state"), r.get("watchlist"),
        r.get("perp_active"), r.get("spot_active"), r.get("weighted_flow"), r.get("price_position"), r.get("pct_1h"), r.get("pct_4h"), r.get("pct_24h"),
        r.get("final_score"), r.get("threshold_score"), r.get("avg_leverage"), r.get("avg_liq_distance"), r.get("longterm_leverage_ratio"), r.get("highrisk_leverage_ratio"), r.get("leverage_note"),
        r.get("alert_score"), r.get("long_score"), r.get("long_candidate_score"), r.get("short_candidate_score"), r.get("signal_category"), r.get("candidate_state"), r.get("candidate_gate"), r.get("candidate_block_reasons"), r.get("candidate_side"),
        r.get("conclusion"), r.get("risk"), r.get("reason"),
    ) for r in rows])
    conn.commit()
    conn.close()


def export_long_short_state_files(run_id: int, signals: List[Dict[str, Any]]) -> None:
    ensure_dirs()
    rows = []
    for s in signals:
        rows.append({
            "run_id": run_id,
            "time_cn": signal_time_cn(run_id),
            "coin": s.get("coin"),
            "direction": s.get("direction"),
            "direction_cn": dir_cn(s.get("direction")),
            "alert_score": round(safe_float(s.get("alert_score")) or 0.0, 4),
            "long_score_raw": round(safe_float(s.get("long_score")) or 0.0, 4),
            "long_candidate_score": round(safe_float(s.get("long_candidate_score")) or 0.0, 4),
            "short_candidate_score": round(safe_float(s.get("short_candidate_score")) or 0.0, 4),
            "candidate_side": s.get("candidate_side"),
            "candidate_state": s.get("candidate_state"),
            "candidate_gate": s.get("candidate_gate"),
            "category": s.get("signal_category"),
            "watchlist": s.get("watchlist"),
            "avg_leverage": s.get("avg_leverage"),
            "avg_liq_distance": s.get("avg_liq_distance"),
            "longterm_leverage_ratio": s.get("longterm_leverage_ratio"),
            "highrisk_leverage_ratio": s.get("highrisk_leverage_ratio"),
            "pct_24h": s.get("pct_24h"),
            "spot_active": s.get("spot_active"),
            "perp_active": s.get("perp_active"),
            "block_reasons": s.get("candidate_block_reasons"),
            "conclusion": s.get("conclusion"),
            "risk": s.get("risk"),
        })
    path = os.path.join(DETAILS_DIR, "long_short_state_latest.csv")
    if rows:
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader(); writer.writerows(rows)
    thresholds = load_thresholds()
    longs = [r for r in rows if r.get("candidate_side") == "long" and r.get("candidate_gate") == "PASS" and safe_float(r.get("long_candidate_score")) >= threshold(thresholds, r.get("coin"), "min_watch_score")]
    shorts = [r for r in rows if r.get("candidate_side") == "short" and r.get("candidate_gate") == "PASS" and safe_float(r.get("short_candidate_score")) >= threshold(thresholds, r.get("coin"), "min_watch_score")]
    blocked = [r for r in rows if r.get("candidate_gate") == "BLOCK" and max(safe_float(r.get("long_candidate_score")) or 0.0, safe_float(r.get("short_candidate_score")) or 0.0) >= threshold(thresholds, r.get("coin"), "min_watch_score")]
    with open(os.path.join(REPORT_DIR, "long_short_state_report.txt"), "w", encoding="utf-8") as f:
        print("【长期多/空分离状态机】", file=f)
        print(f"更新时间{DISPLAY_TZ_NAME}：{display_now_str()} | UTC：{now_str()}", file=f)
        print("说明：alert_score=短线雷达；long_candidate_score/short_candidate_score=长期多/空资格。BLOCK 表示硬门槛未通过。", file=f)
        print("", file=f)
        print("【长期多单形成/候选】", file=f)
        if not longs:
            print("暂无。", file=f)
        for r in sorted(longs, key=lambda x: safe_float(x.get("long_candidate_score")) or 0.0, reverse=True)[:TOP_N]:
            print(f"{r['coin']} 多 | 状态={r['candidate_state']} | 多单分={r['long_candidate_score']:+.1f} | alert={r['alert_score']:+.1f} | 分类={r['category']} | {r['conclusion']}", file=f)
        print("", file=f)
        print("【长期空单形成/候选】", file=f)
        if not shorts:
            print("暂无。", file=f)
        for r in sorted(shorts, key=lambda x: safe_float(x.get("short_candidate_score")) or 0.0, reverse=True)[:TOP_N]:
            print(f"{r['coin']} 空 | 状态={r['candidate_state']} | 空单分={r['short_candidate_score']:+.1f} | alert={r['alert_score']:+.1f} | 分类={r['category']} | {r['conclusion']}", file=f)
        print("", file=f)
        print("【长期资格未通过但有分数】", file=f)
        if not blocked:
            print("暂无。", file=f)
        for r in sorted(blocked, key=lambda x: max(safe_float(x.get("long_candidate_score")) or 0.0, safe_float(x.get("short_candidate_score")) or 0.0), reverse=True)[:TOP_N]:
            score = r['long_candidate_score'] if r.get('candidate_side') == 'long' else r['short_candidate_score']
            print(f"{r['coin']} {r['direction_cn']} | 分={score:+.1f} | gate=BLOCK | 原因={r.get('block_reasons') or '-'}", file=f)


def build_signals(run_id: int, preliminary: Dict[str, Dict[str, Any]], ctx_map: Dict[str, Dict[str, Any]], thresholds: Dict[str, Dict[str, float]], risk_map: Optional[Dict[str, Dict[str, Any]]] = None, rolling_map: Optional[Dict[str, Dict[str, Any]]] = None, gap_minutes: Optional[float] = None) -> List[Dict[str, Any]]:
    rows = _build_signals_dualscore_base(run_id, preliminary, ctx_map, thresholds, risk_map=risk_map, rolling_map=rolling_map, gap_minutes=gap_minutes)
    rows = enhance_long_short_state(run_id, rows, thresholds)
    save_coin_signals(run_id, rows)
    export_long_short_state_files(run_id, rows)
    return rows


def build_long_term_candidates(run_id: int, signals: List[Dict[str, Any]], ctx_map: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not LONG_TERM_MODE:
        return []
    out: List[Dict[str, Any]] = []
    thresholds = load_thresholds()
    for s in signals:
        coin = s.get("coin")
        direction = s.get("direction")
        side = s.get("candidate_side") or _candidate_side_name(direction)
        score = safe_float(s.get("long_candidate_score" if side == "long" else "short_candidate_score")) or 0.0
        min_watch = threshold(thresholds, coin, "min_watch_score")
        th_score = threshold(thresholds, coin, "score_push")
        state = s.get("candidate_state") or "WATCH"
        gate_ok = s.get("candidate_gate") == "PASS"
        if score < min_watch or not gate_ok:
            continue
        if LONG_SHORT_REQUIRE_CANDIDATE_STATE and state != "CANDIDATE":
            continue
        ctx = ctx_map.get(coin, {})
        s_for_plan = dict(s)
        s_for_plan["final_score"] = score if direction == "bullish" else -score
        streak = signal_streak(coin, direction, run_id, min_abs_score=min_watch)
        plan = long_term_entry_plan(s_for_plan, ctx, streak)
        state_bonus = {"FORMING": 0.3, "CONFIRMED": 1.0, "CANDIDATE": 1.8}.get(state, 0.0)
        lt_score = score + state_bonus
        if s.get("confidence") == "高":
            lt_score += 0.7
        elif s.get("confidence") == "低":
            lt_score -= 0.7
        out.append({
            "coin": coin,
            "side": side,
            "direction": direction,
            "direction_cn": "做多" if side == "long" else "做空",
            "candidate_state": state,
            "candidate_gate": s.get("candidate_gate"),
            "candidate_score": score,
            "long_candidate_score": s.get("long_candidate_score"),
            "short_candidate_score": s.get("short_candidate_score"),
            "final_score": s.get("final_score"),
            "alert_score": s.get("alert_score"),
            "long_score": s.get("long_score"),
            "signal_category": s.get("signal_category"),
            "long_term_score": lt_score,
            "threshold_score": th_score,
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
            "funding_rate_pct": s.get("funding_rate_pct"),
            "funding_risk": s.get("funding_risk"),
            "day_volume_usd": s.get("day_volume_usd"),
            "liquidity_risk": s.get("liquidity_risk"),
            "action": plan["action"],
            "entry": plan["entry"],
            "invalid": plan["invalid"],
            "price_note": plan["price_note"],
            "risk_pct": LONG_TERM_RISK_PCT,
            "block_reasons": s.get("candidate_block_reasons"),
        })
    out.sort(key=lambda x: (x.get("side") == "long", x.get("candidate_state") == "CANDIDATE", x.get("long_term_score") or 0), reverse=True)
    return out


def write_long_term_plan(candidates: List[Dict[str, Any]]) -> None:
    ensure_dirs()
    longs = [c for c in candidates if c.get("side") == "long"]
    shorts = [c for c in candidates if c.get("side") == "short"]
    with open(os.path.join(REPORT_DIR, "long_term_plan.txt"), "w", encoding="utf-8") as f:
        print("【低杠杆长期多/空观察计划】", file=f)
        print(f"更新时间{DISPLAY_TZ_NAME}：{display_now_str()} | UTC：{now_str()}", file=f)
        print("说明：长期多单和长期空单分开筛选；短线 alert 不等于长期资格。", file=f)
        print("状态：FORMING=形成中，CONFIRMED=持续确认，CANDIDATE=长期候选。", file=f)
        print(f"风控默认：单币最大亏损控制在账户 {LONG_TERM_RISK_PCT:.1f}% 左右；建议低杠杆，不一次打满。", file=f)
        print("", file=f)
        print("【长期多单】", file=f)
        if not longs:
            print("暂无适合低杠杆长期多单的候选。", file=f)
        for c in longs[:TOP_N]:
            print(f"{c['coin']} 做多 | 状态={c.get('candidate_state')} | 长期分={c['long_term_score']:.1f} | 多单资格={safe_float(c.get('long_candidate_score')) or 0:+.1f} | alert={safe_float(c.get('alert_score')) or 0:+.1f} | 连续={c['streak']}轮 | 可信度={c['confidence']}", file=f)
            print(f"类型：{c.get('signal_category')} | 信号状态：{c['signal_state']} | 价格类型：{c['signal_type']} | 建议杠杆：{c['leverage']}", file=f)
            print(f"动作：{c['action']}", file=f)
            print(f"入场：{c['entry']}", file=f)
            print(f"价格：{c['price_note']}", file=f)
            print(f"失效：{c['invalid']}", file=f)
            print("", file=f)
        print("【长期空单】", file=f)
        if not shorts:
            print("暂无适合低杠杆长期空单的候选。", file=f)
        for c in shorts[:TOP_N]:
            print(f"{c['coin']} 做空 | 状态={c.get('candidate_state')} | 长期分={c['long_term_score']:.1f} | 空单资格={safe_float(c.get('short_candidate_score')) or 0:+.1f} | alert={safe_float(c.get('alert_score')) or 0:+.1f} | 连续={c['streak']}轮 | 可信度={c['confidence']}", file=f)
            print(f"类型：{c.get('signal_category')} | 信号状态：{c['signal_state']} | 价格类型：{c['signal_type']} | 建议杠杆：{c['leverage']}", file=f)
            print(f"动作：{c['action']}", file=f)
            print(f"入场：{c['entry']}", file=f)
            print(f"价格：{c['price_note']}", file=f)
            print(f"失效：{c['invalid']}", file=f)
            print("", file=f)
    if candidates:
        with open(os.path.join(DETAILS_DIR, "long_term_candidates.csv"), "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=list(candidates[0].keys()))
            writer.writeheader(); writer.writerows(candidates)

# ===== end long-short-state-final overrides =====

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hyperliquid Wallet Monitor FINAL")
    parser.add_argument("--rpm", type=int, default=DEFAULT_RPM)
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--note", type=str, default="manual")
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run_once(parse_args()))

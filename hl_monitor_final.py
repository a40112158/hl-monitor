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
- 信号历史 1h/4h/24h 表现追踪
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


def sign_num(x: float, threshold: float = 0.0) -> int:
    if x > threshold:
        return 1
    if x < -threshold:
        return -1
    return 0


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
        liquidation_px REAL
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

    cur.execute("CREATE INDEX IF NOT EXISTS idx_perp_run_addr_coin ON perp_positions(run_id, address, coin)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_spot_run_addr_coin ON spot_balances(run_id, address, coin)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_actions_addr ON wallet_actions(address)")
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
    allowed = {"wallet_states", "perp_positions", "spot_balances", "wallet_actions", "coin_signals", "market_context"}
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
        if isinstance(lev_raw, dict):
            leverage = safe_float(lev_raw.get("value"))
        else:
            leverage = safe_float(lev_raw)
        rows.append({
            "address": address,
            "groups": groups,
            "coin": str(coin),
            "side": "long" if szi > 0 else "short",
            "szi": szi,
            "abs_szi": abs(szi),
            "mark_px": mark,
            "position_value": value,
            "entry_px": safe_float(p.get("entryPx")),
            "unrealized_pnl": safe_float(p.get("unrealizedPnl")),
            "roe": safe_float(p.get("returnOnEquity")),
            "leverage": leverage,
            "liquidation_px": safe_float(p.get("liquidationPx")),
        })
    wallet_part = {
        "perp_account_value": account_value,
        "perp_total_ntl_pos": total_ntl,
        "perp_withdrawable": withdrawable,
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
        perp_account_value, perp_total_ntl_pos, perp_withdrawable, perp_position_count,
        spot_total_value, spot_usdc_value, spot_token_count
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [(
        run_id, w.get("address"), w.get("groups"), w.get("status"), w.get("error"),
        w.get("perp_account_value"), w.get("perp_total_ntl_pos"), w.get("perp_withdrawable"), w.get("perp_position_count"),
        w.get("spot_total_value"), w.get("spot_usdc_value"), w.get("spot_token_count")
    ) for w in wallet_rows])
    cur.executemany("""
    INSERT INTO perp_positions (
        run_id, address, groups, coin, side, szi, abs_szi, mark_px, position_value,
        entry_px, unrealized_pnl, roe, leverage, liquidation_px
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [(
        run_id, p.get("address"), p.get("groups"), p.get("coin"), p.get("side"), p.get("szi"), p.get("abs_szi"), p.get("mark_px"), p.get("position_value"),
        p.get("entry_px"), p.get("unrealized_pnl"), p.get("roe"), p.get("leverage"), p.get("liquidation_px")
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
    ]:
        rows = load_rows(table, run_id)
        if not rows:
            continue
        path = os.path.join(REPORT_DIR, filename)
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)


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


def group_weight(groups: str) -> float:
    g = groups or ""
    if "smart_money" in g and "money_printer" in g:
        return 1.8
    if "smart_money" in g:
        return 1.4
    if "money_printer" in g:
        return 1.2
    return 1.0


def compute_preliminary(run_id: int, prev_run_id: Optional[int], thresholds: Dict[str, Dict[str, float]]) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
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
        w = group_weight(ref.get("groups", ""))
        cm["weighted_flow"] += active * w
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
                "active_delta": active,
                "price_effect": price_effect,
                "qty_delta": qty_delta,
                "entry_px": ref_px,
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
        w = group_weight(ref.get("groups", ""))
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
        cur.execute(f"SELECT * FROM {table} WHERE ret_1h IS NULL OR ret_4h IS NULL OR ret_24h IS NULL")
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
           AVG(CASE WHEN ret_24h > 0 THEN 1.0 WHEN ret_24h <= 0 THEN 0.0 ELSE NULL END) AS win_24h
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


def build_signals(run_id: int, preliminary: Dict[str, Dict[str, Any]], ctx_map: Dict[str, Dict[str, Any]], thresholds: Dict[str, Dict[str, float]]) -> List[Dict[str, Any]]:
    btc_ctx = ctx_map.get("BTC", {})
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
        final_score = score + conf_adj + m_adj + p_adj
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
        risk = "；".join(risk_parts) if risk_parts else "无明显额外风险"
        reason = "；".join(reasons + [conf_reason] + m_reasons + p_reasons)
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
        final_score, threshold_score, conclusion, risk, reason
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [(
        run_id, r["coin"], r["direction"], r["score"], r["confidence"], r["signal_type"], r["signal_state"], r["watchlist"],
        r["perp_active"], r["spot_active"], r["weighted_flow"], r["price_position"], r["pct_1h"], r["pct_4h"], r["pct_24h"],
        r["final_score"], r["threshold_score"], r["conclusion"], r["risk"], r["reason"]
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


def recent_24h_signal_summary() -> List[Dict[str, Any]]:
    conn = db_conn()
    cur = conn.cursor()
    since = (utc_now() - dt.timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    cur.execute("""
    SELECT coin, direction, COUNT(*) AS n, AVG(score) AS avg_score, MAX(ABS(score)) AS max_abs_score
    FROM signal_events
    WHERE created_at >= ?
    GROUP BY coin, direction
    ORDER BY max_abs_score DESC
    LIMIT 30
    """, (since,))
    rows = [dict(x) for x in cur.fetchall()]
    conn.close()
    return rows


def recent_24h_wallet_flow() -> List[Dict[str, Any]]:
    conn = db_conn()
    cur = conn.cursor()
    since = (utc_now() - dt.timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    cur.execute("""
    SELECT coin, market, direction, COUNT(*) AS n, SUM(active_delta) AS active_sum
    FROM wallet_actions
    WHERE created_at >= ?
    GROUP BY coin, market, direction
    ORDER BY ABS(active_sum) DESC
    LIMIT 30
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
    return utc_now().hour == DAILY_PUSH_HOUR_UTC and not already_pushed_today("daily")



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
    if pct4 >= 5 or pct24 >= 12 or stype in ("高位追多", "低位追空"):
        cap = min(cap, 2.0)
    elif confidence == "高" and score >= 9 and stype in ("低位吸筹", "高位加空", "普通趋势", "高位突破", "低位破位"):
        cap = min(cap, 3.0)
    else:
        cap = min(cap, 2.5)

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

    if confidence == "低" or state in avoid_states or bad_position:
        action = "只观察，不适合直接做长期单"
        entry = "等待下一轮确认；不要因为单次异动直接开仓。"
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
            lines.append(f"{a['coin']} {a['market']} {dir_cn(a['direction'])} {short_addr(a['address'])} [{a.get('groups','')}] | 主动={fmt_money(a['active_delta'])} | 价格影响={fmt_money(a['price_effect'])}")
    lines.append("")
    lines.append("【资金流 Lite】")
    lines.append("说明：基于钱包 USDC 和现货余额变化推断，不是外部链上充值提现标签。")
    if not cashflows:
        lines.append("暂无明显 USDC/现货资金流变化。")
    else:
        for c in cashflows[:TOP_N]:
            lines.append(f"{short_addr(c['address'])} [{c['groups']}] | USDC={fmt_money(c['usdc_delta'])} | 现货={fmt_money(c['spot_delta'])} | {c['flow_type']}")
    lines.append("")
    lines.append("【过去24h 信号复盘】")
    summary = recent_24h_signal_summary()
    if not summary:
        lines.append("暂无24h信号数据。")
    else:
        for r in summary[:TOP_N]:
            lines.append(f"{r['coin']} {dir_cn(r['direction'])} | 次数={r['n']} | 均分={r['avg_score']:+.2f} | 最高={r['max_abs_score']:.2f}")
    lines.append("")
    lines.append("【过去24h 钱包主动资金流】")
    flow = recent_24h_wallet_flow()
    if not flow:
        lines.append("暂无24h钱包动作数据。")
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
    with open(os.path.join(REPORT_DIR, f"final_report_run_{run_id}.txt"), "w", encoding="utf-8") as f:
        f.write(report)
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


def prune_reports(keep: int = 5) -> None:
    ensure_dirs()
    try:
        files = []
        for name in os.listdir(REPORT_DIR):
            if name.startswith("final_report_run_") and name.endswith(".txt"):
                path = os.path.join(REPORT_DIR, name)
                files.append((os.path.getmtime(path), path))
        files.sort(reverse=True)
        for _, path in files[keep:]:
            os.remove(path)
    except Exception:
        pass


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
    prev_id = get_previous_run_id(run_id)

    total = len(wallet_rows)
    ok = sum(1 for w in wallet_rows if w.get("status") == "ok")
    partial = sum(1 for w in wallet_rows if w.get("status") == "partial")
    ok_rate = (ok + partial * 0.5) / total if total else 0.0

    updated_actions, updated_signals = evaluate_events({**spot_coin_price, **mid_prices})

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
            f"第一次运行，已建立快照。第二次开始才有趋势对比。"
        )
        with open(os.path.join(REPORT_DIR, "final_latest_report.txt"), "w", encoding="utf-8") as f:
            f.write(report)
        with open(os.path.join(REPORT_DIR, "long_term_plan.txt"), "w", encoding="utf-8") as f:
            f.write("第一次运行，已建立快照。第二次开始生成低杠杆长期单观察计划。\n")
        daily_due = should_push_daily()
        pushed = False
        if PUSH_EVERY_RUN or daily_due:
            pushed = await send_tg(report)
            if pushed and daily_due:
                mark_pushed("daily")
        finish_run(run_id, wallet_rows, perp_rows, spot_rows, pushed)
        print(report, flush=True)
        return

    preliminary, actions, cashflows = compute_preliminary(run_id, prev_id, thresholds)
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

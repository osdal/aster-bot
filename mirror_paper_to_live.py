#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mirror_paper_to_live.py  (Freeze + GlobalReset + Heartbeat + WS watchdog + Paper timeout task)

Логика:
1) PAPER торгует по всем разрешённым символам, считает серии подряд минусов по каждой паре.
   Минус = CLOSE по SL.
   TIMEOUT: если netPnL >= 0 -> плюс (сброс серии); если netPnL < 0 -> минус (увеличиваем серию).
   TP = плюс (сброс серии).

2) Когда по ЛЮБОЙ паре streak_losses достигает LOSS_STREAK_TO_ARM:
   - PAPER перестаёт открывать новые сделки ВООБЩЕ (freeze entries глобально).
   - Запоминаем trigger_symbol = эта пара.
   - Дальше ждём НОВЫЙ сигнал именно по trigger_symbol.
     PAPER по trigger_symbol НЕ открываем (только детектим сигнал).
   - Как только сигнал появился и по trigger_symbol нет открытой PAPER позиции -> открываем LIVE по этому сигналу.

3) После закрытия LIVE (любой исход):
   - Сбрасываем streak_losses у ВСЕХ пар (global reset).
   - Снимаем freeze и PAPER снова открывает сделки.

Добавлено по просьбе:
- Heartbeat раз в 30–60 секунд: печать last_tick_age и режима (NORMAL/FROZEN).
- WS watchdog: если now - last_ws_msg_ts > WS_STALE_SEC несколько раз подряд -> принудительный reconnection.
- Таймауты PAPER не только “на тике”: отдельная async-задача, закрывающая позиции по времени даже если тиков нет
  (цена берётся из last_price, а если её нет/старая — через REST bookTicker).

Требования:
pip install websockets httpx python-dotenv

ВАЖНО:
- Файл самодостаточный.
- Если LIVE_ENABLED=true, нужны ASTER_API_KEY/ASTER_API_SECRET.
"""

from __future__ import annotations

import os
import math
import time
import json
import hmac
import hashlib
import asyncio
import signal
import random
import csv
from dataclasses import dataclass
from typing import Dict, Deque, Optional, Tuple, List
from collections import deque
from pathlib import Path
from urllib.parse import urlencode

import httpx
import websockets
from dotenv import load_dotenv


# =========================
# Helpers / env
# =========================

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name, str(default)).strip().lower()
    return v in ("1", "true", "yes", "y", "on")

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name, str(default)).strip()
    try:
        return int(float(v))
    except Exception:
        return default

def _env_float(name: str, default: float) -> float:
    v = os.getenv(name, str(default)).strip()
    try:
        return float(v)
    except Exception:
        return default

def _env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()

def _csv_safe_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")

def _now_ms() -> int:
    return int(time.time() * 1000)

def _ts_iso() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def _ensure_parent_dir(path: str) -> None:
    p = Path(path)
    if p.parent:
        p.parent.mkdir(parents=True, exist_ok=True)

def _append_csv(path: str, header: List[str], row: Dict[str, object]) -> None:
    _ensure_parent_dir(path)
    file_exists = Path(path).exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not file_exists:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in header})

def _round_step(x: float, step: float) -> float:
    """Round quantity DOWN to the nearest stepSize."""
    if step <= 0:
        return float(x)
    return math.floor(float(x) / float(step)) * float(step)

def _round_tick(price: float, tick: float) -> float:
    """Round price DOWN to the nearest tickSize (safe for stopPrice filters)."""
    if tick <= 0:
        return float(price)
    return math.floor(float(price) / float(tick)) * float(tick)


def _sign_hmac_sha256(secret: str, query_string: str) -> str:
    return hmac.new(secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256).hexdigest()



def _safe_ctx(params: dict) -> dict:
    """Return a small safe subset of params for logging."""
    if not isinstance(params, dict):
        return {}
    keys = ("symbol", "type", "side", "quantity", "stopPrice", "closePosition", "reduceOnly")
    out = {}
    for k in keys:
        if k in params and params.get(k) is not None:
            out[k] = params.get(k)
    return out

# =========================
# Config
# =========================

@dataclass
class Config:
    # endpoints
    REST_BASE: str = "https://fapi.asterdex.com"
    WS_BASE: str = "wss://fstream.asterdex.com"
    WS_MODE: str = "AUTO"  # AUTO | COMBINED | SUBSCRIBE

    # symbol universe
    SYMBOL_MODE: str = "HYBRID_PRIORITY"  # HYBRID_PRIORITY | WHITELIST_ONLY
    WHITELIST: List[str] = None
    BLACKLIST: List[str] = None
    SKIP_SYMBOLS: List[str] = None

    QUOTE: str = "USDT"
    AUTO_TOP_N: int = 40
    TARGET_SYMBOLS: int = 20
    REFRESH_UNIVERSE_SEC: int = 900
    MIN_24H_QUOTE_VOL: float = 30_000_000

    # signal params (impulse breakout)
    IMPULSE_LOOKBACK_SEC: int = 10
    BREAKOUT_BUFFER_PCT: float = 0.10  # %
    MAX_SPREAD_PCT: float = 0.03       # %
    MIN_ATR_PCT: float = 0.03          # %
    TF_SEC: int = 60
    LOOKBACK_MINUTES: int = 20
    ATR_PERIOD: int = 14

    # paper
    PAPER_ENABLED: bool = True
    PAPER_LOG_PATH: str = "data/paper_trades.csv"
    TRADE_NOTIONAL_USD: float = 50.0
    MAX_HOLDING_SEC: int = 600
    MAX_TRADES_PER_HOUR: int = 100000
    COOLDOWN_AFTER_TRADE_SEC: int = 0
    TP_PCT: float = 1.0
    SL_PCT: float = 0.8
    LOSS_STREAK_TO_ARM: int = 2

    # live
    LIVE_ENABLED: bool = True
    LIVE_LOG_PATH: str = "data/live_trades.csv"
    LIVE_NOTIONAL_USD: float = 5.0
    LIVE_LEVERAGE: int = 2
    LIVE_MAX_POSITIONS: int = 1
    MAX_DEVIATION_PCT: float = 0.50
    MIN_NOTIONAL_BUFFER_PCT: float = 5.0

    # watch
    WATCH_POLL_SEC: float = 2.0
    WATCH_PROFIT_TIMEOUT_SEC: int = 6000
    WATCH_HARD_TIMEOUT_SEC: int = 12000
    EMERGENCY_CLOSE_ON_HARD_TIMEOUT: bool = False  # if True: market-close if still open after WATCH_HARD_TIMEOUT_SEC


    # heartbeat / watchdog
    HEARTBEAT_MIN_SEC: int = 30
    HEARTBEAT_MAX_SEC: int = 60
    WS_STALE_SEC: int = 120           # age since last ws message to consider stale
    WS_STALE_HITS_TO_RECONNECT: int = 2

    # auth
    ASTER_API_KEY: str = ""
    ASTER_API_SECRET: str = ""

    DEBUG: bool = False


    # selftest / safety
    STARTUP_SELFTEST: bool = True
    DEEP_SELFTEST: bool = False
    DRY_RUN_LIVE: bool = False
    @staticmethod
    def load() -> "Config":
        load_dotenv(override=False)

        def split_list(v: str) -> List[str]:
            if not v:
                return []
            return [s.strip().upper() for s in v.split(",") if s.strip()]

        cfg = Config()
        cfg.REST_BASE = _env_str("ASTER_REST_BASE", cfg.REST_BASE)
        cfg.WS_BASE = _env_str("ASTER_WS_BASE", cfg.WS_BASE)
        cfg.WS_MODE = _env_str("ASTER_WS_MODE", _env_str("WS_MODE", cfg.WS_MODE)).upper()

        cfg.SYMBOL_MODE = _env_str("SYMBOL_MODE", cfg.SYMBOL_MODE)
        cfg.WHITELIST = split_list(_env_str("WHITELIST", ""))
        cfg.BLACKLIST = split_list(_env_str("BLACKLIST", ""))
        cfg.SKIP_SYMBOLS = split_list(_env_str("SKIP_SYMBOLS", ""))

        cfg.QUOTE = _env_str("QUOTE", cfg.QUOTE).upper()
        cfg.AUTO_TOP_N = _env_int("AUTO_TOP_N", cfg.AUTO_TOP_N)
        cfg.TARGET_SYMBOLS = _env_int("TARGET_SYMBOLS", cfg.TARGET_SYMBOLS)
        cfg.REFRESH_UNIVERSE_SEC = _env_int("REFRESH_UNIVERSE_SEC", cfg.REFRESH_UNIVERSE_SEC)
        cfg.MIN_24H_QUOTE_VOL = _env_float("MIN_24H_QUOTE_VOL", cfg.MIN_24H_QUOTE_VOL)

        cfg.IMPULSE_LOOKBACK_SEC = _env_int("IMPULSE_LOOKBACK_SEC", cfg.IMPULSE_LOOKBACK_SEC)
        cfg.BREAKOUT_BUFFER_PCT = _env_float("BREAKOUT_BUFFER_PCT", cfg.BREAKOUT_BUFFER_PCT)
        cfg.MAX_SPREAD_PCT = _env_float("MAX_SPREAD_PCT", cfg.MAX_SPREAD_PCT)
        cfg.MIN_ATR_PCT = _env_float("MIN_ATR_PCT", cfg.MIN_ATR_PCT)
        cfg.TF_SEC = _env_int("TF_SEC", cfg.TF_SEC)
        cfg.LOOKBACK_MINUTES = _env_int("LOOKBACK_MINUTES", cfg.LOOKBACK_MINUTES)
        cfg.ATR_PERIOD = _env_int("ATR_PERIOD", cfg.ATR_PERIOD)

        cfg.PAPER_ENABLED = _env_bool("PAPER_ENABLED", True)
        cfg.PAPER_LOG_PATH = _env_str("PAPER_LOG_PATH", _env_str("PAPER_LOG", cfg.PAPER_LOG_PATH))
        cfg.TRADE_NOTIONAL_USD = _env_float("TRADE_NOTIONAL_USD", cfg.TRADE_NOTIONAL_USD)
        cfg.MAX_HOLDING_SEC = _env_int("MAX_HOLDING_SEC", cfg.MAX_HOLDING_SEC)
        cfg.MAX_TRADES_PER_HOUR = _env_int("MAX_TRADES_PER_HOUR", cfg.MAX_TRADES_PER_HOUR)
        cfg.COOLDOWN_AFTER_TRADE_SEC = _env_int("COOLDOWN_AFTER_TRADE_SEC", cfg.COOLDOWN_AFTER_TRADE_SEC)
        cfg.TP_PCT = _env_float("TP_PCT", cfg.TP_PCT)
        cfg.SL_PCT = _env_float("SL_PCT", cfg.SL_PCT)
        cfg.LOSS_STREAK_TO_ARM = _env_int("LOSS_STREAK_TO_ARM", cfg.LOSS_STREAK_TO_ARM)

        cfg.LIVE_ENABLED = _env_bool("LIVE_ENABLED", _env_bool("LIVE_MODE", cfg.LIVE_ENABLED))
        cfg.LIVE_LOG_PATH = _env_str("LIVE_LOG_PATH", cfg.LIVE_LOG_PATH)
        cfg.LIVE_NOTIONAL_USD = _env_float("LIVE_NOTIONAL_USD", cfg.LIVE_NOTIONAL_USD)
        cfg.LIVE_LEVERAGE = _env_int("LIVE_LEVERAGE", cfg.LIVE_LEVERAGE)
        cfg.LIVE_MAX_POSITIONS = _env_int("LIVE_MAX_POSITIONS", cfg.LIVE_MAX_POSITIONS)
        cfg.MAX_DEVIATION_PCT = _env_float("MAX_DEVIATION_PCT", cfg.MAX_DEVIATION_PCT)
        cfg.MIN_NOTIONAL_BUFFER_PCT = _env_float("MIN_NOTIONAL_BUFFER_PCT", cfg.MIN_NOTIONAL_BUFFER_PCT)

        cfg.WATCH_POLL_SEC = _env_float("WATCH_POLL_SEC", cfg.WATCH_POLL_SEC)
        cfg.WATCH_PROFIT_TIMEOUT_SEC = _env_int("WATCH_PROFIT_TIMEOUT_SEC", cfg.WATCH_PROFIT_TIMEOUT_SEC)
        cfg.WATCH_HARD_TIMEOUT_SEC = _env_int("WATCH_HARD_TIMEOUT_SEC", cfg.WATCH_HARD_TIMEOUT_SEC)

        cfg.HEARTBEAT_MIN_SEC = _env_int("HEARTBEAT_MIN_SEC", cfg.HEARTBEAT_MIN_SEC)
        cfg.HEARTBEAT_MAX_SEC = _env_int("HEARTBEAT_MAX_SEC", cfg.HEARTBEAT_MAX_SEC)
        cfg.WS_STALE_SEC = _env_int("WS_STALE_SEC", cfg.WS_STALE_SEC)
        cfg.WS_STALE_HITS_TO_RECONNECT = _env_int("WS_STALE_HITS_TO_RECONNECT", cfg.WS_STALE_HITS_TO_RECONNECT)

        cfg.ASTER_API_KEY = _env_str("ASTER_API_KEY", "")
        cfg.ASTER_API_SECRET = _env_str("ASTER_API_SECRET", "")

        cfg.DEBUG = _env_bool("DEBUG", cfg.DEBUG)


        cfg.STARTUP_SELFTEST = _env_bool('STARTUP_SELFTEST', True)
        cfg.DEEP_SELFTEST = _env_bool('DEEP_SELFTEST', False)
        cfg.DRY_RUN_LIVE = _env_bool('DRY_RUN_LIVE', False)
        # sanitize heartbeat bounds
        if cfg.HEARTBEAT_MAX_SEC < cfg.HEARTBEAT_MIN_SEC:
            cfg.HEARTBEAT_MAX_SEC = cfg.HEARTBEAT_MIN_SEC

        return cfg


# =========================
# Exchange (Binance-like Futures)
# =========================

class AsterFapi:
    """Minimal Binance-Futures-like REST wrapper for AsterDex Perp (orderbook) endpoints."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._time_offset_ms = 0
        self._time_sync_ts = 0.0

        headers = {"X-MBX-APIKEY": cfg.ASTER_API_KEY} if cfg.ASTER_API_KEY else {}
        self.client = httpx.AsyncClient(base_url=cfg.REST_BASE, timeout=20.0, headers=headers)

        self._exchange_info = None
        self._symbol_filters: Dict[str, Dict[str, float]] = {}  # stepSize/minQty/minNotional/tickSize

    async def close(self):
        await self.client.aclose()

    async def _public_get(self, path: str, params: dict | None = None):
        r = await self.client.get(path, params=params)
        r.raise_for_status()
        return r.json()

    async def _ensure_time_offset(self, force: bool = False) -> None:
        if self.cfg.DRY_RUN_LIVE:
            return
        now = time.time()
        if (not force) and (now - self._time_sync_ts) < 60:
            return
        try:
            j = await self._public_get("/fapi/v1/time")
            server_ms = int(j.get("serverTime", 0))
            local_ms = int(time.time() * 1000)
            self._time_offset_ms = server_ms - local_ms
            self._time_sync_ts = now
            print(f"[TIME] synced: offset_ms={self._time_offset_ms}")
        except Exception as e:
            print(f"[TIME] WARN: sync failed: {e}")

    async def _signed(self, method: str, path: str, params: dict):
        if self.cfg.DRY_RUN_LIVE:
            if path.endswith("/order") and method.upper() == "POST":
                oid = str(int(time.time() * 1000) % 10_000_000_000)
                return {"orderId": oid, "status": "FILLED"}
            return {"ok": True}

        await self._ensure_time_offset()

        if not self.cfg.ASTER_API_KEY or not self.cfg.ASTER_API_SECRET:
            raise RuntimeError("LIVE enabled but ASTER_API_KEY/ASTER_API_SECRET not set.")

        params = dict(params or {})
        params["recvWindow"] = int(params.get("recvWindow", 5000))
        params["timestamp"] = int(time.time() * 1000 + int(getattr(self, "_time_offset_ms", 0)))

        qs = urlencode(params, doseq=True)
        sig = _sign_hmac_sha256(self.cfg.ASTER_API_SECRET, qs)
        params["signature"] = sig

        r = await self.client.request(method, path, params=params)
        r.raise_for_status()
        j = r.json()
        if isinstance(j, dict) and ("code" in j) and str(j.get("code")) not in ("0", "200", "SUCCESS"):
            raise RuntimeError(f"API error code={j.get('code')} msg={j.get('msg') or j.get('message') or j}")
        return j

    async def exchange_info(self):
        if self._exchange_info is None:
            info = await self._public_get("/fapi/v1/exchangeInfo")
            self._exchange_info = info
            self._parse_exchange_info(info)
        return self._exchange_info

    def _parse_exchange_info(self, info: dict):
        self._symbol_filters.clear()
        for s in info.get("symbols", []):
            sym = s.get("symbol")
            if not sym:
                continue
            filters = {
                f["filterType"]: f
                for f in s.get("filters", [])
                if isinstance(f, dict) and "filterType" in f
            }
            step = 0.0
            min_qty = 0.0
            min_notional = 0.0
            tick = 0.0

            if "LOT_SIZE" in filters:
                step = _csv_safe_float(filters["LOT_SIZE"].get("stepSize", 0))
                min_qty = _csv_safe_float(filters["LOT_SIZE"].get("minQty", 0))

            if "PRICE_FILTER" in filters:
                tick = _csv_safe_float(filters["PRICE_FILTER"].get("tickSize", 0))

            if "MIN_NOTIONAL" in filters:
                min_notional = _csv_safe_float(filters["MIN_NOTIONAL"].get("notional", 0))
            elif "NOTIONAL" in filters:
                min_notional = _csv_safe_float(filters["NOTIONAL"].get("minNotional", 0))

            self._symbol_filters[sym] = {
                "stepSize": float(step),
                "minQty": float(min_qty),
                "minNotional": float(min_notional),
                "tickSize": float(tick),
            }

    def get_symbol_filters(self, symbol: str) -> Dict[str, float]:
        return self._symbol_filters.get(
            symbol, {"stepSize": 0.0, "minQty": 0.0, "minNotional": 0.0, "tickSize": 0.0}
        )

    async def book_ticker(self, symbol: str) -> Tuple[float, float]:
        j = await self._public_get("/fapi/v1/ticker/bookTicker", params={"symbol": symbol})
        bid = _csv_safe_float(j.get("bidPrice"))
        ask = _csv_safe_float(j.get("askPrice"))
        return bid, ask

    async def ticker_price(self, symbol: str) -> float:
        j = await self._public_get("/fapi/v1/ticker/price", params={"symbol": symbol})
        return _csv_safe_float(j.get("price"))

    async def tickers_24h(self):
        return await self._public_get("/fapi/v1/ticker/24hr")

    async def set_leverage(self, symbol: str, leverage: int):
        return await self._signed("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage})

    async def place_order(self, symbol: str, side: str, qty: float, reduce_only: bool = False):
        params = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": f"{qty:.10f}".rstrip("0").rstrip("."),
        }
        if reduce_only:
            params["reduceOnly"] = "true"
        return await self._signed("POST", "/fapi/v1/order", params)

    async def cancel_all_open_orders(self, symbol: str):
        return await self._signed("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol})

    async def open_orders(self, symbol: str):
        return await self._signed("GET", "/fapi/v1/openOrders", {"symbol": symbol})

    async def place_conditional_close_all(
        self,
        symbol: str,
        side: str,
        order_type: str,
        stop_price: float,
        working_type: str = "MARK_PRICE",
        price_protect: bool = True,
        *,
        quantity: float,
    ):
        base = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "stopPrice": f"{stop_price:.10f}".rstrip("0").rstrip("."),
            "quantity": f"{quantity:.10f}".rstrip("0").rstrip("."),
            "reduceOnly": "true",
        }
        try:
            return await self._signed("POST", "/fapi/v1/order", base)
        except Exception as e1:
            params2 = dict(base)
            params2["price"] = params2["stopPrice"]
            params2["timeInForce"] = "GTC"
            params2["workingType"] = working_type
            if price_protect:
                params2["priceProtect"] = "TRUE"
            try:
                return await self._signed("POST", "/fapi/v1/order", params2)
            except Exception as e2:
                raise RuntimeError(f"Failed to place conditional order (attempt1={e1}) (attempt2={e2})")

    async def user_trades(self, symbol: str, start_time_ms: int | None = None, end_time_ms: int | None = None, limit: int = 200):
        params = {"symbol": symbol, "limit": int(limit)}
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)
        return await self._signed("GET", "/fapi/v1/userTrades", params)

    async def all_orders(self, symbol: str, start_time_ms: int | None = None, end_time_ms: int | None = None, limit: int = 200):
        params = {"symbol": symbol, "limit": int(limit)}
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)
        return await self._signed("GET", "/fapi/v1/allOrders", params)

    async def position_risk(self, symbol: str):
        try:
            return await self._signed("GET", "/fapi/v2/positionRisk", {"symbol": symbol})
        except Exception:
            return await self._signed("GET", "/fapi/v1/positionRisk", {"symbol": symbol})
# =========================
# Market data + indicators
# =========================

@dataclass
class Bar:
    ts: int
    o: float
    h: float
    l: float
    c: float

class IndicatorState:
    def __init__(self, tf_sec: int, max_bars: int):
        self.tf_ms = tf_sec * 1000
        self.bars: Deque[Bar] = deque(maxlen=max_bars)
        self._cur_bucket = None  # [bucket_ts, o,h,l,c]

    def update_trade(self, ts_ms: int, price: float):
        bts = (ts_ms // self.tf_ms) * self.tf_ms
        if self._cur_bucket is None or self._cur_bucket[0] != bts:
            if self._cur_bucket is not None:
                _, o, h, l, c = self._cur_bucket
                self.bars.append(Bar(ts=self._cur_bucket[0], o=o, h=h, l=l, c=c))
            self._cur_bucket = [bts, price, price, price, price]
        else:
            self._cur_bucket[2] = max(self._cur_bucket[2], price)
            self._cur_bucket[3] = min(self._cur_bucket[3], price)
            self._cur_bucket[4] = price

    def atr(self, period: int) -> Optional[float]:
        if len(self.bars) < period + 1:
            return None
        bars = list(self.bars)[-(period + 1):]
        trs = []
        for i in range(1, len(bars)):
            prev_close = bars[i - 1].c
            high = bars[i].h
            low = bars[i].l
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            trs.append(tr)
        if len(trs) < period:
            return None
        return sum(trs[-period:]) / period


# =========================
# Paper trading
# =========================

@dataclass
class Position:
    symbol: str
    side: str  # LONG/SHORT
    entry: float
    tp: float
    sl: float
    opened_ts: float

class PaperEngine:
    PAPER_CSV_HEADER = [
        "ts", "symbol", "side", "event", "entry", "exit", "tp", "sl",
        "pnl_pct", "net_pnl_usd", "reason"
    ]

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.positions: Dict[str, Position] = {}
        self.last_trade_ts: Dict[str, float] = {}
        self.trade_counts_hour: Deque[float] = deque(maxlen=200000)

        self.streak_losses: Dict[str, int] = {}
        self.freeze_paper_entries: bool = False
        self.freeze_streak_updates: bool = False
        self.trigger_symbol: Optional[str] = None

    def mode(self) -> str:
        return "FROZEN" if self.freeze_paper_entries else "NORMAL"

    def _can_open(self, symbol: str) -> bool:
        if self.freeze_paper_entries:
            return False
        if symbol in self.positions:
            return False
        now = time.time()
        last = self.last_trade_ts.get(symbol, 0.0)
        if now - last < self.cfg.COOLDOWN_AFTER_TRADE_SEC:
            return False
        cutoff = now - 3600
        while self.trade_counts_hour and self.trade_counts_hour[0] < cutoff:
            self.trade_counts_hour.popleft()
        if len(self.trade_counts_hour) >= self.cfg.MAX_TRADES_PER_HOUR:
            return False
        return True

    def open(self, symbol: str, side: str, price: float):
        tp = price * (1 + self.cfg.TP_PCT / 100.0) if side == "LONG" else price * (1 - self.cfg.TP_PCT / 100.0)
        sl = price * (1 - self.cfg.SL_PCT / 100.0) if side == "LONG" else price * (1 + self.cfg.SL_PCT / 100.0)
        pos = Position(symbol=symbol, side=side, entry=price, tp=tp, sl=sl, opened_ts=time.time())
        self.positions[symbol] = pos
        self.last_trade_ts[symbol] = pos.opened_ts
        self.trade_counts_hour.append(pos.opened_ts)

        print(f"[PAPER] OPEN {symbol} {side} entry={price:.6g} tp={tp:.6g} sl={sl:.6g}")
        _append_csv(self.cfg.PAPER_LOG_PATH, self.PAPER_CSV_HEADER, {
            "ts": _ts_iso(),
            "symbol": symbol,
            "side": side,
            "event": "OPEN",
            "entry": f"{price:.10f}",
            "exit": "",
            "tp": f"{tp:.10f}",
            "sl": f"{sl:.10f}",
            "pnl_pct": "",
            "net_pnl_usd": "",
            "reason": ""
        })

    def _pnl_pct(self, pos: Position, exit_price: float) -> float:
        if pos.side == "LONG":
            return (exit_price - pos.entry) / pos.entry * 100.0
        else:
            return (pos.entry - exit_price) / pos.entry * 100.0

    def close(self, symbol: str, exit_price: float, reason: str):
        pos = self.positions.pop(symbol, None)
        if not pos:
            return None

        pnl_pct = self._pnl_pct(pos, exit_price)
        net = self.cfg.TRADE_NOTIONAL_USD * (pnl_pct / 100.0)

        print(f"[PAPER] CLOSE {symbol} {pos.side} exit={exit_price:.6g} pnl=({pnl_pct:+.3f}%) reason={reason}")
        _append_csv(self.cfg.PAPER_LOG_PATH, self.PAPER_CSV_HEADER, {
            "ts": _ts_iso(),
            "symbol": symbol,
            "side": pos.side,
            "event": "CLOSE",
            "entry": f"{pos.entry:.10f}",
            "exit": f"{exit_price:.10f}",
            "tp": f"{pos.tp:.10f}",
            "sl": f"{pos.sl:.10f}",
            "pnl_pct": f"{pnl_pct:.6f}",
            "net_pnl_usd": f"{net:.10f}",
            "reason": reason
        })

        # streak updates
        if not self.freeze_streak_updates:
            if reason == "SL":
                is_loss = True
            elif reason == "TP":
                is_loss = False
            elif reason == "TIMEOUT":
                is_loss = net < 0
            else:
                is_loss = net < 0

            if is_loss:
                self.streak_losses[symbol] = self.streak_losses.get(symbol, 0) + 1
            else:
                self.streak_losses[symbol] = 0

            streak = self.streak_losses[symbol]
            print(f"[STREAK] {symbol}: paper reason={reason} netPnL={net:+.6f} streak_losses={streak}")

            if (not self.freeze_paper_entries) and streak >= self.cfg.LOSS_STREAK_TO_ARM:
                self.freeze_paper_entries = True
                self.freeze_streak_updates = True
                self.trigger_symbol = symbol
                print(f"[ARM] {symbol}: reached {streak} losses -> FREEZE PAPER and wait LIVE signal")

        return {"symbol": symbol, "pnl_pct": pnl_pct, "net_pnl_usd": net, "reason": reason}

    def maybe_close_on_price(self, symbol: str, price: float):
        pos = self.positions.get(symbol)
        if not pos:
            return None

        if pos.side == "LONG":
            if price >= pos.tp:
                return self.close(symbol, price, "TP")
            if price <= pos.sl:
                return self.close(symbol, price, "SL")
        else:
            if price <= pos.tp:
                return self.close(symbol, price, "TP")
            if price >= pos.sl:
                return self.close(symbol, price, "SL")

        if self.cfg.MAX_HOLDING_SEC > 0 and (time.time() - pos.opened_ts) >= self.cfg.MAX_HOLDING_SEC:
            return self.close(symbol, price, "TIMEOUT")
        return None

    def timed_out_symbols(self, now: float) -> List[str]:
        if self.cfg.MAX_HOLDING_SEC <= 0:
            return []
        out = []
        for sym, pos in list(self.positions.items()):
            if (now - pos.opened_ts) >= self.cfg.MAX_HOLDING_SEC:
                out.append(sym)
        return out

    def reset_all_streaks(self, active_symbols: Optional[List[str]] = None):
        for k in list(self.streak_losses.keys()):
            self.streak_losses[k] = 0
        if active_symbols:
            for s in active_symbols:
                self.streak_losses.setdefault(s, 0)
                self.streak_losses[s] = 0
        self.freeze_paper_entries = False
        self.freeze_streak_updates = False
        self.trigger_symbol = None
        print("[RESET] Global reset: streaks=0 for all, PAPER resumes entries")


# =========================
# Signals
# =========================

class SignalEngine:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.ticks: Dict[str, Deque[Tuple[float, float]]] = {}
        self.maxlen = max(500, cfg.IMPULSE_LOOKBACK_SEC * 5)

    def update(self, symbol: str, price: float):
        dq = self.ticks.get(symbol)
        if dq is None:
            dq = deque(maxlen=self.maxlen)
            self.ticks[symbol] = dq
        dq.append((time.time(), price))

    def impulse_return_pct(self, symbol: str) -> Optional[float]:
        dq = self.ticks.get(symbol)
        if not dq or len(dq) < 2:
            return None
        now = time.time()
        cutoff = now - self.cfg.IMPULSE_LOOKBACK_SEC
        older = None
        for ts, px in dq:
            if ts >= cutoff:
                older = (ts, px)
                break
        if older is None:
            older = dq[0]
        old_px = older[1]
        last_px = dq[-1][1]
        if old_px <= 0:
            return None
        return (last_px - old_px) / old_px * 100.0

    def signal_side(self, symbol: str, atr_pct: Optional[float], spread_pct: Optional[float]) -> Optional[str]:
        ret = self.impulse_return_pct(symbol)
        if ret is None:
            return None
        if abs(ret) < self.cfg.BREAKOUT_BUFFER_PCT:
            return None
        if atr_pct is None or atr_pct < self.cfg.MIN_ATR_PCT:
            return None
        if spread_pct is None or spread_pct > self.cfg.MAX_SPREAD_PCT:
            return None
        return "LONG" if ret > 0 else "SHORT"


# =========================
# Live trading
# =========================

class LiveEngine:
    LIVE_CSV_HEADER = [
        "ts", "symbol", "side", "entry", "exit", "qty", "leverage",
        "pnl_pct", "net_pnl_usd", "outcome", "reason", "order_id_entry", "order_id_exit"
    ]

    def _append_live_csv(
        self,
        ts: float,
        symbol: str,
        side: str,
        entry: float,
        exit_price: float,
        qty: float,
        leverage: int,
        pnl_pct: float,
        net_pnl_usd: float,
        outcome: str,
        reason: str,
        order_id_entry: str,
        order_id_exit: str,
    ) -> None:
        _append_csv(self.cfg.LIVE_LOG_PATH, self.LIVE_CSV_HEADER, {
            'ts': _ts_iso(),
            'symbol': symbol,
            'side': side,
            'entry': f"{entry:.10f}",
            'exit': f"{exit_price:.10f}",
            'qty': f"{qty:.8f}",
            'leverage': str(int(leverage)),
            'pnl_pct': f"{pnl_pct:.6f}",
            'net_pnl_usd': f"{net_pnl_usd:.10f}",
            'outcome': outcome,
            'reason': reason,
            'order_id_entry': order_id_entry,
            'order_id_exit': order_id_exit,
        })


    def __init__(self, cfg: Config, api: AsterFapi):
        self.cfg = cfg
        self.api = api
        self.open_positions: Dict[str, dict] = {}  # symbol -> data

    async def reconcile_position(self, symbol: str) -> Optional[dict]:
        """Sync local open_positions with exchange positionRisk for symbol."""
        try:
            j = await self.api.position_risk(symbol)
        except Exception:
            return self.open_positions.get(symbol)

        pos = None
        if isinstance(j, list) and j:
            pos = j[0]
        elif isinstance(j, dict):
            pos = j

        if not isinstance(pos, dict):
            return self.open_positions.get(symbol)

        amt = _csv_safe_float(pos.get("positionAmt"))
        entry = _csv_safe_float(pos.get("entryPrice"))
        if abs(amt) < 1e-12:
            # no position on exchange
            self.open_positions.pop(symbol, None)
            return None

        side = "LONG" if amt > 0 else "SHORT"
        self.open_positions[symbol] = {
            "symbol": symbol,
            "side": side,
            "qty": abs(amt),
            "entry": entry if entry > 0 else None,
            "opened_ts": self.open_positions.get(symbol, {}).get("opened_ts", time.time()),
            "order_id_entry": self.open_positions.get(symbol, {}).get("order_id_entry", ""),
        }
        return self.open_positions[symbol]

    async def open_live(self, symbol: str, side: str, last_price: float) -> dict:
        # if local says positions full, reconcile for this symbol (most common stuck case)
        if len(self.open_positions) >= self.cfg.LIVE_MAX_POSITIONS:
            await self.reconcile_position(symbol)
        if len(self.open_positions) >= self.cfg.LIVE_MAX_POSITIONS:
            raise RuntimeError(f"LIVE_MAX_POSITIONS reached: {self.cfg.LIVE_MAX_POSITIONS}")

        await self.api.exchange_info()
        await self.api.set_leverage(symbol, int(self.cfg.LIVE_LEVERAGE))

        notional_effective = self.cfg.LIVE_NOTIONAL_USD * float(self.cfg.LIVE_LEVERAGE)
        qty = notional_effective / last_price

        f = self.api.get_symbol_filters(symbol)
        step = float(f.get("stepSize", 0.0) or 0.0)
        min_qty = float(f.get("minQty", 0.0) or 0.0)

        qty = _round_step(qty, step) if step > 0 else qty
        if qty < min_qty:
            raise RuntimeError(f"Calculated qty {qty} < minQty {min_qty} for {symbol}. Increase LIVE_NOTIONAL_USD or leverage.")

        order_side = "BUY" if side == "LONG" else "SELL"
        print(f"[LIVE] ENTRY {symbol} {side} market side={order_side} qty={qty:.8f} last={last_price:.6g}")

        res = await self.api.place_order(symbol, order_side, qty, reduce_only=False)
        order_id = res.get("orderId", "")

        # prefer executed qty/avg price; if bad -> reconcile via positionRisk
        executed = _csv_safe_float(res.get("executedQty"))
        avg = _csv_safe_float(res.get("avgPrice") or res.get("price") or last_price)

        self.open_positions[symbol] = {
            "symbol": symbol,
            "side": side,
            "qty": executed if executed > 0 else qty,
            "entry": avg if avg > 0 else last_price,
            "opened_ts": time.time(),
            "order_id_entry": order_id,
        }

        # confirm with exchange
        pos = await self.reconcile_position(symbol)
        if pos:
            print(f"[LIVE] OPEN CONFIRMED {symbol}: qty={pos['qty']:.8f} side={pos['side']} entry={pos.get('entry')}")
        else:
            raise RuntimeError("Order sent but position not found on exchange (positionAmt=0).")


        # --- Place exchange-side SL/TP (Close-All) right after entry ---
        entry_px = float((pos or {}).get("entry") or self.open_positions[symbol].get("entry") or last_price)
        if entry_px <= 0:
            entry_px = last_price

        tp_px = entry_px * (1 + self.cfg.TP_PCT / 100.0) if side == "LONG" else entry_px * (1 - self.cfg.TP_PCT / 100.0)
        sl_px = entry_px * (1 - self.cfg.SL_PCT / 100.0) if side == "LONG" else entry_px * (1 + self.cfg.SL_PCT / 100.0)

        # round stop prices to tickSize to avoid rejection
        f2 = self.api.get_symbol_filters(symbol)
        tick = float(f2.get("tickSize", 0.0) or 0.0)
        if tick > 0:
            tp_px = _round_tick(tp_px, tick)
            sl_px = _round_tick(sl_px, tick)

        # clean any stale orders from previous runs
        try:
            await self.api.cancel_all_open_orders(symbol)
        except Exception as e:
            print(f"[LIVE] WARN: cancel_all_open_orders failed: {e}")

        close_side = "SELL" if side == "LONG" else "BUY"

        tp_res = await self.api.place_conditional_close_all(
            symbol=symbol,
            side=close_side,
            order_type="TAKE_PROFIT_MARKET",
            stop_price=tp_px,
            working_type="MARK_PRICE",
            price_protect=True,
        )
        sl_res = await self.api.place_conditional_close_all(
            symbol=symbol,
            side=close_side,
            order_type="STOP_MARKET",
            stop_price=sl_px,
            working_type="MARK_PRICE",
            price_protect=True,
        )

        self.open_positions[symbol]["tp_order_id"] = str(tp_res.get("orderId", ""))
        self.open_positions[symbol]["sl_order_id"] = str(sl_res.get("orderId", ""))
        self.open_positions[symbol]["tp_px"] = tp_px
        self.open_positions[symbol]["sl_px"] = sl_px

        print(f\"[LIVE] SL/TP placed on-exchange {symbol}: TP={tp_px:.4f} SL={sl_px:.4f} tpOrder={tp_order_id} slOrder={sl_order_id}\")

        if getattr(self.cfg, 'VERIFY_SLTP_OPEN_ORDERS', True) and (not self.cfg.DRY_RUN_LIVE):

            try:

                oo = await self.fapi.open_orders(symbol)

                cond = [o for o in (oo or []) if str(o.get('type','')).endswith('MARKET') and _csv_safe_float(o.get('stopPrice',0)) > 0]

                if len(cond) < 2:

                    print(f\"[LIVE] WARN: SL/TP not visible in openOrders (count={len(cond)}). Closing position for safety.\")

                    close_side = 'SELL' if side.upper() == 'LONG' else 'BUY'

                    await self.fapi.place_order(symbol, close_side, qty, reduce_only=True)

                    raise RuntimeError('SLTP_NOT_CONFIRMED')

            except Exception as e:

                print(f\"[LIVE] WARN: SL/TP verification failed: {e}\")
        return self.open_positions[symbol]

    @staticmethod
    def _pnl_pct(side: str, entry: float, exit_price: float) -> float:
        if side == "LONG":
            return (exit_price - entry) / entry * 100.0
        else:
            return (entry - exit_price) / entry * 100.0

    async def close_live_market(self, symbol: str, exit_price: float, reason: str) -> dict:
        pos = self.open_positions.get(symbol) or await self.reconcile_position(symbol)
        if not pos:
            raise RuntimeError(f"No live position for {symbol}")

        side = pos["side"]
        qty = float(pos["qty"])
        if qty <= 0:
            raise RuntimeError("Live qty=0, cannot close.")

        # apply step rounding
        f = self.api.get_symbol_filters(symbol)
        step = float(f.get("stepSize", 0.0) or 0.0)
        qty = _round_step(qty, step) if step > 0 else qty
        if qty <= 0:
            raise RuntimeError("Rounded qty=0, cannot close.")

        close_side = "SELL" if side == "LONG" else "BUY"
        res = await self.api.place_order(symbol, close_side, qty, reduce_only=True)
        order_id_exit = res.get("orderId", "")

        avg_exit = _csv_safe_float(res.get("avgPrice") or res.get("price") or exit_price)
        if avg_exit <= 0:
            avg_exit = exit_price

        entry = float(pos.get("entry") or 0) or exit_price
        pnl_pct = self._pnl_pct(side, entry, avg_exit)
        net = self.cfg.LIVE_NOTIONAL_USD * float(self.cfg.LIVE_LEVERAGE) * (pnl_pct / 100.0)

        outcome = "TP" if reason == "TP" else ("SL" if reason == "SL" else "TIMEOUT")

        print(f"[WATCH] {symbol}: CLOSE SENT qty={qty:.8f} reason={reason} orderId={order_id_exit}")
        _append_csv(self.cfg.LIVE_LOG_PATH, self.LIVE_CSV_HEADER, {
            "ts": _ts_iso(),
            "symbol": symbol,
            "side": side,
            "entry": f"{entry:.10f}",
            "exit": f"{avg_exit:.10f}",
            "qty": f"{qty:.8f}",
            "leverage": str(int(self.cfg.LIVE_LEVERAGE)),
            "pnl_pct": f"{pnl_pct:.6f}",
            "net_pnl_usd": f"{net:.10f}",
            "outcome": outcome,
            "reason": reason,
            "order_id_entry": pos.get("order_id_entry", ""),
            "order_id_exit": order_id_exit,
        })

        # best-effort confirm close
        try:
            await asyncio.sleep(0.3)
            await self.reconcile_position(symbol)
        except Exception:
            pass

        self.open_positions.pop(symbol, None)
        return {"symbol": symbol, "pnl_pct": pnl_pct, "net_pnl_usd": net, "outcome": outcome, "reason": reason}

    async def watch_until_close(self, symbol: str, stop_event: asyncio.Event):
        """Monitor only the FACT of position closure (no price-based TP/SL monitoring).

        We rely on exchange-side STOP_MARKET + TAKE_PROFIT_MARKET created at entry (closePosition=true).
        The loop exits when positionRisk shows positionAmt == 0.
        """
        t0 = time.time()
        open_meta = self.open_positions.get(symbol, {})
        entry = float(open_meta.get("entry") or 0.0)
        side = open_meta.get("side") or "LONG"
        entry_order_id = str(open_meta.get("order_id_entry", ""))
        tp_oid = str(open_meta.get("tp_order_id", ""))
        sl_oid = str(open_meta.get("sl_order_id", ""))
        opened_ts = float(open_meta.get("opened_ts") or t0)
        opened_ms = int(opened_ts * 1000)

        print(f"[WATCH] {symbol}: waiting for exchange to close the position... (tpOrder={tp_oid} slOrder={sl_oid})")

        while not stop_event.is_set():
            await asyncio.sleep(self.cfg.WATCH_POLL_SEC)

            pos = await self.reconcile_position(symbol)
            if not pos:
                # nothing on exchange -> treat as closed
                break
            if float(pos.get("qty") or 0.0) == 0.0:
                break

            if self.cfg.WATCH_HARD_TIMEOUT_SEC > 0 and (time.time() - t0) >= self.cfg.WATCH_HARD_TIMEOUT_SEC:
                if getattr(self.cfg, "EMERGENCY_CLOSE_ON_HARD_TIMEOUT", False):
                    px = entry if entry > 0 else float(pos.get("mark_price") or entry or 0.0) or entry
                    return await self.close_live_market(symbol, px, "TIMEOUT_HARD_EMERGENCY")
                print(f"[WATCH] {symbol}: HARD TIMEOUT reached, but EMERGENCY_CLOSE_ON_HARD_TIMEOUT is False. Continuing to wait.")
                t0 = time.time()  # avoid spam; restart timer

        # closed (or forced stop): compute exit price and reason from userTrades
        try:
            now_ms = int(time.time() * 1000)
            trades = await self.api.user_trades(symbol, start_time_ms=max(opened_ms - 10_000, 0), end_time_ms=now_ms, limit=200)
        except Exception as e:
            trades = []
            print(f"[WATCH] {symbol}: WARN: userTrades failed: {e}")

        # pick last trade after entry that is NOT the entry order (best effort)
        exit_trade = None
        if isinstance(trades, list) and trades:
            trades_sorted = sorted(trades, key=lambda x: int(x.get("time") or x.get("T") or 0))
            for tr in reversed(trades_sorted):
                oid = str(tr.get("orderId", ""))
                if entry_order_id and oid == entry_order_id:
                    continue
                exit_trade = tr
                break

        exit_px = None
        realized = 0.0
        reason = "CLOSE_UNKNOWN"
        order_id_exit = ""

        if exit_trade:
            order_id_exit = str(exit_trade.get("orderId", ""))
            exit_px = _csv_safe_float(exit_trade.get("price"))
            realized = _csv_safe_float(exit_trade.get("realizedPnl") or exit_trade.get("realizedPnlValue") or exit_trade.get("realizedPnlUsd"))
            if order_id_exit and tp_oid and order_id_exit == tp_oid:
                reason = "TP_EXCHANGE"
            elif order_id_exit and sl_oid and order_id_exit == sl_oid:
                reason = "SL_EXCHANGE"

        if exit_px is None or exit_px <= 0:
            # fallback to last tick / entry
            exit_px = entry if entry > 0 else 0.0

        # ensure no leftover orders
        try:
            await self.api.cancel_all_open_orders(symbol)
        except Exception:
            pass

        # if we couldn't get realized pnl, approximate by price diff (ignoring fees)
        if realized == 0.0 and entry and exit_px:
            pnl_pct = self._pnl_pct(side, entry, exit_px)
            net = pnl_pct / 100.0 * float(open_meta.get("qty") or 0.0) * entry * float(self.cfg.LIVE_LEVERAGE)
        else:
            pnl_pct = self._pnl_pct(side, entry, exit_px) if entry and exit_px else 0.0
            net = realized

        outcome = "WIN" if pnl_pct > 0 else ("LOSS" if pnl_pct < 0 else "FLAT")

        # log + cleanup
        self._append_live_csv(
            ts=time.time(),
            symbol=symbol,
            side=side,
            entry=entry,
            exit_price=exit_px,
            qty=float(open_meta.get("qty") or 0.0),
            leverage=int(self.cfg.LIVE_LEVERAGE),
            pnl_pct=pnl_pct,
            net_pnl_usd=net,
            outcome=outcome,
            reason=reason,
            order_id_entry=entry_order_id,
            order_id_exit=order_id_exit,
        )
        self.open_positions.pop(symbol, None)
        return {"symbol": symbol, "pnl_pct": pnl_pct, "net_pnl_usd": net, "outcome": outcome, "reason": reason}


# =========================
# Orchestrator
# =========================

class Orchestrator:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.api = AsterFapi(cfg)
        self.paper = PaperEngine(cfg)
        self.signals = SignalEngine(cfg)
        self.live = LiveEngine(cfg, self.api)

        self.indicators: Dict[str, IndicatorState] = {}
        self.last_price: Dict[str, float] = {}
        self.spread_cache: Dict[str, float] = {}

        self.active_symbols: List[str] = []

        self.stop_event = asyncio.Event()
        self.ws_reconnect_requested = asyncio.Event()

        self._ws = None  # current websocket (for forced close)
        self.last_ws_msg_ts = time.time()
        self.last_tick_ts = time.time()

        self._tasks: List[asyncio.Task] = []

    async def build_universe(self) -> List[str]:
        wl = set(self.cfg.WHITELIST or [])
        bl = set(self.cfg.BLACKLIST or [])
        sk = set(self.cfg.SKIP_SYMBOLS or [])

        if self.cfg.SYMBOL_MODE.upper() == "WHITELIST_ONLY":
            syms = [s for s in wl if s and s not in bl and s not in sk]
        else:
            try:
                tickers = await self.api.tickers_24h()
            except Exception as e:
                print(f"[UNIVERSE] WARN: cannot fetch 24h tickers: {e}. Fallback to whitelist.")
                syms = [s for s in wl if s and s not in bl and s not in sk]
            else:
                filtered = []
                for t in tickers:
                    sym = (t.get("symbol") or "").upper()
                    if not sym.endswith(self.cfg.QUOTE):
                        continue
                    if sym in bl or sym in sk:
                        continue
                    qv = _csv_safe_float(t.get("quoteVolume"))
                    if qv < self.cfg.MIN_24H_QUOTE_VOL and (sym not in wl):
                        continue
                    filtered.append((sym, qv))
                filtered.sort(key=lambda x: x[1], reverse=True)
                top = [sym for sym, _ in filtered[: self.cfg.AUTO_TOP_N]]
                syms = []
                if wl:
                    for s in wl:
                        if s in top and s not in syms:
                            syms.append(s)
                    for s in top:
                        if s not in syms:
                            syms.append(s)
                else:
                    syms = top
                syms = syms[: self.cfg.TARGET_SYMBOLS]

        uniq = []
        for s in syms:
            s = s.upper().strip()
            if s and s not in uniq:
                uniq.append(s)
        return uniq

    async def universe_loop(self):
        while not self.stop_event.is_set():
            try:
                syms = await self.build_universe()
                if syms and syms != self.active_symbols:
                    self.active_symbols = syms
                    print(f"[PAPER] Active symbols: {len(syms)} -> {','.join(syms)}")
                    max_bars = max(200, int(self.cfg.LOOKBACK_MINUTES * 60 / self.cfg.TF_SEC) + 10)
                    for s in syms:
                        self.indicators.setdefault(s, IndicatorState(self.cfg.TF_SEC, max_bars))
                        self.paper.streak_losses.setdefault(s, 0)
                await asyncio.sleep(self.cfg.REFRESH_UNIVERSE_SEC)
            except Exception as e:
                print(f"[UNIVERSE] ERROR: {e}")
                await asyncio.sleep(5)

    async def spread_loop(self):
        while not self.stop_event.is_set():
            syms = list(self.active_symbols)
            if not syms:
                await asyncio.sleep(2)
                continue
            for s in syms:
                if self.stop_event.is_set():
                    break
                try:
                    bid, ask = await self.api.book_ticker(s)
                    if bid > 0 and ask > 0:
                        mid = (bid + ask) / 2.0
                        sp = (ask - bid) / mid * 100.0
                        self.spread_cache[s] = sp
                except Exception:
                    pass
                await asyncio.sleep(0.05)
            await asyncio.sleep(1.0)

    def _get_spread_pct(self, symbol: str) -> Optional[float]:
        return self.spread_cache.get(symbol)

    def _get_atr_pct(self, symbol: str, price: float) -> Optional[float]:
        ind = self.indicators.get(symbol)
        if not ind:
            return None
        atr = ind.atr(self.cfg.ATR_PERIOD)
        if atr is None or price <= 0:
            return None
        return (atr / price) * 100.0

    def _tick_price_getter(self, symbol: str) -> Optional[float]:
        return self.last_price.get(symbol)

    async def _rest_price_getter(self, symbol: str) -> float:
        # prefer book mid; fallback to ticker price
        bid, ask = await self.api.book_ticker(symbol)
        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0
        return await self.api.ticker_price(symbol)

    async def _handle_trade_tick(self, symbol: str, price: float, ts_ms: int):
        self.last_ws_msg_ts = time.time()
        self.last_tick_ts = time.time()

        self.last_price[symbol] = price
        self.signals.update(symbol, price)
        if symbol in self.indicators:
            self.indicators[symbol].update_trade(ts_ms, price)

        # close paper by price
        self.paper.maybe_close_on_price(symbol, price)

        # signal decision
        atr_pct = self._get_atr_pct(symbol, price)
        spread_pct = self._get_spread_pct(symbol)
        sig = self.signals.signal_side(symbol, atr_pct, spread_pct)
        if not sig:
            return

        if not self.paper.freeze_paper_entries:
            if self.cfg.PAPER_ENABLED and self.paper._can_open(symbol):
                self.paper.open(symbol, sig, price)
            return

        # frozen: wait signal on trigger_symbol to open live
        trig = self.paper.trigger_symbol
        if not trig or symbol != trig:
            return
        if symbol in self.paper.positions:
            return

        # deviation guard
        try:
            bid, ask = await self.api.book_ticker(symbol)
            mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else price
            dev = abs(price - mid) / mid * 100.0 if mid > 0 else 0.0
            if dev > self.cfg.MAX_DEVIATION_PCT:
                print(f"[LIVE] Skip signal: deviation {dev:.3f}% > MAX_DEVIATION_PCT={self.cfg.MAX_DEVIATION_PCT}")
                return
        except Exception:
            pass

        if not self.cfg.LIVE_ENABLED:
            print("[LIVE] Skipped: LIVE_ENABLED=false. Still resetting.")
            self.paper.reset_all_streaks(self.active_symbols)
            return

        # open live
        try:
            pos = await self.live.open_live(symbol, sig, price)
        except Exception as e:
            print(f"[LIVE] ERROR: cannot open live: {e}")
            return

        # watch, then reset
        try:
            await self.live.watch_until_close(symbol=symbol, stop_event=self.stop_event)
        except Exception as e:
            print(f"[LIVE] WATCHER ERROR: {e}")
        finally:
            self.paper.reset_all_streaks(self.active_symbols)

    async def ws_loop(self):
        while not self.stop_event.is_set():
            syms = list(self.active_symbols)
            if not syms:
                await asyncio.sleep(1)
                continue

            streams = [f"{s.lower()}@trade" for s in syms]
            mode = (self.cfg.WS_MODE or "AUTO").upper()
            if mode == "AUTO":
                mode = "SUBSCRIBE" if self.cfg.WS_BASE.rstrip("/").endswith("/ws") else "COMBINED"

            self.ws_reconnect_requested.clear()
            try:
                if mode == "COMBINED":
                    stream_q = "/".join(streams)
                    ws_url = f"{self.cfg.WS_BASE.rstrip('/')}/stream?streams={stream_q}"
                    print(f"[WS] connecting (COMBINED): {ws_url}")
                    async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                        self._ws = ws
                        self._ws_close_sent = False
                        print("[WS] connected.")
                        self.last_ws_msg_ts = time.time()
                        # Receive with timeout so watchdog/reconnect flag can be handled even when the stream is silent/stalled
                        while not self.stop_event.is_set() and not self.ws_reconnect_requested.is_set():
                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                            except asyncio.TimeoutError:
                                continue
                            except websockets.exceptions.ConnectionClosed:
                                raise
                            self.last_ws_msg_ts = time.time()
                            await self._on_ws_message(msg)
                        self._ws = None
                        self._ws_close_sent = False
            except Exception as e:
                print(f"[WS] ERROR: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3)
                continue

            if self.ws_reconnect_requested.is_set() and not self.stop_event.is_set():
                print("[WS] reconnect requested -> reconnecting in 1s...")
                await asyncio.sleep(1)
                continue

    async def _on_ws_message(self, msg: str):
        self.last_ws_msg_ts = time.time()
        try:
            data = json.loads(msg)
        except Exception:
            return

        # subscribe ack
        if isinstance(data, dict) and data.get("result") is None and data.get("id") is not None:
            return

        payload = data.get("data") if isinstance(data, dict) else None
        if payload is None and isinstance(data, dict):
            payload = data
        if not isinstance(payload, dict):
            return

        sym = (payload.get("s") or payload.get("symbol") or "").upper()
        if not sym:
            return
        if self.active_symbols and sym not in self.active_symbols:
            return

        try:
            price = float(payload.get("p") or payload.get("price"))
        except Exception:
            return

        try:
            ts_ms = int(payload.get("T") or payload.get("tradeTime") or payload.get("E") or _now_ms())
        except Exception:
            ts_ms = _now_ms()

        await self._handle_trade_tick(sym, price, ts_ms)

    async def heartbeat_loop(self):
        while not self.stop_event.is_set():
            await asyncio.sleep(random.uniform(self.cfg.HEARTBEAT_MIN_SEC, self.cfg.HEARTBEAT_MAX_SEC))
            now = time.time()
            last_tick_age = now - self.last_tick_ts
            mode = self.paper.mode()
            trig = self.paper.trigger_symbol or "-"
            paper_open = len(self.paper.positions)
            live_open = len(self.live.open_positions)
            print(f"[HEARTBEAT] mode={mode} trigger={trig} last_tick_age={last_tick_age:.1f}s paper_open={paper_open} live_open={live_open}")

    async def ws_watchdog_loop(self):
        """Monitor WS silence and request reconnect when it's truly stale.

        IMPORTANT: do NOT close the socket on every loop iteration.
        Only close it after we've decided to reconnect, otherwise we self-trigger
        'sent 4000 (private use) stale' in a tight loop.
        """
        stale_hits = 0

        while not self.stop_event.is_set():
            await asyncio.sleep(1.0)

            age = time.time() - self.last_ws_msg_ts

            if age <= self.cfg.WS_STALE_SEC:
                stale_hits = 0
                continue

            stale_hits += 1
            if stale_hits < self.cfg.WS_STALE_HITS_TO_RECONNECT:
                continue

            if not self.ws_reconnect_requested.is_set():
                print(
                    f"[WS-WD] stale ws stream: last_ws_msg_age={age:.1f}s > WS_STALE_SEC={self.cfg.WS_STALE_SEC}. Forcing reconnect..."
                )
                self.ws_reconnect_requested.set()

            ws = getattr(self, "_ws", None)
            if ws is not None and not getattr(self, "_ws_close_sent", False):
                self._ws_close_sent = True
                try:
                    asyncio.create_task(ws.close(code=4000, reason="stale"))
                except Exception:
                    pass


    async def paper_timeout_loop(self):
        # closes paper positions by time even if no ticks
        while not self.stop_event.is_set():
            await asyncio.sleep(2.0)
            now = time.time()
            timeouts = self.paper.timed_out_symbols(now)
            if not timeouts:
                continue
            for sym in timeouts:
                if self.stop_event.is_set():
                    break
                # get best-effort price
                px = self.last_price.get(sym)
                if not px or px <= 0:
                    try:
                        px = await self._rest_price_getter(sym)
                    except Exception:
                        continue
                self.paper.close(sym, px, "TIMEOUT")

    async def run(self):
        # banner
        print("[MIRROR] Strategy A (Freeze+GlobalReset): PAPER loss-streak -> FREEZE -> wait signal -> LIVE -> reset all streaks")
        print("[MIRROR] Flags: LIVE_ENABLED=", self.cfg.LIVE_ENABLED, "PAPER_ENABLED=", self.cfg.PAPER_ENABLED)
        print("[MIRROR] Live: notional_usd=", self.cfg.LIVE_NOTIONAL_USD, "lev=", self.cfg.LIVE_LEVERAGE, f"TP%={self.cfg.TP_PCT:.2f} SL%={self.cfg.SL_PCT:.3f}")
        print("[MIRROR] LOSS_STREAK_TO_ARM=", self.cfg.LOSS_STREAK_TO_ARM, "LIVE_MAX_POSITIONS=", self.cfg.LIVE_MAX_POSITIONS)
        print("[MIRROR] WATCH_PROFIT_TIMEOUT_SEC=", self.cfg.WATCH_PROFIT_TIMEOUT_SEC, "WATCH_HARD_TIMEOUT_SEC=", self.cfg.WATCH_HARD_TIMEOUT_SEC)
        print("[MIRROR] PAPER_LOG_PATH=", self.cfg.PAPER_LOG_PATH)
        print("[MIRROR] LIVE_LOG_PATH =", self.cfg.LIVE_LOG_PATH)
        print("[MIRROR] MAX_DEVIATION_PCT=", self.cfg.MAX_DEVIATION_PCT)
        print("[MIRROR] WS_STALE_SEC=", self.cfg.WS_STALE_SEC, "WS_STALE_HITS_TO_RECONNECT=", self.cfg.WS_STALE_HITS_TO_RECONNECT)

        if self.cfg.LIVE_ENABLED and (not self.cfg.ASTER_API_KEY or not self.cfg.ASTER_API_SECRET):
            raise RuntimeError("LIVE_ENABLED=true but ASTER_API_KEY/ASTER_API_SECRET are missing in environment.")

        self.active_symbols = await self.build_universe()
        print(f"[PAPER] Active symbols: {len(self.active_symbols)} -> {','.join(self.active_symbols)}")
        max_bars = max(200, int(self.cfg.LOOKBACK_MINUTES * 60 / self.cfg.TF_SEC) + 10)
        for s in self.active_symbols:
            self.indicators.setdefault(s, IndicatorState(self.cfg.TF_SEC, max_bars))
            self.paper.streak_losses.setdefault(s, 0)

        # tasks
        self._tasks = [
            asyncio.create_task(self.universe_loop(), name="universe"),
            asyncio.create_task(self.spread_loop(), name="spread"),
            asyncio.create_task(self.ws_loop(), name="ws"),
            asyncio.create_task(self.heartbeat_loop(), name="heartbeat"),
            asyncio.create_task(self.ws_watchdog_loop(), name="ws_watchdog"),
            asyncio.create_task(self.paper_timeout_loop(), name="paper_timeout"),
        ]

        await self.stop_event.wait()

        # shutdown
        for t in self._tasks:
            t.cancel()
        try:
            await self.api.close()
        except Exception:
            pass


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, stop_event: asyncio.Event):
    def _handler():
        if not stop_event.is_set():
            print("\n[STOP] Ctrl+C received. Shutting down...")
            stop_event.set()
    try:
        loop.add_signal_handler(signal.SIGINT, _handler)
        loop.add_signal_handler(signal.SIGTERM, _handler)
    except NotImplementedError:
        signal.signal(signal.SIGINT, lambda *_: _handler())
        signal.signal(signal.SIGTERM, lambda *_: _handler())


async def startup_selftest(cfg: Config, api: AsterFapi) -> None:
    """Fail-fast checks: code integrity + universe health."""
    required = [
        "exchange_info",
        "tickers_24h",
        "ticker_price",
        "book_ticker",
        "position_risk",
        "_ensure_time_offset",
        "place_order",
        "cancel_all_open_orders",
        "place_conditional_close_all",
        "set_leverage",
    ]
    missing = [m for m in required if not hasattr(api, m)]
    if missing:
        raise RuntimeError(f"SELFTEST FAIL: AsterFapi missing methods: {missing}")

    # Public endpoints sanity
    await api.exchange_info()
    # Universe endpoint must work if universe is enabled
    try:
        t = await api.tickers_24h()
        if not isinstance(t, (list, dict)):
            raise RuntimeError(f"tickers_24h returned unexpected type: {type(t)}")
    except Exception as e:
        raise RuntimeError(f"SELFTEST FAIL: tickers_24h failed (universe will break): {e}")

    # Time sync sanity (won't throw hard if fails; but we want fail-fast here)
    if not cfg.DRY_RUN_LIVE:
        await api._ensure_time_offset(force=True)

    print("[SELFTEST] OK: methods present; exchange_info + tickers_24h reachable; time sync ok.")

async def deep_selftest(cfg: Config) -> None:
    """Deeper path test: simulate ARM->LIVE path in DRY_RUN mode."""
    cfg2 = Config.load()
    # copy relevant fields
    cfg2.__dict__.update(cfg.__dict__)
    cfg2.DRY_RUN_LIVE = True
    cfg2.LIVE_ENABLED = True
    api = AsterFapi(cfg2)
    # prime exchange info and universe
    await startup_selftest(cfg2, api)

    # Construct live engine (the class that owns open_live). It's defined later as LiveEngine.
    # We find it by scanning globals for an object with method open_live.
    live_engine_cls = None
    for obj in globals().values():
        if isinstance(obj, type) and hasattr(obj, "open_live") and hasattr(obj, "__name__"):
            # crude filter: must accept (self,cfg,api)
            if obj.__name__ in ("LiveEngine", "LiveManager", "LiveTrader", "MirrorEngine"):
                live_engine_cls = obj
                break

    # If we can't confidently find it, skip (still better than crashing overnight)
    if live_engine_cls is None:
        print("[SELFTEST] WARN: deep_selftest skipped (cannot locate live engine class).")
        return

    engine = live_engine_cls(cfg2, api)  # type: ignore
    # Simulate an open_live call
    await engine.open_live("XRPUSDT", "LONG", 1.0)
    # Reconcile should see dry position
    pos = await engine.reconcile_position("XRPUSDT")
    if not pos:
        raise RuntimeError("DEEP_SELFTEST FAIL: dry position not found after open_live")
    print("[SELFTEST] OK: deep ARM->LIVE path executed in DRY_RUN mode.")



async def main():
    cfg = Config.load()
    # fail-fast selftests
    if getattr(cfg, 'STARTUP_SELFTEST', True):
        await startup_selftest(cfg, AsterFapi(cfg))
    if getattr(cfg, 'DEEP_SELFTEST', False):
        await deep_selftest(cfg)
    orch = Orchestrator(cfg)
    _install_signal_handlers(asyncio.get_running_loop(), orch.stop_event)
    try:
        await orch.run()
    finally:
        try:
            await orch.api.close()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())

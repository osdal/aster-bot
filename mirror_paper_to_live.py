#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mirror_paper_to_live_FIXED.py

Strategy A (Freeze + GlobalReset) — РАБОЧАЯ ВЕРСИЯ.

Логика по ТЗ:
1) PAPER торгует по всем символам и считает "серии подряд минусов" по КАЖДОЙ паре.
   Минус = закрытие по SL.
   TIMEOUT: если netPnL >= 0 -> плюс (сброс серии); если netPnL < 0 -> минус (увеличиваем серию).
   TP = плюс (сброс серии).

2) Когда по ЛЮБОЙ паре серия подряд минусов достигает LOSS_STREAK_TO_ARM:
   - PAPER ПЕРЕСТАЁТ ОТКРЫВАТЬ НОВЫЕ СДЕЛКИ ВООБЩЕ (по всем парам).
   - Запоминаем trigger_symbol = та пара, которая первой достигла порога.
   - Дальше мы ЖДЁМ НОВЫЙ СИГНАЛ НА ВХОД по trigger_symbol.
   - Как только сигнал появился и по trigger_symbol нет открытой PAPER позиции -> ОТКРЫВАЕМ LIVE.

3) После закрытия LIVE (любой исход):
   - СБРАСЫВАЕМ СЧЁТЧИКИ СЕРИЙ МИНУСОВ У ВСЕХ ПАР СРАЗУ.
   - Снимаем глобальную паузу и PAPER снова начинает открывать сделки по всем парам.

Фиксы:
- Исправлен баг подписи запросов (urlencode вместо QueryParams.encode).
- LIVE-watch запускается отдельной asyncio-задачей (WS не блокируется, last_price обновляется).

Зависимости:
pip install websockets httpx python-dotenv
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
from dataclasses import dataclass
from typing import Dict, Deque, Optional, Tuple, List
from collections import deque
from pathlib import Path
from urllib.parse import urlencode

import httpx
import websockets
from dotenv import load_dotenv


# =========================
# Config / helpers
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
    import csv
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not file_exists:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in header})

def _round_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    return math.floor(x / step) * step

def _sign_hmac_sha256(secret: str, query_string: str) -> str:
    return hmac.new(secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256).hexdigest()


@dataclass
class Config:
    # endpoints
    REST_BASE: str = "https://fapi.asterdex.com"
    WS_BASE: str = "wss://fstream.asterdex.com"
    WS_MODE: str = "AUTO"  # AUTO | COMBINED | SUBSCRIBE

    # modes / filters
    SYMBOL_MODE: str = "HYBRID_PRIORITY"  # HYBRID_PRIORITY | WHITELIST_ONLY
    WHITELIST: List[str] = None
    BLACKLIST: List[str] = None
    SKIP_SYMBOLS: List[str] = None

    # universe
    QUOTE: str = "USDT"
    AUTO_TOP_N: int = 40
    TARGET_SYMBOLS: int = 20
    REFRESH_UNIVERSE_SEC: int = 900
    MIN_24H_QUOTE_VOL: float = 30_000_000

    # signal params (simple impulse breakout)
    IMPULSE_LOOKBACK_SEC: int = 10
    BREAKOUT_BUFFER_PCT: float = 0.10  # percent
    MAX_SPREAD_PCT: float = 0.03       # percent
    MIN_ATR_PCT: float = 0.03          # percent
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
    MAX_DEVIATION_PCT: float = 0.20

    # watch / timeouts
    WATCH_POLL_SEC: float = 2.0
    WATCH_PROFIT_TIMEOUT_SEC: int = 6000
    WATCH_HARD_TIMEOUT_SEC: int = 12000

    # auth
    ASTER_API_KEY: str = ""
    ASTER_API_SECRET: str = ""

    # misc
    CLEANUP_ON_EXIT: bool = False
    DEBUG: bool = False

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

        cfg.WATCH_POLL_SEC = _env_float("WATCH_POLL_SEC", cfg.WATCH_POLL_SEC)
        cfg.WATCH_PROFIT_TIMEOUT_SEC = _env_int("WATCH_PROFIT_TIMEOUT_SEC", cfg.WATCH_PROFIT_TIMEOUT_SEC)
        cfg.WATCH_HARD_TIMEOUT_SEC = _env_int("WATCH_HARD_TIMEOUT_SEC", cfg.WATCH_HARD_TIMEOUT_SEC)

        cfg.ASTER_API_KEY = _env_str("ASTER_API_KEY", "")
        cfg.ASTER_API_SECRET = _env_str("ASTER_API_SECRET", "")

        cfg.CLEANUP_ON_EXIT = _env_bool("CLEANUP_ON_EXIT", cfg.CLEANUP_ON_EXIT)
        cfg.DEBUG = _env_bool("DEBUG", cfg.DEBUG)

        return cfg


# =========================
# Exchange (Binance-like Futures)
# =========================

class AsterFapi:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.client = httpx.AsyncClient(
            base_url=cfg.REST_BASE,
            timeout=15.0,
            headers={"X-MBX-APIKEY": cfg.ASTER_API_KEY} if cfg.ASTER_API_KEY else {},
        )
        self._exchange_info = None
        self._symbol_filters: Dict[str, Dict[str, float]] = {}

    async def close(self):
        await self.client.aclose()

    async def _public_get(self, path: str, params: dict | None = None):
        r = await self.client.get(path, params=params)
        r.raise_for_status()
        return r.json()

    async def _signed(self, method: str, path: str, params: dict):
        if not self.cfg.ASTER_API_KEY or not self.cfg.ASTER_API_SECRET:
            raise RuntimeError("LIVE enabled but ASTER_API_KEY/ASTER_API_SECRET not set.")

        params = dict(params or {})
        params["timestamp"] = _now_ms()

        # FIX: правильная строка подписи
        query = urlencode(params, doseq=True)
        sig = _sign_hmac_sha256(self.cfg.ASTER_API_SECRET, query)
        params["signature"] = sig

        r = await self.client.request(method, path, params=params)
        r.raise_for_status()
        return r.json()

    async def exchange_info(self):
        if self._exchange_info is None:
            self._exchange_info = await self._public_get("/fapi/v1/exchangeInfo")
            self._parse_exchange_info(self._exchange_info)
        return self._exchange_info

    def _parse_exchange_info(self, info: dict):
        self._symbol_filters.clear()
        for s in info.get("symbols", []):
            sym = s.get("symbol")
            if not sym:
                continue
            filters = {f["filterType"]: f for f in s.get("filters", []) if isinstance(f, dict) and "filterType" in f}
            step = 0.0
            min_qty = 0.0
            min_notional = 0.0
            if "LOT_SIZE" in filters:
                step = _csv_safe_float(filters["LOT_SIZE"].get("stepSize", 0))
                min_qty = _csv_safe_float(filters["LOT_SIZE"].get("minQty", 0))
            if "MIN_NOTIONAL" in filters:
                min_notional = _csv_safe_float(filters["MIN_NOTIONAL"].get("notional", 0))
            elif "NOTIONAL" in filters:
                min_notional = _csv_safe_float(filters["NOTIONAL"].get("minNotional", 0))
            self._symbol_filters[sym] = {"stepSize": step, "minQty": min_qty, "minNotional": min_notional}

    def get_symbol_filters(self, symbol: str) -> Dict[str, float]:
        return self._symbol_filters.get(symbol, {"stepSize": 0.0, "minQty": 0.0, "minNotional": 0.0})

    async def book_ticker(self, symbol: str) -> Tuple[float, float]:
        j = await self._public_get("/fapi/v1/ticker/bookTicker", params={"symbol": symbol})
        bid = _csv_safe_float(j.get("bidPrice"))
        ask = _csv_safe_float(j.get("askPrice"))
        return bid, ask

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
        self.max_bars = max_bars
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
# Trading state
# =========================

@dataclass
class Position:
    symbol: str
    side: str  # LONG/SHORT
    entry: float
    tp: float
    sl: float
    opened_ts: float  # time.time()

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
            return
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

        if self.freeze_streak_updates:
            return

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

    def maybe_close_on_price(self, symbol: str, price: float):
        pos = self.positions.get(symbol)
        if not pos:
            return

        if pos.side == "LONG":
            if price >= pos.tp:
                self.close(symbol, price, "TP"); return
            if price <= pos.sl:
                self.close(symbol, price, "SL"); return
        else:
            if price <= pos.tp:
                self.close(symbol, price, "TP"); return
            if price >= pos.sl:
                self.close(symbol, price, "SL"); return

        if self.cfg.MAX_HOLDING_SEC > 0 and (time.time() - pos.opened_ts) >= self.cfg.MAX_HOLDING_SEC:
            self.close(symbol, price, "TIMEOUT")

    def reset_all_streaks(self, active_symbols: List[str]):
        for s in active_symbols:
            self.streak_losses[s] = 0
        self.freeze_paper_entries = False
        self.freeze_streak_updates = False
        self.trigger_symbol = None
        print("[RESET] Global reset: streaks=0 for all, PAPER resumes entries")


class SignalEngine:
    """
    Сигнал:
    - берём изменение цены за IMPULSE_LOOKBACK_SEC
    - если abs(return_pct) >= BREAKOUT_BUFFER_PCT и ATR% >= MIN_ATR_PCT и spread <= MAX_SPREAD_PCT:
        LONG если return>0, SHORT если return<0
    """
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


class LiveEngine:
    LIVE_CSV_HEADER = [
        "ts", "symbol", "side", "entry", "exit", "qty", "leverage",
        "pnl_pct", "net_pnl_usd", "outcome", "reason", "order_id_entry", "order_id_exit"
    ]

    def __init__(self, cfg: Config, api: AsterFapi):
        self.cfg = cfg
        self.api = api
        self.open_positions: Dict[str, dict] = {}

    async def open_live(self, symbol: str, side: str, last_price: float) -> dict:
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
        avg = float(res.get("avgPrice") or res.get("price") or last_price)

        self.open_positions[symbol] = {
            "symbol": symbol,
            "side": side,
            "qty": float(res.get("executedQty") or qty),
            "entry": avg,
            "opened_ts": time.time(),
            "order_id_entry": order_id,
        }
        print(f"[LIVE] FILLED {symbol} avg={avg:.10f} executedQty={self.open_positions[symbol]['qty']:.8f} orderId={order_id}")
        return self.open_positions[symbol]

    @staticmethod
    def _pnl_pct(side: str, entry: float, exit_price: float) -> float:
        if side == "LONG":
            return (exit_price - entry) / entry * 100.0
        else:
            return (entry - exit_price) / entry * 100.0

    async def close_live_market(self, symbol: str, exit_price: float, reason: str) -> dict:
        pos = self.open_positions.get(symbol)
        if not pos:
            raise RuntimeError(f"No live position for {symbol}")

        side = pos["side"]
        qty = float(pos["qty"])
        close_side = "SELL" if side == "LONG" else "BUY"

        res = await self.api.place_order(symbol, close_side, qty, reduce_only=True)
        order_id_exit = res.get("orderId", "")
        avg_exit = float(res.get("avgPrice") or res.get("price") or exit_price)

        pnl_pct = self._pnl_pct(side, float(pos["entry"]), avg_exit)
        net = self.cfg.LIVE_NOTIONAL_USD * float(self.cfg.LIVE_LEVERAGE) * (pnl_pct / 100.0)

        outcome = "TP" if reason == "TP" else ("SL" if reason == "SL" else "TIMEOUT")

        print(f"[WATCH] {symbol}: LOGGED -> {self.cfg.LIVE_LOG_PATH} outcome={outcome} netPnL={net:+.10f} reason={reason}")

        _append_csv(self.cfg.LIVE_LOG_PATH, self.LIVE_CSV_HEADER, {
            "ts": _ts_iso(),
            "symbol": symbol,
            "side": side,
            "entry": f"{float(pos['entry']):.10f}",
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

        self.open_positions.pop(symbol, None)
        return {"symbol": symbol, "pnl_pct": pnl_pct, "net_pnl_usd": net, "outcome": outcome, "reason": reason}

    async def watch_until_close(
        self,
        symbol: str,
        side: str,
        entry: float,
        tp_pct: float,
        sl_pct: float,
        tick_price_getter,
        stop_event: asyncio.Event,
    ):
        tp = entry * (1 + tp_pct / 100.0) if side == "LONG" else entry * (1 - tp_pct / 100.0)
        sl = entry * (1 - sl_pct / 100.0) if side == "LONG" else entry * (1 + sl_pct / 100.0)

        t0 = time.time()
        profit_timeout_fired = False

        print(f"[WATCH] {symbol}: waiting position close... (profit-timeout={self.cfg.WATCH_PROFIT_TIMEOUT_SEC}s, hard-timeout={self.cfg.WATCH_HARD_TIMEOUT_SEC}s)")
        while not stop_event.is_set():
            await asyncio.sleep(self.cfg.WATCH_POLL_SEC)
            px = tick_price_getter(symbol)
            if not px or px <= 0:
                continue

            if side == "LONG":
                if px >= tp:
                    return await self.close_live_market(symbol, px, "TP")
                if px <= sl:
                    return await self.close_live_market(symbol, px, "SL")
            else:
                if px <= tp:
                    return await self.close_live_market(symbol, px, "TP")
                if px >= sl:
                    return await self.close_live_market(symbol, px, "SL")

            if (not profit_timeout_fired) and self.cfg.WATCH_PROFIT_TIMEOUT_SEC > 0 and (time.time() - t0) >= self.cfg.WATCH_PROFIT_TIMEOUT_SEC:
                profit_timeout_fired = True
                pnl_pct = self._pnl_pct(side, entry, px)
                if pnl_pct > 0:
                    return await self.close_live_market(symbol, px, "TIMEOUT_PROFIT")

            if self.cfg.WATCH_HARD_TIMEOUT_SEC > 0 and (time.time() - t0) >= self.cfg.WATCH_HARD_TIMEOUT_SEC:
                return await self.close_live_market(symbol, px, "TIMEOUT_HARD")

        px = tick_price_getter(symbol) or entry
        return await self.close_live_market(symbol, px, "FORCE_EXIT")


# =========================
# Orchestrator
# =========================

class Orchestrator:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.api = AsterFapi(cfg)
        self.paper = PaperEngine(cfg)
        self.signals = SignalEngine(cfg)
        self.indicators: Dict[str, IndicatorState] = {}
        self.last_price: Dict[str, float] = {}
        self.spread_cache: Dict[str, float] = {}
        self.live = LiveEngine(cfg, self.api)
        self.stop_event = asyncio.Event()

        self.active_symbols: List[str] = []
        self._ws_task: Optional[asyncio.Task] = None
        self._universe_task: Optional[asyncio.Task] = None
        self._spread_task: Optional[asyncio.Task] = None

        # FIX: watcher task (чтобы WS не блокировался)
        self.live_active: bool = False
        self.live_watch_task: Optional[asyncio.Task] = None

    async def build_universe(self) -> List[str]:
        wl = set(self.cfg.WHITELIST or [])
        bl = set(self.cfg.BLACKLIST or [])
        sk = set(self.cfg.SKIP_SYMBOLS or [])

        syms: List[str] = []
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
            syms = await self.build_universe()
            if syms and syms != self.active_symbols:
                self.active_symbols = syms
                print(f"[PAPER] Active symbols: {len(syms)} -> {','.join(syms)}")
                max_bars = max(200, int(self.cfg.LOOKBACK_MINUTES * 60 / self.cfg.TF_SEC) + 10)
                for s in syms:
                    self.indicators.setdefault(s, IndicatorState(self.cfg.TF_SEC, max_bars))
                    self.paper.streak_losses.setdefault(s, 0)
            await asyncio.sleep(self.cfg.REFRESH_UNIVERSE_SEC)

    async def spread_loop(self):
        while not self.stop_event.is_set():
            syms = list(self.active_symbols)
            if not syms:
                await asyncio.sleep(3)
                continue
            for s in syms:
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

    async def _start_live_watcher(self, symbol: str, side: str, entry_price: float, sig: str):
        """
        Отдельная задача: открываем LIVE и сопровождаем, пока WS продолжает обновлять last_price.
        """
        try:
            pos = await self.live.open_live(symbol, sig, entry_price)
            await self.live.watch_until_close(
                symbol=symbol,
                side=pos["side"],
                entry=float(pos["entry"]),
                tp_pct=self.cfg.TP_PCT,
                sl_pct=self.cfg.SL_PCT,
                tick_price_getter=self._tick_price_getter,
                stop_event=self.stop_event
            )
        except Exception as e:
            print(f"[LIVE] WATCHER ERROR: {e}")
        finally:
            self.paper.reset_all_streaks(self.active_symbols)
            self.live_active = False
            self.live_watch_task = None

    async def _handle_trade_tick(self, symbol: str, price: float, ts_ms: int):
        self.last_price[symbol] = price
        self.signals.update(symbol, price)
        if symbol in self.indicators:
            self.indicators[symbol].update_trade(ts_ms, price)

        # manage paper closes
        self.paper.maybe_close_on_price(symbol, price)

        # generate signals
        atr_pct = self._get_atr_pct(symbol, price)
        spread_pct = self._get_spread_pct(symbol)
        sig = self.signals.signal_side(symbol, atr_pct, spread_pct)
        if not sig:
            return

        if not self.paper.freeze_paper_entries:
            if self.cfg.PAPER_ENABLED and self.paper._can_open(symbol):
                self.paper.open(symbol, sig, price)
            return

        # frozen state: ONLY wait for a NEW signal on trigger_symbol to open LIVE
        trig = self.paper.trigger_symbol
        if not trig or symbol != trig:
            return
        if symbol in self.paper.positions:
            return
        if self.live_active:
            return

        if not self.cfg.LIVE_ENABLED:
            print("[LIVE] Skipped: LIVE_ENABLED=false. Still resetting.")
            self.paper.reset_all_streaks(self.active_symbols)
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

        # START LIVE watcher (НЕ блокируем WS)
        self.live_active = True
        self.live_watch_task = asyncio.create_task(self._start_live_watcher(symbol, sig, price, sig))

    async def ws_loop(self):
        """
        WS modes:
        - COMBINED: /stream?streams=...
        - SUBSCRIBE: /ws + {"method":"SUBSCRIBE","params":[...],"id":1}
        AUTO:
          если WS_BASE оканчивается на /ws -> SUBSCRIBE, иначе COMBINED
        """
        while not self.stop_event.is_set():
            syms = list(self.active_symbols)
            if not syms:
                await asyncio.sleep(1)
                continue

            streams = [f"{s.lower()}@trade" for s in syms]
            mode = (self.cfg.WS_MODE or "AUTO").upper()
            if mode == "AUTO":
                mode = "SUBSCRIBE" if self.cfg.WS_BASE.rstrip("/").endswith("/ws") else "COMBINED"

            try:
                if mode == "COMBINED":
                    stream_q = "/".join(streams)
                    ws_url = f"{self.cfg.WS_BASE.rstrip('/')}/stream?streams={stream_q}"
                    print(f"[WS] connecting (COMBINED): {ws_url}")
                    async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                        print("[WS] connected.")
                        async for msg in ws:
                            if self.stop_event.is_set():
                                break
                            await self._on_ws_message(msg)
                else:
                    base = self.cfg.WS_BASE.rstrip("/")
                    ws_url = base if base.endswith("/ws") else f"{base}/ws"
                    print(f"[WS] connecting (SUBSCRIBE): {ws_url}")
                    async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                        sub = {"method": "SUBSCRIBE", "params": streams, "id": 1}
                        await ws.send(json.dumps(sub))
                        print(f"[WS] connected. SUBSCRIBE {len(streams)} streams")
                        async for msg in ws:
                            if self.stop_event.is_set():
                                break
                            await self._on_ws_message(msg)
            except Exception as e:
                print(f"[WS] ERROR: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3)

    async def _on_ws_message(self, msg: str):
        try:
            data = json.loads(msg)
        except Exception:
            return

        # subscribe ack
        if isinstance(data, dict) and data.get("result") is None and data.get("id") is not None:
            return

        payload = None
        if isinstance(data, dict):
            payload = data.get("data") or data
        if not isinstance(payload, dict):
            return

        sym = (payload.get("s") or payload.get("symbol") or "").upper()
        if not sym:
            return
        if sym not in self.active_symbols:
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

    async def run(self):
        print("[MIRROR] Strategy A (Freeze+GlobalReset): PAPER loss-streak -> FREEZE -> wait signal -> LIVE -> reset all streaks")
        print("[MIRROR] Flags: LIVE_ENABLED=", self.cfg.LIVE_ENABLED, "PAPER_ENABLED=", self.cfg.PAPER_ENABLED)
        print("[MIRROR] Live: notional_usd=", self.cfg.LIVE_NOTIONAL_USD, "lev=", self.cfg.LIVE_LEVERAGE,
              f"TP%={self.cfg.TP_PCT:.2f} SL%={self.cfg.SL_PCT:.3f}")
        print("[MIRROR] LOSS_STREAK_TO_ARM=", self.cfg.LOSS_STREAK_TO_ARM, "LIVE_MAX_POSITIONS=", self.cfg.LIVE_MAX_POSITIONS)
        print("[MIRROR] WATCH_PROFIT_TIMEOUT_SEC=", self.cfg.WATCH_PROFIT_TIMEOUT_SEC, "WATCH_HARD_TIMEOUT_SEC=", self.cfg.WATCH_HARD_TIMEOUT_SEC)
        print("[MIRROR] PAPER_LOG_PATH=", self.cfg.PAPER_LOG_PATH)
        print("[MIRROR] LIVE_LOG_PATH =", self.cfg.LIVE_LOG_PATH)

        if self.cfg.LIVE_ENABLED and (not self.cfg.ASTER_API_KEY or not self.cfg.ASTER_API_SECRET):
            raise RuntimeError("LIVE_ENABLED=true but ASTER_API_KEY/ASTER_API_SECRET are missing in environment.")

        self.active_symbols = await self.build_universe()
        print(f"[PAPER] Active symbols: {len(self.active_symbols)} -> {','.join(self.active_symbols)}")
        max_bars = max(200, int(self.cfg.LOOKBACK_MINUTES * 60 / self.cfg.TF_SEC) + 10)
        for s in self.active_symbols:
            self.indicators.setdefault(s, IndicatorState(self.cfg.TF_SEC, max_bars))
            self.paper.streak_losses.setdefault(s, 0)

        self._universe_task = asyncio.create_task(self.universe_loop())
        self._spread_task = asyncio.create_task(self.spread_loop())
        self._ws_task = asyncio.create_task(self.ws_loop())

        await self.stop_event.wait()

        # shutdown
        for t in [self._ws_task, self._spread_task, self._universe_task, self.live_watch_task]:
            if t:
                t.cancel()
        await self.api.close()


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


async def main():
    cfg = Config.load()
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

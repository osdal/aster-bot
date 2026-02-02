#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mirror_paper_to_live.py

Strategy A (Freeze+GlobalReset) — fully self-contained.

LOGIC (as requested):
1) PAPER trades ALL active symbols and tracks consecutive LOSS streaks per symbol.
   LOSS = reason == "SL"
   TIMEOUT: if netPnL >= 0 => WIN (reset streak), else LOSS (increment streak)
   TP always WIN (reset streak)

2) When ANY symbol reaches LOSS_STREAK_TO_ARM:
   - FREEZE PAPER ENTRIES globally (no new PAPER opens on any symbol)
   - Remember trigger_symbol = that symbol
   - Keep managing (closing) existing PAPER positions, but STOP updating streaks while frozen.
   - Wait for a NEW SIGNAL on trigger_symbol (do not open PAPER on trigger symbol while frozen)
   - When a new signal appears AND trigger_symbol has no open PAPER position:
       open LIVE immediately on that signal and start watching it.

3) After LIVE is CONFIRMED CLOSED (any outcome: TP/SL/TIMEOUT/force):
   - Global reset: streak losses = 0 for ALL symbols
   - Unfreeze PAPER and continue forever

CRITICAL FIXES vs earlier buggy version:
- Robust REST signing: uses urllib.parse.urlencode (no httpx.QueryParams.encode issues).
- Never attempts to close LIVE with qty=0: always reconciles actual position size from positionRisk before closing.
- Does NOT unfreeze/reset if LIVE close failed; instead stays frozen and keeps retrying to close (prevents LIVE_MAX_POSITIONS spam).
- Reconciles "stuck" LIVE position and local state.

Requirements:
- Python 3.10+
- pip install websockets httpx python-dotenv
"""

from __future__ import annotations

import os
import math
import time
import json
import hmac
import hashlib
import asyncio
import random
import signal
from dataclasses import dataclass
from typing import Dict, Deque, Optional, Tuple, List
from collections import deque
from pathlib import Path
from urllib.parse import urlencode

import httpx
import websockets
from dotenv import load_dotenv


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
    REST_BASE: str = "https://fapi.asterdex.com"
    WS_BASE: str = "wss://fstream.asterdex.com"
    WS_MODE: str = "AUTO"  # AUTO | COMBINED | SUBSCRIBE

    SYMBOL_MODE: str = "HYBRID_PRIORITY"  # HYBRID_PRIORITY | WHITELIST_ONLY
    WHITELIST: List[str] = None
    BLACKLIST: List[str] = None
    SKIP_SYMBOLS: List[str] = None

    QUOTE: str = "USDT"
    AUTO_TOP_N: int = 40
    TARGET_SYMBOLS: int = 20
    REFRESH_UNIVERSE_SEC: int = 900
    MIN_24H_QUOTE_VOL: float = 30_000_000

    IMPULSE_LOOKBACK_SEC: int = 10
    BREAKOUT_BUFFER_PCT: float = 0.10
    MAX_SPREAD_PCT: float = 0.03
    MIN_ATR_PCT: float = 0.03
    TF_SEC: int = 60
    LOOKBACK_MINUTES: int = 20
    ATR_PERIOD: int = 14

    PAPER_ENABLED: bool = True
    PAPER_LOG_PATH: str = "data/paper_trades.csv"
    TRADE_NOTIONAL_USD: float = 50.0
    MAX_HOLDING_SEC: int = 600
    MAX_TRADES_PER_HOUR: int = 100000
    COOLDOWN_AFTER_TRADE_SEC: int = 0
    TP_PCT: float = 1.0
    SL_PCT: float = 0.8
    LOSS_STREAK_TO_ARM: int = 2

    LIVE_ENABLED: bool = True
    LIVE_LOG_PATH: str = "data/live_trades.csv"
    LIVE_NOTIONAL_USD: float = 5.0
    LIVE_LEVERAGE: int = 2
    LIVE_MAX_POSITIONS: int = 1
    MAX_DEVIATION_PCT: float = 0.5

    WATCH_POLL_SEC: float = 2.0
    WATCH_PROFIT_TIMEOUT_SEC: int = 6000
    WATCH_HARD_TIMEOUT_SEC: int = 12000

    LIVE_CLOSE_RETRIES: int = 6
    LIVE_CLOSE_RETRY_SLEEP_SEC: float = 2.0
    LIVE_RECONCILE_EVERY_SEC: float = 5.0

    ASTER_API_KEY: str = ""
    ASTER_API_SECRET: str = ""

    DEBUG: bool = False


    # health/monitoring
    HEARTBEAT_MIN_SEC: int = 30
    HEARTBEAT_MAX_SEC: int = 60
    WS_STALE_SEC: int = 45
    WS_WATCHDOG_CHECK_SEC: int = 5
    PAPER_TIMEOUT_CHECK_SEC: int = 5
    PAPER_TIMEOUT_USE_REST: bool = True

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

        cfg.LIVE_CLOSE_RETRIES = _env_int("LIVE_CLOSE_RETRIES", cfg.LIVE_CLOSE_RETRIES)
        cfg.LIVE_CLOSE_RETRY_SLEEP_SEC = _env_float("LIVE_CLOSE_RETRY_SLEEP_SEC", cfg.LIVE_CLOSE_RETRY_SLEEP_SEC)
        cfg.LIVE_RECONCILE_EVERY_SEC = _env_float("LIVE_RECONCILE_EVERY_SEC", cfg.LIVE_RECONCILE_EVERY_SEC)

        cfg.ASTER_API_KEY = _env_str("ASTER_API_KEY", "")
        cfg.ASTER_API_SECRET = _env_str("ASTER_API_SECRET", "")

        cfg.DEBUG = _env_bool("DEBUG", cfg.DEBUG)
        return cfg


class AsterFapi:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        headers = {"X-MBX-APIKEY": cfg.ASTER_API_KEY} if cfg.ASTER_API_KEY else {}
        self.client = httpx.AsyncClient(base_url=cfg.REST_BASE, timeout=20.0, headers=headers)
        self._exchange_info = None
        self._symbol_filters: Dict[str, Dict[str, float]] = {}

    async def close(self):
        await self.client.aclose()

    async def _public_get(self, path: str, params: dict | None = None):
        r = await self.client.get(path, params=params)
        r.raise_for_status()
        return r.json()

    def _make_query(self, params: dict) -> str:
        return urlencode(params, doseq=True)

    async def _signed(self, method: str, path: str, params: dict):
        if not self.cfg.ASTER_API_KEY or not self.cfg.ASTER_API_SECRET:
            raise RuntimeError("LIVE enabled but ASTER_API_KEY/ASTER_API_SECRET not set.")
        params = dict(params or {})
        params["timestamp"] = _now_ms()
        query = self._make_query(params)
        sig = _sign_hmac_sha256(self.cfg.ASTER_API_SECRET, query)
        params["signature"] = sig
        r = await self.client.request(method, path, params=params)
        r.raise_for_status()
        return r.json()

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
            filters = {f.get("filterType"): f for f in s.get("filters", []) if isinstance(f, dict)}
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
        return _csv_safe_float(j.get("bidPrice")), _csv_safe_float(j.get("askPrice"))

    async def tickers_24h(self):
        return await self._public_get("/fapi/v1/ticker/24hr")

    async def set_leverage(self, symbol: str, leverage: int):
        return await self._signed("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage})

    async def place_order_market(self, symbol: str, side: str, qty: float, reduce_only: bool = False):
        params = {"symbol": symbol, "side": side, "type": "MARKET", "quantity": f"{qty:.10f}".rstrip("0").rstrip(".")}
        if reduce_only:
            params["reduceOnly"] = "true"
        return await self._signed("POST", "/fapi/v1/order", params)

    async def position_risk(self, symbol: Optional[str] = None):
        params = {"symbol": symbol} if symbol else {}
        try:
            return await self._signed("GET", "/fapi/v2/positionRisk", params)
        except Exception:
            return await self._signed("GET", "/fapi/v1/positionRisk", params)


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
        self._cur_bucket = None

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
        bars = list(self.bars)[- (period + 1):]
        trs = []
        for i in range(1, len(bars)):
            prev_close = bars[i-1].c
            high = bars[i].h
            low = bars[i].l
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            trs.append(tr)
        if len(trs) < period:
            return None
        return sum(trs[-period:]) / period


@dataclass
class Position:
    symbol: str
    side: str
    entry: float
    tp: float
    sl: float
    opened_ts: float

class PaperEngine:
    PAPER_CSV_HEADER = ["ts","symbol","side","event","entry","exit","tp","sl","pnl_pct","net_pnl_usd","reason"]

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.positions: Dict[str, Position] = {}
        self.last_trade_ts: Dict[str, float] = {}
        self.trade_counts_hour: Deque[float] = deque(maxlen=200000)
        self.streak_losses: Dict[str, int] = {}
        self.freeze_paper_entries = False
        self.freeze_streak_updates = False
        self.trigger_symbol: Optional[str] = None

    def _can_open(self, symbol: str) -> bool:
        if self.freeze_paper_entries:
            return False
        if symbol in self.positions:
            return False
        now = time.time()
        if now - self.last_trade_ts.get(symbol, 0.0) < self.cfg.COOLDOWN_AFTER_TRADE_SEC:
            return False
        cutoff = now - 3600
        while self.trade_counts_hour and self.trade_counts_hour[0] < cutoff:
            self.trade_counts_hour.popleft()
        if len(self.trade_counts_hour) >= self.cfg.MAX_TRADES_PER_HOUR:
            return False
        return True

    def open(self, symbol: str, side: str, price: float):
        tp = price*(1+self.cfg.TP_PCT/100.0) if side=="LONG" else price*(1-self.cfg.TP_PCT/100.0)
        sl = price*(1-self.cfg.SL_PCT/100.0) if side=="LONG" else price*(1+self.cfg.SL_PCT/100.0)
        pos = Position(symbol, side, price, tp, sl, time.time())
        self.positions[symbol]=pos
        self.last_trade_ts[symbol]=pos.opened_ts
        self.trade_counts_hour.append(pos.opened_ts)
        print(f"[PAPER] OPEN {symbol} {side} entry={price:.6g} tp={tp:.6g} sl={sl:.6g}")
        _append_csv(self.cfg.PAPER_LOG_PATH, self.PAPER_CSV_HEADER, {
            "ts": _ts_iso(), "symbol": symbol, "side": side, "event": "OPEN",
            "entry": f"{price:.10f}", "exit": "", "tp": f"{tp:.10f}", "sl": f"{sl:.10f}",
            "pnl_pct": "", "net_pnl_usd": "", "reason": ""
        })

    def _pnl_pct(self, pos: Position, exit_price: float) -> float:
        return (exit_price-pos.entry)/pos.entry*100.0 if pos.side=="LONG" else (pos.entry-exit_price)/pos.entry*100.0

    def close(self, symbol: str, exit_price: float, reason: str):
        pos = self.positions.pop(symbol, None)
        if not pos:
            return
        pnl_pct = self._pnl_pct(pos, exit_price)
        net = self.cfg.TRADE_NOTIONAL_USD*(pnl_pct/100.0)
        print(f"[PAPER] CLOSE {symbol} {pos.side} exit={exit_price:.6g} pnl=({pnl_pct:+.3f}%) reason={reason}")
        _append_csv(self.cfg.PAPER_LOG_PATH, self.PAPER_CSV_HEADER, {
            "ts": _ts_iso(), "symbol": symbol, "side": pos.side, "event": "CLOSE",
            "entry": f"{pos.entry:.10f}", "exit": f"{exit_price:.10f}",
            "tp": f"{pos.tp:.10f}", "sl": f"{pos.sl:.10f}",
            "pnl_pct": f"{pnl_pct:.6f}", "net_pnl_usd": f"{net:.10f}", "reason": reason
        })
        if self.freeze_streak_updates:
            return
        if reason=="SL":
            is_loss=True
        elif reason=="TP":
            is_loss=False
        elif reason=="TIMEOUT":
            is_loss = net < 0
        else:
            is_loss = net < 0

        self.streak_losses[symbol] = (self.streak_losses.get(symbol,0)+1) if is_loss else 0
        streak=self.streak_losses[symbol]
        print(f"[STREAK] {symbol}: paper reason={reason} netPnL={net:+.6f} streak_losses={streak}")
        if (not self.freeze_paper_entries) and streak>=self.cfg.LOSS_STREAK_TO_ARM:
            self.freeze_paper_entries=True
            self.freeze_streak_updates=True
            self.trigger_symbol=symbol
            print(f"[ARM] {symbol}: reached {streak} losses -> FREEZE PAPER and wait LIVE signal")

    def maybe_close_on_price(self, symbol: str, price: float):
        pos=self.positions.get(symbol)
        if not pos:
            return
        if pos.side=="LONG":
            if price>=pos.tp:
                self.close(symbol, price, "TP"); return
            if price<=pos.sl:
                self.close(symbol, price, "SL"); return
        else:
            if price<=pos.tp:
                self.close(symbol, price, "TP"); return
            if price>=pos.sl:
                self.close(symbol, price, "SL"); return
        if self.cfg.MAX_HOLDING_SEC>0 and (time.time()-pos.opened_ts)>=self.cfg.MAX_HOLDING_SEC:
            self.close(symbol, price, "TIMEOUT")

    def reset_all_streaks(self, active_symbols: Optional[List[str]]=None):
        self.streak_losses={k:0 for k in self.streak_losses.keys()}
        if active_symbols:
            for s in active_symbols:
                self.streak_losses.setdefault(s,0)
        self.freeze_paper_entries=False
        self.freeze_streak_updates=False
        self.trigger_symbol=None
        print("[RESET] Global reset: streaks=0 for all, PAPER resumes entries")


class SignalEngine:
    def __init__(self, cfg: Config):
        self.cfg=cfg
        self.ticks: Dict[str, Deque[Tuple[float,float]]] = {}
        self.maxlen=max(500, cfg.IMPULSE_LOOKBACK_SEC*6)

    def update(self, symbol: str, price: float):
        dq=self.ticks.get(symbol)
        if dq is None:
            dq=deque(maxlen=self.maxlen); self.ticks[symbol]=dq
        dq.append((time.time(), price))

    def impulse_return_pct(self, symbol: str) -> Optional[float]:
        dq=self.ticks.get(symbol)
        if not dq or len(dq)<2:
            return None
        now=time.time()
        cutoff=now-self.cfg.IMPULSE_LOOKBACK_SEC
        older=None
        for ts,px in dq:
            if ts>=cutoff:
                older=(ts,px); break
        if older is None:
            older=dq[0]
        old_px=older[1]; last_px=dq[-1][1]
        if old_px<=0:
            return None
        return (last_px-old_px)/old_px*100.0

    def signal_side(self, symbol: str, atr_pct: Optional[float], spread_pct: Optional[float]) -> Optional[str]:
        ret=self.impulse_return_pct(symbol)
        if ret is None:
            return None
        if abs(ret)<self.cfg.BREAKOUT_BUFFER_PCT:
            return None
        if atr_pct is None or atr_pct<self.cfg.MIN_ATR_PCT:
            return None
        if spread_pct is None or spread_pct>self.cfg.MAX_SPREAD_PCT:
            return None
        return "LONG" if ret>0 else "SHORT"


class LiveEngine:
    LIVE_CSV_HEADER = ["ts","symbol","side","entry","exit","qty","leverage","pnl_pct","net_pnl_usd","outcome","reason","order_id_entry","order_id_exit"]

    def __init__(self, cfg: Config, api: AsterFapi):
        self.cfg=cfg; self.api=api
        self.active: Optional[dict]=None

    @staticmethod
    def _pnl_pct(side: str, entry: float, exit_price: float) -> float:
        if entry<=0: return 0.0
        return (exit_price-entry)/entry*100.0 if side=="LONG" else (entry-exit_price)/entry*100.0

    async def _pos_item(self, symbol: str) -> dict:
        pr=await self.api.position_risk(symbol)
        if isinstance(pr, list):
            return pr[0] if pr else {}
        return pr if isinstance(pr, dict) else {}

    async def _get_position_amt(self, symbol: str) -> float:
        item=await self._pos_item(symbol)
        return float(_csv_safe_float(item.get("positionAmt", 0)))

    async def _get_entry_price(self, symbol: str) -> float:
        item=await self._pos_item(symbol)
        return float(_csv_safe_float(item.get("entryPrice", 0)))

    async def reconcile(self) -> bool:
        if not self.active:
            return False
        sym=self.active["symbol"]
        amt=await self._get_position_amt(sym)
        if abs(amt)<=0:
            self.active=None
            return False
        if (self.active.get("entry") or 0)<=0:
            ep=await self._get_entry_price(sym)
            if ep>0: self.active["entry"]=ep
        return True

    async def open_live(self, symbol: str, side: str, last_price: float) -> dict:
        if self.active is not None:
            await self.reconcile()
            if self.active is not None:
                raise RuntimeError(f"LIVE_MAX_POSITIONS reached: {self.cfg.LIVE_MAX_POSITIONS}")

        await self.api.exchange_info()
        await self.api.set_leverage(symbol, int(self.cfg.LIVE_LEVERAGE))

        notional_effective=float(self.cfg.LIVE_NOTIONAL_USD)*float(self.cfg.LIVE_LEVERAGE)
        qty=notional_effective/last_price if last_price>0 else 0.0

        f=self.api.get_symbol_filters(symbol)
        step=float(f.get("stepSize",0.0) or 0.0)
        min_qty=float(f.get("minQty",0.0) or 0.0)

        qty=_round_step(qty, step) if step>0 else qty
        if step>0 and qty<=0:
            qty=step
        if qty<min_qty:
            raise RuntimeError(f"Calculated qty {qty} < minQty {min_qty} for {symbol}. Increase LIVE_NOTIONAL_USD or leverage.")

        order_side="BUY" if side=="LONG" else "SELL"
        print(f"[LIVE] ENTRY {symbol} {side} market side={order_side} qty={qty:.8f} last={last_price:.6g}")

        res=await self.api.place_order_market(symbol, order_side, qty, reduce_only=False)
        order_id=res.get("orderId","")
        avg=_csv_safe_float(res.get("avgPrice") or res.get("price") or last_price)
        if avg<=0: avg=last_price

        self.active={"symbol":symbol,"side":side,"entry":float(avg),"opened_ts":time.time(),"order_id_entry":order_id}

        try:
            amt=await self._get_position_amt(symbol)
            if abs(amt)>0:
                print(f"[LIVE] OPEN CONFIRMED {symbol}: posAmt={amt}")
            else:
                print(f"[LIVE] OPEN UNCERTAIN {symbol}: posAmt=0 (will reconcile)")
        except Exception:
            print(f"[LIVE] OPEN UNCERTAIN {symbol}: reconcile failed (will retry)")
        return dict(self.active)

    async def _close_once_market(self, symbol: str, side: str, reason: str, tick_price_getter):
        amt=await self._get_position_amt(symbol)
        if abs(amt)<=0:
            # already flat
            exit_px = tick_price_getter(symbol) or (self.active.get("entry") if self.active else 0.0) or 0.0
            entry = (self.active.get("entry") if self.active else 0.0) or 0.0
            pnl_pct=self._pnl_pct(side, entry, exit_px) if entry>0 and exit_px>0 else 0.0
            net=float(self.cfg.LIVE_NOTIONAL_USD)*float(self.cfg.LIVE_LEVERAGE)*(pnl_pct/100.0)
            out="TP" if reason=="TP" else ("SL" if reason=="SL" else "TIMEOUT")
            _append_csv(self.cfg.LIVE_LOG_PATH, self.LIVE_CSV_HEADER, {
                "ts": _ts_iso(), "symbol":symbol, "side":side,
                "entry": f"{entry:.10f}", "exit": f"{exit_px:.10f}",
                "qty": f"{0.0:.8f}", "leverage": str(int(self.cfg.LIVE_LEVERAGE)),
                "pnl_pct": f"{pnl_pct:.6f}", "net_pnl_usd": f"{net:.10f}",
                "outcome": out, "reason": f"{reason}|already_flat",
                "order_id_entry": (self.active.get("order_id_entry","") if self.active else ""),
                "order_id_exit": ""
            })
            self.active=None
            return {"symbol":symbol,"pnl_pct":pnl_pct,"net_pnl_usd":net,"outcome":out,"reason":reason}

        close_side="SELL" if side=="LONG" else "BUY"
        qty=abs(float(amt))
        res=await self.api.place_order_market(symbol, close_side, qty, reduce_only=True)
        order_id_exit=res.get("orderId","")
        exit_px = tick_price_getter(symbol) or (self.active.get("entry") if self.active else 0.0) or 0.0
        entry = (self.active.get("entry") if self.active else 0.0) or 0.0
        pnl_pct=self._pnl_pct(side, entry, exit_px) if entry>0 and exit_px>0 else 0.0
        net=float(self.cfg.LIVE_NOTIONAL_USD)*float(self.cfg.LIVE_LEVERAGE)*(pnl_pct/100.0)
        out="TP" if reason=="TP" else ("SL" if reason=="SL" else "TIMEOUT")
        print(f"[WATCH] {symbol}: CLOSE SENT qty={qty:.8f} reason={reason} orderId={order_id_exit}")
        _append_csv(self.cfg.LIVE_LOG_PATH, self.LIVE_CSV_HEADER, {
            "ts": _ts_iso(), "symbol":symbol, "side":side,
            "entry": f"{entry:.10f}", "exit": f"{exit_px:.10f}",
            "qty": f"{qty:.8f}", "leverage": str(int(self.cfg.LIVE_LEVERAGE)),
            "pnl_pct": f"{pnl_pct:.6f}", "net_pnl_usd": f"{net:.10f}",
            "outcome": out, "reason": reason,
            "order_id_entry": (self.active.get("order_id_entry","") if self.active else ""),
            "order_id_exit": order_id_exit
        })
        return {"symbol":symbol,"pnl_pct":pnl_pct,"net_pnl_usd":net,"outcome":out,"reason":reason}

    async def close_live_confirmed(self, reason: str, tick_price_getter):
        if not self.active:
            raise RuntimeError("No active LIVE state to close")
        symbol=self.active["symbol"]; side=self.active["side"]
        last_err=None
        last_res=None
        for i in range(int(self.cfg.LIVE_CLOSE_RETRIES)):
            try:
                last_res=await self._close_once_market(symbol, side, reason, tick_price_getter)
            except Exception as e:
                last_err=e
                print(f"[LIVE] CLOSE ERROR attempt {i+1}/{self.cfg.LIVE_CLOSE_RETRIES}: {e}")
            try:
                await asyncio.sleep(self.cfg.LIVE_CLOSE_RETRY_SLEEP_SEC)
                amt=await self._get_position_amt(symbol)
                if abs(amt)<=0:
                    print(f"[WATCH] {symbol}: CLOSED CONFIRMED (positionAmt=0)")
                    self.active=None
                    return last_res or {"symbol":symbol,"pnl_pct":0.0,"net_pnl_usd":0.0,"outcome":"TIMEOUT","reason":reason}
                else:
                    print(f"[WATCH] {symbol}: still open positionAmt={amt} after close attempt")
            except Exception as e:
                last_err=e
        raise RuntimeError(f"LIVE position not closed after retries. Last error: {last_err}")

    async def watch_until_close_confirmed(self, tick_price_getter, stop_event: asyncio.Event):
        if not self.active:
            raise RuntimeError("No active live position to watch")
        symbol=self.active["symbol"]; side=self.active["side"]
        entry=float(self.active.get("entry",0.0) or 0.0)
        if entry<=0:
            try:
                ep=await self._get_entry_price(symbol)
                if ep>0: entry=ep; self.active["entry"]=ep
            except Exception:
                pass
        tp=entry*(1+self.cfg.TP_PCT/100.0) if side=="LONG" else entry*(1-self.cfg.TP_PCT/100.0)
        sl=entry*(1-self.cfg.SL_PCT/100.0) if side=="LONG" else entry*(1+self.cfg.SL_PCT/100.0)
        t0=time.time()
        profit_timeout_fired=False
        last_reconcile=0.0
        print(f"[WATCH] {symbol}: watching... TP={tp:.10g} SL={sl:.10g} (profit-timeout={self.cfg.WATCH_PROFIT_TIMEOUT_SEC}s, hard-timeout={self.cfg.WATCH_HARD_TIMEOUT_SEC}s)")
        while not stop_event.is_set():
            await asyncio.sleep(self.cfg.WATCH_POLL_SEC)
            if time.time()-last_reconcile>=float(self.cfg.LIVE_RECONCILE_EVERY_SEC):
                last_reconcile=time.time()
                try:
                    amt=await self._get_position_amt(symbol)
                    if abs(amt)<=0:
                        print(f"[WATCH] {symbol}: became flat on exchange (no position).")
                        self.active=None
                        return {"symbol":symbol,"pnl_pct":0.0,"net_pnl_usd":0.0,"outcome":"TIMEOUT","reason":"FLAT_RECONCILE"}
                except Exception:
                    pass

            px=tick_price_getter(symbol)
            if not px or px<=0:
                continue
            if entry>0:
                if side=="LONG":
                    if px>=tp: return await self.close_live_confirmed("TP", tick_price_getter)
                    if px<=sl: return await self.close_live_confirmed("SL", tick_price_getter)
                else:
                    if px<=tp: return await self.close_live_confirmed("TP", tick_price_getter)
                    if px>=sl: return await self.close_live_confirmed("SL", tick_price_getter)

            if (not profit_timeout_fired) and self.cfg.WATCH_PROFIT_TIMEOUT_SEC>0 and (time.time()-t0)>=self.cfg.WATCH_PROFIT_TIMEOUT_SEC:
                profit_timeout_fired=True
                pnl_pct=self._pnl_pct(side, entry, px) if entry>0 else 0.0
                if pnl_pct>0:
                    return await self.close_live_confirmed("TIMEOUT_PROFIT", tick_price_getter)

            if self.cfg.WATCH_HARD_TIMEOUT_SEC>0 and (time.time()-t0)>=self.cfg.WATCH_HARD_TIMEOUT_SEC:
                return await self.close_live_confirmed("TIMEOUT_HARD", tick_price_getter)

        return await self.close_live_confirmed("FORCE_EXIT", tick_price_getter)


class Orchestrator:
    def __init__(self, cfg: Config):
        self.cfg=cfg
        self.api=AsterFapi(cfg)
        self.paper=PaperEngine(cfg)
        self.signals=SignalEngine(cfg)
        self.indicators: Dict[str, IndicatorState] = {}
        self.last_price: Dict[str, float] = {}
        self.spread_cache: Dict[str, float] = {}
        self.live=LiveEngine(cfg, self.api)
        self.stop_event = asyncio.Event()
        self.paper_lock = asyncio.Lock()
        self.last_ws_msg_ts: float = time.time()
        self.last_tick_ts: float = time.time()
        self._ws_reconnect_event = asyncio.Event()

        self.active_symbols: List[str] = []
        self._ws_task=None; self._universe_task=None; self._spread_task=None

    async def build_universe(self) -> List[str]:
        wl=set(self.cfg.WHITELIST or [])
        bl=set(self.cfg.BLACKLIST or [])
        sk=set(self.cfg.SKIP_SYMBOLS or [])
        if self.cfg.SYMBOL_MODE.upper()=="WHITELIST_ONLY":
            syms=[s for s in wl if s and s not in bl and s not in sk]
        else:
            try:
                tickers=await self.api.tickers_24h()
            except Exception as e:
                print(f"[UNIVERSE] WARN: cannot fetch 24h tickers: {e}. Fallback to whitelist.")
                syms=[s for s in wl if s and s not in bl and s not in sk]
            else:
                filtered=[]
                for t in tickers:
                    sym=(t.get("symbol") or "").upper()
                    if not sym.endswith(self.cfg.QUOTE): continue
                    if sym in bl or sym in sk: continue
                    qv=_csv_safe_float(t.get("quoteVolume"))
                    if qv<self.cfg.MIN_24H_QUOTE_VOL and (sym not in wl): continue
                    filtered.append((sym,qv))
                filtered.sort(key=lambda x:x[1], reverse=True)
                top=[sym for sym,_ in filtered[:self.cfg.AUTO_TOP_N]]
                syms=[]
                if wl:
                    for s in wl:
                        if s in top and s not in syms: syms.append(s)
                    for s in top:
                        if s not in syms: syms.append(s)
                else:
                    syms=top
                syms=syms[:self.cfg.TARGET_SYMBOLS]
        uniq=[]
        for s in syms:
            s=s.upper().strip()
            if s and s not in uniq: uniq.append(s)
        return uniq

    async def universe_loop(self):
        while not self.stop_event.is_set():
            syms=await self.build_universe()
            if syms and syms!=self.active_symbols:
                self.active_symbols=syms
                print(f"[PAPER] Active symbols: {len(syms)} -> {','.join(syms)}")
                max_bars=max(200, int(self.cfg.LOOKBACK_MINUTES*60/self.cfg.TF_SEC)+10)
                for s in syms:
                    self.indicators.setdefault(s, IndicatorState(self.cfg.TF_SEC, max_bars))
                    self.paper.streak_losses.setdefault(s,0)
            await asyncio.sleep(self.cfg.REFRESH_UNIVERSE_SEC)

    async def spread_loop(self):
        while not self.stop_event.is_set():
            syms=list(self.active_symbols)
            if not syms:
                await asyncio.sleep(2); continue
            for s in syms:
                try:
                    bid,ask=await self.api.book_ticker(s)
                    if bid>0 and ask>0:
                        mid=(bid+ask)/2.0
                        self.spread_cache[s]=(ask-bid)/mid*100.0
                except Exception:
                    pass
                await asyncio.sleep(0.05)
            await asyncio.sleep(1.0)

    def _get_spread_pct(self, symbol: str) -> Optional[float]:
        return self.spread_cache.get(symbol)

    def _get_atr_pct(self, symbol: str, price: float) -> Optional[float]:
        ind=self.indicators.get(symbol)
        if not ind: return None
        atr=ind.atr(self.cfg.ATR_PERIOD)
        if atr is None or price<=0: return None
        return (atr/price)*100.0

    def _tick_price_getter(self, symbol: str) -> Optional[float]:
        return self.last_price.get(symbol)

    async def _maybe_open_live_on_signal(self, symbol: str, sig: str, price: float):
        try:
            bid,ask=await self.api.book_ticker(symbol)
            mid=(bid+ask)/2.0 if (bid>0 and ask>0) else price
            dev=abs(price-mid)/mid*100.0 if mid>0 else 0.0
            if dev>self.cfg.MAX_DEVIATION_PCT:
                print(f"[LIVE] Skip signal: deviation {dev:.3f}% > MAX_DEVIATION_PCT={self.cfg.MAX_DEVIATION_PCT}")
                return
        except Exception:
            pass

        if not self.cfg.LIVE_ENABLED:
            print("[LIVE] Skipped: LIVE_ENABLED=false. Still resetting.")
            self.paper.reset_all_streaks(active_symbols=self.active_symbols)
            return

        try:
            await self.live.open_live(symbol, sig, price)
        except Exception as e:
            print(f"[LIVE] ERROR: cannot open live: {e}")
            return

        try:
            await self.live.watch_until_close_confirmed(self._tick_price_getter, self.stop_event)
        except Exception as e:
            print(f"[LIVE] WATCH/CLOSE ERROR (staying frozen): {e}")
            return

        self.paper.reset_all_streaks(active_symbols=self.active_symbols)

    async def _handle_trade_tick(self, symbol: str, price: float, ts_ms: int):
        self.last_price[symbol]=price
        self.signals.update(symbol, price)
        if symbol in self.indicators:
            self.indicators[symbol].update_trade(ts_ms, price)

        self.paper.maybe_close_on_price(symbol, price)

        atr_pct=self._get_atr_pct(symbol, price)
        spread_pct=self._get_spread_pct(symbol)
        sig=self.signals.signal_side(symbol, atr_pct, spread_pct)
        if not sig:
            return

        if not self.paper.freeze_paper_entries:
            if self.cfg.PAPER_ENABLED:
                async with self.paper_lock:
                    if self.paper._can_open(symbol):
                        self.paper.open(symbol, sig, price)
            return

        trig=self.paper.trigger_symbol
        if not trig or symbol!=trig:
            return

        # if live exists, do nothing (keep frozen)
        try:
            if await self.live.reconcile():
                return
        except Exception:
            return

        async with self.paper_lock:
            if symbol in self.paper.positions:
                return

        await self._maybe_open_live_on_signal(symbol, sig, price)


    async def heartbeat_loop(self):
        # Heartbeat every 30–60s (configurable) prints last_tick_age and mode.
        while not self.stop_event.is_set():
            a = int(self.cfg.HEARTBEAT_MIN_SEC)
            b = int(self.cfg.HEARTBEAT_MAX_SEC)
            sleep_s = random.randint(a, b) if b > a else a
            await asyncio.sleep(max(1, sleep_s))

            now = time.time()
            last_tick_age = now - (self.last_tick_ts or now)
            last_ws_age = now - (self.last_ws_msg_ts or now)
            mode = "FROZEN" if self.paper.freeze_paper_entries else "NORMAL"
            trig = self.paper.trigger_symbol or "-"
            opens = len(self.paper.positions)
            live_opens = len(self.live.open_positions)
            print(f"[HEARTBEAT] mode={mode} trigger={trig} paper_open={opens} live_open={live_opens} last_tick_age={last_tick_age:.1f}s last_ws_msg_age={last_ws_age:.1f}s")

    async def ws_watchdog_loop(self):
        # If no WS JSON messages for WS_STALE_SEC -> force WS reconnect.
        while not self.stop_event.is_set():
            await asyncio.sleep(max(1, int(self.cfg.WS_WATCHDOG_CHECK_SEC)))
            now = time.time()
            age = now - (self.last_ws_msg_ts or now)
            if age > float(self.cfg.WS_STALE_SEC):
                if not self._ws_reconnect_event.is_set():
                    print(f"[WS-WD] stale ws stream: last_ws_msg_age={age:.1f}s > WS_STALE_SEC={self.cfg.WS_STALE_SEC}. Forcing reconnect...")
                    self._ws_reconnect_event.set()

    async def paper_timeout_loop(self):
        # Closes PAPER positions by time even if no ticks. Uses REST price when possible.
        check_s = max(1, int(self.cfg.PAPER_TIMEOUT_CHECK_SEC))
        while not self.stop_event.is_set():
            await asyncio.sleep(check_s)
            if not self.cfg.PAPER_ENABLED:
                continue
            if self.cfg.MAX_HOLDING_SEC <= 0:
                continue

            # snapshot positions to evaluate without holding lock for REST calls
            async with self.paper_lock:
                positions = list(self.paper.positions.values())

            if not positions:
                continue

            now = time.time()
            for pos in positions:
                if (now - pos.opened_ts) < self.cfg.MAX_HOLDING_SEC:
                    continue

                # choose price: REST or last tick
                exit_px = None
                if self.cfg.PAPER_TIMEOUT_USE_REST:
                    try:
                        px = await self.api.ticker_price(pos.symbol)
                        if px and px > 0:
                            exit_px = px
                    except Exception:
                        exit_px = None
                if exit_px is None:
                    exit_px = self.last_price.get(pos.symbol) or pos.entry

                async with self.paper_lock:
                    # may have been closed by tick while we awaited REST
                    if pos.symbol in self.paper.positions:
                        self.paper.close(pos.symbol, float(exit_px), "TIMEOUT")

    async def ws_loop(self):
        """
        WS reader with watchdog-triggered reconnect.
        - Uses recv() with timeout so we can exit even if market is quiet.
        - If _ws_reconnect_event is set -> break the current connection and reopen.
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
                        self.last_ws_msg_ts = time.time()
                        self._ws_reconnect_event.clear()

                        while not self.stop_event.is_set():
                            if self._ws_reconnect_event.is_set():
                                break
                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                            except asyncio.TimeoutError:
                                continue
                            if msg is None:
                                break
                            await self._on_ws_message(msg)

                else:
                    # SUBSCRIBE
                    base = self.cfg.WS_BASE.rstrip("/")
                    ws_url = base if base.endswith("/ws") else f"{base}/ws"
                    print(f"[WS] connecting (SUBSCRIBE): {ws_url}")
                    async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                        sub = {"method": "SUBSCRIBE", "params": streams, "id": 1}
                        await ws.send(json.dumps(sub))
                        print(f"[WS] connected. SUBSCRIBE {len(streams)} streams")
                        self.last_ws_msg_ts = time.time()
                        self._ws_reconnect_event.clear()

                        while not self.stop_event.is_set():
                            if self._ws_reconnect_event.is_set():
                                break
                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                            except asyncio.TimeoutError:
                                continue
                            if msg is None:
                                break
                            await self._on_ws_message(msg)

            except Exception as e:
                print(f"[WS] ERROR: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3)

            # reconnect requested
            if self._ws_reconnect_event.is_set():
                self._ws_reconnect_event.clear()
                print("[WS] reconnect requested -> reconnecting in 1s...")
                await asyncio.sleep(1)
    async def _on_ws_message(self, msg: str):
        try:
            data=json.loads(msg)
        except Exception:
            return
        if isinstance(data, dict) and data.get("result") is None and data.get("id") is not None:
            return
        payload = data.get("data") if isinstance(data, dict) else None
        if payload is None:
            payload = data
        if not isinstance(payload, dict):
            return
        sym=(payload.get("s") or payload.get("symbol") or "").upper()
        if not sym or sym not in self.active_symbols:
            return
        try:
            price=float(payload.get("p") or payload.get("price"))
        except Exception:
            return
        try:
            ts_ms=int(payload.get("T") or payload.get("tradeTime") or payload.get("E") or _now_ms())
        except Exception:
            ts_ms=_now_ms()
        await self._handle_trade_tick(sym, price, ts_ms)

    async def run(self):
        print("[MIRROR] Strategy A (Freeze+GlobalReset): PAPER loss-streak -> FREEZE -> wait signal -> LIVE -> reset all streaks")
        print("[MIRROR] Flags: LIVE_ENABLED=", self.cfg.LIVE_ENABLED, "PAPER_ENABLED=", self.cfg.PAPER_ENABLED)
        print("[MIRROR] Live: notional_usd=", self.cfg.LIVE_NOTIONAL_USD, "lev=", self.cfg.LIVE_LEVERAGE, f"TP%={self.cfg.TP_PCT:.2f} SL%={self.cfg.SL_PCT:.3f}")
        print("[MIRROR] LOSS_STREAK_TO_ARM=", self.cfg.LOSS_STREAK_TO_ARM, "LIVE_MAX_POSITIONS=", self.cfg.LIVE_MAX_POSITIONS)
        print("[MIRROR] WATCH_PROFIT_TIMEOUT_SEC=", self.cfg.WATCH_PROFIT_TIMEOUT_SEC, "WATCH_HARD_TIMEOUT_SEC=", self.cfg.WATCH_HARD_TIMEOUT_SEC)
        print("[MIRROR] PAPER_LOG_PATH=", self.cfg.PAPER_LOG_PATH)
        print("[MIRROR] LIVE_LOG_PATH =", self.cfg.LIVE_LOG_PATH)
        print("[MIRROR] MAX_DEVIATION_PCT=", self.cfg.MAX_DEVIATION_PCT)
        if self.cfg.LIVE_ENABLED and (not self.cfg.ASTER_API_KEY or not self.cfg.ASTER_API_SECRET):
            raise RuntimeError("LIVE_ENABLED=true but ASTER_API_KEY/ASTER_API_SECRET are missing in environment.")

        self.active_symbols=await self.build_universe()
        print(f"[PAPER] Active symbols: {len(self.active_symbols)} -> {','.join(self.active_symbols)}")
        max_bars=max(200, int(self.cfg.LOOKBACK_MINUTES*60/self.cfg.TF_SEC)+10)
        for s in self.active_symbols:
            self.indicators.setdefault(s, IndicatorState(self.cfg.TF_SEC, max_bars))
            self.paper.streak_losses.setdefault(s,0)

        self._universe_task=asyncio.create_task(self.universe_loop())
        self._spread_task=asyncio.create_task(self.spread_loop())
        self._ws_task = asyncio.create_task(self.ws_loop())
        self._heartbeat_task = asyncio.create_task(self.heartbeat_loop())
        self._ws_watchdog_task = asyncio.create_task(self.ws_watchdog_loop())
        self._paper_timeout_task = asyncio.create_task(self.paper_timeout_loop())
        await self.stop_event.wait()
        for t in [self._ws_task, self._spread_task, self._universe_task, self._heartbeat_task, self._ws_watchdog_task, self._paper_timeout_task]:
            if t: t.cancel()
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
    cfg=Config.load()
    orch=Orchestrator(cfg)
    _install_signal_handlers(asyncio.get_running_loop(), orch.stop_event)
    try:
        await orch.run()
    finally:
        try: await orch.api.close()
        except Exception: pass


if __name__=="__main__":
    asyncio.run(main())

from __future__ import annotations

import time
from typing import Any

from .aster_api import AsterAPI
from .indicators.atr import atr_pct


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _calc_spread_pct(bid: float, ask: float) -> float | None:
    if bid <= 0 or ask <= 0 or ask < bid:
        return None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    return (ask - bid) / mid * 100.0


def build_universe_once(cfg) -> dict:
    """
    Returns:
      {
        "ts": int,
        "activeSymbols": [..],
        "entries": { SYMBOL: {meta...} }
      }
    """
    api = AsterAPI(cfg.REST_BASE)

    quote = getattr(cfg, "QUOTE", "USDT")
    min_qv = float(getattr(cfg, "MIN_24H_QUOTE_VOL", 0))
    max_spread = float(getattr(cfg, "MAX_SPREAD_PCT", 999))
    min_atr = float(getattr(cfg, "MIN_ATR_PCT", 0))
    mode = str(getattr(cfg, "SYMBOL_MODE", "HYBRID_PRIORITY")).upper()

    whitelist = set(getattr(cfg, "WHITELIST", []) or [])
    blacklist = set(getattr(cfg, "BLACKLIST", []) or [])
    forced_active = [s for s in (getattr(cfg, "ACTIVE_SYMBOLS", []) or []) if s]

    wl_priority = bool(getattr(cfg, "WHITELIST_PRIORITY", True))
    wl_bypass_liq = bool(getattr(cfg, "WHITELIST_BYPASS_LIQUIDITY", False))

    target = int(getattr(cfg, "TARGET_SYMBOLS", 20))
    auto_top_n = int(getattr(cfg, "AUTO_TOP_N", 40))

    tf_sec = int(getattr(cfg, "TF_SEC", 60))
    lookback_min = int(getattr(cfg, "LOOKBACK_MINUTES", 20))
    atr_period = int(getattr(cfg, "ATR_PERIOD", 14))

    # Fetch exchangeInfo to validate symbols
    ex = api.exchange_info()
    symbols_info = {s.get("symbol"): s for s in ex.get("symbols", []) if isinstance(s, dict)}
    all_symbols = set(symbols_info.keys())

    # Forced active symbols (great for debugging). If provided, do not apply filters here.
    if forced_active:
        forced = []
        for s in forced_active:
            s = s.upper()
            if s in blacklist:
                continue
            if s not in all_symbols:
                continue
            # keep only correct quote
            if not s.endswith(quote):
                continue
            forced.append(s)
        entries = {s: {"symbol": s, "enabled_for_entry": True, "note": "FORCED_ACTIVE"} for s in forced}
        return {"ts": int(time.time()), "activeSymbols": forced[:target], "entries": entries}

    # Tickers & book tickers
    tickers = api.ticker_24h()
    # Aster API is Binance-like: /fapi/v1/ticker/24hr returns list[dict]
    if not isinstance(tickers, list):
        tickers = []

    # spread requires bid/ask; use bookTicker list
    book = api.book_tickers()
    book_map = {}
    if isinstance(book, list):
        for b in book:
            sym = (b or {}).get("symbol")
            if sym:
                book_map[str(sym).upper()] = b

    # Build candidate list by volume
    candidates: list[dict] = []
    for t in tickers:
        if not isinstance(t, dict):
            continue
        sym = (t.get("symbol") or t.get("s") or "").upper()
        if not sym or not sym.endswith(quote):
            continue
        if sym in blacklist:
            continue

        qv = _safe_float(t.get("quoteVolume") or t.get("quoteVolume24h") or t.get("quoteVol") or 0.0, 0.0)

        # spread from book ticker (preferred)
        b = book_map.get(sym, {})
        bid = _safe_float(b.get("bidPrice") or b.get("bid") or 0.0, 0.0)
        ask = _safe_float(b.get("askPrice") or b.get("ask") or 0.0, 0.0)
        spread = _calc_spread_pct(bid, ask)

        candidates.append({
            "symbol": sym,
            "quoteVol": qv,
            "bid": bid,
            "ask": ask,
            "spreadPct": spread,
        })

    candidates.sort(key=lambda x: x["quoteVol"], reverse=True)
    auto_syms = [x["symbol"] for x in candidates[:auto_top_n]]

    # Merge per mode
    merged: list[str] = []
    if mode == "WHITELIST_ONLY":
        merged = [s for s in sorted(whitelist) if s.endswith(quote) and s not in blacklist]
    elif mode in ("HYBRID_PRIORITY", "HYBRID"):
        wl_list = [s for s in sorted(whitelist) if s.endswith(quote) and s not in blacklist]
        if wl_priority:
            merged.extend(wl_list)
            for s in auto_syms:
                if s not in merged and s not in blacklist:
                    merged.append(s)
        else:
            merged.extend(auto_syms)
            for s in wl_list:
                if s not in merged:
                    merged.append(s)
    else:
        # fallback: just auto
        merged = list(auto_syms)

    merged = merged[:max(1, target)]

    # Compute final entries with filters
    entries: dict[str, dict] = {}
    active: list[str] = []

    for sym in merged:
        sym = sym.upper()
        if sym in blacklist:
            continue
        if sym not in all_symbols:
            continue

        # liquidity/spread
        meta = next((x for x in candidates if x["symbol"] == sym), None) or {"quoteVol": 0.0, "spreadPct": None}
        qv = float(meta.get("quoteVol", 0.0))
        spread = meta.get("spreadPct", None)

        # Liquidity filter (can bypass for whitelist)
        if (qv < min_qv) and not (wl_bypass_liq and sym in whitelist):
            entries[sym] = {"symbol": sym, "enabled_for_entry": False, "reason": "LOW_LIQUIDITY", "quoteVol": qv, "spreadPct": spread}
            continue

        # Spread filter (if unknown, do not block — it’s safer than producing activeSymbols=0)
        if spread is not None and spread > max_spread:
            entries[sym] = {"symbol": sym, "enabled_for_entry": False, "reason": "HIGH_SPREAD", "quoteVol": qv, "spreadPct": spread}
            continue

        # ATR filter (needs klines)
        try:
            kl = api.klines(sym, interval_sec=tf_sec, limit=max(atr_period + 5, 50), lookback_minutes=lookback_min)
            ap = atr_pct(kl, period=atr_period)
        except Exception:
            ap = 0.0

        if ap < min_atr and not (sym in whitelist and wl_bypass_liq):
            entries[sym] = {"symbol": sym, "enabled_for_entry": False, "reason": "LOW_ATR", "quoteVol": qv, "spreadPct": spread, "atrPct": ap}
            continue

        entries[sym] = {"symbol": sym, "enabled_for_entry": True, "quoteVol": qv, "spreadPct": spread, "atrPct": ap}
        active.append(sym)

    return {"ts": int(time.time()), "activeSymbols": active, "entries": entries}

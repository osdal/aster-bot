from __future__ import annotations

from typing import Dict, List, Tuple
import time

from .aster_api import AsterAPI

# Robust ATR import:
#  - Some projects have src/indicators.py (module)
#  - Others have src/indicators/atr.py (package)
#  - We support both; and have a tiny fallback implementation.
def _atr_fallback(ohlc: List[Tuple[float,float,float,float]], period: int) -> float:
    # Wilder ATR
    if not ohlc or len(ohlc) < period + 1:
        return 0.0
    trs = []
    prev_close = ohlc[0][3]
    for i in range(1, len(ohlc)):
        _, h, l, c = ohlc[i]
        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
        prev_close = c
    if len(trs) < period:
        return 0.0
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr

try:
    # src/indicators.py style
    from .indicators import atr as _atr  # type: ignore
except Exception:
    _atr = None

def _atr_value(ohlc: List[Tuple[float,float,float,float]], period: int) -> float:
    if _atr:
        try:
            return float(_atr(ohlc, period))
        except Exception:
            pass
    # try package style
    try:
        from .indicators.atr import atr as _atr2  # type: ignore
        return float(_atr2(ohlc, period))
    except Exception:
        return _atr_fallback(ohlc, period)


def build_universe_once(cfg) -> Dict:
    """
    Returns:
      {"activeSymbols": [...], "meta": {...}}

    Selection logic:
      1) If ACTIVE_SYMBOLS is set in .env -> use exactly these (after blacklist/quote sanity).
      2) Otherwise build from exchange data and apply:
         - QUOTE filter (USDT)
         - SYMBOL_MODE:
            WHITELIST_ONLY: only whitelist
            AUTO_ONLY: ignore whitelist priority
            HYBRID_PRIORITY: whitelist first, then auto selection
         - Liquidity/Spread/ATR filters
    """
    api = AsterAPI(cfg.REST_BASE)

    quote = getattr(cfg, "QUOTE", "USDT")
    mode = (getattr(cfg, "SYMBOL_MODE", "HYBRID_PRIORITY") or "HYBRID_PRIORITY").upper()
    whitelist = set(getattr(cfg, "WHITELIST", []) or [])
    blacklist = set(getattr(cfg, "BLACKLIST", []) or [])
    active_override = list(getattr(cfg, "ACTIVE_SYMBOLS", []) or [])

    # 1) ACTIVE_SYMBOLS override
    if active_override:
        active = []
        for s in active_override:
            s = str(s).upper()
            if not s.endswith(quote):
                continue
            if s in blacklist:
                continue
            active.append(s)
        # optional: keep unique while preserving order
        seen = set()
        active2 = []
        for s in active:
            if s not in seen:
                active2.append(s)
                seen.add(s)
        return {"activeSymbols": active2, "meta": {"mode": "ACTIVE_SYMBOLS_OVERRIDE"}}

    # 2) Automatic selection
    ex = api.exchange_info()
    symbols = []
    for s in ex.get("symbols", []):
        sym = s.get("symbol")
        if not sym or not sym.endswith(quote):
            continue
        if sym in blacklist:
            continue
        # only PERPETUAL contracts if available
        if s.get("contractType") and s.get("contractType") != "PERPETUAL":
            continue
        if s.get("status") and s.get("status") != "TRADING":
            continue
        symbols.append(sym)

    # 24h tickers
    tickers = api.tickers_24h()
    tmap = {t.get("symbol"): t for t in tickers if t.get("symbol")}

    min_qv = float(getattr(cfg, "MIN_24H_QUOTE_VOL", 3_000_000))
    max_spread = float(getattr(cfg, "MAX_SPREAD_PCT", 0.10))
    min_atr = float(getattr(cfg, "MIN_ATR_PCT", 0.03))

    tf_sec = int(getattr(cfg, "TF_SEC", 60))
    lookback_minutes = int(getattr(cfg, "LOOKBACK_MINUTES", 20))
    atr_period = int(getattr(cfg, "ATR_PERIOD", 14))

    klines_limit = max(atr_period + 2, int((lookback_minutes * 60) / max(tf_sec, 1)) + 2)

    candidates = []
    for sym in symbols:
        t = tmap.get(sym)
        if not t:
            continue

        try:
            quote_vol = float(t.get("quoteVolume") or 0)
        except Exception:
            quote_vol = 0.0

        # spread (bid/ask)
        try:
            mp = api.mark_price(sym)
            bid = float(mp.get("bidPrice") or 0)
            ask = float(mp.get("askPrice") or 0)
            mid = (bid + ask) / 2 if (bid and ask) else 0.0
            spread_pct = ((ask - bid) / mid) * 100.0 if mid > 0 else 999.0
        except Exception:
            spread_pct = 999.0

        # OHLC for ATR
        try:
            kl = api.klines(sym, interval_sec=tf_sec, limit=klines_limit)
            ohlc = []
            for k in kl:
                # expected [openTime, open, high, low, close, ...] or dicts
                if isinstance(k, (list, tuple)) and len(k) >= 5:
                    o = float(k[1]); h = float(k[2]); l = float(k[3]); c = float(k[4])
                elif isinstance(k, dict):
                    o = float(k.get("open")); h = float(k.get("high")); l = float(k.get("low")); c = float(k.get("close"))
                else:
                    continue
                ohlc.append((o,h,l,c))
            last_close = ohlc[-1][3] if ohlc else 0.0
            atr_val = _atr_value(ohlc, atr_period)
            atr_pct = (atr_val / last_close * 100.0) if last_close > 0 else 0.0
        except Exception:
            atr_pct = 0.0

        candidates.append({
            "symbol": sym,
            "quote_vol": quote_vol,
            "spread_pct": spread_pct,
            "atr_pct": atr_pct,
        })

    # liquidity/spread/atr filters
    filtered = []
    for c in candidates:
        sym = c["symbol"]
        if sym in blacklist:
            continue

        # whitelist bypass (optional)
        if sym in whitelist and bool(getattr(cfg, "WHITELIST_BYPASS_LIQUIDITY", False)):
            filtered.append(c)
            continue

        if c["quote_vol"] < min_qv:
            continue
        if c["spread_pct"] > max_spread:
            continue
        if c["atr_pct"] < min_atr:
            continue
        filtered.append(c)

    # Ranking
    filtered.sort(key=lambda x: (x["quote_vol"], -x["spread_pct"], x["atr_pct"]), reverse=True)

    auto_top_n = int(getattr(cfg, "AUTO_TOP_N", 40))
    target = int(getattr(cfg, "TARGET_SYMBOLS", 20))

    auto = [c["symbol"] for c in filtered[:max(1, auto_top_n)]]

    active: List[str] = []
    if mode == "WHITELIST_ONLY":
        active = [s for s in whitelist if s.endswith(quote) and s not in blacklist]
    elif mode == "AUTO_ONLY":
        active = auto[:target]
    else:
        # HYBRID_PRIORITY
        wl = [s for s in whitelist if s.endswith(quote) and s not in blacklist]
        if bool(getattr(cfg, "WHITELIST_PRIORITY", True)):
            # whitelist first, then auto without duplicates
            seen = set()
            for s in wl:
                if s not in seen:
                    active.append(s); seen.add(s)
            for s in auto:
                if s not in seen:
                    active.append(s); seen.add(s)
        else:
            # auto first, then whitelist
            seen = set()
            for s in auto:
                if s not in seen:
                    active.append(s); seen.add(s)
            for s in wl:
                if s not in seen:
                    active.append(s); seen.add(s)

        active = active[:target]

    meta = {
        "mode": mode,
        "target": target,
        "quote": quote,
        "min_quote_vol": min_qv,
        "max_spread_pct": max_spread,
        "min_atr_pct": min_atr,
        "count_candidates": len(candidates),
        "count_filtered": len(filtered),
        "ts": int(time.time()),
    }
    return {"activeSymbols": active, "meta": meta}

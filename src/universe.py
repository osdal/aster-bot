import os
import time
from typing import Any

from .aster_api import AsterAPI
from .indicators import atr


def _safe_float(v: Any) -> float | None:
    try:
        return float(v)
    except Exception:
        return None


def _normalize_symbol(s: str) -> str:
    return s.strip().upper()


def _calc_spread_pct(bid: float, ask: float) -> float | None:
    if bid <= 0 or ask <= 0:
        return None
    mid = (bid + ask) / 2.0
    if mid == 0:
        return None
    return (ask - bid) / mid * 100.0


def build_universe_once(cfg) -> dict:
    # Backward/forward compatibility: don't crash if cfg.REST_BASE is missing
    rest_base = getattr(cfg, "REST_BASE", None) or os.getenv("ASTER_REST_BASE", "https://fapi.asterdex.com").rstrip("/")
    api = AsterAPI(rest_base)

    exch = api.exchange_info()
    symbols_raw = exch.get("symbols", [])
    symbols_info: dict[str, dict] = {}

    for s in symbols_raw:
        sym = _normalize_symbol(s.get("symbol", ""))
        if not sym:
            continue

        # only USDT perpetuals (common pattern)
        ct = (s.get("contractType") or "").upper()
        q = (s.get("quoteAsset") or "").upper()
        status = (s.get("status") or "").upper()
        if q != "USDT":
            continue
        if ct and ct not in {"PERPETUAL"}:
            continue
        if status and status not in {"TRADING"}:
            continue

        symbols_info[sym] = s

    tickers = api.ticker_24h()
    if not isinstance(tickers, list):
        tickers = []

    # build candidates with filters
    min_atr = float(getattr(cfg, "MIN_ATR_PCT", 0.0))
    max_spread = float(getattr(cfg, "MAX_SPREAD_PCT", 9999.0))
    min_qv = float(getattr(cfg, "MIN_24H_QUOTE_VOL", 0.0))

    items = []
    for t in tickers:
        sym = _normalize_symbol(t.get("symbol", ""))
        if sym not in symbols_info:
            continue

        qv = _safe_float(t.get("quoteVolume")) or 0.0
        if qv < min_qv:
            continue

        bid = _safe_float(t.get("bidPrice")) or 0.0
        ask = _safe_float(t.get("askPrice")) or 0.0
        spread = _calc_spread_pct(bid, ask)
        if spread is None or spread > max_spread:
            continue

        # ATR on recent klines
        try:
            kl = api.klines(sym, interval="1m", limit=100)
            atr_pct = atr.atr_pct_from_klines(kl)  # returns percent
        except Exception:
            continue

        if atr_pct < min_atr:
            continue

        items.append({
            "symbol": sym,
            "quoteVolume": qv,
            "spreadPct": spread,
            "atrPct": atr_pct,
        })

    mode = str(getattr(cfg, "SYMBOL_MODE", "HYBRID_PRIORITY")).upper()
    target = int(getattr(cfg, "TARGET_SYMBOLS", 15))

    if mode == "LIST":
        raw = str(getattr(cfg, "SYMBOLS_LIST", "")).strip()
        wanted = [_normalize_symbol(x) for x in raw.split(",") if x.strip()]
        active = [s for s in wanted if s in symbols_info]
        return {"activeSymbols": active[:target], "mode": mode, "candidates": items}

    # Sorting helpers
    by_vol = sorted(items, key=lambda x: x["quoteVolume"], reverse=True)
    by_atr = sorted(items, key=lambda x: x["atrPct"], reverse=True)
    by_spread = sorted(items, key=lambda x: x["spreadPct"])

    active: list[str] = []

    if mode == "TOP_VOLUME":
        active = [x["symbol"] for x in by_vol[:target]]
    elif mode == "TOP_ATR":
        active = [x["symbol"] for x in by_atr[:target]]
    elif mode == "LOW_SPREAD":
        active = [x["symbol"] for x in by_spread[:target]]
    else:
        # HYBRID / HYBRID_PRIORITY:
        # take a mix: 50% volume, 30% atr, 20% low spread (dedup, keep order)
        n1 = max(1, int(target * 0.5))
        n2 = max(1, int(target * 0.3))
        n3 = max(1, target - n1 - n2)

        seq = []
        if mode == "HYBRID_PRIORITY":
            seq = (
                [x["symbol"] for x in by_vol[:n1]] +
                [x["symbol"] for x in by_atr[:n2]] +
                [x["symbol"] for x in by_spread[:n3]]
            )
        else:
            # plain hybrid: interleave
            seq = []
            seq.extend([x["symbol"] for x in by_vol[:n1]])
            seq.extend([x["symbol"] for x in by_atr[:n2]])
            seq.extend([x["symbol"] for x in by_spread[:n3]])

        seen = set()
        for s in seq:
            if s not in seen:
                seen.add(s)
                active.append(s)
            if len(active) >= target:
                break

    return {"activeSymbols": active, "mode": mode, "candidates": items}

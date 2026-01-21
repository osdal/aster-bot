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
    api = AsterAPI(cfg.REST_BASE)
    ts = int(time.time())

    exch = api.exchange_info()
    symbols_raw = exch.get("symbols", [])
    symbols_info: dict[str, dict] = {}

    for s in symbols_raw:
        sym = _normalize_symbol(s.get("symbol", ""))
        if not sym:
            continue
        if _normalize_symbol(s.get("quoteAsset", "")) != cfg.QUOTE:
            continue
        status = _normalize_symbol(s.get("status", ""))
        if status and status != "TRADING":
            continue
        symbols_info[sym] = {
            "status": status,
            "baseAsset": s.get("baseAsset"),
            "quoteAsset": s.get("quoteAsset"),
        }

    t24_list = api.ticker_24h()
    t24: dict[str, dict] = {}
    for row in t24_list:
        sym = _normalize_symbol(row.get("symbol", ""))
        if sym in symbols_info:
            t24[sym] = {
                "quoteVolume": _safe_float(row.get("quoteVolume")),
                "lastPrice": _safe_float(row.get("lastPrice")),
            }

    # Auto candidates by quote volume
    auto_candidates = []
    for sym, row in t24.items():
        if sym in cfg.BLACKLIST:
            continue
        qv = row.get("quoteVolume") or 0.0
        if qv >= cfg.MIN_24H_QUOTE_VOL:
            auto_candidates.append((sym, qv))
    auto_candidates.sort(key=lambda x: x[1], reverse=True)
    auto_syms = [sym for sym, _ in auto_candidates[: cfg.AUTO_TOP_N]]

    wl = {s for s in cfg.WHITELIST if s in symbols_info and s not in cfg.BLACKLIST}
    auto_set = {s for s in auto_syms if s in symbols_info and s not in cfg.BLACKLIST}

    mode = cfg.SYMBOL_MODE
    merged: list[tuple[str, str]] = []

    if mode == "WHITELIST_ONLY":
        merged = [(s, "WHITELIST") for s in sorted(wl)]
    elif mode == "AUTO_ONLY":
        merged = [(s, "AUTO") for s in auto_syms if s in auto_set]
    elif mode == "HYBRID_UNION":
        union = list(wl.union(auto_set))

        def score(sym: str) -> float:
            qv = (t24.get(sym, {}) or {}).get("quoteVolume") or 0.0
            return (1e18 if sym in wl else 0.0) + qv

        union.sort(key=score, reverse=True)
        union = union[: cfg.TARGET_SYMBOLS]
        merged = [(s, "WHITELIST" if s in wl else "AUTO") for s in union]
    else:
        # HYBRID_PRIORITY
        out = []
        if cfg.WHITELIST_PRIORITY:
            out.extend([(s, "WHITELIST") for s in sorted(wl)])
        for s in auto_syms:
            if s in auto_set and s not in wl:
                out.append((s, "AUTO"))
            if len(out) >= cfg.TARGET_SYMBOLS:
                break
        merged = out[: cfg.TARGET_SYMBOLS]

    # Spread data
    book_list = api.book_ticker()
    book: dict[str, dict] = {}
    for row in book_list:
        sym = _normalize_symbol(row.get("symbol", ""))
        if sym:
            bid = _safe_float(row.get("bidPrice"))
            ask = _safe_float(row.get("askPrice"))
            if bid is not None and ask is not None:
                book[sym] = {"bid": bid, "ask": ask}

    entries = {}
    for sym, source in merged:
        qv = (t24.get(sym, {}) or {}).get("quoteVolume")
        last_price = (t24.get(sym, {}) or {}).get("lastPrice")

        entry = {
            "symbol": sym,
            "source": source,
            "enabled": True,
            "enabled_for_entry": True,
            "reason_disabled": [],
            "quoteVolume24h": qv,
            "spreadPct": None,
            "atrPct": None,
            "lastUpdateTs": ts,
        }

        # Liquidity
        if source == "AUTO":
            if (qv or 0.0) < cfg.MIN_24H_QUOTE_VOL:
                entry["enabled_for_entry"] = False
                entry["reason_disabled"].append("LOW_LIQUIDITY")
        else:
            if not cfg.WHITELIST_BYPASS_LIQUIDITY and (qv or 0.0) < cfg.MIN_24H_QUOTE_VOL:
                entry["enabled_for_entry"] = False
                entry["reason_disabled"].append("LOW_LIQUIDITY")

        # Spread
        if sym in book:
            sp = _calc_spread_pct(book[sym]["bid"], book[sym]["ask"])
            entry["spreadPct"] = sp
            if sp is not None and sp > cfg.MAX_SPREAD_PCT:
                entry["enabled_for_entry"] = False
                entry["reason_disabled"].append("SPREAD_TOO_HIGH")

        # ATR%
        try:
            limit = max(50, cfg.ATR_PERIOD + 20)
            k = api.klines(sym, interval="1m", limit=limit)
            ohlc = []
            for row in k:
                o = _safe_float(row[1]); h = _safe_float(row[2]); l = _safe_float(row[3]); c = _safe_float(row[4])
                if None not in (o, h, l, c):
                    ohlc.append((o, h, l, c))
            a = atr(ohlc, cfg.ATR_PERIOD)
            if a is not None and last_price and last_price > 0:
                atr_pct = a / last_price * 100.0
                entry["atrPct"] = atr_pct
                if atr_pct < cfg.MIN_ATR_PCT:
                    entry["enabled_for_entry"] = False
                    entry["reason_disabled"].append("ATR_TOO_LOW")
        except Exception:
            entry["enabled_for_entry"] = False
            entry["reason_disabled"].append("KLINES_ERROR")

        entries[sym] = entry

    active = [s for s, e in entries.items() if e["enabled_for_entry"]]

    return {
        "mode": cfg.SYMBOL_MODE,
        "symbols": entries,
        "activeSymbols": active,
        "updatedAt": ts,
    }


def print_universe_summary(universe: dict) -> None:
    print(f"\nUniverse mode: {universe['mode']}")
    print(f"Total symbols: {len(universe['symbols'])}")
    print(f"Active for entry: {len(universe['activeSymbols'])}\n")

    for sym, e in universe["symbols"].items():
        qv = e["quoteVolume24h"]
        sp = e["spreadPct"]
        ap = e["atrPct"]
        flags = "" if e["enabled_for_entry"] else ("DISABLED: " + ",".join(e["reason_disabled"]))
        print(
            f"{sym:12} src={e['source']:<9} "
            f"qVol={qv if qv is not None else 'NA':>10} "
            f"spread%={sp if sp is not None else 'NA':>6} "
            f"atr%={ap if ap is not None else 'NA':>6} "
            f"{flags}"
        )


def save_universe_json(universe: dict, path: str) -> None:
    import json
    import os

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(universe, f, ensure_ascii=False, indent=2)

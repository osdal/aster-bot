import time
from typing import Dict, List, Tuple
import urllib.parse
import urllib.request
import json


def _get_json(url: str, timeout: int = 20):
    req = urllib.request.Request(url, method="GET", headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def _safe_list(x):
    if isinstance(x, list):
        return x
    # some APIs wrap payloads
    if isinstance(x, dict):
        for k in ("data", "result", "rows", "items"):
            v = x.get(k)
            if isinstance(v, list):
                return v
    return []


def _symbols_from_exchange_info(base: str, quote: str) -> List[str]:
    ex = _get_json(f"{base}/fapi/v1/exchangeInfo")
    out = []
    for s in ex.get("symbols", []) or []:
        try:
            if s.get("contractType") not in (None, "", "PERPETUAL"):
                continue
            if s.get("status") not in (None, "", "TRADING"):
                continue
            sym = (s.get("symbol") or "").upper()
            if not sym.endswith(quote):
                continue
            out.append(sym)
        except Exception:
            pass
    return out


def _tickers_24h(base: str):
    # Aster is Binance-compatible on fapi endpoints; 24hr ticker without symbol is a list.
    return _safe_list(_get_json(f"{base}/fapi/v1/ticker/24hr"))


def _spread_pct(base: str, symbol: str) -> float:
    q = urllib.parse.urlencode({"symbol": symbol, "limit": "5"})
    ob = _get_json(f"{base}/fapi/v1/depth?{q}")
    bids = ob.get("bids") or []
    asks = ob.get("asks") or []
    if not bids or not asks:
        return 999.0
    try:
        bid = float(bids[0][0])
        ask = float(asks[0][0])
        mid = (bid + ask) / 2.0 if (bid + ask) > 0 else 0
        if mid <= 0:
            return 999.0
        return abs(ask - bid) / mid * 100.0
    except Exception:
        return 999.0


def _atr_pct_1m(base: str, symbol: str, lookback_minutes: int, atr_period: int) -> float:
    # Simple ATR% on 1m klines. Uses last close as denominator.
    limit = max(atr_period + 2, min(1000, lookback_minutes + 2))
    q = urllib.parse.urlencode({"symbol": symbol, "interval": "1m", "limit": str(limit)})
    kl = _safe_list(_get_json(f"{base}/fapi/v1/klines?{q}"))
    if len(kl) < (atr_period + 1):
        return 0.0

    # Binance kline: [openTime, open, high, low, close, volume, ...]
    highs, lows, closes = [], [], []
    for row in kl:
        try:
            highs.append(float(row[2]))
            lows.append(float(row[3]))
            closes.append(float(row[4]))
        except Exception:
            pass

    if len(closes) < (atr_period + 1):
        return 0.0

    trs = []
    for i in range(1, len(closes)):
        h = highs[i]
        l = lows[i]
        pc = closes[i - 1]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)

    if len(trs) < atr_period:
        return 0.0

    atr = sum(trs[-atr_period:]) / float(atr_period)
    last = closes[-1]
    if last <= 0:
        return 0.0
    return atr / last * 100.0


def build_universe_once(cfg) -> Dict:
    """
    Returns: {"activeSymbols": [...], "meta": {...}}
    Rules:
      - If cfg.ACTIVE_SYMBOLS is set -> use it (minus blacklist).
      - Else build candidates from 24h tickers filtered by quote and blacklist.
      - SYMBOL_MODE:
          WHITELIST_ONLY: only whitelist
          AUTO_ONLY: only auto candidates
          HYBRID_PRIORITY: whitelist first then auto candidates
      - Liquidity filters apply to auto candidates; whitelist can bypass liquidity if WHITELIST_BYPASS_LIQUIDITY=true.
    """
    base = getattr(cfg, "REST_BASE", "https://fapi.asterdex.com").rstrip("/")
    quote = getattr(cfg, "QUOTE", "USDT")
    symbol_mode = (getattr(cfg, "SYMBOL_MODE", "HYBRID_PRIORITY") or "HYBRID_PRIORITY").upper()

    whitelist = [s.upper() for s in (getattr(cfg, "WHITELIST", []) or [])]
    blacklist = set(s.upper() for s in (getattr(cfg, "BLACKLIST", []) or []))
    wl_priority = bool(getattr(cfg, "WHITELIST_PRIORITY", True))
    wl_bypass = bool(getattr(cfg, "WHITELIST_BYPASS_LIQUIDITY", False))

    target = int(getattr(cfg, "TARGET_SYMBOLS", 15))
    auto_top_n = int(getattr(cfg, "AUTO_TOP_N", 40))

    min_vol = float(getattr(cfg, "MIN_24H_QUOTE_VOL", 3_000_000))
    max_spread = float(getattr(cfg, "MAX_SPREAD_PCT", 0.10))
    min_atr = float(getattr(cfg, "MIN_ATR_PCT", 0.03))

    lookback_minutes = int(getattr(cfg, "LOOKBACK_MINUTES", 20))
    atr_period = int(getattr(cfg, "ATR_PERIOD", 14))

    # Hard override
    active_override = [s.upper() for s in (getattr(cfg, "ACTIVE_SYMBOLS", []) or [])]
    if active_override:
        out = [s for s in active_override if s not in blacklist]
        return {"activeSymbols": out, "meta": {"mode": "ACTIVE_SYMBOLS", "ts": int(time.time())}}

    # universe of tradable symbols (for quote validation)
    tradable = set(_symbols_from_exchange_info(base, quote))

    # --- WHITELIST part
    wl = [s for s in whitelist if s in tradable and s not in blacklist]

    # --- AUTO candidates
    tickers = _tickers_24h(base)
    candidates = []
    for t in tickers:
        sym = (t.get("symbol") or "").upper()
        if not sym or sym not in tradable:
            continue
        if sym in blacklist:
            continue
        # 24h quoteVolume is preferred on futures tickers
        qv = t.get("quoteVolume") or t.get("quoteVol") or t.get("q") or "0"
        try:
            qv = float(qv)
        except Exception:
            qv = 0.0
        if qv <= 0:
            continue
        candidates.append((sym, qv))

    # sort by quote volume
    candidates.sort(key=lambda x: x[1], reverse=True)
    candidates = candidates[:max(10, auto_top_n)]

    passed = []
    for sym, qv in candidates:
        if qv < min_vol:
            continue
        sp = _spread_pct(base, sym)
        if sp > max_spread:
            continue
        atrp = _atr_pct_1m(base, sym, lookback_minutes, atr_period)
        if atrp < min_atr:
            continue
        passed.append((sym, qv, sp, atrp))
        if len(passed) >= auto_top_n:
            break

    auto = [x[0] for x in passed]

    # --- merge according to mode
    active: List[str] = []
    if symbol_mode == "WHITELIST_ONLY":
        active = wl
    elif symbol_mode == "AUTO_ONLY":
        active = auto
    else:  # HYBRID_PRIORITY
        if wl_priority:
            active = wl + [s for s in auto if s not in set(wl)]
        else:
            active = auto + [s for s in wl if s not in set(auto)]

    active = active[:max(1, target)]

    return {
        "activeSymbols": active,
        "meta": {
            "mode": symbol_mode,
            "ts": int(time.time()),
            "counts": {"whitelist": len(wl), "auto": len(auto), "active": len(active)},
            "filters": {"min_vol": min_vol, "max_spread_pct": max_spread, "min_atr_pct": min_atr},
        },
    }

import os
import re
from dataclasses import dataclass


def _parse_int(name: str, default: int) -> int:
    """
    Robust int parser for .env values.
    Accepts values like "20", " 20 ", "20A", "20А" (Cyrillic A), "20.0".
    Extracts the first integer token; falls back to default.
    """
    raw = os.getenv(name, "")
    if raw is None:
        return default
    s = str(raw).strip()
    if not s:
        return default

    # try straight int first
    try:
        return int(s)
    except Exception:
        pass

    # extract leading numeric token
    m = re.search(r"-?\d+", s)
    if not m:
        return default
    try:
        return int(m.group(0))
    except Exception:
        return default


def _parse_float(name: str, default: float) -> float:
    raw = os.getenv(name, "")
    if raw is None:
        return default
    s = str(raw).strip().replace(",", ".")
    if not s:
        return default
    try:
        return float(s)
    except Exception:
        m = re.search(r"-?\d+(?:\.\d+)?", s)
        if not m:
            return default
        try:
            return float(m.group(0))
        except Exception:
            return default


@dataclass
class Config:
    # market selection / universe
    SYMBOL_MODE: str = "HYBRID_PRIORITY"     # HYBRID_PRIORITY | MANUAL | TOPVOL | etc (depends on universe.py)
    TARGET_SYMBOLS: int = 15

    MIN_24H_QUOTE_VOL: float = 3_000_000.0  # USDT quote volume filter
    MIN_ATR_PCT: float = 0.12               # % ATR(??) filter (implementation in universe.py)
    MAX_SPREAD_PCT: float = 0.06            # max spread (%)

    # paper execution
    TRADE_NOTIONAL_USD: float = 75.0
    TP_PCT: float = 0.60
    SL_PCT: float = 0.20
    MAX_HOLDING_SEC: int = 420
    COOLDOWN_AFTER_TRADE_SEC: int = 120
    MAX_TRADES_PER_HOUR: int = 6

    # risk pauses (DISABLED by default; set >0 to enable)
    MAX_CONSECUTIVE_LOSSES: int = 0
    PAUSE_AFTER_CONSECUTIVE_LOSSES_SEC: int = 0
    SYMBOL_MAX_SL_STREAK: int = 0
    SYMBOL_PAUSE_AFTER_SL_STREAK_SEC: int = 0

    # websocket base
    WS_BASE: str = "wss://fstream.asterdex.com/stream?streams="


def load_config() -> Config:
    return Config(
        SYMBOL_MODE=os.getenv("SYMBOL_MODE", "HYBRID_PRIORITY").strip(),
        TARGET_SYMBOLS=_parse_int("TARGET_SYMBOLS", 15),

        MIN_24H_QUOTE_VOL=_parse_float("MIN_24H_QUOTE_VOL", 3_000_000.0),
        MIN_ATR_PCT=_parse_float("MIN_ATR_PCT", 0.12),
        MAX_SPREAD_PCT=_parse_float("MAX_SPREAD_PCT", 0.06),

        TRADE_NOTIONAL_USD=_parse_float("TRADE_NOTIONAL_USD", 75.0),
        TP_PCT=_parse_float("TP_PCT", 0.60),
        SL_PCT=_parse_float("SL_PCT", 0.20),
        MAX_HOLDING_SEC=_parse_int("MAX_HOLDING_SEC", 420),
        COOLDOWN_AFTER_TRADE_SEC=_parse_int("COOLDOWN_AFTER_TRADE_SEC", 120),
        MAX_TRADES_PER_HOUR=_parse_int("MAX_TRADES_PER_HOUR", 6),

        MAX_CONSECUTIVE_LOSSES=_parse_int("MAX_CONSECUTIVE_LOSSES", 0),
        PAUSE_AFTER_CONSECUTIVE_LOSSES_SEC=_parse_int("PAUSE_AFTER_CONSECUTIVE_LOSSES_SEC", 0),
        SYMBOL_MAX_SL_STREAK=_parse_int("SYMBOL_MAX_SL_STREAK", 0),
        SYMBOL_PAUSE_AFTER_SL_STREAK_SEC=_parse_int("SYMBOL_PAUSE_AFTER_SL_STREAK_SEC", 0),

        WS_BASE=os.getenv("WS_BASE", "wss://fstream.asterdex.com/stream?streams=").strip(),
    )

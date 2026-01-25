import os
from dataclasses import dataclass
from typing import List


def _to_bool(v: str, default: bool = False) -> bool:
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _to_int(v: str, default: int) -> int:
    if v is None:
        return default
    s = str(v).strip()
    try:
        return int(s)
    except Exception:
        # tolerate accidental extra chars (e.g., "20А")
        digits = "".join(ch for ch in s if ch.isdigit() or ch == "-")
        return int(digits) if digits not in ("", "-") else default


def _to_float(v: str, default: float) -> float:
    if v is None:
        return default
    s = str(v).strip().replace(",", ".")
    try:
        return float(s)
    except Exception:
        return default


def _csv_list(v: str) -> List[str]:
    if not v:
        return []
    return [x.strip().upper() for x in str(v).split(",") if x.strip()]


@dataclass
class Config:
    # endpoints
    REST_BASE: str
    WS_BASE: str

    # universe selection
    SYMBOL_MODE: str              # HYBRID_PRIORITY | WHITELIST_ONLY | AUTO_ONLY
    QUOTE: str                    # USDT
    ACTIVE_SYMBOLS: List[str]     # hard override for paper symbols (if set)
    TARGET_SYMBOLS: int
    AUTO_TOP_N: int
    REFRESH_UNIVERSE_SEC: int

    WHITELIST: List[str]
    BLACKLIST: List[str]
    WHITELIST_PRIORITY: bool
    WHITELIST_BYPASS_LIQUIDITY: bool

    MIN_24H_QUOTE_VOL: float
    MAX_SPREAD_PCT: float
    MIN_ATR_PCT: float
    TF_SEC: int
    LOOKBACK_MINUTES: int
    ATR_PERIOD: int

    # paper params / execution params
    TRADE_NOTIONAL_USD: float
    TP_PCT: float
    SL_PCT: float
    MAX_HOLDING_SEC: int
    IMPULSE_LOOKBACK_SEC: int
    BREAKOUT_BUFFER_PCT: float

    COOLDOWN_AFTER_TRADE_SEC: int
    MAX_TRADES_PER_HOUR: int

    # risk pauses (paper) - can be disabled with 0
    MAX_CONSECUTIVE_LOSSES: int
    PAUSE_AFTER_CONSECUTIVE_LOSSES_SEC: int
    SYMBOL_MAX_SL_STREAK: int
    SYMBOL_PAUSE_AFTER_SL_STREAK_SEC: int


def load_config() -> Config:
    # accept both old and new env var names
    rest = (os.getenv("ASTER_REST_BASE") or os.getenv("REST_BASE") or "https://fapi.asterdex.com").rstrip("/")
    ws = (os.getenv("ASTER_WS_BASE") or os.getenv("WS_BASE") or "wss://fstream.asterdex.com").rstrip("/")

    symbol_mode = (os.getenv("SYMBOL_MODE") or "HYBRID_PRIORITY").strip().upper()
    quote = (os.getenv("QUOTE") or "USDT").strip().upper()

    active_symbols = _csv_list(os.getenv("ACTIVE_SYMBOLS", ""))

    return Config(
        REST_BASE=rest,
        WS_BASE=ws,

        SYMBOL_MODE=symbol_mode,
        QUOTE=quote,
        ACTIVE_SYMBOLS=active_symbols,
        TARGET_SYMBOLS=_to_int(os.getenv("TARGET_SYMBOLS"), 15),
        AUTO_TOP_N=_to_int(os.getenv("AUTO_TOP_N"), 40),
        REFRESH_UNIVERSE_SEC=_to_int(os.getenv("REFRESH_UNIVERSE_SEC"), 900),

        WHITELIST=_csv_list(os.getenv("WHITELIST", "")),
        BLACKLIST=_csv_list(os.getenv("BLACKLIST", "")),
        WHITELIST_PRIORITY=_to_bool(os.getenv("WHITELIST_PRIORITY"), True),
        WHITELIST_BYPASS_LIQUIDITY=_to_bool(os.getenv("WHITELIST_BYPASS_LIQUIDITY"), False),

        MIN_24H_QUOTE_VOL=_to_float(os.getenv("MIN_24H_QUOTE_VOL"), 3_000_000.0),
        MAX_SPREAD_PCT=_to_float(os.getenv("MAX_SPREAD_PCT"), 0.10),
        MIN_ATR_PCT=_to_float(os.getenv("MIN_ATR_PCT"), 0.03),
        TF_SEC=_to_int(os.getenv("TF_SEC"), 60),
        LOOKBACK_MINUTES=_to_int(os.getenv("LOOKBACK_MINUTES"), 20),
        ATR_PERIOD=_to_int(os.getenv("ATR_PERIOD"), 14),

        TRADE_NOTIONAL_USD=_to_float(os.getenv("TRADE_NOTIONAL_USD"), 50.0),
        TP_PCT=_to_float(os.getenv("TP_PCT"), 0.6),
        SL_PCT=_to_float(os.getenv("SL_PCT"), 0.2),
        MAX_HOLDING_SEC=_to_int(os.getenv("MAX_HOLDING_SEC"), 600),
        IMPULSE_LOOKBACK_SEC=_to_int(os.getenv("IMPULSE_LOOKBACK_SEC"), 10),
        BREAKOUT_BUFFER_PCT=_to_float(os.getenv("BREAKOUT_BUFFER_PCT"), 0.10),

        COOLDOWN_AFTER_TRADE_SEC=_to_int(os.getenv("COOLDOWN_AFTER_TRADE_SEC"), 0),
        MAX_TRADES_PER_HOUR=_to_int(os.getenv("MAX_TRADES_PER_HOUR"), 0),

        MAX_CONSECUTIVE_LOSSES=_to_int(os.getenv("MAX_CONSECUTIVE_LOSSES"), 0),
        PAUSE_AFTER_CONSECUTIVE_LOSSES_SEC=_to_int(os.getenv("PAUSE_AFTER_CONSECUTIVE_LOSSES_SEC"), 0),
        SYMBOL_MAX_SL_STREAK=_to_int(os.getenv("SYMBOL_MAX_SL_STREAK"), 0),
        SYMBOL_PAUSE_AFTER_SL_STREAK_SEC=_to_int(os.getenv("SYMBOL_PAUSE_AFTER_SL_STREAK_SEC"), 0),
    )

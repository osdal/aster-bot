import os
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass
class Config:
    REST_BASE: str
    WS_BASE: str

    SYMBOL_MODE: str
    WHITELIST: set[str]
    BLACKLIST: set[str]
    QUOTE: str

    AUTO_TOP_N: int
    TARGET_SYMBOLS: int
    REFRESH_UNIVERSE_SEC: int

    MIN_24H_QUOTE_VOL: float
    MAX_SPREAD_PCT: float
    MIN_ATR_PCT: float

    TF_SEC: int
    LOOKBACK_MINUTES: int
    ATR_PERIOD: int

    WHITELIST_BYPASS_LIQUIDITY: bool
    WHITELIST_PRIORITY: bool

    # paper params
    TRADE_NOTIONAL_USD: float
    BREAKOUT_BUFFER_PCT: float
    TP_PCT: float
    SL_PCT: float
    MAX_HOLDING_SEC: int
    COOLDOWN_AFTER_TRADE_SEC: int

    # extra paper risk controls (0 disables)
    MAX_TRADES_PER_HOUR: int
    MAX_CONSECUTIVE_LOSSES: int
    PAUSE_AFTER_CONSECUTIVE_LOSSES_SEC: int
    SYMBOL_MAX_SL_STREAK: int
    SYMBOL_PAUSE_AFTER_SL_STREAK_SEC: int


def _parse_bool(v: str, default: bool = False) -> bool:
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _parse_list_set(v: str) -> set[str]:
    if not v:
        return set()
    parts = [p.strip().upper() for p in v.split(",")]
    return {p for p in parts if p}


def _parse_int(v: str, default: int) -> int:
    if v is None:
        return default
    # allow accidental "20А" or "20 " etc: keep only leading digits with optional sign
    s = v.strip()
    num = ""
    for i, ch in enumerate(s):
        if i == 0 and ch in "+-":
            num += ch
            continue
        if ch.isdigit():
            num += ch
        else:
            break
    if num in ("", "+", "-"):
        return default
    try:
        return int(num)
    except Exception:
        return default


def _parse_float(v: str, default: float) -> float:
    if v is None:
        return default
    s = v.strip().replace(",", ".")
    try:
        return float(s)
    except Exception:
        return default


def load_config() -> Config:
    load_dotenv()

    return Config(
        REST_BASE=os.getenv("ASTER_REST_BASE", "https://fapi.asterdex.com").rstrip("/"),
        WS_BASE=os.getenv("ASTER_WS_BASE", "wss://fstream.asterdex.com").rstrip("/"),

        SYMBOL_MODE=os.getenv("SYMBOL_MODE", "HYBRID_PRIORITY").strip().upper(),
        WHITELIST=_parse_list_set(os.getenv("WHITELIST", "")),
        BLACKLIST=_parse_list_set(os.getenv("BLACKLIST", "")),
        QUOTE=os.getenv("QUOTE", "USDT").strip().upper(),

        AUTO_TOP_N=_parse_int(os.getenv("AUTO_TOP_N"), 30),
        TARGET_SYMBOLS=_parse_int(os.getenv("TARGET_SYMBOLS"), 15),
        REFRESH_UNIVERSE_SEC=_parse_int(os.getenv("REFRESH_UNIVERSE_SEC"), 900),

        MIN_24H_QUOTE_VOL=_parse_float(os.getenv("MIN_24H_QUOTE_VOL"), 5_000_000.0),
        MAX_SPREAD_PCT=_parse_float(os.getenv("MAX_SPREAD_PCT"), 0.08),
        MIN_ATR_PCT=_parse_float(os.getenv("MIN_ATR_PCT"), 0.025),

        TF_SEC=_parse_int(os.getenv("TF_SEC"), 60),
        LOOKBACK_MINUTES=_parse_int(os.getenv("LOOKBACK_MINUTES"), 20),
        ATR_PERIOD=_parse_int(os.getenv("ATR_PERIOD"), 14),

        WHITELIST_BYPASS_LIQUIDITY=_parse_bool(os.getenv("WHITELIST_BYPASS_LIQUIDITY", "false")),
        WHITELIST_PRIORITY=_parse_bool(os.getenv("WHITELIST_PRIORITY", "true")),

        TRADE_NOTIONAL_USD=_parse_float(os.getenv("TRADE_NOTIONAL_USD"), 75.0),
        BREAKOUT_BUFFER_PCT=_parse_float(os.getenv("BREAKOUT_BUFFER_PCT"), 0.05),
        TP_PCT=_parse_float(os.getenv("TP_PCT"), 0.60),
        SL_PCT=_parse_float(os.getenv("SL_PCT"), 0.20),
        MAX_HOLDING_SEC=_parse_int(os.getenv("MAX_HOLDING_SEC"), 600),
        COOLDOWN_AFTER_TRADE_SEC=_parse_int(os.getenv("COOLDOWN_AFTER_TRADE_SEC"), 0),

        # risk controls in paper engine (0 disables)
        MAX_TRADES_PER_HOUR=_parse_int(os.getenv("MAX_TRADES_PER_HOUR"), 0),
        MAX_CONSECUTIVE_LOSSES=_parse_int(os.getenv("MAX_CONSECUTIVE_LOSSES"), 0),
        PAUSE_AFTER_CONSECUTIVE_LOSSES_SEC=_parse_int(os.getenv("PAUSE_AFTER_CONSECUTIVE_LOSSES_SEC"), 0),
        SYMBOL_MAX_SL_STREAK=_parse_int(os.getenv("SYMBOL_MAX_SL_STREAK"), 0),
        SYMBOL_PAUSE_AFTER_SL_STREAK_SEC=_parse_int(os.getenv("SYMBOL_PAUSE_AFTER_SL_STREAK_SEC"), 0),
    )

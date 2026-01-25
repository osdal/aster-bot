import os
import re
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

def _to_bool(v: str, default: bool = False) -> bool:
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("1","true","yes","y","on"):
        return True
    if s in ("0","false","no","n","off"):
        return False
    return default

def _to_int(v: str, default: int) -> int:
    if v is None:
        return default
    s = str(v).strip()
    if s == "":
        return default
    # accept "20A" / "20А" etc -> take first signed int
    m = re.search(r"[-+]?\d+", s)
    return int(m.group(0)) if m else default

def _to_float(v: str, default: float) -> float:
    if v is None:
        return default
    s = str(v).strip().replace(",", ".")
    if s == "":
        return default
    # take first signed float
    m = re.search(r"[-+]?\d+(?:\.\d+)?", s)
    return float(m.group(0)) if m else default

def _csv_upper(v: str) -> list[str]:
    if not v:
        return []
    return [x.strip().upper() for x in str(v).split(",") if x.strip()]

@dataclass
class Config:
    # endpoints
    REST_BASE: str
    WS_BASE: str

    # universe selection
    QUOTE: str
    SYMBOL_MODE: str  # HYBRID, HYBRID_PRIORITY, WHITELIST_ONLY
    WHITELIST: list[str]
    BLACKLIST: list[str]
    ACTIVE_SYMBOLS: list[str]  # if provided -> force these symbols
    WHITELIST_PRIORITY: bool
    WHITELIST_BYPASS_LIQUIDITY: bool

    AUTO_TOP_N: int
    TARGET_SYMBOLS: int
    REFRESH_UNIVERSE_SEC: int

    MIN_24H_QUOTE_VOL: float
    MAX_SPREAD_PCT: float
    MIN_ATR_PCT: float

    TF_SEC: int
    LOOKBACK_MINUTES: int
    ATR_PERIOD: int

    # paper trading params
    TRADE_NOTIONAL_USD: float
    TP_PCT: float
    SL_PCT: float
    MAX_HOLDING_SEC: int
    COOLDOWN_AFTER_TRADE_SEC: int
    MAX_TRADES_PER_HOUR: int  # 0 => unlimited

    # (legacy pause params – keep for compatibility, but set 0 to disable)
    MAX_CONSECUTIVE_LOSSES: int
    PAUSE_AFTER_CONSECUTIVE_LOSSES_SEC: int
    SYMBOL_MAX_SL_STREAK: int
    SYMBOL_PAUSE_AFTER_SL_STREAK_SEC: int

    # breakout strategy params used by run_paper.py
    IMPULSE_LOOKBACK_SEC: int
    BREAKOUT_BUFFER_PCT: float

def load_config() -> Config:
    rest_base = os.getenv("ASTER_REST_BASE") or os.getenv("REST_BASE") or "https://fapi.asterdex.com"
    ws_base = os.getenv("ASTER_WS_BASE") or os.getenv("WS_BASE") or "wss://fstream.asterdex.com/stream?streams="

    cfg = Config(
        REST_BASE=rest_base.rstrip("/"),
        WS_BASE=ws_base,

        QUOTE=os.getenv("QUOTE", "USDT").strip().upper(),
        SYMBOL_MODE=os.getenv("SYMBOL_MODE", "HYBRID_PRIORITY").strip().upper(),
        WHITELIST=_csv_upper(os.getenv("WHITELIST", "")),
        BLACKLIST=_csv_upper(os.getenv("BLACKLIST", "")),
        ACTIVE_SYMBOLS=_csv_upper(os.getenv("ACTIVE_SYMBOLS", "")),
        WHITELIST_PRIORITY=_to_bool(os.getenv("WHITELIST_PRIORITY", "true"), True),
        WHITELIST_BYPASS_LIQUIDITY=_to_bool(os.getenv("WHITELIST_BYPASS_LIQUIDITY", "false"), False),

        AUTO_TOP_N=_to_int(os.getenv("AUTO_TOP_N", "40"), 40),
        TARGET_SYMBOLS=_to_int(os.getenv("TARGET_SYMBOLS", "20"), 20),
        REFRESH_UNIVERSE_SEC=_to_int(os.getenv("REFRESH_UNIVERSE_SEC", "900"), 900),

        MIN_24H_QUOTE_VOL=_to_float(os.getenv("MIN_24H_QUOTE_VOL", "3000000"), 3_000_000.0),
        MAX_SPREAD_PCT=_to_float(os.getenv("MAX_SPREAD_PCT", "0.10"), 0.10),
        MIN_ATR_PCT=_to_float(os.getenv("MIN_ATR_PCT", "0.03"), 0.03),

        TF_SEC=_to_int(os.getenv("TF_SEC", "60"), 60),
        LOOKBACK_MINUTES=_to_int(os.getenv("LOOKBACK_MINUTES", "20"), 20),
        ATR_PERIOD=_to_int(os.getenv("ATR_PERIOD", "14"), 14),

        TRADE_NOTIONAL_USD=_to_float(os.getenv("TRADE_NOTIONAL_USD", "50"), 50.0),
        TP_PCT=_to_float(os.getenv("TP_PCT", "0.6"), 0.6),
        SL_PCT=_to_float(os.getenv("SL_PCT", "0.2"), 0.2),
        MAX_HOLDING_SEC=_to_int(os.getenv("MAX_HOLDING_SEC", "600"), 600),
        COOLDOWN_AFTER_TRADE_SEC=_to_int(os.getenv("COOLDOWN_AFTER_TRADE_SEC", "0"), 0),
        MAX_TRADES_PER_HOUR=_to_int(os.getenv("MAX_TRADES_PER_HOUR", "0"), 0),

        MAX_CONSECUTIVE_LOSSES=_to_int(os.getenv("MAX_CONSECUTIVE_LOSSES", "0"), 0),
        PAUSE_AFTER_CONSECUTIVE_LOSSES_SEC=_to_int(os.getenv("PAUSE_AFTER_CONSECUTIVE_LOSSES_SEC", "0"), 0),
        SYMBOL_MAX_SL_STREAK=_to_int(os.getenv("SYMBOL_MAX_SL_STREAK", "0"), 0),
        SYMBOL_PAUSE_AFTER_SL_STREAK_SEC=_to_int(os.getenv("SYMBOL_PAUSE_AFTER_SL_STREAK_SEC", "0"), 0),

        IMPULSE_LOOKBACK_SEC=_to_int(os.getenv("IMPULSE_LOOKBACK_SEC", "10"), 10),
        BREAKOUT_BUFFER_PCT=_to_float(os.getenv("BREAKOUT_BUFFER_PCT", "0.10"), 0.10),
    )
    return cfg

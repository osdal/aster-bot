import os
from dataclasses import dataclass
from typing import List

def _b(v: str, default: bool=False) -> bool:
    if v is None:
        return default
    return v.strip().lower() in ("1","true","yes","y","on")

def _i(v: str, default: int) -> int:
    try:
        return int(str(v).strip())
    except Exception:
        return default

def _f(v: str, default: float) -> float:
    try:
        return float(str(v).strip())
    except Exception:
        return default

def _csv(v: str) -> List[str]:
    if not v:
        return []
    return [s.strip().upper() for s in v.split(",") if s.strip()]

@dataclass
class Config:
    # endpoints
    REST_BASE: str
    WS_BASE: str
    QUOTE: str = "USDT"

    # universe / symbols
    SYMBOL_MODE: str = "HYBRID_PRIORITY"  # WHITELIST_ONLY | HYBRID_PRIORITY | AUTO_ONLY
    ACTIVE_SYMBOLS: List[str] = None      # if set -> bypass filters and use exactly these
    WHITELIST: List[str] = None
    BLACKLIST: List[str] = None
    LIVE_ALLOW_SYMBOLS: List[str] = None

    AUTO_TOP_N: int = 40
    TARGET_SYMBOLS: int = 20
    REFRESH_UNIVERSE_SEC: int = 900

    MIN_24H_QUOTE_VOL: float = 3_000_000.0
    MAX_SPREAD_PCT: float = 0.10
    MIN_ATR_PCT: float = 0.03

    TF_SEC: int = 60
    LOOKBACK_MINUTES: int = 20
    ATR_PERIOD: int = 14

    WHITELIST_BYPASS_LIQUIDITY: bool = False
    WHITELIST_PRIORITY: bool = True

    # paper trading params
    TRADE_NOTIONAL_USD: float = 50.0
    TP_PCT: float = 0.6
    SL_PCT: float = 0.2
    MAX_HOLDING_SEC: int = 600
    BREAKOUT_BUFFER_PCT: float = 0.10
    IMPULSE_LOOKBACK_SEC: int = 10

    COOLDOWN_AFTER_TRADE_SEC: int = 0
    MAX_TRADES_PER_HOUR: int = 0  # 0 => unlimited (disabled)

    # risk pauses (0 => disabled)
    MAX_CONSECUTIVE_LOSSES: int = 0
    PAUSE_AFTER_CONSECUTIVE_LOSSES_SEC: int = 0
    SYMBOL_MAX_SL_STREAK: int = 0
    SYMBOL_PAUSE_AFTER_SL_STREAK_SEC: int = 0

def load_config() -> Config:
    rest = os.getenv("ASTER_REST_BASE", "https://fapi.asterdex.com").rstrip("/")
    ws = os.getenv("ASTER_WS_BASE", "wss://fstream.asterdex.com").rstrip("/")

    cfg = Config(
        REST_BASE=rest,
        WS_BASE=ws,
        QUOTE=os.getenv("QUOTE", "USDT").strip().upper(),

        SYMBOL_MODE=os.getenv("SYMBOL_MODE", "HYBRID_PRIORITY").strip().upper(),
        ACTIVE_SYMBOLS=_csv(os.getenv("ACTIVE_SYMBOLS", "")),
        WHITELIST=_csv(os.getenv("WHITELIST", "")),
        BLACKLIST=_csv(os.getenv("BLACKLIST", "")),
        LIVE_ALLOW_SYMBOLS=_csv(os.getenv("LIVE_ALLOW_SYMBOLS", "")),

        AUTO_TOP_N=_i(os.getenv("AUTO_TOP_N", "40"), 40),
        TARGET_SYMBOLS=_i(os.getenv("TARGET_SYMBOLS", "20"), 20),
        REFRESH_UNIVERSE_SEC=_i(os.getenv("REFRESH_UNIVERSE_SEC", "900"), 900),

        MIN_24H_QUOTE_VOL=_f(os.getenv("MIN_24H_QUOTE_VOL", "3000000"), 3_000_000.0),
        MAX_SPREAD_PCT=_f(os.getenv("MAX_SPREAD_PCT", "0.1"), 0.10),
        MIN_ATR_PCT=_f(os.getenv("MIN_ATR_PCT", "0.03"), 0.03),

        TF_SEC=_i(os.getenv("TF_SEC", "60"), 60),
        LOOKBACK_MINUTES=_i(os.getenv("LOOKBACK_MINUTES", "20"), 20),
        ATR_PERIOD=_i(os.getenv("ATR_PERIOD", "14"), 14),

        WHITELIST_BYPASS_LIQUIDITY=_b(os.getenv("WHITELIST_BYPASS_LIQUIDITY", "false")),
        WHITELIST_PRIORITY=_b(os.getenv("WHITELIST_PRIORITY", "true"), True),

        TRADE_NOTIONAL_USD=_f(os.getenv("TRADE_NOTIONAL_USD", "50"), 50.0),
        TP_PCT=_f(os.getenv("TP_PCT", "0.6"), 0.6),
        SL_PCT=_f(os.getenv("SL_PCT", "0.2"), 0.2),
        MAX_HOLDING_SEC=_i(os.getenv("MAX_HOLDING_SEC", "600"), 600),
        BREAKOUT_BUFFER_PCT=_f(os.getenv("BREAKOUT_BUFFER_PCT", "0.1"), 0.10),
        IMPULSE_LOOKBACK_SEC=_i(os.getenv("IMPULSE_LOOKBACK_SEC", "10"), 10),

        COOLDOWN_AFTER_TRADE_SEC=_i(os.getenv("COOLDOWN_AFTER_TRADE_SEC", "0"), 0),
        MAX_TRADES_PER_HOUR=_i(os.getenv("MAX_TRADES_PER_HOUR", "0"), 0),

        MAX_CONSECUTIVE_LOSSES=_i(os.getenv("MAX_CONSECUTIVE_LOSSES", "0"), 0),
        PAUSE_AFTER_CONSECUTIVE_LOSSES_SEC=_i(os.getenv("PAUSE_AFTER_CONSECUTIVE_LOSSES_SEC", "0"), 0),
        SYMBOL_MAX_SL_STREAK=_i(os.getenv("SYMBOL_MAX_SL_STREAK", "0"), 0),
        SYMBOL_PAUSE_AFTER_SL_STREAK_SEC=_i(os.getenv("SYMBOL_PAUSE_AFTER_SL_STREAK_SEC", "0"), 0),
    )

    # Normalize empties to []
    cfg.ACTIVE_SYMBOLS = cfg.ACTIVE_SYMBOLS or []
    cfg.WHITELIST = cfg.WHITELIST or []
    cfg.BLACKLIST = cfg.BLACKLIST or []
    cfg.LIVE_ALLOW_SYMBOLS = cfg.LIVE_ALLOW_SYMBOLS or []
    return cfg

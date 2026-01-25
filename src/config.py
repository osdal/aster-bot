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

    # optional throttle (PaperEngine reads via getattr)
    MAX_TRADES_PER_HOUR: int


def _parse_bool(v: str, default: bool = False) -> bool:
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _parse_list_set(v: str) -> set[str]:
    if not v:
        return set()
    parts = [p.strip().upper() for p in v.split(",")]
    return {p for p in parts if p}


def load_config() -> Config:
    load_dotenv()

    return Config(
        REST_BASE=os.getenv("ASTER_REST_BASE", "https://fapi.asterdex.com").rstrip("/"),
        WS_BASE=os.getenv("ASTER_WS_BASE", "wss://fstream.asterdex.com").rstrip("/"),

        # Yes: SYMBOL_MODE=HYBRID_PRIORITY will work (it’s just a string selector for universe.py)
        SYMBOL_MODE=os.getenv("SYMBOL_MODE", "HYBRID_PRIORITY").strip().upper(),

        WHITELIST=_parse_list_set(os.getenv("WHITELIST", "")),
        BLACKLIST=_parse_list_set(os.getenv("BLACKLIST", "")),
        QUOTE=os.getenv("QUOTE", "USDT").strip().upper(),

        AUTO_TOP_N=int(os.getenv("AUTO_TOP_N", "30")),
        TARGET_SYMBOLS=int(os.getenv("TARGET_SYMBOLS", "15")),
        REFRESH_UNIVERSE_SEC=int(os.getenv("REFRESH_UNIVERSE_SEC", "900")),

        MIN_24H_QUOTE_VOL=float(os.getenv("MIN_24H_QUOTE_VOL", "5000000")),
        MAX_SPREAD_PCT=float(os.getenv("MAX_SPREAD_PCT", "0.08")),
        MIN_ATR_PCT=float(os.getenv("MIN_ATR_PCT", "0.025")),

        TF_SEC=int(os.getenv("TF_SEC", "60")),
        LOOKBACK_MINUTES=int(os.getenv("LOOKBACK_MINUTES", "20")),
        ATR_PERIOD=int(os.getenv("ATR_PERIOD", "14")),

        WHITELIST_BYPASS_LIQUIDITY=_parse_bool(os.getenv("WHITELIST_BYPASS_LIQUIDITY", "false")),
        WHITELIST_PRIORITY=_parse_bool(os.getenv("WHITELIST_PRIORITY", "true")),

        TRADE_NOTIONAL_USD=float(os.getenv("TRADE_NOTIONAL_USD", "75")),
        BREAKOUT_BUFFER_PCT=float(os.getenv("BREAKOUT_BUFFER_PCT", "0.05")),

        # Your current target: TP=0.6%, SL=0.2%
        TP_PCT=float(os.getenv("TP_PCT", "0.6")),
        SL_PCT=float(os.getenv("SL_PCT", "0.2")),

        MAX_HOLDING_SEC=int(os.getenv("MAX_HOLDING_SEC", "600")),
        COOLDOWN_AFTER_TRADE_SEC=int(os.getenv("COOLDOWN_AFTER_TRADE_SEC", "0")),

        # set very high to effectively disable throttling
        MAX_TRADES_PER_HOUR=int(os.getenv("MAX_TRADES_PER_HOUR", "999999")),
    )

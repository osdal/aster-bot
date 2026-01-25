import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass
class Config:
    # Endpoints
    REST_BASE: str
    WS_BASE: str

    # Universe selection
    SYMBOL_MODE: str              # e.g. "HYBRID_PRIORITY", "HYBRID", "TOP_VOLUME", "LIST"
    TARGET_SYMBOLS: int           # how many symbols to subscribe/trade in paper
    SYMBOLS_LIST: str             # comma-separated, used when SYMBOL_MODE="LIST"

    MIN_ATR_PCT: float            # e.g. 0.20  (percent)
    MAX_SPREAD_PCT: float         # e.g. 0.15  (percent)
    MIN_24H_QUOTE_VOL: float      # in USDT

    # Paper trading params
    TRADE_NOTIONAL_USD: float     # paper notional per position
    TP_PCT: float                 # take profit in percent, e.g. 0.60
    SL_PCT: float                 # stop loss in percent, e.g. 0.20
    MAX_HOLDING_SEC: int          # timeout in seconds
    COOLDOWN_AFTER_TRADE_SEC: int # per-symbol cooldown (paper)
    MAX_TRADES_PER_HOUR: int      # paper global rate limit

    # Breakout/impulse logic (run_paper.py uses these)
    IMPULSE_LOOKBACK_SEC: int
    BREAKOUT_BUFFER_PCT: float

    # (legacy risk controls; may be ignored by NO_PAUSES paper_engine)
    MAX_CONSECUTIVE_LOSSES: int
    PAUSE_AFTER_CONSECUTIVE_LOSSES_SEC: int
    SYMBOL_MAX_SL_STREAK: int
    SYMBOL_PAUSE_AFTER_SL_STREAK_SEC: int


def _env_float(name: str, default: str) -> float:
    v = os.getenv(name, default).strip()
    return float(v.replace(",", "."))


def _env_int(name: str, default: str) -> int:
    v = os.getenv(name, default).strip()
    # protect from accidental Cyrillic 'А' etc.
    v = "".join(ch for ch in v if ch.isdigit() or ch in "+-")
    return int(v) if v else int(default)


def load_config() -> Config:
    load_dotenv()

    rest_base = os.getenv("ASTER_REST_BASE", "https://fapi.asterdex.com").strip().rstrip("/")
    ws_base = os.getenv("ASTER_WS_BASE", "wss://fstream.asterdex.com").strip().rstrip("/")

    return Config(
        REST_BASE=rest_base,
        WS_BASE=ws_base,

        SYMBOL_MODE=os.getenv("SYMBOL_MODE", "HYBRID_PRIORITY").strip().upper(),
        TARGET_SYMBOLS=_env_int("TARGET_SYMBOLS", "15"),
        SYMBOLS_LIST=os.getenv("SYMBOLS_LIST", "").strip(),

        MIN_ATR_PCT=_env_float("MIN_ATR_PCT", "0.20"),
        MAX_SPREAD_PCT=_env_float("MAX_SPREAD_PCT", "0.15"),
        MIN_24H_QUOTE_VOL=_env_float("MIN_24H_QUOTE_VOL", "2000000"),

        TRADE_NOTIONAL_USD=_env_float("TRADE_NOTIONAL_USD", "75"),
        TP_PCT=_env_float("TP_PCT", "0.60"),
        SL_PCT=_env_float("SL_PCT", "0.20"),
        MAX_HOLDING_SEC=_env_int("MAX_HOLDING_SEC", "420"),
        COOLDOWN_AFTER_TRADE_SEC=_env_int("COOLDOWN_AFTER_TRADE_SEC", "0"),
        MAX_TRADES_PER_HOUR=_env_int("MAX_TRADES_PER_HOUR", "999999"),

        IMPULSE_LOOKBACK_SEC=_env_int("IMPULSE_LOOKBACK_SEC", "10"),
        BREAKOUT_BUFFER_PCT=_env_float("BREAKOUT_BUFFER_PCT", "0.10"),

        # defaults = disabled (your new logic uses per-symbol streaks, not global pauses)
        MAX_CONSECUTIVE_LOSSES=_env_int("MAX_CONSECUTIVE_LOSSES", "999999"),
        PAUSE_AFTER_CONSECUTIVE_LOSSES_SEC=_env_int("PAUSE_AFTER_CONSECUTIVE_LOSSES_SEC", "0"),
        SYMBOL_MAX_SL_STREAK=_env_int("SYMBOL_MAX_SL_STREAK", "999999"),
        SYMBOL_PAUSE_AFTER_SL_STREAK_SEC=_env_int("SYMBOL_PAUSE_AFTER_SL_STREAK_SEC", "0"),
    )

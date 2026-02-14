import os
import time
import math
import csv
import requests
import numpy as np
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

BASE = os.getenv("ASTER_REST_BASE")
SYMBOL = os.getenv("LIVE_SYMBOL", "XRPUSDT")
NOTIONAL = float(os.getenv("LIVE_NOTIONAL_USD", "5"))

TP_PCT = float(os.getenv("TP_PCT", "1.0")) / 100
SL_PCT = float(os.getenv("SL_PCT", "0.8")) / 100

TIMEFRAME = os.getenv("TIMEFRAME", "1m")

# ===== FILTER SWITCHES =====
ENABLE_ATR_FILTER = os.getenv("ENABLE_ATR_FILTER", "1") == "1"
ENABLE_SLOPE_FILTER = os.getenv("ENABLE_SLOPE_FILTER", "1") == "1"
ENABLE_COOLDOWN = os.getenv("ENABLE_COOLDOWN", "1") == "1"

# ===== EQUITY =====
equity = float(os.getenv("START_EQUITY", "1000"))

# ===== COOLDOWN =====
COOLDOWN_SEC = int(os.getenv("COOLDOWN_SEC", "30"))
last_trade_time = 0

# ===== ATR =====
ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))
ATR_THRESHOLD = float(os.getenv("ATR_THRESHOLD", "0.0015"))
KLINES_LIMIT = int(os.getenv("KLINES_LIMIT", "100"))

# ===== EMA =====
EMA_FAST = int(os.getenv("EMA_FAST", "9"))
EMA_SLOW = int(os.getenv("EMA_SLOW", "21"))

# ===== SLOPE =====
SLOPE_LOOKBACK = int(os.getenv("SLOPE_LOOKBACK", "3"))
SLOPE_THRESHOLD = float(os.getenv("SLOPE_THRESHOLD", "0.0003"))

# ===== FILE =====
DATA_DIR = "data"
CSV_FILE = os.path.join(DATA_DIR, "trades.csv")
os.makedirs(DATA_DIR, exist_ok=True)

# ========= HELPERS =========
def public_get(path):
    return requests.get(BASE + path).json()

def get_price():
    ticker = public_get("/fapi/v1/ticker/price")
    return float([x for x in ticker if x["symbol"] == SYMBOL][0]["price"])

def get_klines():
    return public_get(
        f"/fapi/v1/klines?symbol={SYMBOL}&interval={TIMEFRAME}&limit={KLINES_LIMIT}"
    )

# ========= EMA =========
def ema(values, period):
    weights = np.exp(np.linspace(-1., 0., period))
    weights /= weights.sum()
    a = np.convolve(values, weights, mode='full')[:len(values)]
    a[:period] = a[period]
    return a

def ema_slope(series):
    return series[-1] - series[-SLOPE_LOOKBACK]

def get_trend_and_slope():
    kl = get_klines()
    closes = np.array([float(x[4]) for x in kl])

    fast = ema(closes, EMA_FAST)
    slow = ema(closes, EMA_SLOW)

    slope = ema_slope(fast)
    trend = "UPTREND" if fast[-1] > slow[-1] else "DOWNTREND"

    return trend, slope

# ========= ATR =========
def get_atr():
    kl = get_klines()

    highs = np.array([float(x[2]) for x in kl])
    lows = np.array([float(x[3]) for x in kl])
    closes = np.array([float(x[4]) for x in kl])

    trs = []
    for i in range(1, len(highs)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1]),
        )
        trs.append(tr)

    return np.mean(trs[-ATR_PERIOD:])

# ========= CSV =========
if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp","symbol","side","entry","exit",
            "result","pnl","equity"
        ])

def record_trade(side,entry,exit_price,result,pnl,equity):
    with open(CSV_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.now(),
            SYMBOL,
            side,
            entry,
            exit_price,
            result,
            pnl,
            equity
        ])

# ========= MAIN =========
print("=== VIRTUAL BOT STARTED ===")
print(f"START EQUITY: {equity}")

while True:

    if ENABLE_COOLDOWN:
        elapsed = time.time() - last_trade_time
        if elapsed < COOLDOWN_SEC:
            print(f"COOLDOWN {int(COOLDOWN_SEC-elapsed)}s")
            time.sleep(1)
            continue

    price = get_price()

    if ENABLE_ATR_FILTER:
        atr = get_atr()
        if atr / price < ATR_THRESHOLD:
            time.sleep(2)
            continue

    trend, slope = get_trend_and_slope()

    if ENABLE_SLOPE_FILTER:
        if abs(slope)/price < SLOPE_THRESHOLD:
            time.sleep(2)
            continue

    side = "BUY" if trend == "UPTREND" else "SELL"
    qty = math.floor((NOTIONAL / price) * 10) / 10

    tp = price*(1+TP_PCT) if side=="BUY" else price*(1-TP_PCT)
    sl = price*(1-SL_PCT) if side=="BUY" else price*(1+SL_PCT)

    entry = price
    print(f"OPEN {side} @ {entry}")

    while True:
        current = get_price()

        hit=False
        if side=="BUY":
            hit = current>=tp or current<=sl
        else:
            hit = current<=tp or current>=sl

        if hit:
            exit_price=current
            change=(exit_price-entry)/entry
            pnl=change*NOTIONAL
            if side=="SELL":
                pnl*=-1

            equity+=pnl
            result="TP" if (
                (side=="BUY" and exit_price>=tp) or
                (side=="SELL" and exit_price<=tp)
            ) else "SL"

            print(f"{result} | PnL={pnl:.4f} | EQUITY={equity:.2f}")

            record_trade(side,entry,exit_price,result,pnl,equity)
            last_trade_time=time.time()
            break

        time.sleep(1)

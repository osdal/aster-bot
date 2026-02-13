import os
import time
import math
import csv
import requests
import numpy as np
from datetime import datetime
from dotenv import load_dotenv

# ========= LOAD ENV =========
load_dotenv()

BASE = os.getenv("ASTER_REST_BASE")
SYMBOL = os.getenv("LIVE_SYMBOL", "XRPUSDT")
NOTIONAL = float(os.getenv("LIVE_NOTIONAL_USD", "5"))

TP_PCT = float(os.getenv("TP_PCT", "1.0")) / 100
SL_PCT = float(os.getenv("SL_PCT", "0.8")) / 100

# ===== ATR FILTER SETTINGS =====
ATR_PERIOD = 14
ATR_THRESHOLD = 0.0015
KLINES_LIMIT = 100

# ===== EMA SETTINGS =====
EMA_FAST = 9
EMA_SLOW = 21

# ===== FILE PATH =====
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
        f"/fapi/v1/klines?symbol={SYMBOL}&interval=1m&limit={KLINES_LIMIT}"
    )

# ========= EMA =========
def ema(values, period):
    weights = np.exp(np.linspace(-1., 0., period))
    weights /= weights.sum()
    a = np.convolve(values, weights, mode='full')[:len(values)]
    a[:period] = a[period]
    return a

# ========= TREND =========
def get_trend():
    kl = get_klines()
    closes = np.array([float(x[4]) for x in kl])

    fast = ema(closes, EMA_FAST)
    slow = ema(closes, EMA_SLOW)

    return "UPTREND" if fast[-1] > slow[-1] else "DOWNTREND"

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
            "timestamp","symbol","side","trend",
            "entry","tp","sl","result","exit"
        ])

def record_trade(entry,tp,sl,side,trend,result,exit_price):
    with open(CSV_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.now(),
            SYMBOL,
            side,
            trend,
            entry,
            tp,
            sl,
            result,
            exit_price
        ])

# ========= MAIN LOOP =========
print("=== VIRTUAL BOT STARTED ===")

while True:

    price = get_price()

    # ----- ATR FILTER -----
    atr = get_atr()
    volatility = atr / price

    print(f"ATR={atr:.6f}  VOL={volatility:.6f}")

    if volatility < ATR_THRESHOLD:
        print("SKIP — low volatility\n")
        time.sleep(2)
        continue

    # ----- TREND -----
    trend = get_trend()
    side = "BUY" if trend == "UPTREND" else "SELL"

    qty = math.floor((NOTIONAL / price) * 10) / 10

    if side == "BUY":
        tp = price * (1 + TP_PCT)
        sl = price * (1 - SL_PCT)
    else:
        tp = price * (1 - TP_PCT)
        sl = price * (1 + SL_PCT)

    print(f"\nOPEN {side} {qty} @ {price}")
    print(f"TP={tp} SL={sl} TREND={trend}")

    # ----- MONITOR -----
    while True:
        current = get_price()

        if side == "BUY":
            if current >= tp:
                print("TP HIT\n")
                record_trade(price,tp,sl,side,trend,"TP",current)
                break
            if current <= sl:
                print("SL HIT\n")
                record_trade(price,tp,sl,side,trend,"SL",current)
                break

        else:
            if current <= tp:
                print("TP HIT\n")
                record_trade(price,tp,sl,side,trend,"TP",current)
                break
            if current >= sl:
                print("SL HIT\n")
                record_trade(price,tp,sl,side,trend,"SL",current)
                break

        time.sleep(1)

import os
import time
import math
import csv
from datetime import datetime
from dotenv import load_dotenv
import requests

# ========= LOAD ENV =========
load_dotenv()

BASE = os.getenv("ASTER_REST_BASE")
SYMBOL = os.getenv("LIVE_SYMBOL", "XRPUSDT")
LEVERAGE = int(os.getenv("LIVE_LEVERAGE", "2"))
NOTIONAL = float(os.getenv("LIVE_NOTIONAL_USD", "5"))
TP_PCT = float(os.getenv("TP_PCT", "0.8")) / 100
SL_PCT = float(os.getenv("SL_PCT", "0.2")) / 100

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
TRADES_FILE = os.path.join(DATA_DIR, "trades.csv")

# ========= INIT CSV FILE =========
if not os.path.exists(TRADES_FILE):
    with open(TRADES_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp","symbol","side","entry_price","tp_price","sl_price","result","exit_price"])

# ========= HELPERS =========
def public_get(path):
    return requests.get(BASE + path).json()

def get_price(symbol):
    ticker = public_get("/fapi/v1/ticker/price")
    return float([x for x in ticker if x["symbol"]==symbol][0]["price"])

def get_symbol_info(symbol):
    info = public_get("/fapi/v1/exchangeInfo")
    return [s for s in info["symbols"] if s["symbol"]==symbol][0]

def record_trade(symbol, side, entry, tp, sl, result, exit_price):
    with open(TRADES_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.now().isoformat(),
            symbol,
            side,
            entry,
            tp,
            sl,
            result,
            exit_price
        ])

# ========= SIMULATION LOOP =========
symbol_info = get_symbol_info(SYMBOL)
step_size = float([f for f in symbol_info["filters"] if f["filterType"]=="LOT_SIZE"][0]["stepSize"])
decimals = abs(int(round(math.log10(step_size))))

print("Starting virtual trading loop for", SYMBOL)
while True:
    price = get_price(SYMBOL)
    usd_position = NOTIONAL * LEVERAGE
    qty = math.floor(usd_position / price * (10**decimals)) / (10**decimals)

    entry_price = price
    tp_price = round(entry_price * (1 + TP_PCT), 4)
    sl_price = max(round(entry_price * (1 - SL_PCT), 4), 0.0001)

    print(f"\nOPEN VIRTUAL POSITION: {qty} {SYMBOL} at {entry_price}")
    print(f"TP: {tp_price}, SL: {sl_price}")

    # Мониторим цену
    position_open = True
    while position_open:
        time.sleep(2)
        current_price = get_price(SYMBOL)

        if current_price >= tp_price:
            print(f"TP HIT at {current_price}")
            record_trade(SYMBOL, "BUY", entry_price, tp_price, sl_price, "TP", current_price)
            position_open = False
        elif current_price <= sl_price:
            print(f"SL HIT at {current_price}")
            record_trade(SYMBOL, "BUY", entry_price, tp_price, sl_price, "SL", current_price)
            position_open = False

    print("POSITION CLOSED. Restarting loop...")

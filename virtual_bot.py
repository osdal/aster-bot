import os
import time
import math
import csv
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

# ========= LOAD ENV =========
load_dotenv()
BASE = os.getenv("ASTER_REST_BASE", "https://fapi.asterdex.com")
SYMBOL = os.getenv("LIVE_SYMBOL", "XRPUSDT")
LEVERAGE = float(os.getenv("LIVE_LEVERAGE", "2"))
NOTIONAL = float(os.getenv("LIVE_NOTIONAL_USD", "2.8"))
TP_PCT = float(os.getenv("TP_PCT", "1.0")) / 100
SL_PCT = float(os.getenv("SL_PCT", "0.8")) / 100

os.makedirs("data", exist_ok=True)
CSV_FILE = "data/trades.csv"

# создаем CSV с заголовком если нет
if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp","symbol","side","trend","entry_price","tp_price","sl_price","result","exit_price"
        ])

# ========= HELPERS =========
def public_get(path, params={}):
    url = BASE + path
    return requests.get(url, params=params).json()

def get_latest_prices(symbol, limit=100, interval="1m"):
    """
    Получение исторических свечей (закрытие)
    interval: '1m', '3m', '5m', etc.
    """
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    data = public_get("/fapi/v1/klines", params)
    closes = [float(x[4]) for x in data]  # закрытие свечи
    return closes

def compute_trend(prices, fast=9, slow=50):
    """
    Возвращает направление тренда по EMA
    """
    df = pd.DataFrame(prices, columns=["close"])
    df["ema_fast"] = df["close"].ewm(span=fast, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=slow, adjust=False).mean()
    if df["ema_fast"].iloc[-1] > df["ema_slow"].iloc[-1]:
        return "UPTREND"
    else:
        return "DOWNTREND"

def get_current_price(symbol):
    ticker = public_get("/fapi/v1/ticker/price")
    for x in ticker:
        if x["symbol"] == symbol:
            return float(x["price"])
    return None

def record_trade(entry_price, tp_price, sl_price, side, trend, result, exit_price):
    with open(CSV_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.now(), SYMBOL, side, trend, entry_price, tp_price, sl_price, result, exit_price
        ])

# ========= VIRTUAL BOT LOOP =========
print("=== Starting Virtual Bot ===")
while True:
    try:
        # 1️⃣ получаем последние свечи
        prices = get_latest_prices(SYMBOL, limit=100, interval="1m")
        if len(prices) < 50:
            print("Недостаточно данных для EMA, ждем следующую свечу...")
            time.sleep(10)
            continue

        # 2️⃣ определяем тренд
        trend = compute_trend(prices)
        side = "BUY" if trend == "UPTREND" else "SELL"

        # 3️⃣ текущая цена и расчет qty
        price = get_current_price(SYMBOL)
        usd_position = NOTIONAL * LEVERAGE
        qty = usd_position / price
        qty = round(qty, 4)  # округление виртуальное

        # 4️⃣ TP / SL
        if side == "BUY":
            tp_price = price * (1 + TP_PCT)
            sl_price = price * (1 - SL_PCT)
        else:  # SELL
            tp_price = price * (1 - TP_PCT)
            sl_price = price * (1 + SL_PCT)

        tp_price = round(tp_price, 4)
        sl_price = round(sl_price, 4)

        print(f"\nNew virtual position: {side} {qty} {SYMBOL} @ {price}")
        print(f"TP: {tp_price}, SL: {sl_price}, Trend: {trend}")

        # 5️⃣ мониторим цену
        position_closed = False
        while not position_closed:
            current_price = get_current_price(SYMBOL)
            if current_price is None:
                time.sleep(1)
                continue

            if side == "BUY":
                if current_price >= tp_price:
                    result = "TP"
                    position_closed = True
                elif current_price <= sl_price:
                    result = "SL"
                    position_closed = True
            else:  # SELL
                if current_price <= tp_price:
                    result = "TP"
                    position_closed = True
                elif current_price >= sl_price:
                    result = "SL"
                    position_closed = True

            if position_closed:
                print(f"Position closed: {result} at {current_price}")
                record_trade(price, tp_price, sl_price, side, trend, result, current_price)
            else:
                time.sleep(2)

        # небольшая пауза перед следующей виртуальной позицией
        time.sleep(2)

    except Exception as e:
        print("Error:", e)
        time.sleep(5)

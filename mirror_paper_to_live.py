import os
import sys
import time
import math
import hmac
import hashlib
import requests
from urllib.parse import urlencode
from dotenv import load_dotenv

# ========= LOAD ENV =========
load_dotenv()

BASE = os.getenv("ASTER_REST_BASE")
KEY = os.getenv("ASTER_API_KEY")
SECRET = os.getenv("ASTER_API_SECRET").encode()

SYMBOL = os.getenv("LIVE_SYMBOL", "XRPUSDT")
LEVERAGE = int(os.getenv("LIVE_LEVERAGE", "2"))
NOTIONAL = float(os.getenv("LIVE_NOTIONAL_USD", "5"))

TP_PCT = float(os.getenv("TP_PCT", "0.8")) / 100
SL_PCT = float(os.getenv("SL_PCT", "0.2")) / 100

# ========= DEBUG HEADER =========
print("\n========== BOT START ==========")
print("FILE:", os.path.abspath(__file__))
print("PYTHON:", sys.executable)
print("BASE:", BASE)
print("SYMBOL:", SYMBOL)
print("NOTIONAL:", NOTIONAL)
print("LEVERAGE:", LEVERAGE)
print("TP%:", TP_PCT)
print("SL%:", SL_PCT)
print("================================\n")

# ========= HELPERS =========
def sign(params):
    qs = urlencode(params)
    sig = hmac.new(SECRET, qs.encode(), hashlib.sha256).hexdigest()
    return qs + "&signature=" + sig


def private_post(path, params):
    params["timestamp"] = int(time.time() * 1000)
    url = BASE + path + "?" + sign(params)
    return requests.post(url, headers={"X-MBX-APIKEY": KEY}).json()


def private_get(path, params={}):
    params["timestamp"] = int(time.time() * 1000)
    url = BASE + path + "?" + sign(params)
    return requests.get(url, headers={"X-MBX-APIKEY": KEY}).json()


def public_get(path):
    return requests.get(BASE + path).json()


# ========= SET LEVERAGE =========
print("Setting leverage...")
print(private_post("/fapi/v1/leverage", {
    "symbol": SYMBOL,
    "leverage": LEVERAGE
}))

# ========= PRICE =========
ticker = public_get("/fapi/v1/ticker/price")
price = float([x for x in ticker if x["symbol"] == SYMBOL][0]["price"])
print("Market price:", price)

# ========= QUANTITY =========
usd_position = NOTIONAL * LEVERAGE
qty = usd_position / price

# округляем безопасно
qty = math.floor(qty * 1000) / 1000

print("Final qty:", qty)

# ========= OPEN MARKET =========
side = "BUY"

order = private_post("/fapi/v1/order", {
    "symbol": SYMBOL,
    "side": side,
    "type": "MARKET",
    "quantity": qty
})

print("Order response:", order)

# ========= TP / SL =========
tp_price = price * (1 + TP_PCT)
sl_price = price * (1 - SL_PCT)

tp_price = round(tp_price, 4)
sl_price = max(round(sl_price, 4), 0.0001)

print("TP:", tp_price)
print("SL:", sl_price)

close_side = "SELL"

tp = private_post("/fapi/v1/order", {
    "symbol": SYMBOL,
    "side": close_side,
    "type": "TAKE_PROFIT_MARKET",
    "stopPrice": tp_price,
    "closePosition": "true",
    "timeInForce": "GTE_GTC"
})

print("TP response:", tp)

sl = private_post("/fapi/v1/order", {
    "symbol": SYMBOL,
    "side": close_side,
    "type": "STOP_MARKET",
    "stopPrice": sl_price,
    "closePosition": "true",
    "timeInForce": "GTE_GTC"
})

print("SL response:", sl)

# ========= MONITOR =========
print("\nMonitoring position...\n")

while True:
    pos = private_get("/fapi/v2/positionRisk")
    p = [x for x in pos if x["symbol"] == SYMBOL][0]

    amt = float(p["positionAmt"])

    print("Position size:", amt)

    if abs(amt) < 1e-9:
        print("POSITION CLOSED")
        break

    time.sleep(2)

# mirror_paper_to_live_real.py
import os
import time
import hmac
import hashlib
import requests
from urllib.parse import urlencode
from dotenv import load_dotenv

# Загрузка ключей из .env
load_dotenv()
API_KEY = os.getenv("ASTER_API_KEY")
API_SECRET = os.getenv("ASTER_API_SECRET")

BASE_URL = "https://fapi.asterdex.com"

SYMBOL = "XRPUSDT"
POSITION_SIZE = 100  # пример: 100 XRP
LEVERAGE = 2
TP_PERCENT = 0.006  # 0.6%
SL_PERCENT = 0.002  # 0.2%
POLL_INTERVAL = 2  # сек

def sign_request(params, secret):
    query_string = urlencode(params)
    signature = hmac.new(secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
    return signature

def place_order(symbol, side, quantity, price=None, order_type="MARKET"):
    url = f"{BASE_URL}/fapi/v1/order"
    params = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "quantity": quantity,
        "timestamp": int(time.time() * 1000),
        "recvWindow": 5000,
        "leverage": LEVERAGE
    }
    if price:
        params["price"] = price
    params["signature"] = sign_request(params, API_SECRET)
    headers = {"X-MBX-APIKEY": API_KEY}
    resp = requests.post(url, params=params, headers=headers)
    try:
        data = resp.json()
    except ValueError:
        print("Response not JSON:", resp.text)
        data = {}
    print(f"Order response: {data}")
    return data

def get_open_positions():
    url = f"{BASE_URL}/fapi/v2/positionRisk"
    params = {
        "timestamp": int(time.time() * 1000),
        "recvWindow": 5000
    }
    params["signature"] = sign_request(params, API_SECRET)
    headers = {"X-MBX-APIKEY": API_KEY}
    resp = requests.get(url, params=params, headers=headers)
    try:
        data = resp.json()
    except ValueError:
        print("Response not JSON:", resp.text)
        data = []
    return data

def place_tp_sl(symbol, side, entry_price, quantity):
    if side == "BUY":
        tp_price = round(entry_price * (1 + TP_PERCENT), 5)
        sl_price = round(entry_price * (1 - SL_PERCENT), 5)
    else:
        tp_price = round(entry_price * (1 - TP_PERCENT), 5)
        sl_price = round(entry_price * (1 + SL_PERCENT), 5)
    print(f"Placing TP at {tp_price}, SL at {sl_price}")
    # Take Profit
    place_order(symbol, "SELL" if side=="BUY" else "BUY", quantity, price=tp_price, order_type="LIMIT")
    # Stop Loss
    place_order(symbol, "SELL" if side=="BUY" else "BUY", quantity, price=sl_price, order_type="STOP")

def monitor_position(symbol):
    while True:
        positions = get_open_positions()
        pos = next((p for p in positions if p["symbol"] == symbol), None)
        if not pos or float(pos.get("positionAmt", 0)) == 0:
            print(f"Position for {symbol} closed")
            break
        time.sleep(POLL_INTERVAL)

def main():
    side = "BUY"  # или "SELL"
    quantity = POSITION_SIZE
    order_resp = place_order(SYMBOL, side, quantity)
    entry_price = float(order_resp.get("price", 0)) or 0.5  # fallback
    place_tp_sl(SYMBOL, side, entry_price, quantity)
    monitor_position(SYMBOL)

if __name__ == "__main__":
    main()

# mirror_paper_to_live_rest.py
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

SYMBOL = "BTCUSDT"
POSITION_SIZE = 0.01
TP_PERCENT = 0.006  # 0.6%
SL_PERCENT = 0.002  # 0.2%
POLL_INTERVAL = 2  # сек

def sign_request(params, secret):
    query_string = urlencode(params)
    signature = hmac.new(secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
    return signature

def place_order(symbol, side, quantity, price=None, order_type="MARKET"):
    url = f"{BASE_URL}/v1/order"
    params = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "quantity": quantity,
        "timestamp": int(time.time() * 1000)
    }
    if price:
        params["price"] = price
    params["signature"] = sign_request(params, API_SECRET)
    headers = {"X-MBX-APIKEY": API_KEY}
    resp = requests.post(url, params=params, headers=headers)
    data = resp.json()
    print(f"Order response: {data}")
    return data

def get_open_positions():
    url = f"{BASE_URL}/v1/position"
    params = {
        "timestamp": int(time.time() * 1000)
    }
    params["signature"] = sign_request(params, API_SECRET)
    headers = {"X-MBX-APIKEY": API_KEY}
    resp = requests.get(url, params=params, headers=headers)
    return resp.json()

def place_tp_sl(symbol, side, entry_price, quantity):
    if side == "BUY":
        tp_price = entry_price * (1 + TP_PERCENT)
        sl_price = entry_price * (1 - SL_PERCENT)
    else:
        tp_price = entry_price * (1 - TP_PERCENT)
        sl_price = entry_price * (1 + SL_PERCENT)
    print(f"Placing TP at {tp_price:.2f}, SL at {sl_price:.2f}")
    # Take Profit
    place_order(symbol, "SELL" if side=="BUY" else "BUY", quantity, price=tp_price, order_type="LIMIT")
    # Stop Loss
    place_order(symbol, "SELL" if side=="BUY" else "BUY", quantity, price=sl_price, order_type="STOP")

def monitor_position(symbol):
    while True:
        positions = get_open_positions()
        if not positions or all(float(p["positionAmt"]) == 0 for p in positions):
            print(f"Position for {symbol} closed")
            break
        time.sleep(POLL_INTERVAL)

def main():
    side = "BUY"
    quantity = POSITION_SIZE
    order_resp = place_order(SYMBOL, side, quantity)
    entry_price = float(order_resp.get("price", 0)) or 50000
    place_tp_sl(SYMBOL, side, entry_price, quantity)
    monitor_position(SYMBOL)

if __name__ == "__main__":
    main()

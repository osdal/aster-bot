# mirror_paper_to_live_fixed.py
import os
import time
import hmac
import hashlib
import requests
from urllib.parse import urlencode
from dotenv import load_dotenv

# Загрузка переменных из .env
load_dotenv()
API_KEY = os.getenv("ASTER_API_KEY")
API_SECRET = os.getenv("ASTER_API_SECRET")
BASE_URL = os.getenv("ASTER_REST_BASE", "https://fapi.asterdex.com")

SYMBOL = "XRPUSDT"
QUOTE = os.getenv("QUOTE", "USDT")
LIVE_NOTIONAL_USD = float(os.getenv("LIVE_NOTIONAL_USD", 5))
LEVERAGE = int(os.getenv("LIVE_LEVERAGE", 2))
TP_PCT = float(os.getenv("TP_PCT", 0.8)) / 100  # 0.8% -> 0.008
SL_PCT = float(os.getenv("SL_PCT", 0.2)) / 100  # 0.2% -> 0.002
POLL_INTERVAL = int(os.getenv("WATCH_POLL_SEC", 2))

def sign_request(params, secret):
    query_string = urlencode(params)
    signature = hmac.new(secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
    return signature

def get_current_price(symbol):
    url = f"{BASE_URL}/fapi/v1/ticker/price"
    resp = requests.get(url, params={"symbol": symbol})
    data = resp.json()
    return float(data["price"])

def get_available_balance():
    url = f"{BASE_URL}/fapi/v2/balance"
    params = {"timestamp": int(time.time() * 1000), "recvWindow": 5000}
    params["signature"] = sign_request(params, API_SECRET)
    headers = {"X-MBX-APIKEY": API_KEY}
    resp = requests.get(url, params=params, headers=headers)
    data = resp.json()
    # Ищем баланс в QUOTE
    for b in data:
        if b["asset"] == QUOTE:
            return float(b["availableBalance"])
    return 0

def place_order(symbol, side, quantity, price=None, order_type="MARKET", timeInForce=None):
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
    if timeInForce:
        params["timeInForce"] = timeInForce
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
    params = {"timestamp": int(time.time() * 1000), "recvWindow": 5000}
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
        tp_price = round(entry_price * (1 + TP_PCT), 5)
        sl_price = round(entry_price * (1 - SL_PCT), 5)
    else:
        tp_price = round(entry_price * (1 - TP_PCT), 5)
        sl_price = round(entry_price * (1 + SL_PCT), 5)
    print(f"Placing TP at {tp_price}, SL at {sl_price}")
    # Take Profit (LIMIT)
    place_order(symbol, "SELL" if side=="BUY" else "BUY", quantity,
                price=tp_price, order_type="LIMIT", timeInForce="GTC")
    # Stop Loss (STOP)
    place_order(symbol, "SELL" if side=="BUY" else "BUY", quantity,
                price=sl_price, order_type="STOP", timeInForce="GTC")

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
    current_price = get_current_price(SYMBOL)
    # Вычисляем размер позиции исходя из баланса
    available_balance = get_available_balance()
    max_position_usd = min(LIVE_NOTIONAL_USD, available_balance * LEVERAGE)
    quantity = round(max_position_usd / current_price, 4)
    if quantity <= 0:
        print("Недостаточно средств для открытия позиции")
        return

    print(f"Opening {side} position for {quantity} {SYMBOL} at market price {current_price}")
    order_resp = place_order(SYMBOL, side, quantity)
    # Берём реальную цену открытия
    entry_price = float(order_resp.get("avgFillPrice") or current_price)
    place_tp_sl(SYMBOL, side, entry_price, quantity)
    monitor_position(SYMBOL)

if __name__ == "__main__":
    main()

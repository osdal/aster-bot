
# mirror_paper_to_live_FINAL.py
# Patched stop logic using reduceOnly + quantity

import time
import hmac
import hashlib
import requests
from urllib.parse import urlencode

API_KEY = "PUT_KEY"
API_SECRET = "PUT_SECRET"
BASE = "https://fapi.asterdex.com"

session = requests.Session()
session.headers.update({"X-MBX-APIKEY": API_KEY})


def sign(params):
    q = urlencode(params)
    sig = hmac.new(API_SECRET.encode(), q.encode(), hashlib.sha256).hexdigest()
    return q + "&signature=" + sig


def post(path, params):
    params["timestamp"] = int(time.time() * 1000)
    url = BASE + path + "?" + sign(params)
    r = session.post(url)
    if r.status_code != 200:
        raise RuntimeError(f"{r.status_code} {r.text}")
    return r.json()


def open_market(symbol, side, qty):
    return post("/fapi/v1/order", {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": qty
    })


def place_stop(symbol, exit_side, qty, stop_price, typ):
    return post("/fapi/v1/order", {
        "symbol": symbol,
        "side": exit_side,
        "type": typ,
        "stopPrice": stop_price,
        "quantity": qty,
        "reduceOnly": "true",
        "workingType": "MARK_PRICE"
    })


def open_with_stops(symbol, side, qty, tp, sl):
    print("OPEN", symbol, side, qty)
    open_market(symbol, side, qty)

    exit_side = "SELL" if side == "BUY" else "BUY"

    print("SL")
    place_stop(symbol, exit_side, qty, sl, "STOP_MARKET")

    print("TP")
    place_stop(symbol, exit_side, qty, tp, "TAKE_PROFIT_MARKET")


if __name__ == "__main__":
    open_with_stops("ASTERUSDT", "BUY", 10, 0.66, 0.64)

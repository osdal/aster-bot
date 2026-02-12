# ===============================
# mirror_paper_to_live.py
# Stable base version (PowerShell ENV compatible)
# ===============================

import os
import time
import hmac
import hashlib
import requests
import csv
from datetime import datetime

BASE = "https://fapi.asterdex.com"

API_KEY = os.getenv("ASTER_API_KEY")
API_SECRET = os.getenv("ASTER_API_SECRET")

if not API_KEY or not API_SECRET:
    raise RuntimeError("ASTER_API_KEY / ASTER_API_SECRET not found in ENV")

HEADERS = {"X-MBX-APIKEY": API_KEY}

TP_PCT = 0.006
SL_PCT = 0.002
LIVE_MAX_POSITIONS = 1

os.makedirs("data", exist_ok=True)
CSV_FILE = "data/live_trades.csv"

live_positions = {}

# ---------- signing ----------

def sign(params: dict):
    query = "&".join(f"{k}={params[k]}" for k in params)
    sig = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    return query + "&signature=" + sig


def post(path, params):
    params["timestamp"] = int(time.time() * 1000)
    url = BASE + path + "?" + sign(params)

    r = requests.post(url, headers=HEADERS)
    if r.status_code != 200:
        raise RuntimeError(f"{r.status_code} {r.text}")
    return r.json()


# ---------- exchange ----------

def open_market(symbol, side, qty):
    print(f"[LIVE] ENTRY {symbol} {side} qty={qty}")

    return post("/fapi/v1/order", {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quan

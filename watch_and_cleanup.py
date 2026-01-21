import os, time, hmac, hashlib, urllib.parse, urllib.request, json
from urllib.error import HTTPError
from decimal import Decimal

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BASE = os.getenv("ASTER_REST_BASE","https://fapi.asterdex.com").rstrip("/")
KEY  = os.getenv("ASTER_API_KEY")
SEC  = os.getenv("ASTER_API_SECRET")
SYM  = os.getenv("LIVE_SYMBOL","ASTERUSDT")
POLL_SEC = float(os.getenv("WATCH_POLL_SEC","2.0"))
RECV = "5000"

def sign(params):
    q = urllib.parse.urlencode(params, doseq=True)
    sig = hmac.new(SEC.encode(), q.encode(), hashlib.sha256).hexdigest()
    return q + "&signature=" + sig

def req(method, path, params=None, signed=False):
    params = params or {}
    headers = {}
    if signed:
        params["timestamp"] = str(int(time.time()*1000))
        params["recvWindow"] = RECV
        url = f"{BASE}{path}?{sign(params)}"
        headers["X-MBX-APIKEY"] = KEY
    else:
        q = urllib.parse.urlencode(params, doseq=True)
        url = f"{BASE}{path}" + (("?" + q) if q else "")
    r = urllib.request.Request(url, method=method, headers=headers)
    try:
        with urllib.request.urlopen(r, timeout=20) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, json.loads(raw) if raw else {}
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print("HTTP", e.code, e.reason, body)
        raise

def position_amt(symbol):
    _, pr = req("GET","/fapi/v2/positionRisk",{"symbol":symbol},signed=True)
    if isinstance(pr, list):
        pr = pr[0] if pr else {}
    return Decimal(str(pr.get("positionAmt","0") or "0"))

def open_orders(symbol):
    _, oo = req("GET","/fapi/v1/openOrders",{"symbol":symbol},signed=True)
    return oo if isinstance(oo, list) else []

def cancel_order(symbol, order_id):
    req("DELETE","/fapi/v1/order",{"symbol":symbol,"orderId":str(order_id)},signed=True)

print(f"[WATCH] Monitoring {SYM}. Will cancel leftover orders after position closes.")
while True:
    amt = position_amt(SYM)
    if amt == 0:
        orders = open_orders(SYM)
        if orders:
            print(f"[WATCH] Position closed. Canceling {len(orders)} leftover orders...")
            for o in orders:
                oid = o.get("orderId")
                try:
                    cancel_order(SYM, oid)
                    print("[WATCH] canceled orderId", oid)
                except Exception:
                    pass
        else:
            print("[WATCH] Position closed. No open orders.")
        print("[WATCH] DONE.")
        break
    else:
        # краткий статус
        print(f"[WATCH] positionAmt={amt} openOrders={len(open_orders(SYM))}")
        time.sleep(POLL_SEC)

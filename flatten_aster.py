import os, time, hmac, hashlib, urllib.parse, urllib.request, json
from urllib.error import HTTPError

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BASE = os.getenv("ASTER_REST_BASE","https://fapi.asterdex.com").rstrip("/")
KEY  = os.getenv("ASTER_API_KEY")
SEC  = os.getenv("ASTER_API_SECRET")
SYM  = os.getenv("LIVE_SYMBOL","ASTERUSDT")
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

print("[FLATTEN] Cancel all open orders:", SYM)
req("DELETE", "/fapi/v1/allOpenOrders", {"symbol": SYM}, signed=True)

_, pr = req("GET", "/fapi/v2/positionRisk", {"symbol": SYM}, signed=True)
if isinstance(pr, list):
    pr = pr[0] if pr else {}
pos_amt = float(pr.get("positionAmt", 0) or 0)
print("[FLATTEN] positionAmt:", pos_amt)

if abs(pos_amt) > 0:
    side = "SELL" if pos_amt > 0 else "BUY"
    qty = str(abs(pos_amt))
    print("[FLATTEN] MARKET close", side, "qty", qty)
    req("POST", "/fapi/v1/order", {
        "symbol": SYM,
        "side": side,
        "type": "MARKET",
        "quantity": qty,
        "reduceOnly": "true",
    }, signed=True)
else:
    print("[FLATTEN] No position to close.")

print("[FLATTEN] Done.")

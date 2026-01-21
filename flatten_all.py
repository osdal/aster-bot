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
RECV = "5000"

if not KEY or not SEC:
    raise SystemExit("Нет ASTER_API_KEY/ASTER_API_SECRET в .env")

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
        return e.code, {"_http_error": True, "code": e.code, "reason": e.reason, "body": body}

def get_all_positions():
    st, pr = req("GET", "/fapi/v2/positionRisk", {}, signed=True)
    if isinstance(pr, dict) and pr.get("_http_error"):
        raise SystemExit(f"positionRisk error: {pr}")
    return pr if isinstance(pr, list) else []

def get_all_open_orders():
    # Пытаемся без symbol (если поддерживается)
    st, oo = req("GET", "/fapi/v1/openOrders", {}, signed=True)
    if isinstance(oo, dict) and oo.get("_http_error"):
        return []
    return oo if isinstance(oo, list) else []

def cancel_all_orders_for_symbol(sym):
    st, out = req("DELETE", "/fapi/v1/allOpenOrders", {"symbol": sym}, signed=True)
    if isinstance(out, dict) and out.get("_http_error"):
        print(f"[WARN] allOpenOrders {sym}: {out.get('body')}")
    else:
        print(f"[OK] Canceled open orders: {sym}")

def market_close_symbol(sym, position_amt: Decimal):
    if position_amt == 0:
        return
    side = "SELL" if position_amt > 0 else "BUY"
    qty = str(abs(position_amt))
    st, out = req("POST", "/fapi/v1/order", {
        "symbol": sym,
        "side": side,
        "type": "MARKET",
        "quantity": qty,
        "reduceOnly": "true",
    }, signed=True)
    if isinstance(out, dict) and out.get("_http_error"):
        print(f"[WARN] close {sym}: {out.get('body')}")
    else:
        print(f"[OK] Closed {sym} side={side} qty={qty} orderId={out.get('orderId')}")

print("[FLATTEN_ALL] Gathering positions...")
positions = get_all_positions()

open_pos = []
for p in positions:
    sym = p.get("symbol")
    try:
        amt = Decimal(str(p.get("positionAmt","0") or "0"))
    except Exception:
        amt = Decimal("0")
    if sym and amt != 0:
        open_pos.append((sym, amt))

print(f"[FLATTEN_ALL] Open positions: {len(open_pos)}")
for sym, amt in open_pos:
    print(" -", sym, "positionAmt=", amt)

print("[FLATTEN_ALL] Gathering open orders...")
oo = get_all_open_orders()
order_syms = sorted({o.get("symbol") for o in oo if o.get("symbol")})

print(f"[FLATTEN_ALL] Symbols with open orders: {len(order_syms)}")
if order_syms:
    print(" -", ", ".join(order_syms[:30]) + (" ..." if len(order_syms) > 30 else ""))

# Union of symbols (orders + positions)
syms = sorted(set(order_syms) | {s for s,_ in open_pos})
print(f"[FLATTEN_ALL] Total symbols to clean: {len(syms)}")

# 1) Cancel orders everywhere
for sym in syms:
    cancel_all_orders_for_symbol(sym)

# 2) Close positions everywhere
for sym, amt in open_pos:
    market_close_symbol(sym, amt)

print("[FLATTEN_ALL] DONE.")

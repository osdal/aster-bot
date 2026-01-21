import os, time, hmac, hashlib, urllib.parse, urllib.request, json
from decimal import Decimal, ROUND_DOWN
from urllib.error import HTTPError

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BASE = os.getenv("ASTER_REST_BASE", "https://fapi.asterdex.com").rstrip("/")
API_KEY = os.getenv("ASTER_API_KEY")
API_SECRET = os.getenv("ASTER_API_SECRET")

SYMBOL = os.getenv("LIVE_SYMBOL", "ASTERUSDT")
ENTRY_PRICE = Decimal(os.getenv("ENTRY_PRICE", "0.6013000"))
QTY = Decimal(os.getenv("ENTRY_QTY", "16.63"))

TP_PCT = Decimal(os.getenv("TP_PCT", "0.40")) / Decimal("100")
SL_PCT = Decimal(os.getenv("SL_PCT", "0.18")) / Decimal("100")
RECV_WINDOW = "5000"

def sign(params: dict) -> str:
    query = urllib.parse.urlencode(params, doseq=True)
    sig = hmac.new(API_SECRET.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    return query + "&signature=" + sig

def http_json(method, path, params=None, signed=False):
    params = params or {}
    headers = {}
    if signed:
        params["timestamp"] = str(int(time.time() * 1000))
        params["recvWindow"] = RECV_WINDOW
        url = f"{BASE}{path}?{sign(params)}"
        headers["X-MBX-APIKEY"] = API_KEY
    else:
        q = urllib.parse.urlencode(params, doseq=True)
        url = f"{BASE}{path}" + (("?" + q) if q else "")

    req = urllib.request.Request(url, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, json.loads(raw) if raw else {}
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"[HTTP ERROR] {e.code} {e.reason}")
        print("[HTTP ERROR BODY]", body)
        raise

def quantize_down(x: Decimal, step: Decimal) -> Decimal:
    return (x / step).to_integral_value(rounding=ROUND_DOWN) * step

# exchangeInfo tick size
_, ex = http_json("GET", "/fapi/v1/exchangeInfo")
sym = next((s for s in ex.get("symbols", []) if s.get("symbol") == SYMBOL), None)
if not sym:
    raise SystemExit(f"Символ {SYMBOL} не найден")
filters = {f.get("filterType"): f for f in sym.get("filters", [])}
tickSize = Decimal(filters.get("PRICE_FILTER", {}).get("tickSize", "0.000001"))

tp = quantize_down(ENTRY_PRICE * (Decimal("1") + TP_PCT), tickSize)
sl = quantize_down(ENTRY_PRICE * (Decimal("1") - SL_PCT), tickSize)

cid_tp = f"tp_{int(time.time())}"
cid_sl = f"sl_{int(time.time())}"

print(f"[BRACKET] symbol={SYMBOL} entry={ENTRY_PRICE} qty={QTY} TP={tp} SL={sl}")

# TP LIMIT (reduceOnly)
st, tp_order = http_json(
    "POST", "/fapi/v1/order",
    {
        "symbol": SYMBOL,
        "side": "SELL",
        "type": "LIMIT",
        "timeInForce": "GTC",
        "quantity": format(QTY, "f"),
        "price": format(tp, "f"),
        "reduceOnly": "true",
        "newClientOrderId": cid_tp,
    },
    signed=True
)
print("[BRACKET] TP", st, tp_order.get("status"), "orderId=", tp_order.get("orderId"))

# SL STOP_MARKET (closePosition)
st, sl_order = http_json(
    "POST", "/fapi/v1/order",
    {
        "symbol": SYMBOL,
        "side": "SELL",
        "type": "STOP_MARKET",
        "stopPrice": format(sl, "f"),
        "closePosition": "true",
        "reduceOnly": "true",
        "newClientOrderId": cid_sl,
    },
    signed=True
)
print("[BRACKET] SL", st, sl_order.get("status"), "orderId=", sl_order.get("orderId"))

print("[BRACKET] DONE")

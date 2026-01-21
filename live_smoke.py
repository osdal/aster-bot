import os, time, hmac, hashlib, urllib.parse, urllib.request, json
from decimal import Decimal, ROUND_DOWN

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BASE = os.getenv("ASTER_REST_BASE", "https://fapi.asterdex.com").rstrip("/")
API_KEY = os.getenv("ASTER_API_KEY")
API_SECRET = os.getenv("ASTER_API_SECRET")

if not API_KEY or not API_SECRET:
    raise SystemExit("Нет ASTER_API_KEY/ASTER_API_SECRET в .env")

SYMBOL = os.getenv("SMOKE_SYMBOL", "BTCUSDT")
NOTIONAL_USD = Decimal(os.getenv("SMOKE_NOTIONAL_USD", "5"))
RECV_WINDOW = "5000"

def http_json(method, path, params=None, signed=False):
    params = params or {}
    headers = {}
    if signed:
        params["timestamp"] = str(int(time.time() * 1000))
        params["recvWindow"] = RECV_WINDOW
        query = urllib.parse.urlencode(params, doseq=True)
        sig = hmac.new(API_SECRET.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
        url = f"{BASE}{path}?{query}&signature={sig}"
        headers["X-MBX-APIKEY"] = API_KEY
    else:
        query = urllib.parse.urlencode(params, doseq=True)
        url = f"{BASE}{path}" + (("?" + query) if query else "")

    req = urllib.request.Request(url, method=method, headers=headers)
    with urllib.request.urlopen(req, timeout=20) as resp:
        return resp.status, json.loads(resp.read().decode("utf-8"))

def quantize_down(x: Decimal, step: Decimal) -> Decimal:
    return (x / step).to_integral_value(rounding=ROUND_DOWN) * step

# exchangeInfo filters
_, ex = http_json("GET", "/fapi/v1/exchangeInfo")
sym = next((s for s in ex.get("symbols", []) if s.get("symbol") == SYMBOL), None)
if not sym:
    raise SystemExit(f"Символ {SYMBOL} не найден в exchangeInfo")

filters = {f.get("filterType"): f for f in sym.get("filters", [])}
lot = filters.get("LOT_SIZE", {})
price_f = filters.get("PRICE_FILTER", {})
percent = filters.get("PERCENT_PRICE") or filters.get("PERCENT_PRICE_BY_SIDE") or {}

stepSize = Decimal(lot.get("stepSize", "0.000001"))
minQty   = Decimal(lot.get("minQty", "0"))
tickSize = Decimal(price_f.get("tickSize", "0.01"))

# current price
_, p = http_json("GET", "/fapi/v1/ticker/price", {"symbol": SYMBOL})
last = Decimal(str(p.get("price")))

# price far below market but within percent filter (fallback 0.80)
mult_down = Decimal(str(percent.get("multiplierDown", "0.80")))
target_price = (last * mult_down * Decimal("0.99"))
price = quantize_down(target_price, tickSize)
if price <= 0:
    raise SystemExit("Некорректная цена после округления")

# qty for small notional
qty_raw = (NOTIONAL_USD / last)
qty = quantize_down(qty_raw, stepSize)
if qty < minQty:
    qty = quantize_down(minQty, stepSize)

client_id = f"smoke_{int(time.time())}"
print(f"[SMOKE] symbol={SYMBOL} last={last} place BUY LIMIT price={price} qty={qty} clientId={client_id}")

# PLACE
status, placed = http_json(
    "POST",
    "/fapi/v1/order",
    {
        "symbol": SYMBOL,
        "side": "BUY",
        "type": "LIMIT",
        "timeInForce": "GTC",
        "quantity": format(qty, "f"),
        "price": format(price, "f"),
        "newClientOrderId": client_id,
    },
    signed=True
)
print("[SMOKE] place HTTP", status, "orderId=", placed.get("orderId"), "status=", placed.get("status"))

# CANCEL
status, canceled = http_json(
    "DELETE",
    "/fapi/v1/order",
    {"symbol": SYMBOL, "origClientOrderId": client_id},
    signed=True
)
print("[SMOKE] cancel HTTP", status, "status=", canceled.get("status"), "orderId=", canceled.get("orderId"))
print("[SMOKE] DONE OK (order placed+cancelled).")

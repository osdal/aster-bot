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

if not API_KEY or not API_SECRET:
    raise SystemExit("Нет ASTER_API_KEY/ASTER_API_SECRET в .env")

LIVE_ENABLED = (os.getenv("LIVE_ENABLED", "false").strip().lower() == "true")
SYMBOL = os.getenv("LIVE_SYMBOL", "ASTERUSDT").strip().upper()

# LIVE notional: сначала LIVE_NOTIONAL_USD, иначе TRADE_NOTIONAL_USD
NOTIONAL_USD = Decimal(os.getenv("LIVE_NOTIONAL_USD", os.getenv("TRADE_NOTIONAL_USD", "5")))
LEVERAGE = int(os.getenv("LIVE_LEVERAGE", "2"))

TP_PCT = Decimal(os.getenv("TP_PCT", "0.40")) / Decimal("100")
SL_PCT = Decimal(os.getenv("SL_PCT", "0.18")) / Decimal("100")
RECV_WINDOW = "5000"

def sign(params: dict) -> str:
    q = urllib.parse.urlencode(params, doseq=True)
    sig = hmac.new(API_SECRET.encode("utf-8"), q.encode("utf-8"), hashlib.sha256).hexdigest()
    return q + "&signature=" + sig

def http_json(method, path, params=None, signed=False):
    params = params or {}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
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

def get_tick_and_steps(symbol: str):
    _, ex = http_json("GET", "/fapi/v1/exchangeInfo")
    sym = next((s for s in ex.get("symbols", []) if s.get("symbol") == symbol), None)
    if not sym:
        raise SystemExit(f"Символ {symbol} не найден в exchangeInfo")

    filters = {f.get("filterType"): f for f in sym.get("filters", [])}
    lot = filters.get("LOT_SIZE", {})
    price_f = filters.get("PRICE_FILTER", {})
    stepSize = Decimal(lot.get("stepSize", "0.000001"))
    minQty   = Decimal(lot.get("minQty", "0"))
    tickSize = Decimal(price_f.get("tickSize", "0.000001"))
    return tickSize, stepSize, minQty

def get_position_amt(symbol: str) -> Decimal:
    # Aster обычно поддерживает /fapi/v2/positionRisk
    _, pr = http_json("GET", "/fapi/v2/positionRisk", {"symbol": symbol}, signed=True)
    if isinstance(pr, list):
        pr = pr[0] if pr else {}
    amt = Decimal(str(pr.get("positionAmt", "0") or "0"))
    return amt

def cancel_all_open_orders(symbol: str):
    http_json("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol}, signed=True)

def place_tp_sl_long(symbol: str, qty: Decimal, entry: Decimal, tick: Decimal):
    tp = quantize_down(entry * (Decimal("1") + TP_PCT), tick)
    sl = quantize_down(entry * (Decimal("1") - SL_PCT), tick)

    cid_tp = f"tp_{int(time.time())}"
    cid_sl = f"sl_{int(time.time())}"

    print(f"[LIVE] Brackets TP={tp} SL(stop)={sl}")

    # TP: LIMIT SELL reduceOnly=true
    st, tp_order = http_json(
        "POST", "/fapi/v1/order",
        {
            "symbol": symbol,
            "side": "SELL",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": format(qty, "f"),
            "price": format(tp, "f"),
            "reduceOnly": "true",
            "newClientOrderId": cid_tp,
        },
        signed=True
    )
    tp_id = tp_order.get("orderId")
    print("[LIVE] TP placed:", st, tp_order.get("status"), "orderId=", tp_id)

    try:
        # SL: STOP_MARKET closePosition=true (ВАЖНО: без reduceOnly)
        st, sl_order = http_json(
            "POST", "/fapi/v1/order",
            {
                "symbol": symbol,
                "side": "SELL",
                "type": "STOP_MARKET",
                "stopPrice": format(sl, "f"),
                "closePosition": "true",
                "newClientOrderId": cid_sl,
            },
            signed=True
        )
        sl_id = sl_order.get("orderId")
        print("[LIVE] SL placed:", st, sl_order.get("status"), "orderId=", sl_id)
        return tp_id, sl_id
    except Exception:
        # если SL не поставился  срочно отменяем TP и закрываем позицию (чтобы не остаться без стопа)
        print("[LIVE] SL placement failed -> cancel TP and MARKET close position")
        try:
            http_json("DELETE", "/fapi/v1/order", {"symbol": symbol, "orderId": str(tp_id)}, signed=True)
        except Exception:
            pass
        try:
            http_json("POST", "/fapi/v1/order", {
                "symbol": symbol,
                "side": "SELL",
                "type": "MARKET",
                "quantity": format(qty, "f"),
                "reduceOnly": "true",
            }, signed=True)
        except Exception:
            pass
        raise

def main():
    if not LIVE_ENABLED:
        print("[SAFE] LIVE_ENABLED=false. Ничего не делаю. Чтобы разрешить торговлю: LIVE_ENABLED=true в .env")
        return

    # Safety: не открывать, если уже есть позиция
    pos_amt = get_position_amt(SYMBOL)
    if pos_amt != 0:
        print(f"[SAFE] Уже есть позиция по {SYMBOL}: positionAmt={pos_amt}. Вход запрещён.")
        return

    # Safety: чистим старые ордера по символу (на всякий случай)
    cancel_all_open_orders(SYMBOL)

    tick, step, minQty = get_tick_and_steps(SYMBOL)

    # price
    _, p = http_json("GET", "/fapi/v1/ticker/price", {"symbol": SYMBOL})
    last = Decimal(str(p.get("price")))

    # qty ~ notional*leverage / price
    qty = quantize_down((NOTIONAL_USD * Decimal(LEVERAGE) / last), step)
    if qty < minQty:
        qty = quantize_down(minQty, step)

    print(f"[LIVE] MARKET BUY {SYMBOL} last={last} qty={qty} lev={LEVERAGE} notional_usd={NOTIONAL_USD}")

    # MARKET entry
    st, entry = http_json(
        "POST", "/fapi/v1/order",
        {
            "symbol": SYMBOL,
            "side": "BUY",
            "type": "MARKET",
            "quantity": format(qty, "f"),
            "newClientOrderId": f"entry_{int(time.time())}",
        },
        signed=True
    )
    oid = entry.get("orderId")
    print("[LIVE] entry ACK:", st, "orderId=", oid, "status=", entry.get("status"))

    # confirm fill
    time.sleep(0.5)
    st, od = http_json("GET", "/fapi/v1/order", {"symbol": SYMBOL, "orderId": str(oid)}, signed=True)
    if od.get("status") != "FILLED":
        print("[SAFE] Ордер не FILLED, статус:", od.get("status"), "-> выхожу без постановки bracket.")
        return

    avg = Decimal(str(od.get("avgPrice") or od.get("avgFillPrice") or last))
    print("[LIVE] FILLED avgPrice=", avg, "executedQty=", od.get("executedQty"))

    place_tp_sl_long(SYMBOL, qty, avg, tick)
    print("[LIVE] DONE. TP+SL активны.")

if __name__ == "__main__":
    main()

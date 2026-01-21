import os, sys, time, re, hmac, hashlib, urllib.parse, urllib.request, json, subprocess
from decimal import Decimal, ROUND_DOWN
from urllib.error import HTTPError

from dotenv import load_dotenv
load_dotenv()

BASE = os.getenv("ASTER_REST_BASE", "https://fapi.asterdex.com").rstrip("/")
API_KEY = os.getenv("ASTER_API_KEY")
API_SECRET = os.getenv("ASTER_API_SECRET")

LIVE_ENABLED = (os.getenv("LIVE_ENABLED", "false").strip().lower() == "true")
MIRROR_ENABLED = (os.getenv("MIRROR_ENABLED", "false").strip().lower() == "true")

LIVE_NOTIONAL_USD = Decimal(os.getenv("LIVE_NOTIONAL_USD", "5"))
LIVE_LEVERAGE = int(os.getenv("LIVE_LEVERAGE", "2"))
TP_PCT = Decimal(os.getenv("TP_PCT", "0.40")) / Decimal("100")
SL_PCT = Decimal(os.getenv("SL_PCT", "0.18")) / Decimal("100")
COOLDOWN_SEC = int(os.getenv("COOLDOWN_AFTER_TRADE_SEC", "300"))
POLL_SEC = float(os.getenv("WATCH_POLL_SEC", "2.0"))
RECV_WINDOW = "5000"
MAX_OPEN_POSITIONS = int(os.getenv("LIVE_MAX_POSITIONS", "1"))

if not API_KEY or not API_SECRET:
    raise SystemExit("Нет ASTER_API_KEY/ASTER_API_SECRET в .env")

OPEN_RE = re.compile(r"\[PAPER\]\s+OPEN\s+([A-Z0-9]+)\s+(LONG|SHORT)\s+entry=", re.I)

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
        print(f"[HTTP ERROR] {e.code} {e.reason} {body}")
        raise

def quantize_down(x: Decimal, step: Decimal) -> Decimal:
    return (x / step).to_integral_value(rounding=ROUND_DOWN) * step

def get_filters(symbol: str):
    _, ex = http_json("GET", "/fapi/v1/exchangeInfo")
    sym = next((s for s in ex.get("symbols", []) if s.get("symbol") == symbol), None)
    if not sym:
        raise RuntimeError(f"Символ {symbol} не найден")
    filters = {f.get("filterType"): f for f in sym.get("filters", [])}
    lot = filters.get("LOT_SIZE", {})
    price_f = filters.get("PRICE_FILTER", {})
    step = Decimal(lot.get("stepSize", "0.000001"))
    minq = Decimal(lot.get("minQty", "0"))
    tick = Decimal(price_f.get("tickSize", "0.000001"))
    return tick, step, minq

def get_positions_any():
    # пробуем получить все позиции (если endpoint поддерживает без symbol)
    try:
        _, pr = http_json("GET", "/fapi/v2/positionRisk", {}, signed=True)
        if isinstance(pr, list):
            return pr
    except Exception:
        pass
    return []

def count_open_positions():
    pr = get_positions_any()
    n = 0
    for p in pr:
        try:
            amt = Decimal(str(p.get("positionAmt", "0") or "0"))
            if amt != 0:
                n += 1
        except Exception:
            pass
    return n

def position_amt(symbol: str) -> Decimal:
    _, pr = http_json("GET", "/fapi/v2/positionRisk", {"symbol": symbol}, signed=True)
    if isinstance(pr, list):
        pr = pr[0] if pr else {}
    return Decimal(str(pr.get("positionAmt", "0") or "0"))

def cancel_all_open_orders(symbol: str):
    http_json("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol}, signed=True)

def open_orders(symbol: str):
    _, oo = http_json("GET", "/fapi/v1/openOrders", {"symbol": symbol}, signed=True)
    return oo if isinstance(oo, list) else []

def place_entry_and_brackets(symbol: str, direction: str):
    # Safety gates
    if not (LIVE_ENABLED and MIRROR_ENABLED):
        print("[SAFE] LIVE_ENABLED/MIRROR_ENABLED выключены. Пропуск.")
        return

    # Safety: ограничение по количеству позиций
    npos = count_open_positions()
    if npos >= MAX_OPEN_POSITIONS:
        print(f"[SAFE] Уже открыто позиций: {npos} (лимит {MAX_OPEN_POSITIONS}). Пропуск.")
        return

    # Safety: не входить если по символу уже есть позиция
    if position_amt(symbol) != 0:
        print(f"[SAFE] Уже есть позиция по {symbol}. Пропуск.")
        return

    # чистим хвосты на всякий случай
    cancel_all_open_orders(symbol)

    tick, step, minq = get_filters(symbol)

    # текущая цена
    _, p = http_json("GET", "/fapi/v1/ticker/price", {"symbol": symbol})
    last = Decimal(str(p.get("price")))

    # qty = notional*lev/price
    qty = quantize_down((LIVE_NOTIONAL_USD * Decimal(LIVE_LEVERAGE) / last), step)
    if qty < minq:
        qty = quantize_down(minq, step)

    side_entry = "BUY" if direction == "LONG" else "SELL"
    print(f"[LIVE] ENTRY {symbol} {direction} market side={side_entry} qty={qty} last={last}")

    st, entry = http_json(
        "POST", "/fapi/v1/order",
        {
            "symbol": symbol,
            "side": side_entry,
            "type": "MARKET",
            "quantity": format(qty, "f"),
            "newClientOrderId": f"mirror_entry_{int(time.time())}",
        },
        signed=True
    )
    oid = entry.get("orderId")
    time.sleep(0.5)

    # confirm filled
    _, od = http_json("GET", "/fapi/v1/order", {"symbol": symbol, "orderId": str(oid)}, signed=True)
    if od.get("status") != "FILLED":
        print("[SAFE] ENTRY не FILLED:", od.get("status"), "-> выходим без bracket.")
        return

    avg = Decimal(str(od.get("avgPrice") or last))
    print(f"[LIVE] FILLED {symbol} avg={avg} executedQty={od.get('executedQty')} orderId={oid}")

    # brackets
    if direction == "LONG":
        tp_price = quantize_down(avg * (Decimal("1") + TP_PCT), tick)
        sl_price = quantize_down(avg * (Decimal("1") - SL_PCT), tick)
        tp_side = "SELL"
        sl_side = "SELL"
        tp_qty = qty
        sl_close = True
    else:
        tp_price = quantize_down(avg * (Decimal("1") - TP_PCT), tick)
        sl_price = quantize_down(avg * (Decimal("1") + SL_PCT), tick)
        tp_side = "BUY"
        sl_side = "BUY"
        tp_qty = qty
        sl_close = True

    print(f"[LIVE] BRACKETS {symbol} TP={tp_price} SL(stop)={sl_price}")

    # TP LIMIT reduceOnly=true
    st, tp = http_json(
        "POST", "/fapi/v1/order",
        {
            "symbol": symbol,
            "side": tp_side,
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": format(tp_qty, "f"),
            "price": format(tp_price, "f"),
            "reduceOnly": "true",
            "newClientOrderId": f"mirror_tp_{int(time.time())}",
        },
        signed=True
    )
    print("[LIVE] TP placed:", tp.get("status"), "orderId=", tp.get("orderId"))

    # SL STOP_MARKET closePosition=true (без reduceOnly  чтобы не ловить -1106)
    st, sl = http_json(
        "POST", "/fapi/v1/order",
        {
            "symbol": symbol,
            "side": sl_side,
            "type": "STOP_MARKET",
            "stopPrice": format(sl_price, "f"),
            "closePosition": "true" if sl_close else "false",
            "newClientOrderId": f"mirror_sl_{int(time.time())}",
        },
        signed=True
    )
    print("[LIVE] SL placed:", sl.get("status"), "orderId=", sl.get("orderId"))

    # watch & cleanup
    print(f"[WATCH] {symbol}: waiting position close...")
    while True:
        amt = position_amt(symbol)
        if amt == 0:
            oo = open_orders(symbol)
            if oo:
                print(f"[WATCH] {symbol}: position closed -> cancel leftover {len(oo)} orders")
                cancel_all_open_orders(symbol)
            print(f"[WATCH] {symbol}: DONE. Cooldown {COOLDOWN_SEC}s")
            time.sleep(COOLDOWN_SEC)
            break
        time.sleep(POLL_SEC)

def main():
    print("[MIRROR] Starting PAPER -> LIVE mirror")
    print("[MIRROR] Flags: LIVE_ENABLED=", LIVE_ENABLED, "MIRROR_ENABLED=", MIRROR_ENABLED)
    print("[MIRROR] Live: notional_usd=", LIVE_NOTIONAL_USD, "lev=", LIVE_LEVERAGE, "TP%=", TP_PCT*100, "SL%=", SL_PCT*100)

    # запускаем paper бота как отдельный процесс и читаем stdout
    cmd = [sys.executable, "-u", "run_paper.py"]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)

    try:
        for line in p.stdout:
            line = line.rstrip("\n")
            print(line)
            m = OPEN_RE.search(line)
            if m:
                symbol = m.group(1).upper()
                direction = m.group(2).upper()
                try:
                    place_entry_and_brackets(symbol, direction)
                except Exception as e:
                    print("[MIRROR] ERROR while mirroring:", e)
    finally:
        try:
            p.terminate()
        except Exception:
            pass

if __name__ == "__main__":
    main()

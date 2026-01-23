import os, sys, time, re, hmac, hashlib, urllib.parse, urllib.request, json, subprocess, csv
from decimal import Decimal, ROUND_DOWN
from urllib.error import HTTPError
from datetime import datetime, timezone

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

# Если поставить >0 — по таймауту форсируем закрытие позиции (market reduceOnly) и чистим ордера.
WATCH_HARD_TIMEOUT_SEC = int(os.getenv("WATCH_HARD_TIMEOUT_SEC", "0"))

# Список символов, которые не зеркалим в LIVE
SKIP_SYMBOLS = set(s.strip().upper() for s in os.getenv("SKIP_SYMBOLS", "").split(",") if s.strip())

LIVE_LOG_PATH = os.getenv("LIVE_LOG_PATH", r"data\live_trades.csv")

# Оценка комиссий, если userTrades недоступен (в % от notional за одну сторону)
# Например: taker 0.06% => 0.0006 ; maker 0.02% => 0.0002
FEE_TAKER = Decimal(os.getenv("FEE_TAKER", "0.0006"))
FEE_MAKER = Decimal(os.getenv("FEE_MAKER", "0.0002"))

if not API_KEY or not API_SECRET:
    raise SystemExit("Нет ASTER_API_KEY/ASTER_API_SECRET в .env")

OPEN_RE = re.compile(r"\[PAPER\]\s+OPEN\s+([A-Z0-9]+)\s+(LONG|SHORT)\s+entry=", re.I)

def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

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

def ensure_log_header():
    os.makedirs(os.path.dirname(LIVE_LOG_PATH) or ".", exist_ok=True)
    if not os.path.exists(LIVE_LOG_PATH):
        with open(LIVE_LOG_PATH, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "entry_ts","exit_ts","symbol","side","qty",
                "entry_price","exit_price",
                "gross_pnl","commission","net_pnl",
                "outcome",
                "entry_order_id","tp_order_id","sl_order_id","close_order_id",
                "duration_sec","note"
            ])

def log_live_trade(row: dict):
    ensure_log_header()
    with open(LIVE_LOG_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            row.get("entry_ts",""),
            row.get("exit_ts",""),
            row.get("symbol",""),
            row.get("side",""),
            str(row.get("qty","")),
            str(row.get("entry_price","")),
            str(row.get("exit_price","")),
            str(row.get("gross_pnl","")),
            str(row.get("commission","")),
            str(row.get("net_pnl","")),
            row.get("outcome",""),
            row.get("entry_order_id",""),
            row.get("tp_order_id",""),
            row.get("sl_order_id",""),
            row.get("close_order_id",""),
            str(row.get("duration_sec","")),
            row.get("note",""),
        ])

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

def set_leverage(symbol: str, lev: int):
    try:
        http_json("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": str(lev)}, signed=True)
    except Exception as e:
        print(f"[WARN] set_leverage failed for {symbol}: {e}")

def get_order(symbol: str, order_id: str):
    _, od = http_json("GET", "/fapi/v1/order", {"symbol": symbol, "orderId": str(order_id)}, signed=True)
    return od if isinstance(od, dict) else {}

def get_user_trades(symbol: str, start_ms: int, end_ms: int):
    # Если у Aster этот эндпоинт совместим с Binance — будет работать
    _, tr = http_json("GET", "/fapi/v1/userTrades",
                      {"symbol": symbol, "startTime": str(start_ms), "endTime": str(end_ms), "limit": "1000"},
                      signed=True)
    return tr if isinstance(tr, list) else []

def compute_commission(symbol: str, entry_oid: str, tp_oid: str, sl_oid: str, close_oid: str,
                       entry_ms: int, exit_ms: int,
                       assumed_exit_is_maker: bool, qty: Decimal, entry_price: Decimal):
    # 1) Пробуем userTrades
    try:
        trades = get_user_trades(symbol, max(entry_ms - 10_000, 0), exit_ms + 10_000)
        comm = Decimal("0")
        # Если есть orderId — фильтруем по ним
        oids = {str(x) for x in [entry_oid, tp_oid, sl_oid, close_oid] if x}
        for t in trades:
            oid = str(t.get("orderId", ""))
            if oids and oid and oid not in oids:
                continue
            c = t.get("commission")
            if c is not None:
                try:
                    comm += Decimal(str(c))
                except Exception:
                    pass
        if comm != 0:
            return comm
    except Exception:
        pass

    # 2) Фоллбек: оценка по notional и ставкам
    # entry всегда MARKET => taker
    notional = qty * entry_price
    entry_fee = notional * FEE_TAKER
    exit_fee = notional * (FEE_MAKER if assumed_exit_is_maker else FEE_TAKER)
    return (entry_fee + exit_fee)

def flatten_position(symbol: str):
    amt = position_amt(symbol)
    if amt == 0:
        cancel_all_open_orders(symbol)
        return None

    side = "SELL" if amt > 0 else "BUY"
    qty = abs(amt)

    # Чистим ордера, чтобы не было гонок TP/SL
    cancel_all_open_orders(symbol)

    st, close = http_json(
        "POST", "/fapi/v1/order",
        {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": format(qty, "f"),
            "reduceOnly": "true",
            "newClientOrderId": f"mirror_flatten_{int(time.time())}",
        },
        signed=True
    )
    return close.get("orderId")

def place_entry_and_brackets(symbol: str, direction: str):
    # Safety gates
    if not (LIVE_ENABLED and MIRROR_ENABLED):
        print("[SAFE] LIVE_ENABLED/MIRROR_ENABLED выключены. Пропуск.")
        return

    if symbol in SKIP_SYMBOLS:
        print(f"[SAFE] SKIP_SYMBOLS: {symbol}. Пропуск.")
        return

    # лимит позиций
    npos = count_open_positions()
    if npos >= MAX_OPEN_POSITIONS:
        # Чтобы не спамить — просто пропускаем без шума
        return

    # уже есть позиция по символу
    if position_amt(symbol) != 0:
        return

    # чистим хвосты
    cancel_all_open_orders(symbol)

    tick, step, minq = get_filters(symbol)

    # текущая цена
    _, p = http_json("GET", "/fapi/v1/ticker/price", {"symbol": symbol})
    last = Decimal(str(p.get("price")))

    # qty = notional*lev/price
    qty = quantize_down((LIVE_NOTIONAL_USD * Decimal(LIVE_LEVERAGE) / last), step)

    # ВАЖНО: НЕ увеличиваем до minQty — иначе сделка может стать гигантской (BTC) и дать -2019.
    if qty < minq:
        print(f"[SAFE] {symbol}: qty={qty} < minQty={minq} при notional={LIVE_NOTIONAL_USD}, пропуск символа.")
        return

    set_leverage(symbol, LIVE_LEVERAGE)

    side_entry = "BUY" if direction == "LONG" else "SELL"
    entry_ts = utc_now_iso()
    entry_ms = int(time.time() * 1000)

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
    entry_oid = str(entry.get("orderId", ""))
    time.sleep(0.5)

    od = get_order(symbol, entry_oid)
    if od.get("status") != "FILLED":
        print("[SAFE] ENTRY не FILLED:", od.get("status"), "-> выходим без bracket.")
        return

    avg = Decimal(str(od.get("avgPrice") or last))
    print(f"[LIVE] FILLED {symbol} avg={avg} executedQty={od.get('executedQty')} orderId={entry_oid}")

    # brackets
    if direction == "LONG":
        tp_price = quantize_down(avg * (Decimal("1") + TP_PCT), tick)
        sl_price = quantize_down(avg * (Decimal("1") - SL_PCT), tick)
        tp_side = "SELL"
        sl_side = "SELL"
    else:
        tp_price = quantize_down(avg * (Decimal("1") - TP_PCT), tick)
        sl_price = quantize_down(avg * (Decimal("1") + SL_PCT), tick)
        tp_side = "BUY"
        sl_side = "BUY"

    print(f"[LIVE] BRACKETS {symbol} TP={tp_price} SL(stop)={sl_price}")

    # TP LIMIT reduceOnly=true
    st, tp = http_json(
        "POST", "/fapi/v1/order",
        {
            "symbol": symbol,
            "side": tp_side,
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": format(qty, "f"),
            "price": format(tp_price, "f"),
            "reduceOnly": "true",
            "newClientOrderId": f"mirror_tp_{int(time.time())}",
        },
        signed=True
    )
    tp_oid = str(tp.get("orderId", ""))
    print("[LIVE] TP placed:", tp.get("status"), "orderId=", tp_oid)

    # SL STOP_MARKET closePosition=true
    st, sl = http_json(
        "POST", "/fapi/v1/order",
        {
            "symbol": symbol,
            "side": sl_side,
            "type": "STOP_MARKET",
            "stopPrice": format(sl_price, "f"),
            "closePosition": "true",
            "newClientOrderId": f"mirror_sl_{int(time.time())}",
        },
        signed=True
    )
    sl_oid = str(sl.get("orderId", ""))
    print("[LIVE] SL placed:", sl.get("status"), "orderId=", sl_oid)

    # watch & finalize
    print(f"[WATCH] {symbol}: waiting position close...")
    t0 = time.time()
    close_oid = ""

    while True:
        amt = position_amt(symbol)
        if amt == 0:
            break

        if WATCH_HARD_TIMEOUT_SEC > 0 and (time.time() - t0) >= WATCH_HARD_TIMEOUT_SEC:
            print(f"[WATCH] {symbol}: HARD TIMEOUT -> flatten position now")
            try:
                close_oid = str(flatten_position(symbol) or "")
            except Exception as e:
                print(f"[WATCH] {symbol}: flatten failed: {e}")
            break

        time.sleep(POLL_SEC)

    # cleanup leftover
    oo = open_orders(symbol)
    if oo:
        print(f"[WATCH] {symbol}: position closed -> cancel leftover {len(oo)} orders")
        cancel_all_open_orders(symbol)

    exit_ts = utc_now_iso()
    exit_ms = int(time.time() * 1000)

    # determine outcome and exit_price
    outcome = "UNKNOWN"
    exit_price = Decimal("0")

    tp_od = get_order(symbol, tp_oid) if tp_oid else {}
    sl_od = get_order(symbol, sl_oid) if sl_oid else {}

    if tp_od.get("status") == "FILLED":
        outcome = "TP"
        ep = tp_od.get("avgPrice") or tp_od.get("price")
        exit_price = Decimal(str(ep or "0"))
        assumed_exit_is_maker = True
    elif sl_od.get("status") == "FILLED":
        outcome = "SL"
        ep = sl_od.get("avgPrice") or sl_od.get("price")
        exit_price = Decimal(str(ep or "0"))
        assumed_exit_is_maker = False
    elif close_oid:
        outcome = "FORCED_FLATTEN"
        cl = get_order(symbol, close_oid)
        ep = cl.get("avgPrice") or cl.get("price")
        exit_price = Decimal(str(ep or "0"))
        assumed_exit_is_maker = False
    else:
        # на всякий случай: если позиция закрылась, но ордера не дают FILLED (особенности API)
        # пробуем взять last price как приближение
        _, p2 = http_json("GET", "/fapi/v1/ticker/price", {"symbol": symbol})
        exit_price = Decimal(str(p2.get("price") or "0"))
        assumed_exit_is_maker = False

    # gross pnl
    if direction == "LONG":
        gross = (exit_price - avg) * qty
    else:
        gross = (avg - exit_price) * qty

    # commission
    commission = compute_commission(
        symbol, entry_oid, tp_oid, sl_oid, close_oid,
        entry_ms, exit_ms,
        assumed_exit_is_maker=assumed_exit_is_maker,
        qty=qty, entry_price=avg
    )
    net = gross - commission

    duration_sec = int(max(0, exit_ms - entry_ms) / 1000)

    log_live_trade({
        "entry_ts": entry_ts,
        "exit_ts": exit_ts,
        "symbol": symbol,
        "side": direction,
        "qty": qty,
        "entry_price": avg,
        "exit_price": exit_price,
        "gross_pnl": gross,
        "commission": commission,
        "net_pnl": net,
        "outcome": outcome,
        "entry_order_id": entry_oid,
        "tp_order_id": tp_oid,
        "sl_order_id": sl_oid,
        "close_order_id": close_oid,
        "duration_sec": duration_sec,
        "note": "",
    })

    print(f"[WATCH] {symbol}: LOGGED -> {LIVE_LOG_PATH} outcome={outcome} netPnL={net}")
    print(f"[WATCH] {symbol}: DONE. Cooldown {COOLDOWN_SEC}s")
    time.sleep(COOLDOWN_SEC)

def main():
    print("[MIRROR] Starting PAPER -> LIVE mirror")
    print("[MIRROR] Flags: LIVE_ENABLED=", LIVE_ENABLED, "MIRROR_ENABLED=", MIRROR_ENABLED)
    print("[MIRROR] Live: notional_usd=", LIVE_NOTIONAL_USD, "lev=", LIVE_LEVERAGE, "TP%=", TP_PCT*100, "SL%=", SL_PCT*100)
    print("[MIRROR] Live log:", LIVE_LOG_PATH)
    if SKIP_SYMBOLS:
        print("[MIRROR] SKIP_SYMBOLS:", ",".join(sorted(SKIP_SYMBOLS)))

    cmd = [sys.executable, "-u", "run_paper.py"]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)

    try:
        for line in p.stdout:
            line = line.rstrip("\n")
            print(line)

            # Если уже заняты (MAX_POSITIONS=1) — даже не пытаемся парсить OPEN
            if count_open_positions() >= MAX_OPEN_POSITIONS:
                continue

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

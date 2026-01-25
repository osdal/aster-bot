import os, sys, time, re, hmac, hashlib, urllib.parse, urllib.request, json, subprocess, csv, queue, threading
from decimal import Decimal, ROUND_DOWN
from urllib.error import HTTPError
from datetime import datetime, timezone
from collections import defaultdict, deque

from dotenv import load_dotenv
load_dotenv()

# =========================================================
# CONFIG
# =========================================================
BASE = os.getenv("ASTER_REST_BASE", "https://fapi.asterdex.com").rstrip("/")
API_KEY = os.getenv("ASTER_API_KEY")
API_SECRET = os.getenv("ASTER_API_SECRET")

LIVE_ENABLED = (os.getenv("LIVE_ENABLED", "false").strip().lower() == "true")
MIRROR_ENABLED = (os.getenv("MIRROR_ENABLED", "false").strip().lower() == "true")

LIVE_NOTIONAL_USD = Decimal(os.getenv("LIVE_NOTIONAL_USD", "5"))
LIVE_LEVERAGE = int(os.getenv("LIVE_LEVERAGE", "2"))
TP_PCT = Decimal(os.getenv("TP_PCT", "0.60")) / Decimal("100")
SL_PCT = Decimal(os.getenv("SL_PCT", "0.20")) / Decimal("100")

POLL_SEC = float(os.getenv("WATCH_POLL_SEC", "2.0"))
RECV_WINDOW = "5000"

# Strategy A:
LOSS_STREAK_TO_ARM = int(os.getenv("LOSS_STREAK_TO_ARM", "3"))
MAX_OPEN_POSITIONS = int(os.getenv("LIVE_MAX_POSITIONS", "1"))

WATCH_PROFIT_TIMEOUT_SEC = int(os.getenv("WATCH_PROFIT_TIMEOUT_SEC", "0"))

SKIP_SYMBOLS = set(s.strip().upper() for s in os.getenv("SKIP_SYMBOLS", "").split(",") if s.strip())
LIVE_ALLOW_SYMBOLS = set(s.strip().upper() for s in os.getenv("LIVE_ALLOW_SYMBOLS", "").split(",") if s.strip())

LIVE_LOG_PATH = os.getenv("LIVE_LOG_PATH", r"data\live_trades.csv")

FEE_TAKER = Decimal(os.getenv("FEE_TAKER", "0.0006"))
FEE_MAKER = Decimal(os.getenv("FEE_MAKER", "0.0002"))

COUNT_POS_CACHE_SEC = float(os.getenv("COUNT_POS_CACHE_SEC", "2.0"))

if not API_KEY or not API_SECRET:
    raise SystemExit("Нет ASTER_API_KEY/ASTER_API_SECRET в .env")

# =========================================================
# REGEX (PAPER stdout parsing)
# =========================================================
OPEN_RE = re.compile(r"\[PAPER\]\s+OPEN\s+([A-Z0-9]+)\s+(LONG|SHORT)\s+entry=", re.I)
CLOSE_RE = re.compile(
    r"\[PAPER\]\s+CLOSE\s+([A-Z0-9]+)\s+(LONG|SHORT)\s+exit=.*?pnl=\s*\(\s*([-\+\d\.]+)\s*%\s*\)",
    re.I
)

# =========================================================
# HELPERS
# =========================================================
def utc_now_iso() -> str:
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
    with urllib.request.urlopen(req, timeout=20) as resp:
        raw = resp.read().decode("utf-8")
        return resp.status, json.loads(raw) if raw else {}

def quantize_down(x: Decimal, step: Decimal) -> Decimal:
    return (x / step).to_integral_value(rounding=ROUND_DOWN) * step

def parse_decimal(s: str, default=Decimal("0")) -> Decimal:
    try:
        return Decimal(str(s).strip())
    except Exception:
        return default

# =========================================================
# EXCHANGE/ACCOUNT QUERIES
# =========================================================
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

def position_risk(symbol: str) -> dict:
    _, pr = http_json("GET", "/fapi/v2/positionRisk", {"symbol": symbol}, signed=True)
    if isinstance(pr, list):
        pr = pr[0] if pr else {}
    return pr if isinstance(pr, dict) else {}

def position_amt(symbol: str) -> Decimal:
    pr = position_risk(symbol)
    return Decimal(str(pr.get("positionAmt", "0") or "0"))

def unrealized_pnl(symbol: str) -> Decimal:
    pr = position_risk(symbol)
    u = pr.get("unRealizedProfit")
    if u is None:
        entry = parse_decimal(pr.get("entryPrice", "0"))
        mark = parse_decimal(pr.get("markPrice", "0"))
        amt = parse_decimal(pr.get("positionAmt", "0"))
        return (mark - entry) * amt
    return parse_decimal(u)

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
    _, tr = http_json(
        "GET", "/fapi/v1/userTrades",
        {"symbol": symbol, "startTime": str(start_ms), "endTime": str(end_ms), "limit": "1000"},
        signed=True
    )
    return tr if isinstance(tr, list) else []

# =========================================================
# LOGGING
# =========================================================
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

# =========================================================
# COMMISSION & FLATTEN
# =========================================================
def compute_commission(symbol: str, entry_oid: str, tp_oid: str, sl_oid: str, close_oid: str,
                       entry_ms: int, exit_ms: int,
                       assumed_exit_is_maker: bool, qty: Decimal, entry_price: Decimal) -> Decimal:
    try:
        trades = get_user_trades(symbol, max(entry_ms - 10_000, 0), exit_ms + 10_000)
        comm = Decimal("0")
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

    notional = qty * entry_price
    entry_fee = notional * FEE_TAKER
    exit_fee = notional * (FEE_MAKER if assumed_exit_is_maker else FEE_TAKER)
    return entry_fee + exit_fee

def flatten_position(symbol: str) -> str:
    amt = position_amt(symbol)
    if amt == 0:
        cancel_all_open_orders(symbol)
        return ""

    side = "SELL" if amt > 0 else "BUY"
    qty = abs(amt)

    cancel_all_open_orders(symbol)

    _, close = http_json(
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
    return str(close.get("orderId", "") or "")

# =========================================================
# LIVE TRADE EXECUTION (blocks until position is closed)
# =========================================================
def place_entry_and_brackets(symbol: str, direction: str) -> dict:
    if not (LIVE_ENABLED and MIRROR_ENABLED):
        return {"symbol": symbol, "outcome": "SKIPPED_FLAGS", "net_pnl": Decimal("0")}

    if symbol in SKIP_SYMBOLS:
        return {"symbol": symbol, "outcome": "SKIPPED_SYMBOL", "net_pnl": Decimal("0")}

    if LIVE_ALLOW_SYMBOLS and symbol not in LIVE_ALLOW_SYMBOLS:
        return {"symbol": symbol, "outcome": "SKIPPED_NOT_ALLOWED", "net_pnl": Decimal("0")}

    if MAX_OPEN_POSITIONS <= 0:
        return {"symbol": symbol, "outcome": "SKIPPED_BAD_MAXPOS", "net_pnl": Decimal("0")}

    if position_amt(symbol) != 0:
        return {"symbol": symbol, "outcome": "SKIPPED_ALREADY_IN_POS", "net_pnl": Decimal("0")}

    cancel_all_open_orders(symbol)

    tick, step, minq = get_filters(symbol)

    _, p = http_json("GET", "/fapi/v1/ticker/price", {"symbol": symbol})
    last = Decimal(str(p.get("price")))

    qty = quantize_down((LIVE_NOTIONAL_USD * Decimal(LIVE_LEVERAGE) / last), step)

    if qty < minq:
        msg = f"[SAFE] {symbol}: qty={qty} < minQty={minq} при notional={LIVE_NOTIONAL_USD}, пропуск."
        print(msg)
        return {"symbol": symbol, "outcome": "SKIPPED_MINQ", "net_pnl": Decimal("0"), "note": msg}

    set_leverage(symbol, LIVE_LEVERAGE)

    side_entry = "BUY" if direction == "LONG" else "SELL"
    entry_ts = utc_now_iso()
    entry_ms = int(time.time() * 1000)

    print(f"[LIVE] ENTRY {symbol} {direction} market side={side_entry} qty={qty} last={last}")

    _, entry = http_json(
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
    entry_oid = str(entry.get("orderId", "") or "")
    time.sleep(0.5)

    od = get_order(symbol, entry_oid)
    if od.get("status") != "FILLED":
        msg = f"[SAFE] ENTRY not FILLED: {od.get('status')}"
        print(msg)
        return {"symbol": symbol, "outcome": "ENTRY_NOT_FILLED", "net_pnl": Decimal("0"), "note": msg}

    avg = Decimal(str(od.get("avgPrice") or last))
    print(f"[LIVE] FILLED {symbol} avg={avg} executedQty={od.get('executedQty')} orderId={entry_oid}")

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

    _, tp = http_json(
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
    tp_oid = str(tp.get("orderId", "") or "")
    print("[LIVE] TP placed:", tp.get("status"), "orderId=", tp_oid)

    _, sl = http_json(
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
    sl_oid = str(sl.get("orderId", "") or "")
    print("[LIVE] SL placed:", sl.get("status"), "orderId=", sl_oid)

    print(f"[WATCH] {symbol}: waiting position close...")

    t0 = time.time()
    timeout_checked = False
    close_oid = ""

    while True:
        amt = position_amt(symbol)
        if amt == 0:
            break

        if (not timeout_checked) and WATCH_PROFIT_TIMEOUT_SEC > 0 and (time.time() - t0) >= WATCH_PROFIT_TIMEOUT_SEC:
            timeout_checked = True
            try:
                upnl = unrealized_pnl(symbol)
                print(f"[WATCH] {symbol}: TIMEOUT check unrealizedPnL={upnl}")
                if upnl > 0:
                    print(f"[WATCH] {symbol}: TIMEOUT in profit -> close now (market reduceOnly)")
                    close_oid = flatten_position(symbol)
                    break
                else:
                    print(f"[WATCH] {symbol}: TIMEOUT in loss -> keep waiting TP/SL")
            except Exception as e:
                print(f"[WATCH] {symbol}: TIMEOUT check failed: {e} (continue waiting)")

        time.sleep(POLL_SEC)

    oo = open_orders(symbol)
    if oo:
        print(f"[WATCH] {symbol}: position closed -> cancel leftover {len(oo)} orders")
        cancel_all_open_orders(symbol)

    exit_ts = utc_now_iso()
    exit_ms = int(time.time() * 1000)

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
        ep = sl_od.get("avgPrice") or sl_od.get("price") or sl_od.get("stopPrice")
        exit_price = Decimal(str(ep or "0"))
        assumed_exit_is_maker = False
    elif close_oid:
        outcome = "TIMEOUT_PROFIT_CLOSE"
        cl = get_order(symbol, close_oid)
        ep = cl.get("avgPrice") or cl.get("price")
        exit_price = Decimal(str(ep or "0"))
        assumed_exit_is_maker = False
    else:
        _, p2 = http_json("GET", "/fapi/v1/ticker/price", {"symbol": symbol})
        exit_price = Decimal(str(p2.get("price") or "0"))
        assumed_exit_is_maker = False

    if direction == "LONG":
        gross = (exit_price - avg) * qty
    else:
        gross = (avg - exit_price) * qty

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
    return {"symbol": symbol, "outcome": outcome, "net_pnl": net}

# =========================================================
# PAPER -> LIVE COORDINATION (Strategy A)
# =========================================================
paper_open_events = queue.Queue()  # (symbol, direction, ts)
loss_streak = defaultdict(int)

armed_queue = deque()
active_symbol = None

state_lock = threading.Lock()
stop_event = threading.Event()

_last_pos_count_t = 0.0
_last_pos_count_v = 0

def count_open_positions_cached() -> int:
    global _last_pos_count_t, _last_pos_count_v
    now = time.time()
    if (now - _last_pos_count_t) < COUNT_POS_CACHE_SEC:
        return _last_pos_count_v
    pr = get_positions_any()
    n = 0
    for p in pr:
        try:
            amt = Decimal(str(p.get("positionAmt", "0") or "0"))
            if amt != 0:
                n += 1
        except Exception:
            pass
    _last_pos_count_t = now
    _last_pos_count_v = n
    return n

def arm_symbol(symbol: str):
    global active_symbol
    if symbol in SKIP_SYMBOLS:
        return
    if LIVE_ALLOW_SYMBOLS and symbol not in LIVE_ALLOW_SYMBOLS:
        # Do not arm symbols that will never be traded in LIVE.
        return
    with state_lock:
        if symbol == active_symbol:
            return
        if symbol in armed_queue:
            return
        armed_queue.append(symbol)
        print(f"[ARM] {symbol} armed for LIVE (paper loss-streak >= {LOSS_STREAK_TO_ARM}). Queue={list(armed_queue)}")

def handle_paper_close(symbol: str, pnl_pct: Decimal):
    if pnl_pct < 0:
        loss_streak[symbol] += 1
    else:
        loss_streak[symbol] = 0

    st = loss_streak[symbol]
    print(f"[STREAK] {symbol}: paper pnl%={pnl_pct} streak={st}")

    if st >= LOSS_STREAK_TO_ARM:
        arm_symbol(symbol)

def paper_reader_thread(proc: subprocess.Popen):
    try:
        for raw in proc.stdout:
            if stop_event.is_set():
                break
            line = raw.rstrip("\n")
            print(line)

            mcl = CLOSE_RE.search(line)
            if mcl:
                sym = mcl.group(1).upper()
                if sym not in SKIP_SYMBOLS:
                    pnl_pct = parse_decimal(mcl.group(3), default=Decimal("0"))
                    handle_paper_close(sym, pnl_pct)
                continue

            mop = OPEN_RE.search(line)
            if mop:
                sym = mop.group(1).upper()
                direction = mop.group(2).upper()
                paper_open_events.put((sym, direction, time.time()))
    except Exception as e:
        print(f"[PAPER] reader error: {e}")
    finally:
        stop_event.set()

def live_worker_thread():
    global active_symbol

    while not stop_event.is_set():
        with state_lock:
            if active_symbol is None and armed_queue:
                active_symbol = armed_queue[0]
                print(f"[LIVE-FOCUS] Active symbol set to {active_symbol} (from queue)")

        if active_symbol is None:
            time.sleep(0.5)
            continue

        if count_open_positions_cached() >= MAX_OPEN_POSITIONS:
            time.sleep(0.5)
            continue

        try:
            sym, direction, _ts = paper_open_events.get(timeout=1.0)
        except queue.Empty:
            continue

        if sym != active_symbol:
            continue

        try:
            res = place_entry_and_brackets(sym, direction)
        except Exception as e:
            print(f"[LIVE] ERROR while mirroring {sym}: {e}")
            continue

        net = res.get("net_pnl", Decimal("0"))
        try:
            net = Decimal(str(net))
        except Exception:
            net = Decimal("0")

        if net > 0:
            with state_lock:
                print(f"[LIVE-FOCUS] {active_symbol}: got positive LIVE trade (netPnL={net}) -> deactivate")
                loss_streak[active_symbol] = 0
                if armed_queue and armed_queue[0] == active_symbol:
                    armed_queue.popleft()
                else:
                    try:
                        armed_queue.remove(active_symbol)
                    except Exception:
                        pass
                active_symbol = None

def main():
    print("[MIRROR] Strategy A: PAPER loss-streak -> arm symbol -> LIVE until first netPnL>0")
    print("[MIRROR] Flags: LIVE_ENABLED=", LIVE_ENABLED, "MIRROR_ENABLED=", MIRROR_ENABLED)
    print("[MIRROR] Live: notional_usd=", LIVE_NOTIONAL_USD, "lev=", LIVE_LEVERAGE, "TP%=", TP_PCT*100, "SL%=", SL_PCT*100)
    print("[MIRROR] Live log:", LIVE_LOG_PATH)
    print("[MIRROR] LOSS_STREAK_TO_ARM=", LOSS_STREAK_TO_ARM, "MAX_OPEN_POSITIONS=", MAX_OPEN_POSITIONS)
    print("[MIRROR] WATCH_PROFIT_TIMEOUT_SEC=", WATCH_PROFIT_TIMEOUT_SEC)
    if SKIP_SYMBOLS:
        print("[MIRROR] SKIP_SYMBOLS:", ",".join(sorted(SKIP_SYMBOLS)))
    if LIVE_ALLOW_SYMBOLS:
        print("[MIRROR] LIVE_ALLOW_SYMBOLS:", ",".join(sorted(LIVE_ALLOW_SYMBOLS)))

    cmd = [sys.executable, "-u", "run_paper.py"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)

    t_reader = threading.Thread(target=paper_reader_thread, args=(proc,), daemon=True)
    t_live = threading.Thread(target=live_worker_thread, daemon=True)
    t_reader.start()
    t_live.start()

    try:
        while not stop_event.is_set():
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("[MIRROR] Ctrl+C -> stopping...")
    finally:
        stop_event.set()
        try:
            proc.terminate()
        except Exception:
            pass

if __name__ == "__main__":
    main()

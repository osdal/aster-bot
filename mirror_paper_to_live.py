# mirror_paper_to_live.py
import os, sys, time, re, hmac, hashlib, urllib.parse, urllib.request, json, subprocess, csv
from decimal import Decimal, ROUND_DOWN
from urllib.error import HTTPError
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

# =========================
# CONFIG
# =========================
BASE = os.getenv("ASTER_REST_BASE", "https://fapi.asterdex.com").rstrip("/")
API_KEY = os.getenv("ASTER_API_KEY")
API_SECRET = os.getenv("ASTER_API_SECRET")

LIVE_ENABLED   = (os.getenv("LIVE_ENABLED", "false").strip().lower() == "true")
MIRROR_ENABLED = (os.getenv("MIRROR_ENABLED", "false").strip().lower() == "true")

LIVE_NOTIONAL_USD = Decimal(os.getenv("LIVE_NOTIONAL_USD", "5"))
LIVE_LEVERAGE = int(os.getenv("LIVE_LEVERAGE", "2"))

TP_PCT = (Decimal(os.getenv("TP_PCT", "0.40")) / Decimal("100"))
SL_PCT = (Decimal(os.getenv("SL_PCT", "0.18")) / Decimal("100"))

RECV_WINDOW = "5000"

MAX_OPEN_POSITIONS = int(os.getenv("LIVE_MAX_POSITIONS", "1"))

COOLDOWN_SEC = int(os.getenv("COOLDOWN_AFTER_TRADE_SEC", "300"))
POLL_SEC = float(os.getenv("WATCH_POLL_SEC", "2.0"))

# Не закрываем насильно. Но не ждём бесконечно: если не закрылось за N секунд,
# прекращаем WATCH и идём дальше (позиция и ордера остаются).
WATCH_MAX_SEC = int(os.getenv("WATCH_MAX_SEC", "900"))  # 15 минут по умолчанию

# Разница paper-entry vs live-last: если слишком большая — пропускаем вход
MAX_DEVIATION_PCT = (Decimal(os.getenv("MAX_DEVIATION_PCT", "0.20")) / Decimal("100"))  # 0.20% по умолчанию

# Защитный буфер на notional
MIN_NOTIONAL_BUFFER_PCT = (Decimal(os.getenv("MIN_NOTIONAL_BUFFER_PCT", "5")) / Decimal("100"))  # +5%

# Символы в blacklist (через запятую)
SKIP_SYMBOLS = set(s.strip().upper() for s in os.getenv("SKIP_SYMBOLS", "BTCUSDT").split(",") if s.strip())

# Лог файл
LIVE_LOG_PATH = os.getenv("LIVE_LOG_PATH", r"data\live_trades.csv")

if not API_KEY or not API_SECRET:
    raise SystemExit("Нет ASTER_API_KEY/ASTER_API_SECRET в .env")

OPEN_RE = re.compile(r"\[PAPER\]\s+OPEN\s+([A-Z0-9]+)\s+(LONG|SHORT)\s+entry=([0-9]*\.?[0-9]+)", re.I)

# =========================
# HTTP helpers
# =========================
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

# =========================
# Utils
# =========================
def quantize_down(x: Decimal, step: Decimal) -> Decimal:
    return (x / step).to_integral_value(rounding=ROUND_DOWN) * step

def now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def ensure_log_path(path: str):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

def init_csv_if_needed(path: str):
    ensure_log_path(path)
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "ts",
                "symbol",
                "direction",
                "qty",
                "entry_price",
                "exit_price",
                "gross_pnl",
                "commission",
                "net_pnl",
                "reason",
                "entry_order_id",
                "tp_order_id",
                "sl_order_id",
                "duration_sec",
                "note",
            ])

def append_trade_log(path: str, row: dict):
    init_csv_if_needed(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            row.get("ts", ""),
            row.get("symbol", ""),
            row.get("direction", ""),
            row.get("qty", ""),
            row.get("entry_price", ""),
            row.get("exit_price", ""),
            row.get("gross_pnl", ""),
            row.get("commission", ""),
            row.get("net_pnl", ""),
            row.get("reason", ""),
            row.get("entry_order_id", ""),
            row.get("tp_order_id", ""),
            row.get("sl_order_id", ""),
            row.get("duration_sec", ""),
            row.get("note", ""),
        ])

# =========================
# Exchange info / filters
# =========================
_EXINFO_CACHE = None
_EXINFO_TS = 0

def get_exchange_info_cached(ttl_sec=60):
    global _EXINFO_CACHE, _EXINFO_TS
    if _EXINFO_CACHE and (time.time() - _EXINFO_TS) < ttl_sec:
        return _EXINFO_CACHE
    _, ex = http_json("GET", "/fapi/v1/exchangeInfo")
    _EXINFO_CACHE = ex
    _EXINFO_TS = time.time()
    return ex

def get_filters(symbol: str):
    ex = get_exchange_info_cached()
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

def get_last_price(symbol: str) -> Decimal:
    _, p = http_json("GET", "/fapi/v1/ticker/price", {"symbol": symbol})
    return Decimal(str(p.get("price")))

# =========================
# Positions / Orders
# =========================
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

def order_status(symbol: str, order_id: int):
    _, od = http_json("GET", "/fapi/v1/order", {"symbol": symbol, "orderId": str(order_id)}, signed=True)
    return od

def set_leverage(symbol: str, lev: int):
    # Если endpoint поддерживается — отлично. Если нет — просто игнорируем.
    try:
        http_json("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": str(lev)}, signed=True)
    except Exception:
        pass

# =========================
# Trades / PnL
# =========================
def user_trades(symbol: str, start_ms: int):
    # Берём сделки начиная с момента входа. На Binance Futures это /fapi/v1/userTrades.
    # На Aster обычно совместимо.
    try:
        _, tr = http_json("GET", "/fapi/v1/userTrades", {"symbol": symbol, "startTime": str(start_ms)}, signed=True)
        if isinstance(tr, list):
            return tr
    except Exception:
        return []
    return []

def compute_realized_pnl_and_commission(symbol: str, start_ms: int):
    tr = user_trades(symbol, start_ms)
    gross = Decimal("0")
    comm = Decimal("0")
    # userTrades обычно содержит realizedPnl + commission
    for t in tr:
        rp = t.get("realizedPnl")
        c  = t.get("commission")
        if rp is not None:
            try:
                gross += Decimal(str(rp))
            except Exception:
                pass
        if c is not None:
            try:
                comm += Decimal(str(c))
            except Exception:
                pass
    net = gross - comm
    return gross, comm, net

# =========================
# Core logic
# =========================
def place_entry_and_brackets(symbol: str, direction: str, paper_entry: Decimal):
    # Safety gates
    if not (LIVE_ENABLED and MIRROR_ENABLED):
        print("[SAFE] LIVE_ENABLED/MIRROR_ENABLED выключены. Пропуск.")
        return

    symbol = symbol.upper()
    direction = direction.upper()

    if symbol in SKIP_SYMBOLS:
        print(f"[SAFE] {symbol} в SKIP_SYMBOLS. Пропуск.")
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

    # чистим хвосты
    cancel_all_open_orders(symbol)

    # set leverage best-effort
    set_leverage(symbol, LIVE_LEVERAGE)

    tick, step, minq = get_filters(symbol)
    last = get_last_price(symbol)

    # фильтр по отклонению paper entry vs live last
    if paper_entry > 0:
        dev = (last - paper_entry).copy_abs() / paper_entry
        if dev > MAX_DEVIATION_PCT:
            print(f"[SAFE] {symbol} dev too high: paper={paper_entry} live_last={last} dev={dev*100:.4f}% > {MAX_DEVIATION_PCT*100}% -> skip")
            return

    # qty = notional*lev/price
    qty = quantize_down((LIVE_NOTIONAL_USD * Decimal(LIVE_LEVERAGE) / last), step)
    if qty < minq:
        qty = quantize_down(minq, step)

    # minNotional guard: если minQty заставляет входить на сумму больше бюджета — пропускаем
    budget_notional = (LIVE_NOTIONAL_USD * Decimal(LIVE_LEVERAGE)) * (Decimal("1") + MIN_NOTIONAL_BUFFER_PCT)
    min_notional = (minq * last)
    if min_notional > budget_notional:
        print(f"[SAFE] {symbol} minNotional too high: minQty={minq} last={last} -> {min_notional:.6f} > budget {budget_notional:.6f}. Skip.")
        return

    side_entry = "BUY" if direction == "LONG" else "SELL"
    print(f"[LIVE] ENTRY {symbol} {direction} market side={side_entry} qty={qty} last={last}")

    entry_ts_ms = int(time.time() * 1000)
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

    entry_oid = int(entry.get("orderId"))
    time.sleep(0.5)

    od = order_status(symbol, entry_oid)
    if od.get("status") != "FILLED":
        print("[SAFE] ENTRY не FILLED:", od.get("status"), "-> выходим без bracket.")
        return

    avg = Decimal(str(od.get("avgPrice") or last))
    executed_qty = Decimal(str(od.get("executedQty") or qty))
    print(f"[LIVE] FILLED {symbol} avg={avg} executedQty={executed_qty} orderId={entry_oid}")

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
            "quantity": format(executed_qty, "f"),
            "price": format(tp_price, "f"),
            "reduceOnly": "true",
            "newClientOrderId": f"mirror_tp_{int(time.time())}",
        },
        signed=True
    )
    tp_oid = int(tp.get("orderId"))
    print("[LIVE] TP placed:", tp.get("status"), "orderId=", tp_oid)

    # SL STOP_MARKET closePosition=true (без reduceOnly)
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
    sl_oid = int(sl.get("orderId"))
    print("[LIVE] SL placed:", sl.get("status"), "orderId=", sl_oid)

    # WATCH & LOG
    init_csv_if_needed(LIVE_LOG_PATH)
    print(f"[WATCH] {symbol}: waiting position close...")

    t0 = time.time()
    reason = "UNKNOWN"
    exit_price = ""
    gross_pnl = commission = net_pnl = ""
    note = ""

    while True:
        # timeout watch (не закрываем, просто перестаём ждать)
        if WATCH_MAX_SEC > 0 and (time.time() - t0) > WATCH_MAX_SEC:
            note = f"watch_timeout>{WATCH_MAX_SEC}s (position left open)"
            print(f"[WATCH] {symbol}: TIMEOUT watch -> leave position+orders as-is, continue.")
            # логируем как WATCH_TIMEOUT без PnL (реализованного) — он может быть 0
            g, c, n = compute_realized_pnl_and_commission(symbol, entry_ts_ms - 1000)
            gross_pnl = str(g)
            commission = str(c)
            net_pnl = str(n)
            reason = "WATCH_TIMEOUT"
            break

        amt = position_amt(symbol)
        if amt == 0:
            # позиция закрылась — определяем по какому ордеру
            try:
                tp_od = order_status(symbol, tp_oid)
                sl_od = order_status(symbol, sl_oid)
                if tp_od.get("status") == "FILLED":
                    reason = "TP"
                    exit_price = str(tp_od.get("avgPrice") or tp_od.get("price") or "")
                elif sl_od.get("status") == "FILLED":
                    reason = "SL"
                    exit_price = str(sl_od.get("avgPrice") or "")
                else:
                    reason = "CLOSED"
            except Exception:
                reason = "CLOSED"

            # PnL по фактическим trades
            g, c, n = compute_realized_pnl_and_commission(symbol, entry_ts_ms - 1000)
            gross_pnl = str(g)
            commission = str(c)
            net_pnl = str(n)

            # cleanup leftovers
            oo = open_orders(symbol)
            if oo:
                print(f"[WATCH] {symbol}: position closed -> cancel leftover {len(oo)} orders")
                cancel_all_open_orders(symbol)

            duration = int(time.time() - t0)
            append_trade_log(LIVE_LOG_PATH, {
                "ts": now_iso(),
                "symbol": symbol,
                "direction": direction,
                "qty": str(executed_qty),
                "entry_price": str(avg),
                "exit_price": exit_price,
                "gross_pnl": gross_pnl,
                "commission": commission,
                "net_pnl": net_pnl,
                "reason": reason,
                "entry_order_id": str(entry_oid),
                "tp_order_id": str(tp_oid),
                "sl_order_id": str(sl_oid),
                "duration_sec": str(duration),
                "note": note,
            })

            print(f"[WATCH] {symbol}: LOGGED -> {LIVE_LOG_PATH} reason={reason} netPnL={net_pnl}")
            print(f"[WATCH] {symbol}: DONE. Cooldown {COOLDOWN_SEC}s")
            time.sleep(COOLDOWN_SEC)
            return

        time.sleep(POLL_SEC)

    # если вышли по WATCH_TIMEOUT — логируем строку и выходим без отмены ордеров
    duration = int(time.time() - t0)
    append_trade_log(LIVE_LOG_PATH, {
        "ts": now_iso(),
        "symbol": symbol,
        "direction": direction,
        "qty": str(executed_qty),
        "entry_price": str(avg),
        "exit_price": exit_price,
        "gross_pnl": gross_pnl,
        "commission": commission,
        "net_pnl": net_pnl,
        "reason": reason,
        "entry_order_id": str(entry_oid),
        "tp_order_id": str(tp_oid),
        "sl_order_id": str(sl_oid),
        "duration_sec": str(duration),
        "note": note,
    })
    print(f"[WATCH] {symbol}: LOGGED (timeout) -> {LIVE_LOG_PATH} reason={reason} netPnL={net_pnl}")
    time.sleep(COOLDOWN_SEC)

def main():
    print("[MIRROR] Starting PAPER -> LIVE mirror")
    print("[MIRROR] Flags: LIVE_ENABLED=", LIVE_ENABLED, "MIRROR_ENABLED=", MIRROR_ENABLED)
    print("[MIRROR] Live: notional_usd=", LIVE_NOTIONAL_USD, "lev=", LIVE_LEVERAGE, "TP%=", TP_PCT*100, "SL%=", SL_PCT*100)
    print("[MIRROR] Live log:", LIVE_LOG_PATH)
    print("[MIRROR] SKIP_SYMBOLS:", ", ".join(sorted(SKIP_SYMBOLS)) if SKIP_SYMBOLS else "(none)")

    init_csv_if_needed(LIVE_LOG_PATH)

    # запуск paper бота как отдельный процесс и чтение stdout
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
                paper_entry = Decimal(m.group(3))

                try:
                    place_entry_and_brackets(symbol, direction, paper_entry)
                except Exception as e:
                    print("[MIRROR] ERROR while mirroring:", e)
    finally:
        try:
            p.terminate()
        except Exception:
            pass

if __name__ == "__main__":
    main()

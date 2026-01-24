import os, sys, time, re, hmac, hashlib, urllib.parse, urllib.request, json, subprocess, csv, threading, queue
from decimal import Decimal, ROUND_DOWN
from urllib.error import HTTPError
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

# =========================
# Config (.env)
# =========================
BASE = os.getenv("ASTER_REST_BASE", "https://fapi.asterdex.com").rstrip("/")
API_KEY = os.getenv("ASTER_API_KEY")
API_SECRET = os.getenv("ASTER_API_SECRET")

LIVE_ENABLED = (os.getenv("LIVE_ENABLED", "false").strip().lower() == "true")
MIRROR_ENABLED = (os.getenv("MIRROR_ENABLED", "false").strip().lower() == "true")

LIVE_NOTIONAL_USD = Decimal(os.getenv("LIVE_NOTIONAL_USD", "5"))
LIVE_LEVERAGE = int(os.getenv("LIVE_LEVERAGE", "2"))
TP_PCT = Decimal(os.getenv("TP_PCT", "0.40")) / Decimal("100")
SL_PCT = Decimal(os.getenv("SL_PCT", "0.18")) / Decimal("100")

POLL_SEC = float(os.getenv("WATCH_POLL_SEC", "2.0"))
COOLDOWN_SEC = int(os.getenv("COOLDOWN_AFTER_TRADE_SEC", "300"))
RECV_WINDOW = "5000"

MAX_OPEN_POSITIONS = int(os.getenv("LIVE_MAX_POSITIONS", "1"))

# Вариант A: НИКАКИХ WATCH_TIMEOUT, НИКАКИХ "leave as-is".
# Если вам нужно аварийное закрытие — это уже НЕ вариант A.
WATCH_HARD_TIMEOUT_SEC = int(os.getenv("WATCH_HARD_TIMEOUT_SEC", "0"))

# Остановиться при "непонятном" закрытии (позиция закрылась, но TP/SL не FILLED)
STOP_ON_UNEXPECTED_CLOSE = (os.getenv("STOP_ON_UNEXPECTED_CLOSE", "true").strip().lower() == "true")

# Пропуск символов (например "BTCUSDT,ASTERUSDT")
SKIP_SYMBOLS = set(s.strip().upper() for s in os.getenv("SKIP_SYMBOLS", "").split(",") if s.strip())

# CSV log path
LIVE_LOG_PATH = os.getenv("LIVE_LOG_PATH", r"data\live_trades.csv")

# Оценка комиссий, если userTrades недоступен (в долях от notional на сторону)
FEE_TAKER = Decimal(os.getenv("FEE_TAKER", "0.0006"))
FEE_MAKER = Decimal(os.getenv("FEE_MAKER", "0.0002"))

# Ограничение очереди сигналов OPEN, чтобы не копить бесконечно
MAX_SIGNAL_QUEUE = int(os.getenv("MAX_SIGNAL_QUEUE", "500"))

if not API_KEY or not API_SECRET:
    raise SystemExit("Нет ASTER_API_KEY/ASTER_API_SECRET в .env")

# Вариант A — строгие инварианты
if MAX_OPEN_POSITIONS != 1:
    raise SystemExit("Вариант A: LIVE_MAX_POSITIONS должен быть строго 1")
if WATCH_HARD_TIMEOUT_SEC != 0:
    raise SystemExit("Вариант A: WATCH_HARD_TIMEOUT_SEC должен быть 0 (таймауты запрещены)")

OPEN_RE = re.compile(r"\[PAPER\]\s+OPEN\s+([A-Z0-9]+)\s+(LONG|SHORT)\s+entry=", re.I)

# =========================
# Helpers
# =========================
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
    d = os.path.dirname(LIVE_LOG_PATH) or "."
    os.makedirs(d, exist_ok=True)
    if not os.path.exists(LIVE_LOG_PATH):
        with open(LIVE_LOG_PATH, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "entry_ts","exit_ts","symbol","side","qty",
                "entry_price","exit_price",
                "gross_pnl","commission","commission_asset","net_pnl",
                "outcome",
                "entry_order_id","tp_order_id","sl_order_id",
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
            row.get("commission_asset",""),
            str(row.get("net_pnl","")),
            row.get("outcome",""),
            row.get("entry_order_id",""),
            row.get("tp_order_id",""),
            row.get("sl_order_id",""),
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
    _, tr = http_json(
        "GET",
        "/fapi/v1/userTrades",
        {"symbol": symbol, "startTime": str(start_ms), "endTime": str(end_ms), "limit": "1000"},
        signed=True
    )
    return tr if isinstance(tr, list) else []

def avg_price_and_commission_from_trades(symbol: str, order_id: str, start_ms: int, end_ms: int):
    """
    Возвращает (avg_price, commission_sum, commission_asset).
    Если нет данных — (None, 0, "").
    """
    try:
        trades = get_user_trades(symbol, max(start_ms - 10_000, 0), end_ms + 10_000)
        qty_sum = Decimal("0")
        notional_sum = Decimal("0")
        comm_sum = Decimal("0")
        comm_asset = ""
        for t in trades:
            if str(t.get("orderId", "")) != str(order_id):
                continue
            q = Decimal(str(t.get("qty", "0") or "0"))
            p = Decimal(str(t.get("price", "0") or "0"))
            qty_sum += q
            notional_sum += q * p
            c = t.get("commission")
            if c is not None:
                comm_sum += Decimal(str(c))
                comm_asset = t.get("commissionAsset", comm_asset) or comm_asset
        if qty_sum > 0:
            return (notional_sum / qty_sum, comm_sum, comm_asset)
        return (None, comm_sum, comm_asset)
    except Exception:
        return (None, Decimal("0"), "")

def estimate_commission_fallback(notional: Decimal, exit_is_maker: bool):
    # entry MARKET => taker
    entry_fee = notional * FEE_TAKER
    exit_fee = notional * (FEE_MAKER if exit_is_maker else FEE_TAKER)
    return entry_fee + exit_fee

# =========================
# Core: Variant A (sequential TP/SL)
# =========================
def place_entry_and_wait_tp_sl(symbol: str, direction: str):
    # Gates
    if not (LIVE_ENABLED and MIRROR_ENABLED):
        print("[SAFE] LIVE_ENABLED/MIRROR_ENABLED выключены. Пропуск.")
        return

    if symbol in SKIP_SYMBOLS:
        print(f"[SAFE] SKIP_SYMBOLS: {symbol}. Пропуск.")
        return

    # Variant A: строго одна позиция в момент времени
    if count_open_positions() >= 1:
        # уже занято — не должны сюда попадать, т.к. очередь/лок,
        # но на всякий случай:
        return

    if position_amt(symbol) != 0:
        return

    # Clean
    cancel_all_open_orders(symbol)

    tick, step, minq = get_filters(symbol)

    # Price
    _, p = http_json("GET", "/fapi/v1/ticker/price", {"symbol": symbol})
    last = Decimal(str(p.get("price")))

    # qty = notional*lev/price
    qty = quantize_down((LIVE_NOTIONAL_USD * Decimal(LIVE_LEVERAGE) / last), step)

    # ВАЖНО: не увеличиваем до minQty (иначе риск "гигантской" сделки на дорогих инструментах).
    if qty < minq:
        print(f"[SAFE] {symbol}: qty={qty} < minQty={minq} при notional={LIVE_NOTIONAL_USD}, пропуск.")
        return

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
    entry_oid = str(entry.get("orderId", ""))

    time.sleep(0.5)
    od = get_order(symbol, entry_oid)
    if od.get("status") != "FILLED":
        print("[SAFE] ENTRY не FILLED:", od.get("status"), "-> выходим.")
        return

    entry_avg = Decimal(str(od.get("avgPrice") or last))
    print(f"[LIVE] FILLED {symbol} avg={entry_avg} executedQty={od.get('executedQty')} orderId={entry_oid}")

    # Brackets
    if direction == "LONG":
        tp_price = quantize_down(entry_avg * (Decimal("1") + TP_PCT), tick)
        sl_price = quantize_down(entry_avg * (Decimal("1") - SL_PCT), tick)
        tp_side = "SELL"
        sl_side = "SELL"
    else:
        tp_price = quantize_down(entry_avg * (Decimal("1") - TP_PCT), tick)
        sl_price = quantize_down(entry_avg * (Decimal("1") + SL_PCT), tick)
        tp_side = "BUY"
        sl_side = "BUY"

    print(f"[LIVE] BRACKETS {symbol} TP={tp_price} SL(stop)={sl_price}")

    # TP: LIMIT reduceOnly=true
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
    tp_oid = str(tp.get("orderId", ""))

    # SL: STOP_MARKET closePosition=true (без quantity)
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
    sl_oid = str(sl.get("orderId", ""))

    print("[LIVE] TP placed:", tp.get("status"), "orderId=", tp_oid)
    print("[LIVE] SL placed:", sl.get("status"), "orderId=", sl_oid)

    # WAIT: Variant A — ждать до тех пор, пока позиция станет 0
    print(f"[WATCH] {symbol}: waiting TP/SL close (Variant A, no timeout)...")
    t0 = time.time()

    while True:
        amt = position_amt(symbol)
        if amt == 0:
            break
        time.sleep(POLL_SEC)

    exit_ts = utc_now_iso()
    exit_ms = int(time.time() * 1000)
    duration_sec = int(max(0, exit_ms - entry_ms) / 1000)

    # Determine outcome: TP or SL must be FILLED
    tp_od = get_order(symbol, tp_oid) if tp_oid else {}
    sl_od = get_order(symbol, sl_oid) if sl_oid else {}

    outcome = "UNKNOWN"
    exit_is_maker = False
    exit_price = Decimal("0")
    commission_asset = ""
    commission = Decimal("0")

    # Комиссия за вход (по userTrades если получится) — иначе оценка
    entry_fill_price_ut, entry_comm_ut, entry_comm_asset = avg_price_and_commission_from_trades(
        symbol, entry_oid, entry_ms, exit_ms
    )
    # entry_avg уже есть; entry_fill_price_ut используем только для комиссии
    entry_comm = entry_comm_ut
    if entry_comm_asset:
        commission_asset = entry_comm_asset

    if tp_od.get("status") == "FILLED":
        outcome = "TP"
        exit_is_maker = True  # лимитка чаще maker, но зависит от исполнения
        # пробуем userTrades точный avg
        ex_avg_ut, ex_comm_ut, ex_comm_asset = avg_price_and_commission_from_trades(symbol, tp_oid, entry_ms, exit_ms)
        if ex_avg_ut is not None:
            exit_price = ex_avg_ut
        else:
            ep = tp_od.get("avgPrice") or tp_od.get("price")
            exit_price = Decimal(str(ep or "0"))
        commission += ex_comm_ut
        if ex_comm_asset:
            commission_asset = ex_comm_asset or commission_asset

    elif sl_od.get("status") == "FILLED":
        outcome = "SL"
        exit_is_maker = False
        ex_avg_ut, ex_comm_ut, ex_comm_asset = avg_price_and_commission_from_trades(symbol, sl_oid, entry_ms, exit_ms)
        if ex_avg_ut is not None:
            exit_price = ex_avg_ut
        else:
            ep = sl_od.get("avgPrice") or sl_od.get("price")
            exit_price = Decimal(str(ep or "0"))
        commission += ex_comm_ut
        if ex_comm_asset:
            commission_asset = ex_comm_asset or commission_asset

    else:
        # Позиция закрылась, но TP/SL не FILLED => это нарушение инварианта Variant A.
        note = f"Position closed but TP/SL not FILLED. tp={tp_od.get('status')} sl={sl_od.get('status')}"
        print(f"[ALERT] {symbol}: {note}")

        # Чистим хвостовые ордера (если остались)
        try:
            oo = open_orders(symbol)
            if oo:
                cancel_all_open_orders(symbol)
        except Exception:
            pass

        # Логируем как ошибку с приблизительной ценой last (только чтобы запись была)
        _, p2 = http_json("GET", "/fapi/v1/ticker/price", {"symbol": symbol})
        exit_price = Decimal(str(p2.get("price") or "0"))
        outcome = "UNEXPECTED_CLOSE"

        # комиссия (если userTrades есть — попробуем суммарно за окно; иначе fallback)
        try:
            # суммируем все комиссии по userTrades в окне
            trades = get_user_trades(symbol, max(entry_ms - 10_000, 0), exit_ms + 10_000)
            comm_sum = Decimal("0")
            for t in trades:
                c = t.get("commission")
                if c is None:
                    continue
                comm_sum += Decimal(str(c))
                commission_asset = t.get("commissionAsset", commission_asset) or commission_asset
            commission = comm_sum
        except Exception:
            notional = qty * entry_avg
            commission = estimate_commission_fallback(notional, exit_is_maker=False)
            commission_asset = "USDT_EST"

        gross = (exit_price - entry_avg) * qty if direction == "LONG" else (entry_avg - exit_price) * qty
        net = gross - commission

        log_live_trade({
            "entry_ts": entry_ts,
            "exit_ts": exit_ts,
            "symbol": symbol,
            "side": direction,
            "qty": qty,
            "entry_price": entry_avg,
            "exit_price": exit_price,
            "gross_pnl": gross,
            "commission": commission,
            "commission_asset": commission_asset,
            "net_pnl": net,
            "outcome": outcome,
            "entry_order_id": entry_oid,
            "tp_order_id": tp_oid,
            "sl_order_id": sl_oid,
            "duration_sec": duration_sec,
            "note": note,
        })

        print(f"[WATCH] {symbol}: LOGGED -> {LIVE_LOG_PATH} outcome={outcome} netPnL={net}")

        if STOP_ON_UNEXPECTED_CLOSE:
            raise RuntimeError(f"Variant A violated: {note}")
        # иначе просто выходим (но это уже мягче, чем Variant A по духу)
        return

    # Cleanup leftover orders
    try:
        oo = open_orders(symbol)
        if oo:
            print(f"[WATCH] {symbol}: position closed -> cancel leftover {len(oo)} orders")
            cancel_all_open_orders(symbol)
    except Exception:
        pass

    # Commission: если userTrades по выходу не дали комиссию (0), то fallback
    commission += entry_comm

    if commission == 0:
        notional = qty * entry_avg
        commission = estimate_commission_fallback(notional, exit_is_maker=exit_is_maker)
        commission_asset = "USDT_EST"

    # Gross/Net PnL
    gross = (exit_price - entry_avg) * qty if direction == "LONG" else (entry_avg - exit_price) * qty
    net = gross - commission

    log_live_trade({
        "entry_ts": entry_ts,
        "exit_ts": exit_ts,
        "symbol": symbol,
        "side": direction,
        "qty": qty,
        "entry_price": entry_avg,
        "exit_price": exit_price,
        "gross_pnl": gross,
        "commission": commission,
        "commission_asset": commission_asset,
        "net_pnl": net,
        "outcome": outcome,
        "entry_order_id": entry_oid,
        "tp_order_id": tp_oid,
        "sl_order_id": sl_oid,
        "duration_sec": duration_sec,
        "note": "",
    })

    print(f"[WATCH] {symbol}: LOGGED -> {LIVE_LOG_PATH} outcome={outcome} netPnL={net}")
    print(f"[WATCH] {symbol}: DONE. Cooldown {COOLDOWN_SEC}s")
    time.sleep(COOLDOWN_SEC)

# =========================
# IO Thread: read PAPER stdout continuously
# =========================
def stdout_reader(proc, sig_q: queue.Queue, stop_event: threading.Event):
    try:
        for raw in proc.stdout:
            if stop_event.is_set():
                break
            line = raw.rstrip("\n")
            print(line)

            m = OPEN_RE.search(line)
            if m:
                symbol = m.group(1).upper()
                direction = m.group(2).upper()

                # В очередь — но ограничиваем размер
                if sig_q.qsize() >= MAX_SIGNAL_QUEUE:
                    print(f"[MIRROR] SIGNAL QUEUE FULL ({MAX_SIGNAL_QUEUE}), drop: {symbol} {direction}")
                    continue

                sig_q.put((time.time(), symbol, direction))
    except Exception as e:
        print("[MIRROR] stdout_reader error:", e)

# =========================
# Main loop: sequential processing (Variant A)
# =========================
def main():
    print("[MIRROR] Starting PAPER -> LIVE mirror (Variant A)")
    print("[MIRROR] Flags: LIVE_ENABLED=", LIVE_ENABLED, "MIRROR_ENABLED=", MIRROR_ENABLED)
    print("[MIRROR] Live: notional_usd=", LIVE_NOTIONAL_USD, "lev=", LIVE_LEVERAGE, "TP%=", TP_PCT*100, "SL%=", SL_PCT*100)
    print("[MIRROR] Live log:", LIVE_LOG_PATH)
    if SKIP_SYMBOLS:
        print("[MIRROR] SKIP_SYMBOLS:", ",".join(sorted(SKIP_SYMBOLS)))

    # Start paper bot
    cmd = [sys.executable, "-u", "run_paper.py"]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)

    sig_q = queue.Queue()
    stop_event = threading.Event()

    t = threading.Thread(target=stdout_reader, args=(p, sig_q, stop_event), daemon=True)
    t.start()

    try:
        # Sequential worker loop
        while True:
            # Если процесс paper умер — выходим
            if p.poll() is not None:
                raise RuntimeError(f"run_paper.py stopped with code {p.returncode}")

            # Если уже есть открытая позиция (вдруг руками) — Variant A ждёт, ничего не открывает
            if count_open_positions() >= 1:
                time.sleep(1.0)
                continue

            # Ждём сигнал
            try:
                _, symbol, direction = sig_q.get(timeout=1.0)
            except queue.Empty:
                continue

            # Перед исполнением ещё раз проверка: Variant A
            if count_open_positions() >= 1:
                # не должны сюда попадать, но ок
                continue

            if symbol in SKIP_SYMBOLS:
                print(f"[SAFE] SKIP_SYMBOLS: {symbol}. Пропуск.")
                continue

            try:
                place_entry_and_wait_tp_sl(symbol, direction)
            except Exception as e:
                print("[MIRROR] ERROR:", e)
                if STOP_ON_UNEXPECTED_CLOSE:
                    raise

    except KeyboardInterrupt:
        print("\n[MIRROR] Stop requested (Ctrl+C).")
    finally:
        stop_event.set()
        try:
            p.terminate()
        except Exception:
            pass

if __name__ == "__main__":
    main()

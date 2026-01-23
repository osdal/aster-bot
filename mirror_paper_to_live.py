import os, sys, time, re, hmac, hashlib, urllib.parse, urllib.request, json, subprocess, csv
from decimal import Decimal, ROUND_DOWN
from urllib.error import HTTPError
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

# =========================
# ENV / CONFIG
# =========================
BASE = os.getenv("ASTER_REST_BASE", "https://fapi.asterdex.com").rstrip("/")
API_KEY = os.getenv("ASTER_API_KEY")
API_SECRET = os.getenv("ASTER_API_SECRET")

LIVE_ENABLED   = (os.getenv("LIVE_ENABLED", "false").strip().lower() == "true")
MIRROR_ENABLED = (os.getenv("MIRROR_ENABLED", "false").strip().lower() == "true")

LIVE_NOTIONAL_USD = Decimal(os.getenv("LIVE_NOTIONAL_USD", "5"))
LIVE_LEVERAGE     = int(os.getenv("LIVE_LEVERAGE", "2"))

TP_PCT = (Decimal(os.getenv("TP_PCT", "0.40")) / Decimal("100"))   # 0.40% -> 0.004
SL_PCT = (Decimal(os.getenv("SL_PCT", "0.18")) / Decimal("100"))   # 0.18% -> 0.0018

COOLDOWN_SEC = int(os.getenv("COOLDOWN_AFTER_TRADE_SEC", "300"))
POLL_SEC     = float(os.getenv("WATCH_POLL_SEC", "2.0"))
RECV_WINDOW  = "5000"

MAX_OPEN_POSITIONS = int(os.getenv("LIVE_MAX_POSITIONS", "1"))  # ваш режим: 1

# Символы, которые не торгуем (пример: "BTCUSDT,ETHUSDT")
SKIP_SYMBOLS = set(s.strip().upper() for s in os.getenv("SKIP_SYMBOLS", "").split(",") if s.strip())

# Лог live сделок
LIVE_LOG_PATH = os.path.join("data", "live_trades.csv")

# Если true — при Ctrl+C сделаем “пожарную остановку”: закроем позицию и отменим ордера по текущему символу
# По умолчанию false: при остановке оставляем рынок как есть.
CLEANUP_ON_EXIT = (os.getenv("CLEANUP_ON_EXIT", "false").strip().lower() == "true")

if not API_KEY or not API_SECRET:
    raise SystemExit("Нет ASTER_API_KEY/ASTER_API_SECRET в .env")

OPEN_RE = re.compile(r"\[PAPER\]\s+OPEN\s+([A-Z0-9]+)\s+(LONG|SHORT)\s+entry=", re.I)

# =========================
# HELPERS
# =========================
def utc_ts() -> str:
    # timezone-aware UTC timestamp, no utcnow()
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def ensure_log_file(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "ts_open_utc",
                "ts_close_utc",
                "symbol",
                "direction",
                "qty",
                "entry_price",
                "exit_price",
                "reason",
                "gross_pnl_usd",
                "fees_usd",
                "net_pnl_usd",
                "entry_order_id",
                "exit_order_id",
                "tp_order_id",
                "sl_order_id",
            ])

def append_live_trade(row: dict):
    ensure_log_file(LIVE_LOG_PATH)
    with open(LIVE_LOG_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            row.get("ts_open_utc", ""),
            row.get("ts_close_utc", ""),
            row.get("symbol", ""),
            row.get("direction", ""),
            row.get("qty", ""),
            row.get("entry_price", ""),
            row.get("exit_price", ""),
            row.get("reason", ""),
            row.get("gross_pnl_usd", ""),
            row.get("fees_usd", ""),
            row.get("net_pnl_usd", ""),
            row.get("entry_order_id", ""),
            row.get("exit_order_id", ""),
            row.get("tp_order_id", ""),
            row.get("sl_order_id", ""),
        ])

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
    # не все биржи/прокси строго требуют — но если endpoint поддерживается, это полезно
    try:
        http_json("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": str(lev)}, signed=True)
    except Exception:
        # не падаем из-за этого
        pass

def get_order(symbol: str, order_id: int):
    _, od = http_json("GET", "/fapi/v1/order", {"symbol": symbol, "orderId": str(order_id)}, signed=True)
    return od if isinstance(od, dict) else {}

def get_user_trades(symbol: str, start_time_ms: int, end_time_ms: int):
    # Binance-like endpoint: /fapi/v1/userTrades
    try:
        _, tr = http_json(
            "GET", "/fapi/v1/userTrades",
            {"symbol": symbol, "startTime": str(start_time_ms), "endTime": str(end_time_ms)},
            signed=True
        )
        return tr if isinstance(tr, list) else []
    except Exception:
        return []

def sum_fees_usd_from_trades(trades: list, order_ids: set) -> Decimal:
    # Суммируем комиссии только по нужным orderId, и только если комиссия в USDT
    fees = Decimal("0")
    for t in trades:
        try:
            oid = int(t.get("orderId"))
            if oid not in order_ids:
                continue
            comm = Decimal(str(t.get("commission", "0") or "0"))
            asset = (t.get("commissionAsset") or "").upper()
            if asset == "USDT":
                fees += comm
        except Exception:
            continue
    return fees

def emergency_flatten_symbol(symbol: str):
    # Отмена хвостов и закрытие позиции маркетом
    try:
        cancel_all_open_orders(symbol)
    except Exception:
        pass

    try:
        amt = position_amt(symbol)
        if amt == 0:
            return
        side = "SELL" if amt > 0 else "BUY"
        qty = abs(amt)
        http_json(
            "POST", "/fapi/v1/order",
            {
                "symbol": symbol,
                "side": side,
                "type": "MARKET",
                "quantity": format(qty, "f"),
                "newClientOrderId": f"emg_close_{int(time.time())}",
            },
            signed=True
        )
    except Exception:
        pass

# =========================
# CORE
# =========================
def place_entry_and_brackets(symbol: str, direction: str):
    if symbol in SKIP_SYMBOLS:
        print(f"[SAFE] {symbol} в SKIP_SYMBOLS. Пропуск.")
        return

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

    # на всякий случай чистим хвосты
    cancel_all_open_orders(symbol)

    # выставим leverage (если возможно)
    set_leverage(symbol, LIVE_LEVERAGE)

    tick, step, minq = get_filters(symbol)

    # текущая цена
    _, p = http_json("GET", "/fapi/v1/ticker/price", {"symbol": symbol})
    last = Decimal(str(p.get("price")))

    # qty = notional*lev/price
    qty = quantize_down((LIVE_NOTIONAL_USD * Decimal(LIVE_LEVERAGE) / last), step)
    if qty < minq:
        qty = quantize_down(minq, step)

    side_entry = "BUY" if direction == "LONG" else "SELL"
    ts_open = utc_ts()
    t0_ms = int(time.time() * 1000)

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
    entry_oid = int(entry.get("orderId"))
    time.sleep(0.5)

    # confirm filled
    od = get_order(symbol, entry_oid)
    if od.get("status") != "FILLED":
        print("[SAFE] ENTRY не FILLED:", od.get("status"), "-> выходим без bracket.")
        return

    avg = Decimal(str(od.get("avgPrice") or last))
    print(f"[LIVE] FILLED {symbol} avg={avg} executedQty={od.get('executedQty')} orderId={entry_oid}")

    # brackets prices
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
    tp_oid = int(tp.get("orderId"))
    print("[LIVE] TP placed:", tp.get("status"), "orderId=", tp_oid)

    # SL STOP_MARKET closePosition=true (без reduceOnly, чтобы не ловить -1106)
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

    # WATCH until TP/SL реально закрыли позицию
    print(f"[WATCH] {symbol}: waiting position close (TP/SL only)...")
    try:
        while True:
            amt = position_amt(symbol)
            if amt == 0:
                break
            time.sleep(POLL_SEC)
    except KeyboardInterrupt:
        print(f"[WATCH] {symbol}: Ctrl+C received.")
        if CLEANUP_ON_EXIT:
            print(f"[WATCH] {symbol}: CLEANUP_ON_EXIT=true -> emergency flatten.")
            emergency_flatten_symbol(symbol)
        raise

    # позиция закрыта — определим причину (TP/SL/UNKNOWN), цену выхода и комиссии
    ts_close = utc_ts()
    t1_ms = int(time.time() * 1000)

    tp_od = get_order(symbol, tp_oid)
    sl_od = get_order(symbol, sl_oid)

    reason = "UNKNOWN"
    exit_oid = None
    exit_price = None

    if tp_od.get("status") == "FILLED":
        reason = "TP"
        exit_oid = tp_oid
        exit_price = Decimal(str(tp_od.get("avgPrice") or tp_od.get("price") or "0"))
    elif sl_od.get("status") == "FILLED":
        reason = "SL"
        exit_oid = sl_oid
        exit_price = Decimal(str(sl_od.get("avgPrice") or sl_od.get("price") or "0"))
    else:
        # На всякий случай: позиция могла закрыться руками/ликвидацией/другим ордером.
        # Пытаемся хоть как-то получить exit через последние userTrades по символу.
        reason = "UNKNOWN"

    # Если exit_price не смогли достать из ордера — попробуем по userTrades
    order_ids = {entry_oid, tp_oid, sl_oid}
    trades = get_user_trades(symbol, start_time_ms=max(0, t0_ms - 10_000), end_time_ms=t1_ms + 10_000)

    if exit_price is None or exit_price == 0:
        # найдём последнюю сделку по symbol и возьмём её цену как approximation
        # (лучше чем пусто)
        try:
            # берем последнюю по времени
            trades_sorted = sorted(trades, key=lambda x: int(x.get("time", 0)))
            if trades_sorted:
                exit_price = Decimal(str(trades_sorted[-1].get("price") or "0"))
        except Exception:
            exit_price = Decimal("0")

    # gross pnl
    if direction == "LONG":
        gross = (exit_price - avg) * qty
    else:
        gross = (avg - exit_price) * qty

    # fees in USDT (если endpoint отдаёт)
    fees = sum_fees_usd_from_trades(trades, order_ids)
    net = gross - fees

    # отменим хвосты (обычно один ордер останется NEW)
    oo = open_orders(symbol)
    if oo:
        print(f"[WATCH] {symbol}: position closed -> cancel leftover {len(oo)} orders")
        cancel_all_open_orders(symbol)

    append_live_trade({
        "ts_open_utc": ts_open,
        "ts_close_utc": ts_close,
        "symbol": symbol,
        "direction": direction,
        "qty": format(qty, "f"),
        "entry_price": format(avg, "f"),
        "exit_price": format(exit_price, "f"),
        "reason": reason,
        "gross_pnl_usd": format(gross, "f"),
        "fees_usd": format(fees, "f"),
        "net_pnl_usd": format(net, "f"),
        "entry_order_id": str(entry_oid),
        "exit_order_id": str(exit_oid) if exit_oid else "",
        "tp_order_id": str(tp_oid),
        "sl_order_id": str(sl_oid),
    })

    print(f"[WATCH] {symbol}: LOGGED -> {LIVE_LOG_PATH} reason={reason} netPnL={format(net, 'f')}")
    print(f"[WATCH] {symbol}: DONE. Cooldown {COOLDOWN_SEC}s")
    time.sleep(COOLDOWN_SEC)

def main():
    print("[MIRROR] Starting PAPER -> LIVE mirror")
    print("[MIRROR] Flags: LIVE_ENABLED=", LIVE_ENABLED, "MIRROR_ENABLED=", MIRROR_ENABLED)
    print("[MIRROR] Live: notional_usd=", LIVE_NOTIONAL_USD, "lev=", LIVE_LEVERAGE, "TP%=", TP_PCT*100, "SL%=", SL_PCT*100)
    print("[MIRROR] Live log:", LIVE_LOG_PATH)
    if SKIP_SYMBOLS:
        print("[MIRROR] SKIP_SYMBOLS:", ",".join(sorted(SKIP_SYMBOLS)))
    if MAX_OPEN_POSITIONS != 1:
        print("[MIRROR] WARNING: LIVE_MAX_POSITIONS != 1 (сейчас", MAX_OPEN_POSITIONS, ")")

    # ensure log file exists
    ensure_log_file(LIVE_LOG_PATH)

    # запускаем paper бота как отдельный процесс и читаем stdout
    cmd = [sys.executable, "-u", "run_paper.py"]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)

    current_live_symbol = None
    try:
        for line in p.stdout:
            line = line.rstrip("\n")
            print(line)

            m = OPEN_RE.search(line)
            if not m:
                continue

            symbol = m.group(1).upper()
            direction = m.group(2).upper()

            # Запоминаем, чтобы при Ctrl+C можно было “пожарно” закрыть
            current_live_symbol = symbol

            try:
                place_entry_and_brackets(symbol, direction)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                print("[MIRROR] ERROR while mirroring:", e)

    except KeyboardInterrupt:
        print("[MIRROR] Ctrl+C -> stopping.")
        if CLEANUP_ON_EXIT and current_live_symbol:
            print("[MIRROR] CLEANUP_ON_EXIT=true -> emergency flatten last symbol:", current_live_symbol)
            emergency_flatten_symbol(current_live_symbol)
    finally:
        try:
            p.terminate()
        except Exception:
            pass

if __name__ == "__main__":
    main()

# mirror_paper_to_live.py
# Mirrors PAPER trades (stdout from run_paper.py) into LIVE market entries + TP/SL brackets on Aster (Binance-Futures-like API),
# and logs LIVE trades with realized PnL/commission into data/live_trades.csv.

import os
import sys
import time
import re
import csv
import hmac
import json
import hashlib
import urllib.parse
import urllib.request
import subprocess
from pathlib import Path
from decimal import Decimal, ROUND_DOWN
from urllib.error import HTTPError

from dotenv import load_dotenv
load_dotenv()

# ------------------------
# ENV / CONFIG
# ------------------------
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

# PnL fetch tuning
PNL_FETCH_BUFFER_MS = int(os.getenv("PNL_FETCH_BUFFER_MS", "30000"))  # 30s back in time from entry timestamp
USERTRADES_LIMIT = int(os.getenv("USERTRADES_LIMIT", "1000"))
USERTRADES_SLEEP_SEC = float(os.getenv("USERTRADES_SLEEP_SEC", "0.20"))  # small delay between pages

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
LIVE_LOG = DATA_DIR / "live_trades.csv"

if not API_KEY or not API_SECRET:
    raise SystemExit("Нет ASTER_API_KEY/ASTER_API_SECRET в .env")

# Example line:
# [PAPER] OPEN SOLUSDT SHORT entry=127.08 tp=126.572 sl=127.309
OPEN_RE = re.compile(r"\[PAPER\]\s+OPEN\s+([A-Z0-9]+)\s+(LONG|SHORT)\s+entry=", re.I)

# ------------------------
# HTTP / SIGNING
# ------------------------
def sign(params: dict) -> str:
    q = urllib.parse.urlencode(params, doseq=True)
    sig = hmac.new(API_SECRET.encode("utf-8"), q.encode("utf-8"), hashlib.sha256).hexdigest()
    return q + "&signature=" + sig

def http_json(method: str, path: str, params=None, signed=False):
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

# ------------------------
# HELPERS
# ------------------------
def quantize_down(x: Decimal, step: Decimal) -> Decimal:
    return (x / step).to_integral_value(rounding=ROUND_DOWN) * step

def ensure_live_log_header():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not LIVE_LOG.exists():
        with LIVE_LOG.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "ts",
                "symbol",
                "direction",
                "entry_order_id",
                "tp_order_id",
                "sl_order_id",
                "entry_avg_price",
                "exit_avg_price",
                "realized_pnl",
                "commission",
                "net_pnl",
                "holding_sec",
                "exit_reason"
            ])

def log_live_trade(row: dict):
    ensure_live_log_header()
    with LIVE_LOG.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            row.get("ts"),
            row.get("symbol"),
            row.get("direction"),
            row.get("entry_order_id"),
            row.get("tp_order_id"),
            row.get("sl_order_id"),
            row.get("entry_avg_price"),
            row.get("exit_avg_price"),
            row.get("realized_pnl"),
            row.get("commission"),
            row.get("net_pnl"),
            row.get("holding_sec"),
            row.get("exit_reason"),
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
    # Try to fetch all positions at once (if endpoint supports no symbol param)
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

def get_order(symbol: str, order_id: int):
    _, od = http_json("GET", "/fapi/v1/order", {"symbol": symbol, "orderId": str(order_id)}, signed=True)
    return od if isinstance(od, dict) else {}

def get_usertTrades_paged(symbol: str, start_ms: int, end_ms: int):
    """
    Fetch /fapi/v1/userTrades in pages (Binance-Futures style).
    Expected fields per trade (if supported): time, price, qty, side, realizedPnl, commission, commissionAsset, orderId.
    """
    all_trades = []
    cursor_start = int(start_ms)
    end_ms = int(end_ms)

    while True:
        params = {
            "symbol": symbol,
            "startTime": str(cursor_start),
            "endTime": str(end_ms),
            "limit": str(USERTRADES_LIMIT),
        }
        _, trades = http_json("GET", "/fapi/v1/userTrades", params, signed=True)
        if not isinstance(trades, list) or not trades:
            break

        all_trades.extend(trades)

        # If we got a full page, advance startTime to last trade time + 1
        if len(trades) < USERTRADES_LIMIT:
            break

        # Advance cursor by max(time)+1 to avoid infinite loop on same page
        last_t = 0
        for t in trades:
            try:
                last_t = max(last_t, int(t.get("time", 0) or 0))
            except Exception:
                pass
        if last_t <= cursor_start:
            break
        cursor_start = last_t + 1

        time.sleep(USERTRADES_SLEEP_SEC)

    return all_trades

def compute_pnl_from_trades(trades: list, direction: str):
    """
    Compute realized pnl and commission totals from userTrades list.
    Also compute exit VWAP from closing-side trades.
    """
    realized = Decimal("0")
    commission = Decimal("0")

    # For futures, closing trades side is opposite of entry direction:
    # LONG closes with SELL; SHORT closes with BUY.
    close_side = "SELL" if direction == "LONG" else "BUY"

    exit_qty = Decimal("0")
    exit_notional = Decimal("0")

    for t in trades:
        # realizedPnl
        rp = t.get("realizedPnl", None)
        if rp is not None:
            try:
                realized += Decimal(str(rp))
            except Exception:
                pass

        # commission
        cm = t.get("commission", None)
        if cm is not None:
            try:
                commission += Decimal(str(cm))
            except Exception:
                pass

        # exit vwap based on closing-side fills
        try:
            side = str(t.get("side", "")).upper()
            if side == close_side:
                price = Decimal(str(t.get("price", "0") or "0"))
                qty = Decimal(str(t.get("qty", "0") or "0"))
                if price > 0 and qty > 0:
                    exit_qty += qty
                    exit_notional += price * qty
        except Exception:
            pass

    exit_avg = (exit_notional / exit_qty) if exit_qty > 0 else None
    net = realized - commission
    return realized, commission, net, exit_avg

def fetch_live_pnl(symbol: str, direction: str, entry_ts_ms: int, close_ts_ms: int):
    """
    Returns (realized_pnl, commission, net_pnl, exit_avg_price) as Decimals (or None for exit_avg_price).
    Uses /fapi/v1/userTrades if available.
    """
    start_ms = max(0, int(entry_ts_ms) - PNL_FETCH_BUFFER_MS)
    end_ms = int(close_ts_ms) + 2000  # small buffer to include last fill

    trades = get_usertTrades_paged(symbol, start_ms, end_ms)

    # Filter strictly within [entry_ts_ms, close_ts_ms+buffer], and only for this symbol already.
    filtered = []
    for t in trades:
        try:
            tt = int(t.get("time", 0) or 0)
            if tt >= entry_ts_ms and tt <= end_ms:
                filtered.append(t)
        except Exception:
            # if missing time, keep it (better than losing)
            filtered.append(t)

    realized, commission, net, exit_avg = compute_pnl_from_trades(filtered, direction)
    return realized, commission, net, exit_avg

# ------------------------
# CORE: ENTRY + BRACKETS + WATCH + LOG
# ------------------------
def place_entry_and_brackets(symbol: str, direction: str):
    # Safety gates
    if not (LIVE_ENABLED and MIRROR_ENABLED):
        print("[SAFE] LIVE_ENABLED/MIRROR_ENABLED выключены. Пропуск.")
        return

    # Safety: limit open positions
    npos = count_open_positions()
    if npos >= MAX_OPEN_POSITIONS:
        print(f"[SAFE] Уже открыто позиций: {npos} (лимит {MAX_OPEN_POSITIONS}). Пропуск.")
        return

    # Safety: don't enter if already in position on this symbol
    if position_amt(symbol) != 0:
        print(f"[SAFE] Уже есть позиция по {symbol}. Пропуск.")
        return

    # Cleanup leftovers
    cancel_all_open_orders(symbol)

    tick, step, minq = get_filters(symbol)

    # Current price
    _, p = http_json("GET", "/fapi/v1/ticker/price", {"symbol": symbol})
    last = Decimal(str(p.get("price")))

    # qty = notional * leverage / price
    qty = quantize_down((LIVE_NOTIONAL_USD * Decimal(LIVE_LEVERAGE) / last), step)
    if qty < minq:
        qty = quantize_down(minq, step)

    side_entry = "BUY" if direction == "LONG" else "SELL"
    entry_client_id = f"mirror_entry_{int(time.time())}"
    entry_ts_ms = int(time.time() * 1000)

    print(f"[LIVE] ENTRY {symbol} {direction} market side={side_entry} qty={qty} last={last}")

    _, entry = http_json(
        "POST", "/fapi/v1/order",
        {
            "symbol": symbol,
            "side": side_entry,
            "type": "MARKET",
            "quantity": format(qty, "f"),
            "newClientOrderId": entry_client_id,
        },
        signed=True
    )
    entry_oid = entry.get("orderId")
    time.sleep(0.5)

    # Confirm FILLED
    od = get_order(symbol, int(entry_oid))
    if od.get("status") != "FILLED":
        print("[SAFE] ENTRY не FILLED:", od.get("status"), "-> выходим без bracket.")
        return

    avg = Decimal(str(od.get("avgPrice") or last))
    executed_qty = od.get("executedQty")

    print(f"[LIVE] FILLED {symbol} avg={avg} executedQty={executed_qty} orderId={entry_oid}")

    # Brackets
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
    tp_id = tp.get("orderId")
    print("[LIVE] TP placed:", tp.get("status"), "orderId=", tp_id)

    # SL STOP_MARKET closePosition=true (without reduceOnly to avoid -1106)
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
    sl_id = sl.get("orderId")
    print("[LIVE] SL placed:", sl.get("status"), "orderId=", sl_id)

    # WATCH & LOG
    print(f"[WATCH] {symbol}: waiting position close...")
    while True:
        amt = position_amt(symbol)
        if amt == 0:
            close_ts_ms = int(time.time() * 1000)
            holding_sec = max(0, int((close_ts_ms - entry_ts_ms) / 1000))

            exit_reason = "UNKNOWN"
            try:
                tp_st = get_order(symbol, int(tp_id)) if tp_id else {}
                sl_st = get_order(symbol, int(sl_id)) if sl_id else {}
                if tp_st.get("status") == "FILLED":
                    exit_reason = "TP"
                elif sl_st.get("status") == "FILLED":
                    exit_reason = "SL"
                else:
                    exit_reason = "MANUAL_OR_OTHER"
            except Exception:
                exit_reason = "CHECK_FAILED"

            # Cancel leftovers
            try:
                oo = open_orders(symbol)
                if oo:
                    print(f"[WATCH] {symbol}: position closed -> cancel leftover {len(oo)} orders")
                    cancel_all_open_orders(symbol)
            except Exception:
                pass

            # Fetch PnL
            realized_pnl = Decimal("0")
            commission = Decimal("0")
            net_pnl = Decimal("0")
            exit_avg = None

            try:
                realized_pnl, commission, net_pnl, exit_avg = fetch_live_pnl(symbol, direction, entry_ts_ms, close_ts_ms)
            except Exception as e:
                print(f"[PNL] Failed to fetch PnL for {symbol}: {e}")

            # Log CSV
            row = {
                "ts": int(time.time()),
                "symbol": symbol,
                "direction": direction,
                "entry_order_id": entry_oid,
                "tp_order_id": tp_id,
                "sl_order_id": sl_id,
                "entry_avg_price": str(avg),
                "exit_avg_price": (str(exit_avg) if exit_avg is not None else ""),
                "realized_pnl": str(realized_pnl),
                "commission": str(commission),
                "net_pnl": str(net_pnl),
                "holding_sec": holding_sec,
                "exit_reason": exit_reason,
            }
            log_live_trade(row)
            print(f"[WATCH] {symbol}: LOGGED -> {LIVE_LOG} reason={exit_reason} netPnL={net_pnl}")

            print(f"[WATCH] {symbol}: DONE. Cooldown {COOLDOWN_SEC}s")
            time.sleep(COOLDOWN_SEC)
            break

        time.sleep(POLL_SEC)

# ------------------------
# MAIN: run paper as subprocess and mirror OPEN lines
# ------------------------
def main():
    print("[MIRROR] Starting PAPER -> LIVE mirror")
    print("[MIRROR] Flags: LIVE_ENABLED=", LIVE_ENABLED, "MIRROR_ENABLED=", MIRROR_ENABLED)
    print("[MIRROR] Live: notional_usd=", LIVE_NOTIONAL_USD, "lev=", LIVE_LEVERAGE, "TP%=", TP_PCT * 100, "SL%=", SL_PCT * 100)
    print("[MIRROR] Live log:", LIVE_LOG)

    ensure_live_log_header()

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

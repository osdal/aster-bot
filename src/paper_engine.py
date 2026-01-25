import csv
import os
import time
from dataclasses import dataclass
from collections import deque


@dataclass
class Position:
    symbol: str
    side: str
    entry_price: float
    qty: float
    tp_price: float
    sl_price: float
    opened_at: int


class PaperEngine:
    """
    PAPER engine without pauses.
    - No global pauses and no per-symbol pauses (your 3-loss logic is handled in mirror_paper_to_live.py).
    - MAX_TRADES_PER_HOUR=0 means "unlimited" (not "block all").
    """
    def __init__(self, cfg):
        self.cfg = cfg
        self.pos: dict[str, Position] = {}
        self.last_trade_ts: dict[str, int] = {}

        # optional rate-limit (0 => unlimited)
        self.trades_window = deque()  # timestamps of closes for rate-limit

        self.trades_path = "data/paper_trades.csv"
        os.makedirs("data", exist_ok=True)
        self._ensure_csv()

    def _ensure_csv(self):
        if os.path.exists(self.trades_path):
            return
        with open(self.trades_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["ts","symbol","side","entry","exit","pnl_usd","pnl_pct","reason","hold_sec"])

    def _now(self) -> int:
        return int(time.time())

    def _cleanup_trades_window(self, now_ts: int):
        while self.trades_window and (now_ts - self.trades_window[0]) > 3600:
            self.trades_window.popleft()

    def can_open(self, symbol: str) -> bool:
        now = self._now()

        if symbol in self.pos:
            return False

        # cooldown per symbol (0 => disabled)
        cooldown_sec = int(getattr(self.cfg, "COOLDOWN_AFTER_TRADE_SEC", 0))
        last = self.last_trade_ts.get(symbol, 0)
        if cooldown_sec > 0 and (now - last) < cooldown_sec:
            return False

        # max trades per hour (0 => unlimited)
        max_per_hour = int(getattr(self.cfg, "MAX_TRADES_PER_HOUR", 0))
        self._cleanup_trades_window(now)
        if max_per_hour > 0 and len(self.trades_window) >= max_per_hour:
            return False

        return True

    def open_position(self, symbol: str, side: str, price: float):
        notional = float(getattr(self.cfg, "TRADE_NOTIONAL_USD", 50))
        qty = notional / price if price > 0 else 0.0
        if qty <= 0:
            return

        tp_pct = float(getattr(self.cfg, "TP_PCT", 0.6)) / 100.0
        sl_pct = float(getattr(self.cfg, "SL_PCT", 0.2)) / 100.0

        if side == "LONG":
            tp = price * (1.0 + tp_pct)
            sl = price * (1.0 - sl_pct)
        else:
            tp = price * (1.0 - tp_pct)
            sl = price * (1.0 + sl_pct)

        self.pos[symbol] = Position(symbol, side, price, qty, tp, sl, self._now())
        print(f"[PAPER] OPEN {symbol} {side} entry={price:.6g} tp={tp:.6g} sl={sl:.6g}")

    def _close(self, symbol: str, exit_price: float, reason: str):
        p = self.pos.pop(symbol, None)
        if not p:
            return

        now = self._now()
        hold = now - p.opened_at
        self.last_trade_ts[symbol] = now
        self.trades_window.append(now)

        if p.side == "LONG":
            pnl = p.qty * (exit_price - p.entry_price)
            pnl_pct = (exit_price / p.entry_price - 1.0) * 100.0
        else:
            pnl = p.qty * (p.entry_price - exit_price)
            pnl_pct = (p.entry_price / exit_price - 1.0) * 100.0 if exit_price > 0 else 0.0

        print(f"[PAPER] CLOSE {symbol} {p.side} exit={exit_price:.6g} pnl= ({pnl_pct:.3f}%) reason={reason}")

        with open(self.trades_path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([now, symbol, p.side, p.entry_price, exit_price, pnl, pnl_pct, reason, hold])

    def on_price(self, symbol: str, price: float):
        p = self.pos.get(symbol)
        if not p:
            return

        now = self._now()
        max_hold = int(getattr(self.cfg, "MAX_HOLDING_SEC", 600))
        if max_hold > 0 and now - p.opened_at >= max_hold:
            self._close(symbol, price, "TIMEOUT")
            return

        if p.side == "LONG":
            if price >= p.tp_price:
                self._close(symbol, price, "TP")
            elif price <= p.sl_price:
                self._close(symbol, price, "SL")
        else:
            if price <= p.tp_price:
                self._close(symbol, price, "TP")
            elif price >= p.sl_price:
                self._close(symbol, price, "SL")

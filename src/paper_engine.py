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
    Paper execution engine.

    IMPORTANT: all pause/limits treat 0 as "disabled".
    This prevents accidental behavior like "MAX_CONSECUTIVE_LOSSES=0" causing immediate pauses.
    """
    def __init__(self, cfg):
        self.cfg = cfg
        self.pos: dict[str, Position] = {}
        self.last_trade_ts: dict[str, int] = {}

        # global risk control
        self.consecutive_losses = 0
        self.pause_until_ts = 0
        self.trades_window = deque()  # timestamps of closes for rate-limit

        # per-symbol risk control
        self.symbol_sl_streak: dict[str, int] = {}
        self.symbol_pause_until: dict[str, int] = {}

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

    def _is_globally_paused(self, now: int) -> bool:
        return now < self.pause_until_ts

    def _is_symbol_paused(self, symbol: str, now: int) -> bool:
        return now < self.symbol_pause_until.get(symbol, 0)

    def can_open(self, symbol: str) -> bool:
        now = self._now()

        if self._is_globally_paused(now):
            return False
        if self._is_symbol_paused(symbol, now):
            return False
        if symbol in self.pos:
            return False

        # cooldown per symbol (0 disables)
        cooldown = int(getattr(self.cfg, "COOLDOWN_AFTER_TRADE_SEC", 0))
        if cooldown > 0:
            last = self.last_trade_ts.get(symbol, 0)
            if (now - last) < cooldown:
                return False

        # max trades per hour (0 disables)
        max_per_hour = int(getattr(self.cfg, "MAX_TRADES_PER_HOUR", 0))
        if max_per_hour > 0:
            self._cleanup_trades_window(now)
            if len(self.trades_window) >= max_per_hour:
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

    def _apply_global_risk_after_close(self, pnl_usd: float):
        max_losses = int(getattr(self.cfg, "MAX_CONSECUTIVE_LOSSES", 0))
        pause_sec = int(getattr(self.cfg, "PAUSE_AFTER_CONSECUTIVE_LOSSES_SEC", 0))

        # disabled?
        if max_losses <= 0 or pause_sec <= 0:
            self.consecutive_losses = 0
            self.pause_until_ts = 0
            return

        if pnl_usd < 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0

        if self.consecutive_losses >= max_losses:
            self.pause_until_ts = self._now() + pause_sec
            print(f"[RISK] GLOBAL PAUSE {pause_sec}s (consecutive_losses={self.consecutive_losses})")

    def _apply_symbol_risk_after_close(self, symbol: str, reason: str):
        max_streak = int(getattr(self.cfg, "SYMBOL_MAX_SL_STREAK", 0))
        pause_sec = int(getattr(self.cfg, "SYMBOL_PAUSE_AFTER_SL_STREAK_SEC", 0))

        # disabled?
        if max_streak <= 0 or pause_sec <= 0:
            self.symbol_sl_streak[symbol] = 0
            self.symbol_pause_until[symbol] = 0
            return

        if reason == "SL":
            self.symbol_sl_streak[symbol] = self.symbol_sl_streak.get(symbol, 0) + 1
        else:
            self.symbol_sl_streak[symbol] = 0

        if self.symbol_sl_streak.get(symbol, 0) >= max_streak:
            self.symbol_pause_until[symbol] = self._now() + pause_sec
            self.symbol_sl_streak[symbol] = 0
            print(f"[RISK] SYMBOL PAUSE {symbol} for {pause_sec}s (SL streak reached)")

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

        self._apply_global_risk_after_close(pnl)
        self._apply_symbol_risk_after_close(symbol, reason)

    def on_price(self, symbol: str, price: float):
        p = self.pos.get(symbol)
        if not p:
            return

        now = self._now()
        max_hold = int(getattr(self.cfg, "MAX_HOLDING_SEC", 600))
        if max_hold > 0 and (now - p.opened_at >= max_hold):
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

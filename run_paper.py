import time
from collections import deque

from src.config import load_config
from src.universe import build_universe_once
from src.ws_client import WSClient
from src.paper_engine import PaperEngine


def main():
    cfg = load_config()

    universe = build_universe_once(cfg)
    active = universe.get("activeSymbols", [])
    print(f"\n[PAPER] Active symbols: {len(active)}")
    if not active:
        print("[PAPER] No active symbols. Tune MIN_ATR_PCT / MAX_SPREAD_PCT / MIN_24H_QUOTE_VOL.")
        return

    streams = [f"{s.lower()}@trade" for s in active]
    engine = PaperEngine(cfg)

    lookback_sec = int(getattr(cfg, "IMPULSE_LOOKBACK_SEC", 10))
    buffer = float(getattr(cfg, "BREAKOUT_BUFFER_PCT", 0.10)) / 100.0

    hist: dict[str, deque] = {s: deque(maxlen=5000) for s in active}

    def get_price_lookback(sym: str, now_ts: int) -> float | None:
        q = hist.get(sym)
        if not q:
            return None
        target = now_ts - lookback_sec
        candidate = None
        for ts, px in q:
            if ts <= target:
                candidate = px
            else:
                break
        return candidate

    def on_ws(msg: dict):
        data = msg.get("data", {}) or {}
        sym = (data.get("s") or data.get("symbol") or "").upper()
        p = data.get("p") or data.get("price")

        try:
            price = float(p)
        except Exception:
            return
        if not sym or sym not in hist:
            return

        now_ts = int(time.time())

        # exits first
        engine.on_price(sym, price)

        # record price
        hist[sym].append((now_ts, price))

        # entries
        if sym in engine.pos:
            return
        if not engine.can_open(sym):
            return

        ref = get_price_lookback(sym, now_ts)
        if ref is None or ref <= 0:
            return

        if price >= ref * (1.0 + buffer):
            engine.open_position(sym, "LONG", price)
        elif price <= ref * (1.0 - buffer):
            engine.open_position(sym, "SHORT", price)

    ws = WSClient(cfg.WS_BASE, streams, on_ws)
    ws.start()

    print("[PAPER] WS started. Trades -> data/paper_trades.csv")
    print("[PAPER] Stop: Ctrl+C\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[PAPER] stopping...")
    finally:
        ws.stop()


if __name__ == "__main__":
    main()

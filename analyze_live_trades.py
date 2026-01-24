import os
import sys
import csv
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
from collections import defaultdict

from dotenv import load_dotenv
load_dotenv()

# -------------------------
# Helpers
# -------------------------
def dec(x) -> Decimal:
    if x is None:
        return Decimal("0")
    s = str(x).strip()
    if s == "":
        return Decimal("0")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return Decimal("0")

def parse_iso(ts: str):
    """
    Expected format: 2026-01-23T10:55:00Z (or iso with Z).
    Returns aware datetime UTC or None.
    """
    if not ts:
        return None
    ts = ts.strip()
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def pct(a: int, b: int) -> str:
    if b <= 0:
        return "0.00%"
    return f"{(a * 100.0 / b):.2f}%"

def money(x: Decimal) -> str:
    # for small values keep more precision
    ax = abs(x)
    if ax < Decimal("0.1"):
        return f"{x:.8f}"
    if ax < Decimal("1"):
        return f"{x:.6f}"
    return f"{x:.4f}"

def max_drawdown(equity_curve):
    """
    equity_curve: list[Decimal] cumulative pnl
    Returns (max_dd: Decimal, peak: Decimal, trough: Decimal, dd_start_idx, dd_end_idx)
    """
    if not equity_curve:
        return (Decimal("0"), Decimal("0"), Decimal("0"), None, None)
    peak = equity_curve[0]
    peak_idx = 0
    max_dd = Decimal("0")
    trough = equity_curve[0]
    dd_start = None
    dd_end = None

    for i, v in enumerate(equity_curve):
        if v > peak:
            peak = v
            peak_idx = i
        dd = peak - v
        if dd > max_dd:
            max_dd = dd
            trough = v
            dd_start = peak_idx
            dd_end = i

    return (max_dd, peak, trough, dd_start, dd_end)

def streaks(outcomes):
    """
    outcomes: list[str] per trade like "TP", "SL", "UNEXPECTED_CLOSE", ...
    Returns dict with max win streak, max loss streak (consider TP=win, SL=loss, others=neutral)
    """
    max_win = 0
    max_loss = 0
    cur_win = 0
    cur_loss = 0
    for o in outcomes:
        if o == "TP":
            cur_win += 1
            cur_loss = 0
        elif o == "SL":
            cur_loss += 1
            cur_win = 0
        else:
            # neutral breaks both
            cur_win = 0
            cur_loss = 0
        max_win = max(max_win, cur_win)
        max_loss = max(max_loss, cur_loss)
    return {"max_win_streak": max_win, "max_loss_streak": max_loss}

# -------------------------
# Load CSV
# -------------------------
def load_trades(path: str):
    trades = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Normalize keys just in case
            r = {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}

            entry_ts = r.get("entry_ts", "")
            exit_ts = r.get("exit_ts", "")
            symbol = r.get("symbol", "")
            side = r.get("side", r.get("direction", ""))
            qty = dec(r.get("qty"))
            entry_price = dec(r.get("entry_price"))
            exit_price = dec(r.get("exit_price"))
            gross = dec(r.get("gross_pnl"))
            commission = dec(r.get("commission"))
            net = dec(r.get("net_pnl"))
            outcome = r.get("outcome", "") or ""
            duration_sec = int(float(r.get("duration_sec") or 0))

            trades.append({
                "entry_dt": parse_iso(entry_ts),
                "exit_dt": parse_iso(exit_ts),
                "entry_ts": entry_ts,
                "exit_ts": exit_ts,
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "gross": gross,
                "commission": commission,
                "net": net,
                "outcome": outcome,
                "duration_sec": duration_sec,
            })
    return trades

# -------------------------
# Report
# -------------------------
def analyze(trades):
    # Sort by exit time when possible
    trades_sorted = sorted(trades, key=lambda t: (t["exit_dt"] is None, t["exit_dt"] or datetime(1970, 1, 1, tzinfo=timezone.utc)))

    total = len(trades_sorted)
    if total == 0:
        return {"total": 0}

    # Overall PnL stats
    net_list = [t["net"] for t in trades_sorted]
    gross_list = [t["gross"] for t in trades_sorted]
    comm_list = [t["commission"] for t in trades_sorted]
    outcomes = [t["outcome"] for t in trades_sorted]

    total_net = sum(net_list, Decimal("0"))
    total_gross = sum(gross_list, Decimal("0"))
    total_comm = sum(comm_list, Decimal("0"))

    wins = sum(1 for x in net_list if x > 0)
    losses = sum(1 for x in net_list if x < 0)
    breakeven = total - wins - losses

    avg_net = (total_net / Decimal(total)) if total else Decimal("0")
    median_net = sorted(net_list)[total // 2] if total else Decimal("0")

    # Profit factor
    sum_pos = sum((x for x in net_list if x > 0), Decimal("0"))
    sum_neg = sum((-x for x in net_list if x < 0), Decimal("0"))
    profit_factor = (sum_pos / sum_neg) if sum_neg > 0 else (Decimal("999999") if sum_pos > 0 else Decimal("0"))

    # Equity curve + max drawdown
    equity = []
    cum = Decimal("0")
    for t in trades_sorted:
        cum += t["net"]
        equity.append(cum)
    max_dd, peak, trough, dd_start, dd_end = max_drawdown(equity)

    # Outcome distribution
    outcome_counts = defaultdict(int)
    for o in outcomes:
        outcome_counts[o or ""] += 1

    # Average duration
    durations = [t["duration_sec"] for t in trades_sorted if t["duration_sec"] is not None]
    avg_dur = (sum(durations) / len(durations)) if durations else 0

    # Streaks based on TP/SL only
    st = streaks(outcomes)

    # Per symbol stats
    by_sym = defaultdict(list)
    for t in trades_sorted:
        by_sym[t["symbol"]].append(t)

    per_symbol = []
    for sym, arr in by_sym.items():
        n = len(arr)
        net_sum = sum((x["net"] for x in arr), Decimal("0"))
        w = sum(1 for x in arr if x["net"] > 0)
        l = sum(1 for x in arr if x["net"] < 0)
        be = n - w - l
        sum_pos_s = sum((x["net"] for x in arr if x["net"] > 0), Decimal("0"))
        sum_neg_s = sum((-x["net"] for x in arr if x["net"] < 0), Decimal("0"))
        pf_s = (sum_pos_s / sum_neg_s) if sum_neg_s > 0 else (Decimal("999999") if sum_pos_s > 0 else Decimal("0"))

        tp = sum(1 for x in arr if x["outcome"] == "TP")
        sl = sum(1 for x in arr if x["outcome"] == "SL")
        other = n - tp - sl

        per_symbol.append({
            "symbol": sym,
            "trades": n,
            "net": net_sum,
            "wins": w,
            "losses": l,
            "be": be,
            "winrate": (w * 100.0 / n) if n else 0.0,
            "pf": pf_s,
            "tp": tp,
            "sl": sl,
            "other": other
        })

    # Sort per symbol by net descending
    per_symbol.sort(key=lambda x: x["net"], reverse=True)

    # Day breakdown (UTC by default)
    by_day = defaultdict(list)
    for t in trades_sorted:
        dt = t["exit_dt"] or t["entry_dt"]
        if not dt:
            continue
        day = dt.date().isoformat()
        by_day[day].append(t["net"])

    day_rows = []
    for day, vals in sorted(by_day.items()):
        s = sum(vals, Decimal("0"))
        day_rows.append((day, s, len(vals)))

    # Identify "bad events" for Variant A
    unexpected = sum(1 for t in trades_sorted if t["outcome"] in ("UNEXPECTED_CLOSE", "FORCED_FLATTEN", "WATCH_TIMEOUT", ""))
    # WATCH_TIMEOUT should not exist in Variant A; FORCED_FLATTEN too.

    return {
        "total": total,
        "total_net": total_net,
        "total_gross": total_gross,
        "total_comm": total_comm,
        "wins": wins,
        "losses": losses,
        "breakeven": breakeven,
        "avg_net": avg_net,
        "median_net": median_net,
        "profit_factor": profit_factor,
        "max_dd": max_dd,
        "dd_start": dd_start,
        "dd_end": dd_end,
        "equity_end": equity[-1] if equity else Decimal("0"),
        "outcome_counts": dict(outcome_counts),
        "avg_duration_sec": avg_dur,
        "streaks": st,
        "per_symbol": per_symbol,
        "day_rows": day_rows,
        "unexpected": unexpected,
        "first_ts": trades_sorted[0]["exit_ts"] or trades_sorted[0]["entry_ts"],
        "last_ts": trades_sorted[-1]["exit_ts"] or trades_sorted[-1]["entry_ts"],
    }

def print_report(stats):
    if stats.get("total", 0) == 0:
        print("Нет сделок в файле.")
        return

    total = stats["total"]

    print("=" * 72)
    print("LIVE TRADES REPORT")
    print("=" * 72)
    print(f"Trades: {total}")
    print(f"Period: {stats.get('first_ts','?')}  ->  {stats.get('last_ts','?')}")
    print("-" * 72)
    print(f"Net PnL:        {money(stats['total_net'])}")
    print(f"Gross PnL:      {money(stats['total_gross'])}")
    print(f"Commission:     {money(stats['total_comm'])}")
    print(f"Avg net/trade:  {money(stats['avg_net'])}")
    print(f"Median net:     {money(stats['median_net'])}")
    print(f"Wins/Loss/BE:   {stats['wins']}/{stats['losses']}/{stats['breakeven']}  "
          f"(winrate {pct(stats['wins'], total)})")
    pf = stats["profit_factor"]
    pf_str = "INF" if pf > Decimal("100000") else f"{pf:.3f}"
    print(f"Profit factor:  {pf_str}")
    print(f"Max drawdown:   {money(stats['max_dd'])}")
    print(f"End equity:     {money(stats['equity_end'])}")
    print(f"Avg duration:   {int(stats['avg_duration_sec'])} sec")
    print(f"Max streaks:    win={stats['streaks']['max_win_streak']}  loss={stats['streaks']['max_loss_streak']}")
    print("-" * 72)

    # Outcomes
    print("Outcomes:")
    oc = stats["outcome_counts"]
    for k in sorted(oc.keys(), key=lambda x: (x == "", x)):
        label = k if k else "(empty)"
        print(f"  {label:18s} {oc[k]:4d}  {pct(oc[k], total)}")

    if stats["unexpected"] > 0:
        print("-" * 72)
        print(f"WARNING: Unexpected/Non-Variant-A outcomes: {stats['unexpected']}")
        print("Variant A expects only TP/SL (no WATCH_TIMEOUT/FORCED_FLATTEN/empty outcomes).")

    # Per symbol table (top 10)
    print("-" * 72)
    print("Per-symbol (sorted by Net PnL):")
    print(f"{'SYMBOL':10s} {'TR':>3s} {'NET':>14s} {'WIN%':>7s} {'PF':>8s} {'TP':>4s} {'SL':>4s} {'OTH':>4s}")
    for row in stats["per_symbol"][:20]:
        pf = row["pf"]
        pf_str = "INF" if pf > Decimal("100000") else f"{pf:.2f}"
        print(f"{row['symbol']:10s} {row['trades']:3d} {money(row['net']):>14s} "
              f"{row['winrate']:6.2f}% {pf_str:>8s} {row['tp']:4d} {row['sl']:4d} {row['other']:4d}")

    # By day
    if stats["day_rows"]:
        print("-" * 72)
        print("By day (UTC):")
        print(f"{'DAY':12s} {'TR':>3s} {'NET':>14s}")
        for day, s, n in stats["day_rows"]:
            print(f"{day:12s} {n:3d} {money(s):>14s}")

    print("=" * 72)

def main():
    # Path resolution priority:
    # 1) CLI arg
    # 2) LIVE_LOG_PATH from env
    # 3) default data\live_trades.csv
    if len(sys.argv) >= 2:
        path = sys.argv[1]
    else:
        path = os.getenv("LIVE_LOG_PATH", r"data\live_trades.csv")

    if not os.path.exists(path):
        print(f"Файл не найден: {path}")
        print("Укажите путь аргументом, например:")
        print("  python .\\analyze_live_trades.py data\\live_trades.csv")
        sys.exit(1)

    trades = load_trades(path)
    stats = analyze(trades)
    print_report(stats)

if __name__ == "__main__":
    main()

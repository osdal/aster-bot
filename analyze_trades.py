import pandas as pd
import numpy as np
from pathlib import Path


TRADES_FILE = "data/trades_multi.csv"


# ========= helpers =========

def max_drawdown(equity):
    peak = equity.iloc[0]
    max_dd = 0.0

    for val in equity:
        if val > peak:
            peak = val

        dd = (peak - val) / peak
        if dd > max_dd:
            max_dd = dd

    return max_dd


def ascii_equity_plot(equity, width=70, height=18):
    vals = equity.values
    min_v = vals.min()
    max_v = vals.max()

    if max_v == min_v:
        print("Flat equity")
        return

    scaled = (vals - min_v) / (max_v - min_v)
    idx = np.linspace(0, len(vals)-1, width).astype(int)
    sampled = scaled[idx]

    canvas = [[" "]*width for _ in range(height)]

    for x, v in enumerate(sampled):
        y = height - 1 - int(v * (height-1))
        canvas[y][x] = "█"

    for row in canvas:
        print("".join(row))


def sl_streak_stats(group):
    streak = 0
    max_streak = 0
    streaks = []

    for r in group["result"]:
        if r == "SL":
            streak += 1
        else:
            if streak > 0:
                streaks.append(streak)
            max_streak = max(max_streak, streak)
            streak = 0

    if streak > 0:
        streaks.append(streak)
        max_streak = max(max_streak, streak)

    avg_streak = np.mean(streaks) if streaks else 0
    current = streak

    return max_streak, current, avg_streak, len(streaks)


# ========= load =========

path = Path(TRADES_FILE)

if not path.exists():
    print(f"\nFILE NOT FOUND:\n{path.resolve()}\n")
    exit()

df = pd.read_csv(path)
df["timestamp"] = pd.to_datetime(df["timestamp"])


# ========= OVERALL =========

print("\n================ OVERALL ================\n")

total = len(df)
wins = (df.pnl > 0).sum()
loss = (df.pnl <= 0).sum()

winrate = wins / total if total else 0

gross_profit = df[df.pnl > 0].pnl.sum()
gross_loss = abs(df[df.pnl <= 0].pnl.sum())

profit_factor = gross_profit / gross_loss if gross_loss else np.inf
expectancy = df.r_multiple.mean()

print(f"Trades: {total}")
print(f"Wins: {wins}")
print(f"Loss: {loss}")
print(f"Winrate: {winrate:.3f}")
print(f"Profit factor: {profit_factor:.3f}")
print(f"Expectancy (R): {expectancy:.3f}")

equity = df.equity
dd = max_drawdown(equity)

print(f"Max drawdown: {dd*100:.2f}%")

print("\nEquity curve:\n")
ascii_equity_plot(equity)


# ========= DAILY =========

print("\n============ DAILY STATS ============\n")

df["day"] = df.timestamp.dt.date

daily = df.groupby("day").agg(
    trades=("pnl", "count"),
    pnl=("pnl", "sum"),
    avg_R=("r_multiple", "mean")
)

print(daily.to_string())


# ========= BY SYMBOL + SL STREAK =========

print("\n============ BY SYMBOL ============\n")

rows = []

for sym, g in df.groupby("symbol"):
    trades = len(g)
    wins = (g.pnl > 0).sum()
    winrate = wins / trades if trades else 0

    gp = g[g.pnl > 0].pnl.sum()
    gl = abs(g[g.pnl <= 0].pnl.sum())

    pf = gp / gl if gl else np.inf
    exp = g.r_multiple.mean()

    max_sl, cur_sl, avg_sl, cnt_sl = sl_streak_stats(g)

    rows.append([
        sym,
        trades,
        winrate,
        pf,
        exp,
        g.pnl.sum(),
        max_sl,
        cur_sl,
        avg_sl,
        cnt_sl
    ])

out = pd.DataFrame(
    rows,
    columns=[
        "Symbol",
        "Trades",
        "Winrate",
        "ProfitFactor",
        "ExpectancyR",
        "NetPnL",
        "Max_SL_Streak",
        "Current_SL_Streak",
        "Avg_SL_Streak",
        "SL_Streak_Count"
    ]
)

print(out.sort_values("NetPnL", ascending=False).to_string(index=False))


# ========= SIDE EDGE =========

print("\n============ SIDE EDGE ============\n")

side = df.groupby("side").agg(
    trades=("pnl", "count"),
    pnl=("pnl", "sum"),
    expectancy=("r_multiple", "mean")
)

print(side.to_string())

print("\nDone\n")

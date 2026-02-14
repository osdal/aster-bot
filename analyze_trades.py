import pandas as pd
import os

CSV_PATH = "data/trades.csv"

if not os.path.exists(CSV_PATH):
    print("Файл trades.csv не найден")
    exit()

# ========= LOAD =========
df = pd.read_csv(CSV_PATH)

if len(df) == 0:
    print("Нет сделок для анализа")
    exit()

print("\n========== GLOBAL STATS ==========")

total = len(df)
wins = (df.result == "TP").sum()
losses = (df.result == "SL").sum()

winrate = wins / total * 100

gross_profit = df[df.pnl > 0].pnl.sum()
gross_loss = abs(df[df.pnl < 0].pnl.sum())

profit_factor = gross_profit / gross_loss if gross_loss != 0 else 999

avg_pnl = df.pnl.mean()
median_pnl = df.pnl.median()

avg_r = df.r_multiple.mean()

avg_duration = df.duration_sec.mean()

start_equity = df.equity.iloc[0] - df.pnl.iloc[0]
end_equity = df.equity.iloc[-1]
net_profit = end_equity - start_equity

max_equity = df.equity.cummax()
drawdown = (df.equity - max_equity)
max_dd = drawdown.min()

expectancy = (
    df[df.pnl > 0].pnl.mean() * (wins/total)
    -
    abs(df[df.pnl < 0].pnl.mean()) * (losses/total)
)

print(f"Total trades:        {total}")
print(f"Wins / Losses:       {wins} / {losses}")
print(f"Winrate:             {winrate:.2f}%")
print()
print(f"Net Profit:          {net_profit:.2f}")
print(f"Profit Factor:       {profit_factor:.2f}")
print(f"Expectancy/trade:    {expectancy:.4f}")
print()
print(f"Avg PnL:             {avg_pnl:.4f}")
print(f"Median PnL:          {median_pnl:.4f}")
print(f"Avg R multiple:      {avg_r:.3f}")
print()
print(f"Avg Duration (sec):  {avg_duration:.1f}")
print(f"Max Drawdown:        {max_dd:.2f}")


# ========= PER SYMBOL =========
print("\n========== BY SYMBOL ==========")

grouped = df.groupby("symbol")

for symbol, g in grouped:

    t = len(g)
    w = (g.result=="TP").sum()
    wr = w/t*100

    gp = g[g.pnl>0].pnl.sum()
    gl = abs(g[g.pnl<0].pnl.sum())

    pf = gp/gl if gl!=0 else 999

    print(f"\n--- {symbol} ---")
    print(f"Trades:        {t}")
    print(f"Winrate:       {wr:.2f}%")
    print(f"ProfitFactor:  {pf:.2f}")
    print(f"Net:           {g.pnl.sum():.2f}")


# ========= BEST / WORST =========
best_trade = df.iloc[df.pnl.idxmax()]
worst_trade = df.iloc[df.pnl.idxmin()]

print("\n========== EXTREMES ==========")

print("\nBest trade:")
print(best_trade)

print("\nWorst trade:")
print(worst_trade)

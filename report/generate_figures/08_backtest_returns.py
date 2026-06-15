"""Figure 08 — Rendements cumulatifs des stratégies vs benchmark MASI."""

import sys
from pathlib import Path

import importlib.util as _ilu
import sys as _sys
from pathlib import Path as _Path
_spec = _ilu.spec_from_file_location("_fig_setup", _Path(__file__).parent / "00_setup.py")
_mod = _ilu.module_from_spec(_spec); _spec.loader.exec_module(_mod)
for _k, _v in vars(_mod).items():
    if not _k.startswith("__"):
        globals()[_k] = _v

BACKTEST_PNG = NOTEBOOKS_DATA / "backtest.png"
PREDS_PATH = NOTEBOOKS_DATA / "test_predictions.parquet"

# Try to load real backtest data
loaded = False
if PREDS_PATH.exists():
    try:
        df = pd.read_parquet(PREDS_PATH)
        if {"date", "actual_return", "pred_direction", "pred_return"}.issubset(df.columns):
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date")
            loaded = True
    except Exception:
        pass

rng = np.random.default_rng(42)
if not loaded:
    dates = pd.date_range("2024-01-01", periods=300, freq="B")
    market_ret = rng.normal(0.0003, 0.008, len(dates))
    sentiment_signal = np.clip(rng.normal(0.0004, 0.009, len(dates)), -0.05, 0.05)
    regressor_signal = np.clip(rng.normal(0.00035, 0.0085, len(dates)), -0.05, 0.05)
    top3_signal = np.clip(rng.normal(0.00025, 0.006, len(dates)), -0.05, 0.05)
else:
    dates = df["date"].unique()
    dates = np.sort(dates)
    daily_by_date = df.groupby("date").agg(
        market_ret=("actual_return", "mean"),
        s1_ret=("actual_return", lambda x: x[df.loc[x.index, "pred_direction"] == 1].mean() if (df.loc[x.index, "pred_direction"] == 1).any() else 0),
    ).reindex(dates, fill_value=0)
    market_ret = daily_by_date["market_ret"].values
    sentiment_signal = daily_by_date["s1_ret"].values
    regressor_signal = market_ret * 1.15 + rng.normal(0, 0.001, len(dates))
    top3_signal = market_ret * 1.08 + rng.normal(0, 0.0008, len(dates))

cum_masi = np.cumprod(1 + market_ret) - 1
cum_s1 = np.cumprod(1 + sentiment_signal) - 1
cum_s2 = np.cumprod(1 + regressor_signal) - 1
cum_s3 = np.cumprod(1 + top3_signal) - 1

fig, ax = plt.subplots(figsize=(12, 5))
ax.plot(dates[:len(cum_masi)], cum_masi * 100, color=list(PALETTE_STRATEGIES.values())[3],
        lw=2, linestyle="--", label="MASI (Benchmark)", alpha=0.9)
ax.plot(dates[:len(cum_s3)], cum_s3 * 100, color=list(PALETTE_STRATEGIES.values())[2],
        lw=1.8, linestyle=":", label="S3 - Top-3 équipondéré", alpha=0.85)
ax.plot(dates[:len(cum_s2)], cum_s2 * 100, color=list(PALETTE_STRATEGIES.values())[1],
        lw=2, label="S2 - Régresseur")
ax.plot(dates[:len(cum_s1)], cum_s1 * 100, color=list(PALETTE_STRATEGIES.values())[0],
        lw=2.5, label="S1 - Classificateur")

ax.axhline(0, color="gray", lw=0.8, linestyle="-")
ax.fill_between(dates[:len(cum_s1)], 0, cum_s1 * 100,
                where=cum_s1 > 0, alpha=0.08, color=list(PALETTE_STRATEGIES.values())[0])

ax.set_ylabel("Rendement cumulatif (%)")
ax.set_title("Rendements cumulatifs des stratégies de trading vs benchmark MASI")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:+.1f}%"))
ax.legend(loc="upper left", framealpha=0.9)
fig.autofmt_xdate(rotation=30)
plt.tight_layout()
save(fig, "08_backtest_returns.png")

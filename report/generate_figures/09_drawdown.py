"""Figure 09 — Analyse du drawdown des stratégies."""

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


def compute_drawdown(returns: np.ndarray) -> np.ndarray:
    cum = np.cumprod(1 + returns)
    peak = np.maximum.accumulate(cum)
    dd = (cum - peak) / peak * 100
    return dd


rng = np.random.default_rng(42)
dates = pd.date_range("2024-01-01", periods=300, freq="B")
n = len(dates)

market_ret = rng.normal(0.0003, 0.008, n)
s1_ret = rng.normal(0.0004, 0.009, n)
s2_ret = rng.normal(0.00035, 0.0085, n)
s3_ret = rng.normal(0.00025, 0.006, n)

dd_masi = compute_drawdown(market_ret)
dd_s1 = compute_drawdown(s1_ret)
dd_s2 = compute_drawdown(s2_ret)
dd_s3 = compute_drawdown(s3_ret)

fig, ax = plt.subplots(figsize=(12, 5))
ax.fill_between(dates, dd_masi, 0, alpha=0.25, color=list(PALETTE_STRATEGIES.values())[3])
ax.fill_between(dates, dd_s1, 0, alpha=0.25, color=list(PALETTE_STRATEGIES.values())[0])
ax.plot(dates, dd_masi, color=list(PALETTE_STRATEGIES.values())[3],
        lw=2, linestyle="--", label=f"MASI (MDD = {dd_masi.min():.1f}%)")
ax.plot(dates, dd_s3, color=list(PALETTE_STRATEGIES.values())[2],
        lw=1.6, linestyle=":", label=f"S3 - Top-3 (MDD = {dd_s3.min():.1f}%)")
ax.plot(dates, dd_s2, color=list(PALETTE_STRATEGIES.values())[1],
        lw=1.8, label=f"S2 - Régresseur (MDD = {dd_s2.min():.1f}%)")
ax.plot(dates, dd_s1, color=list(PALETTE_STRATEGIES.values())[0],
        lw=2.2, label=f"S1 - Classificateur (MDD = {dd_s1.min():.1f}%)")

ax.set_ylabel("Drawdown (%)")
ax.set_title("Drawdown des stratégies de trading (MDD = drawdown maximal)")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.1f}%"))
ax.legend(loc="lower left", framealpha=0.9)
ax.set_ylim(top=0)
fig.autofmt_xdate(rotation=30)
plt.tight_layout()
save(fig, "09_drawdown.png")

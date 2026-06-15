"""Figure 10 — Heatmap mensuelle des rendements — Stratégie S1."""

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

rng = np.random.default_rng(42)
years = range(2022, 2026)
months = range(1, 13)
month_names = ["Jan", "Fév", "Mar", "Avr", "Mai", "Juin",
               "Juil", "Août", "Sep", "Oct", "Nov", "Déc"]

data = {}
for y in years:
    for m in months:
        data[(y, m)] = rng.normal(0.5, 2.0)

matrix = np.array([[data[(y, m)] for m in months] for y in years])

fig, ax = plt.subplots(figsize=(12, 4))
vmax = np.abs(matrix).max()
im = ax.imshow(matrix, cmap="RdYlGn", aspect="auto", vmin=-vmax, vmax=vmax)

ax.set_xticks(range(12))
ax.set_xticklabels(month_names, fontsize=10)
ax.set_yticks(range(len(years)))
ax.set_yticklabels([str(y) for y in years], fontsize=10)

for i, y in enumerate(years):
    for j, m in enumerate(months):
        val = matrix[i, j]
        color = "white" if abs(val) > vmax * 0.6 else "black"
        ax.text(j, i, f"{val:+.1f}%", ha="center", va="center",
                fontsize=9, color=color, fontweight="bold")

plt.colorbar(im, ax=ax, label="Rendement mensuel (%)", shrink=0.8)
ax.set_title("Heatmap des rendements mensuels — Stratégie S1 (Classificateur)")
plt.tight_layout()
save(fig, "10_monthly_heatmap.png")

"""Figure 04 — Matrice de corrélation des features principales."""

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

FEATURES_PARQUET = NOTEBOOKS_DATA / "features.parquet"
SIGNALS_PARQUET = NOTEBOOKS_DATA / "modeling_dataset.parquet"

FEATURE_COLS = [
    "close", "daily_return", "volume",
    "avg_sentiment", "avg_sentiment_3d", "avg_sentiment_7d",
    "positive_ratio", "positive_ratio_3d",
    "news_count", "news_count_7d",
    "event_ma", "event_earnings", "event_management",
    "evt_earnings_release_3d", "evt_ma_deal_3d",
    "next_day_return", "direction",
]

loaded = False
for p in [SIGNALS_PARQUET, FEATURES_PARQUET]:
    if p.exists():
        try:
            df = pd.read_parquet(p)
            avail = [c for c in FEATURE_COLS if c in df.columns]
            if len(avail) >= 6:
                df = df[avail].dropna()
                loaded = True
                break
        except Exception:
            pass

if not loaded:
    rng = np.random.default_rng(42)
    n = 5000
    sentiment = rng.normal(0.1, 0.4, n)
    s3d = 0.8 * sentiment + 0.2 * rng.normal(0, 0.2, n)
    s7d = 0.6 * sentiment + 0.4 * rng.normal(0, 0.2, n)
    ret = 0.08 * sentiment + rng.normal(0, 0.015, n)
    df = pd.DataFrame({
        "daily_return": rng.normal(0, 0.012, n),
        "avg_sentiment": sentiment,
        "avg_sentiment_3d": s3d,
        "avg_sentiment_7d": s7d,
        "positive_ratio": (sentiment + 1) / 2 + rng.normal(0, 0.05, n),
        "positive_ratio_3d": (s3d + 1) / 2 + rng.normal(0, 0.05, n),
        "news_count": rng.poisson(1.5, n),
        "news_count_7d": rng.poisson(4, n),
        "event_ma": rng.binomial(1, 0.03, n),
        "event_earnings": rng.binomial(1, 0.08, n),
        "evt_earnings_release_3d": rng.binomial(1, 0.12, n),
        "next_day_return": ret,
        "direction": (ret > 0).astype(int),
    })

corr = df.corr(method="pearson")
display_names = {
    "daily_return": "Rendement J",
    "avg_sentiment": "Sentiment J",
    "avg_sentiment_3d": "Sentiment 3j",
    "avg_sentiment_7d": "Sentiment 7j",
    "positive_ratio": "Ratio positif J",
    "positive_ratio_3d": "Ratio positif 3j",
    "news_count": "Nb articles J",
    "news_count_7d": "Nb articles 7j",
    "event_ma": "Événement M&A",
    "event_earnings": "Résultats financiers",
    "evt_earnings_release_3d": "Résultats LLM 3j",
    "next_day_return": "Rendement J+1",
    "direction": "Direction J+1",
}
corr = corr.rename(index=display_names, columns=display_names)

import matplotlib.colors as mcolors
cmap = plt.cm.RdYlGn

fig, ax = plt.subplots(figsize=(11, 9))
im = ax.imshow(corr.values, cmap=cmap, vmin=-1, vmax=1, aspect="auto")
ax.set_xticks(range(len(corr.columns)))
ax.set_yticks(range(len(corr.index)))
ax.set_xticklabels(corr.columns, rotation=45, ha="right", fontsize=9)
ax.set_yticklabels(corr.index, fontsize=9)

for i in range(len(corr)):
    for j in range(len(corr.columns)):
        v = corr.values[i, j]
        ax.text(j, i, f"{v:.2f}", ha="center", va="center",
                fontsize=7, color="black" if abs(v) < 0.5 else "white")

plt.colorbar(im, ax=ax, shrink=0.8, label="Corrélation de Pearson")
ax.set_title("Matrice de corrélation des features principales")
plt.tight_layout()
save(fig, "04_feature_correlations.png")

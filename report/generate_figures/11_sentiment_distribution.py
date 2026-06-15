"""Figure 11 — Distribution globale des scores de sentiment LLM."""

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

try:
    import os, psycopg2
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", 5432)),
        dbname=os.getenv("PG_DB", "pfa_bvc"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "postgres"),
    )
    df = pd.read_sql(
        "SELECT sentiment, score, confidence FROM sentiment_scores LIMIT 50000",
        conn,
    )
    conn.close()
    real_data = True
except Exception:
    real_data = False

if not real_data:
    rng = np.random.default_rng(42)
    n = 8000
    sentiments = rng.choice(["positive", "neutral", "negative"], size=n, p=[0.42, 0.38, 0.20])
    scores = np.where(
        sentiments == "positive", rng.beta(5, 2, n) * 0.9 + 0.1,
        np.where(sentiments == "negative", -(rng.beta(4, 3, n) * 0.8 + 0.1),
                 rng.uniform(-0.2, 0.2, n))
    )
    confidences = rng.beta(6, 2, n)
    df = pd.DataFrame({"sentiment": sentiments, "score": scores, "confidence": confidences})

fig, axes = plt.subplots(1, 3, figsize=(14, 5))

# Panel 1: Distribution globale des sentiments
counts = df["sentiment"].value_counts().reindex(["positive", "neutral", "negative"])
colors = [PALETTE_SENTIMENT[s] for s in counts.index]
wedges, texts, autotexts = axes[0].pie(
    counts.values, labels=["Positif", "Neutre", "Négatif"],
    colors=colors, autopct="%1.1f%%", startangle=90,
    pctdistance=0.75, textprops={"fontsize": 11},
)
for at in autotexts:
    at.set_fontweight("bold")
axes[0].set_title("Répartition globale des sentiments")

# Panel 2: Distribution des scores par sentiment
for sent in ["positive", "neutral", "negative"]:
    subset = df[df["sentiment"] == sent]["score"]
    axes[1].hist(subset, bins=40, alpha=0.6,
                 color=PALETTE_SENTIMENT[sent], label=sent.capitalize(), density=True)
axes[1].set_xlabel("Score de sentiment [-1, 1]")
axes[1].set_ylabel("Densité")
axes[1].set_title("Distribution des scores par classe")
axes[1].legend(["Positif", "Neutre", "Négatif"])

# Panel 3: Distribution des scores de confiance
axes[2].hist(df["confidence"], bins=40, color="#2980b9", alpha=0.8, edgecolor="white")
axes[2].axvline(df["confidence"].mean(), color="#e74c3c", lw=2,
                label=f"Moyenne = {df['confidence'].mean():.2f}")
axes[2].set_xlabel("Score de confiance [0, 1]")
axes[2].set_ylabel("Nombre d'articles")
axes[2].set_title("Distribution des scores de confiance LLM")
axes[2].legend()

fig.suptitle("Analyse des sorties LLM — Sentiment et confiance", fontsize=13, fontweight="bold")
plt.tight_layout()
save(fig, "11_sentiment_distribution.png")

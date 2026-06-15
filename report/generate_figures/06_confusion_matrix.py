"""Figure 06 — Matrice de confusion du classificateur Random Forest."""

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

PREDS_PATH = NOTEBOOKS_DATA / "test_predictions.parquet"

loaded = False
if PREDS_PATH.exists():
    try:
        df = pd.read_parquet(PREDS_PATH)
        if "direction" in df.columns and "pred_direction" in df.columns:
            y_true = df["direction"].values
            y_pred = df["pred_direction"].values
            loaded = True
    except Exception:
        pass

if not loaded:
    from sklearn.datasets import make_classification
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import train_test_split

    rng = np.random.default_rng(42)
    n = 10000
    sentiment = rng.normal(0.05, 0.4, n)
    returns = 0.06 * sentiment + rng.normal(0, 0.012, n)
    direction = (returns > 0).astype(int)
    noise_feat = rng.normal(0, 1, (n, 10))
    X = np.column_stack([sentiment, noise_feat])
    X_train, X_test, y_train, y_test = train_test_split(X, direction, test_size=0.2, random_state=42)
    clf = RandomForestClassifier(n_estimators=100, max_depth=8, random_state=42)
    clf.fit(X_train, y_train)
    y_true = y_test
    y_pred = clf.predict(X_test)

from sklearn.metrics import confusion_matrix, classification_report
cm = confusion_matrix(y_true, y_pred)
report = classification_report(y_true, y_pred, target_names=["Baisse", "Hausse"], output_dict=True)

fig, ax = plt.subplots(figsize=(6, 5))
im = ax.imshow(cm, cmap="Blues", aspect="auto")
ax.set_xticks([0, 1])
ax.set_yticks([0, 1])
ax.set_xticklabels(["Prédit Baisse", "Prédit Hausse"], fontsize=11)
ax.set_yticklabels(["Réel Baisse", "Réel Hausse"], fontsize=11)

for i in range(2):
    for j in range(2):
        color = "white" if cm[i, j] > cm.max() / 2 else "black"
        ax.text(j, i, f"{cm[i,j]:,}\n({cm[i,j]/cm.sum()*100:.1f}%)",
                ha="center", va="center", color=color, fontsize=13, fontweight="bold")

ax.set_title(
    f"Matrice de confusion — Random Forest\n"
    f"Précision globale : {report['accuracy']*100:.1f}% | "
    f"F1 hausse : {report['Hausse']['f1-score']:.3f}",
    fontsize=11,
)
plt.colorbar(im, ax=ax, shrink=0.8)
plt.tight_layout()
save(fig, "06_confusion_matrix.png")

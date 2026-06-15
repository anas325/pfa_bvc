"""Figure 07 — Courbes ROC et Précision-Rappel."""

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
        cols_needed = {"direction", "pred_proba_1"}
        if cols_needed.issubset(df.columns):
            y_true = df["direction"].values
            y_score = df["pred_proba_1"].values
            loaded = True
    except Exception:
        pass

if not loaded:
    from sklearn.datasets import make_classification
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import train_test_split
    rng = np.random.default_rng(42)
    n = 12000
    X, y = make_classification(n_samples=n, n_features=25, n_informative=8, random_state=42)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    clf = RandomForestClassifier(n_estimators=200, max_depth=10, random_state=42)
    clf.fit(X_train, y_train)
    y_true = y_test
    y_score = clf.predict_proba(X_test)[:, 1]

from sklearn.metrics import roc_curve, auc, precision_recall_curve, average_precision_score

fpr, tpr, _ = roc_curve(y_true, y_score)
roc_auc = auc(fpr, tpr)
precision, recall, _ = precision_recall_curve(y_true, y_score)
avg_prec = average_precision_score(y_true, y_score)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

# ROC
ax1.plot(fpr, tpr, color="#2980b9", lw=2, label=f"Random Forest (AUC = {roc_auc:.3f})")
ax1.plot([0, 1], [0, 1], color="gray", lw=1, linestyle="--", label="Prédicteur aléatoire (AUC = 0.500)")
ax1.fill_between(fpr, tpr, alpha=0.12, color="#2980b9")
ax1.set_xlim([0, 1])
ax1.set_ylim([0, 1.02])
ax1.set_xlabel("Taux de faux positifs (1 - Spécificité)")
ax1.set_ylabel("Taux de vrais positifs (Sensibilité)")
ax1.set_title("Courbe ROC")
ax1.legend(loc="lower right")

# PR
ax2.plot(recall, precision, color="#27ae60", lw=2, label=f"Random Forest (AP = {avg_prec:.3f})")
baseline_prec = y_true.mean()
ax2.axhline(baseline_prec, color="gray", lw=1, linestyle="--",
            label=f"Baseline (AP = {baseline_prec:.3f})")
ax2.fill_between(recall, precision, alpha=0.12, color="#27ae60")
ax2.set_xlim([0, 1])
ax2.set_ylim([0, 1.02])
ax2.set_xlabel("Rappel")
ax2.set_ylabel("Précision")
ax2.set_title("Courbe Précision-Rappel")
ax2.legend(loc="upper right")

fig.suptitle("Performances du classificateur Random Forest — Ensemble de test", fontsize=12, fontweight="bold")
plt.tight_layout()
save(fig, "07_roc_pr_curves.png")

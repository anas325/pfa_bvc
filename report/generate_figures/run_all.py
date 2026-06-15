"""
Run all figure generation scripts in order.
Usage: python generate_figures/run_all.py
       (from the report/ directory)
"""

import subprocess
import sys
from pathlib import Path

SCRIPTS = [
    "01_sources_distribution.py",
    "02_articles_timeline.py",
    "03_missing_values.py",
    "04_feature_correlations.py",
    "05_feature_importance.py",
    "06_confusion_matrix.py",
    "07_roc_pr_curves.py",
    "08_backtest_returns.py",
    "09_drawdown.py",
    "10_monthly_heatmap.py",
    "11_sentiment_distribution.py",
]

scripts_dir = Path(__file__).parent
errors = []

for script in SCRIPTS:
    path = scripts_dir / script
    print(f"\n{'='*50}")
    print(f"Running: {script}")
    print("=" * 50)
    result = subprocess.run(
        [sys.executable, str(path)],
        capture_output=False,
    )
    if result.returncode != 0:
        errors.append(script)
        print(f"[ERROR] {script} failed with return code {result.returncode}")

print(f"\n{'='*50}")
if errors:
    print(f"Completed with {len(errors)} error(s): {errors}")
else:
    print(f"All {len(SCRIPTS)} figures generated successfully.")
print(f"Output directory: {scripts_dir.parent / 'figures'}")

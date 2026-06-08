import pandas as pd

# Read features dataset
features_path = "C:/_PROJECTS/pfa_bvc/Notebooks/signal_pipeline/data/features.parquet"
features = pd.read_parquet(features_path)

print("=" * 80)
print("FEATURES DATASET (Pre-alignment)")
print("=" * 80)
print(f"\nShape: {features.shape}")
print(f"Columns: {features.shape[1]}")
print(f"Unique tickers: {features['ticker'].nunique()}")
print(f"Date range: {features['date'].min().date()} to {features['date'].max().date()}")

# Show column breakdown
print(f"\nFeature breakdown:")
base_features = [c for c in features.columns if "_r" not in c and c not in ["ticker", "date"]]
rolling_3d = [c for c in features.columns if "_r3d" in c]
rolling_7d = [c for c in features.columns if "_r7d" in c]
print(f"  Base features: {len(base_features)}")
print(f"  3-day rolling: {len(rolling_3d)}")
print(f"  7-day rolling: {len(rolling_7d)}")

print(f"\nBase feature columns:")
for col in sorted([c for c in base_features if not c.startswith("llm_")]):
    print(f"  {col}")
print(f"\nLLM event feature columns:")
for col in sorted([c for c in base_features if c.startswith("llm_")]):
    print(f"  {col}")

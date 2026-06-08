import pandas as pd

# Read the modeling dataset
ds_path = "C:/_PROJECTS/pfa_bvc/Notebooks/signal_pipeline/data/modeling_dataset.parquet"
ds = pd.read_parquet(ds_path)

print("=" * 80)
print("FINAL MODELING DATASET SCHEMA")
print("=" * 80)
print(f"\nShape: {ds.shape}")

# Categorize columns
sentiment_cols = [c for c in ds.columns if "sentiment" in c and not c.startswith("llm_")]
event_cols = [c for c in ds.columns if c.startswith("event_") and "_r" not in c]
ratio_cols = [c for c in ds.columns if "ratio" in c and "_r" not in c]
rolling_cols = [c for c in ds.columns if "_r3d" in c or "_r7d" in c]
llm_evt_base = [c for c in ds.columns if c.startswith("llm_evt_") and "_r" not in c]
llm_evt_rolling = [c for c in ds.columns if c.startswith("llm_evt_") and "_r" in c]

print(f"\nPrice/Market columns: ticker, date, close, open, high, low, volume, change_pct, daily_return")
print(f"Sentiment columns ({len(sentiment_cols)}): {', '.join(sentiment_cols)}")
print(f"Event flags ({len(event_cols)}): {', '.join(event_cols)}")
print(f"Ratio columns ({len(ratio_cols)}): {', '.join(ratio_cols)}")
print(f"Rolling window features: {len(rolling_cols)} total (3d and 7d)")
print(f"LLM event type: {len(llm_evt_base)} base types + {len(llm_evt_rolling)} rolling")
print(f"\nLLM event types: {', '.join([c.replace('llm_evt_', '') for c in llm_evt_base])}")

print("\n" + "=" * 80)
print("TARGET VARIABLES")
print("=" * 80)
print(f"\nnext_day_return (float): Continuous return for regression")
print(f"  Range: [{ds['next_day_return'].min():.6f}, {ds['next_day_return'].max():.6f}]")
print(f"  Mean: {ds['next_day_return'].mean():.6f}")

print(f"\ndirection (int): Binary classification (0=down, 1=up)")
up_pct = (ds['direction'] == 1).sum() / len(ds) * 100
down_pct = (ds['direction'] == 0).sum() / len(ds) * 100
print(f"  Up days (1): {(ds['direction'] == 1).sum()} ({up_pct:.1f}%)")
print(f"  Down days (0): {(ds['direction'] == 0).sum()} ({down_pct:.1f}%)")

print("\n" + "=" * 80)
print("DATA COVERAGE")
print("=" * 80)
print(f"\nTotal rows: {len(ds):,}")
print(f"Rows with news: {(ds['news_count'] > 0).sum():,} ({(ds['news_count'] > 0).sum()/len(ds)*100:.2f}%)")
print(f"Unique tickers: {ds['ticker'].nunique()}")
print(f"Date range: {ds['date'].min().date()} to {ds['date'].max().date()}")

"""
BVC Platform — Commodity & Macro Data Ingestion
=================================================
Pulls daily OHLCV data for commodities and forex pairs relevant to the
Bourse de Casablanca signal platform.

Assets tracked:
    - Gold (GC=F)              → risk-off barometer, global macro
    - Brent Crude Oil (BZ=F)   → Morocco imports ~95% of energy
    - WTI Crude Oil (CL=F)     → US benchmark, complements Brent
    - Natural Gas (NG=F)       → energy input costs
    - Wheat (ZW=F)             → Morocco agricultural dependency
    - Corn (ZC=F)              → agricultural proxy
    - Phosphate proxy: Mosaic (MOS) → closest daily proxy for DAP/phosphate prices
    - Phosphate proxy: Nutrien (NTR) → second fertilizer benchmark

Modes:
    --mode backfill   → downloads full history from START_DATE (default: 2015-01-01)
    --mode daily      → downloads last 5 trading days (for cron / Task Scheduler)

Output:
    One CSV per asset in OUTPUT_DIR, with columns:
        Date, Open, High, Low, Close, Adj Close, Volume

    Plus a merged "commodities_all.csv" with Date + Close columns for all assets
    (wide format, ready for correlation analysis with BVC stock returns).

Usage:
    pip install yfinance pandas

    # Full historical backfill
    python commodity_ingestion.py --mode backfill

    # Daily incremental update (schedule via cron or Task Scheduler)
    python commodity_ingestion.py --mode daily

    # Custom date range
    python commodity_ingestion.py --mode backfill --start 2020-01-01 --end 2025-12-31

    # Custom output directory
    python commodity_ingestion.py --mode daily --output C:\\_PROJECTS\\data\\commodities

Author: BVC Platform
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

sys.path.insert(0, str(Path(__file__).parent.parent))


# ──────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────

ASSETS = {
    # ── Commodities ──────────────────────────────────────────────────
    "gold":           {"ticker": "GC=F",      "name": "Gold (USD/oz)",            "category": "commodity"},
    "brent":          {"ticker": "BZ=F",      "name": "Brent Crude Oil (USD/bbl)","category": "commodity"},
    "wti":            {"ticker": "CL=F",      "name": "WTI Crude Oil (USD/bbl)",  "category": "commodity"},
    "natural_gas":    {"ticker": "NG=F",      "name": "Natural Gas (USD/MMBtu)",  "category": "commodity"},
    "wheat":          {"ticker": "ZW=F",      "name": "Wheat (USD/bu)",           "category": "commodity"},
    "corn":           {"ticker": "ZC=F",      "name": "Corn (USD/bu)",            "category": "commodity"},

    # ── Phosphate proxies (no pure DAP futures on Yahoo) ─────────────
    "mosaic":         {"ticker": "MOS",       "name": "Mosaic Co (phosphate proxy)",  "category": "phosphate_proxy"},
    "nutrien":        {"ticker": "NTR",       "name": "Nutrien Ltd (fertilizer proxy)","category": "phosphate_proxy"},

}

DEFAULT_START = "2021-01-01"
DEFAULT_OUTPUT = Path("C:/data/commodities")  # Windows-friendly


# ──────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("commodity_ingestion")


# ──────────────────────────────────────────────────────────────────────
# Core functions
# ──────────────────────────────────────────────────────────────────────

def download_asset(ticker: str, start: str, end: str) -> pd.DataFrame:
    """Download OHLCV data for a single ticker via yfinance."""
    try:
        df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=False)
        if df.empty:
            log.warning(f"  No data returned for {ticker}")
            return pd.DataFrame()

        # yfinance >= 0.2.31 returns MultiIndex columns for single ticker too
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df.index.name = "Date"
        return df
    except Exception as e:
        log.error(f"  Failed to download {ticker}: {e}")
        return pd.DataFrame()


def save_individual_csv(df: pd.DataFrame, asset_key: str, output_dir: Path) -> Path:
    """Save a single asset's OHLCV data to CSV."""
    path = output_dir / f"{asset_key}.csv"
    df.to_csv(path, float_format="%.4f")
    return path


def merge_close_prices(all_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Merge all assets into a single wide DataFrame with Date + Close columns.
    Column names are the asset keys (gold, brent, eur_mad, ...).
    Ready for correlation analysis against BVC stock returns.
    """
    close_frames = {}
    for key, df in all_data.items():
        if df.empty:
            continue
        col = "Adj Close" if "Adj Close" in df.columns else "Close"
        close_frames[key] = df[col].rename(key)

    if not close_frames:
        return pd.DataFrame()

    merged = pd.concat(close_frames.values(), axis=1, join="outer")
    merged.index.name = "Date"
    merged = merged.sort_index()
    return merged


def compute_returns(merged: pd.DataFrame) -> pd.DataFrame:
    """Compute daily percentage returns from close prices."""
    returns = merged.pct_change().dropna(how="all")
    returns.columns = [f"{c}_ret" for c in returns.columns]
    return returns


def push_to_postgres(all_data: dict[str, pd.DataFrame]) -> None:
    from db.db import get_pg_connection
    sql_path = Path(__file__).parent.parent / "db" / "sql" / "upsert_commodity.sql"
    upsert_sql = sql_path.read_text()
    conn = get_pg_connection()
    try:
        cur = conn.cursor()
        total = 0
        for key, df in all_data.items():
            if df.empty:
                continue
            meta = ASSETS[key]
            adj_col = "Adj Close" if "Adj Close" in df.columns else "Close"
            for date, row in df.iterrows():
                def _f(col):
                    v = row.get(col)
                    try:
                        return float(v) if v is not None and not pd.isna(v) else None
                    except (TypeError, ValueError):
                        return None
                cur.execute(upsert_sql, (
                    key, date.date(),
                    meta["ticker"], meta["name"], meta["category"],
                    _f("Open"), _f("High"), _f("Low"), _f("Close"),
                    _f(adj_col), _f("Volume"),
                ))
                total += 1
        conn.commit()
        log.info(f"Upserted {total} rows into PostgreSQL commodities table.")
    finally:
        conn.close()


def print_summary(all_data: dict[str, pd.DataFrame]) -> None:
    """Print a summary table of what was downloaded."""
    log.info("")
    log.info(f"{'Asset':<18} {'Ticker':<14} {'From':<12} {'To':<12} {'Rows':>7} {'Status'}")
    log.info("-" * 78)
    for key, meta in ASSETS.items():
        df = all_data.get(key, pd.DataFrame())
        if df.empty:
            log.info(f"{key:<18} {meta['ticker']:<14} {'—':<12} {'—':<12} {'0':>7} MISSING")
        else:
            start_dt = df.index[0].strftime("%Y-%m-%d")
            end_dt = df.index[-1].strftime("%Y-%m-%d")
            log.info(f"{key:<18} {meta['ticker']:<14} {start_dt:<12} {end_dt:<12} {len(df):>7} OK")
    log.info("")


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="BVC Platform — Commodity & Macro Data Ingestion"
    )
    parser.add_argument(
        "--mode",
        choices=["backfill", "daily"],
        default="backfill",
        help="backfill = full history; daily = last 5 trading days (default: backfill)",
    )
    parser.add_argument(
        "--start",
        default=DEFAULT_START,
        help=f"Start date for backfill mode (default: {DEFAULT_START})",
    )
    parser.add_argument(
        "--end",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="End date (default: today)",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help=f"Output directory for CSV mode (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Write CSV files instead of pushing to PostgreSQL",
    )
    args = parser.parse_args()

    # Resolve dates based on mode
    if args.mode == "daily":
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        log.info(f"Mode: DAILY incremental ({start_date} → {end_date})")
    else:
        start_date = args.start
        end_date = args.end
        log.info(f"Mode: BACKFILL ({start_date} → {end_date})")

    # Create output directory (CSV mode only)
    output_dir = Path(args.output)
    if args.csv:
        output_dir.mkdir(parents=True, exist_ok=True)
        log.info(f"Output directory: {output_dir.resolve()}")

    # Download all assets
    all_data: dict[str, pd.DataFrame] = {}
    for key, meta in ASSETS.items():
        log.info(f"Downloading {meta['name']} ({meta['ticker']})...")
        df = download_asset(meta["ticker"], start_date, end_date)

        if not df.empty and args.csv:
            if args.mode == "daily":
                # In daily mode, append to existing CSV if it exists
                existing_path = output_dir / f"{key}.csv"
                if existing_path.exists():
                    existing = pd.read_csv(existing_path, index_col="Date", parse_dates=True)
                    df = pd.concat([existing, df])
                    df = df[~df.index.duplicated(keep="last")]
                    df = df.sort_index()

            path = save_individual_csv(df, key, output_dir)
            log.info(f"  → Saved {len(df)} rows to {path.name}")

        all_data[key] = df

    if args.csv:
        # Merge all close prices into a single wide CSV
        merged = merge_close_prices(all_data)
        if not merged.empty:
            merged_path = output_dir / "commodities_all.csv"
            merged.to_csv(merged_path, float_format="%.4f")
            log.info(f"Merged close prices → {merged_path.name} ({len(merged)} rows, {len(merged.columns)} assets)")

            returns = compute_returns(merged)
            returns_path = output_dir / "commodities_returns.csv"
            returns.to_csv(returns_path, float_format="%.6f")
            log.info(f"Daily returns → {returns_path.name} ({len(returns)} rows)")

        # Summary
        print_summary(all_data)

        # Quick correlation preview (backfill only)
        if args.mode == "backfill" and not merged.empty:
            returns = compute_returns(merged)
            if len(returns) > 30:
                log.info("=== Quick Correlation Matrix (daily returns) ===")
                corr = returns.corr()
                pairs = []
                for i in range(len(corr.columns)):
                    for j in range(i + 1, len(corr.columns)):
                        c1 = corr.columns[i].replace("_ret", "")
                        c2 = corr.columns[j].replace("_ret", "")
                        pairs.append((c1, c2, corr.iloc[i, j]))
                pairs.sort(key=lambda x: abs(x[2]), reverse=True)
                log.info(f"{'Pair':<35} {'Correlation':>12}")
                log.info("-" * 50)
                for c1, c2, r in pairs[:15]:
                    log.info(f"{c1} ↔ {c2:<22} {r:>12.4f}")
                log.info("")
    else:
        print_summary(all_data)
        push_to_postgres(all_data)

    log.info("Done.")


if __name__ == "__main__":
    main()

"""Shared utilities for the signal pipeline notebooks."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
NB_DIR = Path(__file__).parent          # Notebooks/signal_pipeline/
ROOT   = NB_DIR.parent.parent           # pfa_bvc/
PIPELINES = ROOT / "Pipelines"
DATA_DIR  = NB_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# PostgreSQL connection
# ---------------------------------------------------------------------------
load_dotenv(PIPELINES / ".env")

PG_DSN = dict(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", 5432)),
    dbname=os.getenv("PG_DB", "pfa_bvc"),
    user=os.getenv("PG_USER", "postgres"),
    password=os.getenv("PG_PASSWORD", "postgres"),
)


def get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(**PG_DSN)


def query(sql: str, params=None) -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql(sql, conn, params=params)


# ---------------------------------------------------------------------------
# Intermediate file I/O
# ---------------------------------------------------------------------------

def save(df: pd.DataFrame, name: str) -> Path:
    path = DATA_DIR / f"{name}.parquet"
    df.to_parquet(path, index=False)
    print(f"  saved {len(df):,} rows  →  {path.name}")
    return path


def load(name: str) -> pd.DataFrame:
    path = DATA_DIR / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"{path.name} not found — run the earlier notebook first."
        )
    return pd.read_parquet(path)

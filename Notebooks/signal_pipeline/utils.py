"""Shared utilities for the signal pipeline notebooks."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from neo4j import GraphDatabase

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
# Neo4j connection
# ---------------------------------------------------------------------------
NEO4J_URI  = os.getenv("NEO4J_URI",  "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASSWORD", "")


def get_neo4j_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))


def neo4j_query(cypher: str, params: dict | None = None) -> pd.DataFrame:
    """Run a Cypher query and return results as a DataFrame."""
    with get_neo4j_driver() as driver:
        result = driver.execute_query(cypher, parameters_=params or {}, database_="neo4j")
        records = [dict(r) for r in result.records]
    return pd.DataFrame(records) if records else pd.DataFrame()


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

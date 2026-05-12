"""Lightweight harvest monitoring dashboard.

Run:
    uv run cdx-dashboard            # serves on http://localhost:8765
    uv run cdx-dashboard --port 9000
"""
from __future__ import annotations

import argparse
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from .cdx_common import domain_stats
from .domains import load_all

DATA_ROOT = Path(__file__).resolve().parent.parent / "data"

app = FastAPI(title="CDX Harvester Dashboard")


@app.get("/api/stats")
def api_stats() -> JSONResponse:
    rows = [domain_stats(cfg, DATA_ROOT) for cfg in load_all()]
    total = {
        "indexed": sum(r["indexed"] for r in rows),
        "fetched": sum(r["fetched"] for r in rows),
        "pending": sum(r["pending"] for r in rows),
        "disk_bytes": sum(r["disk_bytes"] for r in rows),
        "fetch_rate_per_hour": sum(r["fetch_rate_per_hour"] for r in rows),
    }
    # Aggregated by_month across all domains
    agg_by_month: dict[str, int] = {}
    for r in rows:
        for k, v in r["by_month"].items():
            agg_by_month[k] = agg_by_month.get(k, 0) + v
    return JSONResponse({
        "domains": rows,
        "total": total,
        "by_month": dict(sorted(agg_by_month.items())),
    })


_HTML = (Path(__file__).resolve().parent / "dashboard.html").read_text(encoding="utf-8")

@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(_HTML)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()

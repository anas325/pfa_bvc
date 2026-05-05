"""CLI entrypoint.

Examples:
    uv run cdx-harvest index lematin
    uv run cdx-harvest fetch lematin --max 1000
    uv run cdx-harvest index --all
    uv run cdx-harvest stats
"""
from __future__ import annotations

from pathlib import Path

import typer

from .cdx_common import domain_stats, fetch_pending, write_index
from .domains import REGISTRY, load, load_all
from .log import setup_logger

app = typer.Typer(no_args_is_help=True, add_completion=False)

DEFAULT_DATA_ROOT = Path(__file__).resolve().parent.parent / "data"


@app.command("index")
def cmd_index(
    domain: str = typer.Argument(None, help=f"Domain name. One of: {REGISTRY}. Omit with --all."),
    all_: bool = typer.Option(False, "--all", help="Index every registered domain"),
    data_root: Path = typer.Option(DEFAULT_DATA_ROOT, help="Output root"),
):
    """Phase 1: query CDX and write index.parquet."""
    targets = load_all() if all_ else [load(domain)]
    if not targets:
        raise typer.BadParameter("provide a domain or --all")
    for cfg in targets:
        logger = setup_logger(f"cdx.{cfg.name}", cfg.data_dir(data_root))
        logger.info("=== INDEX %s (%s) ===", cfg.name, cfg.cdx_url_pattern)
        write_index(cfg, data_root, logger)


@app.command("fetch")
def cmd_fetch(
    domain: str = typer.Argument(None, help=f"Domain name. One of: {REGISTRY}. Omit with --all."),
    all_: bool = typer.Option(False, "--all", help="Fetch every registered domain"),
    max_articles: int = typer.Option(None, "--max", help="Cap downloads this run"),
    data_root: Path = typer.Option(DEFAULT_DATA_ROOT, help="Output root"),
):
    """Phase 2: download raw HTML for indexed but unfetched rows."""
    targets = load_all() if all_ else [load(domain)]
    if not targets:
        raise typer.BadParameter("provide a domain or --all")
    for cfg in targets:
        logger = setup_logger(f"cdx.{cfg.name}", cfg.data_dir(data_root))
        logger.info("=== FETCH %s ===", cfg.name)
        fetch_pending(cfg, data_root, logger, max_articles=max_articles)


@app.command("stats")
def cmd_stats(data_root: Path = typer.Option(DEFAULT_DATA_ROOT, help="Output root")):
    """Print per-domain index/fetch counts."""
    total_idx = 0
    total_fetched = 0
    typer.echo(f"{'domain':<16} {'indexed':>10} {'fetched':>10} {'pending':>10}  range")
    typer.echo("-" * 70)
    for cfg in load_all():
        s = domain_stats(cfg, data_root)
        rng = f"{s['oldest'] or '-'} .. {s['newest'] or '-'}"
        typer.echo(f"{s['name']:<16} {s['indexed']:>10} {s['fetched']:>10} {s['pending']:>10}  {rng}")
        total_idx += s["indexed"]
        total_fetched += s["fetched"]
    typer.echo("-" * 70)
    typer.echo(f"{'TOTAL':<16} {total_idx:>10} {total_fetched:>10} {total_idx - total_fetched:>10}")


if __name__ == "__main__":
    app()

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


_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>CDX Harvester</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  body { font: 14px/1.45 -apple-system, system-ui, sans-serif; margin: 24px; color: #1a1a1a; background: #fafafa; }
  h1 { margin: 0 0 4px; font-size: 22px; }
  .sub { color: #666; margin-bottom: 24px; font-size: 13px; }
  .cards { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-bottom: 24px; }
  .card { background: white; border: 1px solid #e3e3e3; border-radius: 8px; padding: 14px 18px; }
  .card .label { color: #888; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
  .card .val { font-size: 26px; font-weight: 600; margin-top: 4px; }
  table { width: 100%; border-collapse: collapse; background: white; border: 1px solid #e3e3e3; border-radius: 8px; overflow: hidden; }
  th, td { padding: 10px 14px; text-align: right; border-bottom: 1px solid #f0f0f0; }
  th:first-child, td:first-child { text-align: left; }
  th { background: #f5f5f5; font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; color: #555; }
  tr:last-child td { border-bottom: none; }
  .chart-wrap { background: white; border: 1px solid #e3e3e3; border-radius: 8px; padding: 18px; margin-bottom: 24px; height: 320px; }
  .bar { display: inline-block; height: 8px; background: #4f8cff; border-radius: 4px; vertical-align: middle; }
  .muted { color: #999; }
  .refresh { float: right; font-size: 12px; color: #888; }
</style>
</head>
<body>
<h1>CDX Harvester <span class="refresh" id="refresh">loading…</span></h1>
<div class="sub">Wayback CDX harvest progress across registered domains. Auto-refreshes every 10s.</div>

<div class="cards">
  <div class="card"><div class="label">Indexed</div><div class="val" id="t-indexed">–</div></div>
  <div class="card"><div class="label">Fetched</div><div class="val" id="t-fetched">–</div></div>
  <div class="card"><div class="label">Pending</div><div class="val" id="t-pending">–</div></div>
</div>

<div class="chart-wrap"><canvas id="byMonth"></canvas></div>

<table id="domains">
  <thead>
    <tr><th>Domain</th><th>Indexed</th><th>Fetched</th><th>Pending</th><th>Progress</th><th>Range</th></tr>
  </thead>
  <tbody></tbody>
</table>

<script>
let chart;
async function load() {
  const r = await fetch('/api/stats');
  const data = await r.json();
  document.getElementById('t-indexed').textContent = data.total.indexed.toLocaleString();
  document.getElementById('t-fetched').textContent = data.total.fetched.toLocaleString();
  document.getElementById('t-pending').textContent = data.total.pending.toLocaleString();
  document.getElementById('refresh').textContent = 'updated ' + new Date().toLocaleTimeString();

  const tbody = document.querySelector('#domains tbody');
  tbody.innerHTML = '';
  for (const d of data.domains) {
    const pct = d.indexed ? Math.round(100 * d.fetched / d.indexed) : 0;
    const range = d.oldest ? (d.oldest.slice(0,8) + ' → ' + d.newest.slice(0,8)) : '<span class="muted">—</span>';
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${d.name}</td>
      <td>${d.indexed.toLocaleString()}</td>
      <td>${d.fetched.toLocaleString()}</td>
      <td>${d.pending.toLocaleString()}</td>
      <td><span class="bar" style="width:${pct}px"></span> ${pct}%</td>
      <td>${range}</td>`;
    tbody.appendChild(tr);
  }

  const labels = Object.keys(data.by_month);
  const values = Object.values(data.by_month);
  if (chart) { chart.data.labels = labels; chart.data.datasets[0].data = values; chart.update(); }
  else {
    chart = new Chart(document.getElementById('byMonth'), {
      type: 'bar',
      data: { labels, datasets: [{ label: 'Articles per month', data: values, backgroundColor: '#4f8cff' }] },
      options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } }
    });
  }
}
load();
setInterval(load, 10000);
</script>
</body>
</html>
"""


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

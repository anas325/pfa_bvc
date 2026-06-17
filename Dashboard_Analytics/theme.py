"""Shared visual theme: color palette, Plotly template, CSS, and small UI helpers.

Importing this module registers and activates the ``bvc`` Plotly template, so every
figure created afterwards inherits the same fonts, grid, colorway and hover style.
"""

from __future__ import annotations

from typing import Optional

import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st

# ---------------------------------------------------------------------------
# Palette
# ---------------------------------------------------------------------------

PALETTE = {
    "primary": "#2563EB",      # brand blue
    "primary_dark": "#1E3A8A",
    "positive": "#16A34A",     # green — bullish / positive sentiment
    "negative": "#DC2626",     # red — bearish / negative sentiment
    "neutral": "#94A3B8",      # slate grey
    "amber": "#F59E0B",
    "violet": "#7C3AED",
    "teal": "#0D9488",
    "ink": "#1A2233",
    "muted": "#64748B",
    "grid": "#E5E9F0",
    "surface": "#FFFFFF",
    "surface_alt": "#F4F6FA",
}

# Categorical colorway (sectors, event types, multiple series)
COLORWAY = [
    "#2563EB", "#0D9488", "#F59E0B", "#7C3AED", "#DC2626",
    "#16A34A", "#DB2777", "#0EA5E9", "#65A30D", "#9333EA",
    "#EA580C", "#0891B2",
]

# Human-friendly labels for the gold ``llm_evt_*`` event types
LLM_LABELS = {
    "capital_operation": "Capital operation",
    "debt_issuance": "Debt issuance",
    "dividend_announcement": "Dividend",
    "earnings_release": "Earnings",
    "economic_indicator": "Economic indicator",
    "ipo_listing": "IPO / listing",
    "leadership_change": "Leadership change",
    "ma_deal": "M&A deal",
    "market_data": "Market data",
    "project_contract": "Project / contract",
    "regulatory_action": "Regulatory action",
    "strategic_plan": "Strategic plan",
    "other": "Other",
}

# Diverging scale for sentiment (red -> grey -> green)
SENTIMENT_SCALE = [
    [0.0, PALETTE["negative"]],
    [0.5, "#E2E8F0"],
    [1.0, PALETTE["positive"]],
]


def sentiment_color(value: Optional[float]) -> str:
    """Map a sentiment score in [-1, 1] to a palette color."""
    if value is None:
        return PALETTE["neutral"]
    if value > 0.05:
        return PALETTE["positive"]
    if value < -0.05:
        return PALETTE["negative"]
    return PALETTE["neutral"]


# ---------------------------------------------------------------------------
# Plotly template
# ---------------------------------------------------------------------------

_TEMPLATE = go.layout.Template(
    layout=go.Layout(
        font=dict(family="Inter, Segoe UI, system-ui, sans-serif", size=13, color=PALETTE["ink"]),
        colorway=COLORWAY,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=48, r=24, t=56, b=40),
        title=dict(font=dict(size=17, color=PALETTE["ink"]), x=0.01, xanchor="left"),
        hovermode="x unified",
        hoverlabel=dict(bgcolor="white", font_size=12, bordercolor=PALETTE["grid"]),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
            bgcolor="rgba(0,0,0,0)", font=dict(size=12),
        ),
        xaxis=dict(
            showgrid=False, zeroline=False, showline=True,
            linecolor=PALETTE["grid"], ticks="outside", tickcolor=PALETTE["grid"],
        ),
        yaxis=dict(
            showgrid=True, gridcolor=PALETTE["grid"], zeroline=False,
            showline=False, ticks="",
        ),
        colorscale=dict(diverging=SENTIMENT_SCALE),
    )
)

pio.templates["bvc"] = _TEMPLATE
pio.templates.default = "bvc"


def style_fig(fig: go.Figure, height: int = 360, title: Optional[str] = None) -> go.Figure:
    """Apply consistent sizing/title to a figure built elsewhere."""
    fig.update_layout(template="bvc", height=height)
    if title is not None:
        fig.update_layout(title=title)
    return fig


# ---------------------------------------------------------------------------
# CSS + page chrome
# ---------------------------------------------------------------------------

_CSS = """
<style>
  .block-container { padding-top: 2.2rem; padding-bottom: 3rem; max-width: 1400px; }
  #MainMenu, footer { visibility: hidden; }
  h1, h2, h3 { color: #1A2233; font-weight: 700; letter-spacing: -0.01em; }
  h1 { font-size: 2.0rem; }

  /* KPI cards */
  .kpi-grid { display: flex; gap: 1rem; flex-wrap: wrap; margin: 0.4rem 0 0.8rem; }
  .kpi-card {
    flex: 1 1 0; min-width: 150px;
    background: #FFFFFF; border: 1px solid #E5E9F0; border-radius: 14px;
    padding: 1.0rem 1.2rem; box-shadow: 0 1px 3px rgba(16,24,40,0.04);
  }
  .kpi-label { font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.04em;
               color: #64748B; font-weight: 600; }
  .kpi-value { font-size: 1.85rem; font-weight: 700; color: #1A2233; line-height: 1.15;
               margin-top: 0.25rem; }
  .kpi-delta { font-size: 0.85rem; font-weight: 600; margin-top: 0.15rem; }
  .kpi-up { color: #16A34A; }
  .kpi-down { color: #DC2626; }
  .kpi-flat { color: #64748B; }

  .hero {
    background: linear-gradient(120deg, #1E3A8A 0%, #2563EB 60%, #0D9488 100%);
    border-radius: 18px; padding: 1.8rem 2rem; color: #fff; margin-bottom: 1.4rem;
  }
  .hero h1 { color: #fff; margin-bottom: 0.2rem; }
  .hero p { color: #DBEAFE; font-size: 1.02rem; margin: 0; }

  .pill { display: inline-block; padding: 0.15rem 0.6rem; border-radius: 999px;
          font-size: 0.78rem; font-weight: 600; background: #EFF3FB; color: #2563EB; }
</style>
"""


def inject_css() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)


def hero(title: str, subtitle: str) -> None:
    st.markdown(
        f"<div class='hero'><h1>{title}</h1><p>{subtitle}</p></div>",
        unsafe_allow_html=True,
    )


def kpi_row(cards: list[dict]) -> None:
    """Render a row of KPI cards.

    Each card dict: {label, value, delta (optional str), trend ('up'|'down'|'flat')}.
    """
    html = ["<div class='kpi-grid'>"]
    for c in cards:
        delta_html = ""
        if c.get("delta"):
            cls = {"up": "kpi-up", "down": "kpi-down"}.get(c.get("trend", "flat"), "kpi-flat")
            arrow = {"up": "▲ ", "down": "▼ "}.get(c.get("trend", "flat"), "")
            delta_html = f"<div class='kpi-delta {cls}'>{arrow}{c['delta']}</div>"
        html.append(
            "<div class='kpi-card'>"
            f"<div class='kpi-label'>{c['label']}</div>"
            f"<div class='kpi-value'>{c['value']}</div>"
            f"{delta_html}</div>"
        )
    html.append("</div>")
    st.markdown("".join(html), unsafe_allow_html=True)


def page_header(title: str, subtitle: str, icon: str = "📊") -> None:
    inject_css()
    st.markdown(f"### {icon} {title}")
    st.caption(subtitle)

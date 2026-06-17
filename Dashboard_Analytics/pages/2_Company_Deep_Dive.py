import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

import db
from theme import COLORWAY, PALETTE, page_header, style_fig

st.set_page_config(page_title="Company Deep-Dive", page_icon="🔎", layout="wide")
page_header(
    "Company Deep-Dive",
    "Price action overlaid with news sentiment and detected events for a single company.",
    icon="🔎",
)

companies = db.companies()
if companies.empty:
    st.info("No companies in `gold.company_dim`. Run the gold pipeline first.")
    st.stop()

labels = {
    f"{r.ticker} — {r.company_name}": r.ticker
    for r in companies.itertuples()
}
choice = st.sidebar.selectbox("Company", list(labels.keys()))
ticker = labels[choice]
days = st.sidebar.slider("Lookback (days)", 60, 730, 365, step=30)

prof = companies[companies["ticker"] == ticker].iloc[0]
sig = db.daily_signals(ticker, days)
evts = db.ticker_events(ticker)

# --- Profile card -----------------------------------------------------------
st.markdown(f"## {prof['company_name']}  <span class='pill'>{ticker}</span>", unsafe_allow_html=True)
p1, p2, p3, p4, p5 = st.columns(5)
p1.metric("Sector", prof["sector"] or "—")
p2.metric("Founded", int(prof["founded"]) if pd.notna(prof["founded"]) else "—")
p3.metric("Employees", f"{int(prof['employees']):,}" if pd.notna(prof["employees"]) else "—")
p4.metric("Total news", f"{int(prof['total_news_count']):,}" if pd.notna(prof["total_news_count"]) else "0")
sent = prof["avg_sentiment_all_time"]
p5.metric("Sentiment (all-time)", f"{sent:+.2f}" if pd.notna(sent) else "—")
meta = []
if pd.notna(prof["ceo"]):
    meta.append(f"**CEO:** {prof['ceo']}")
if pd.notna(prof["headquarters"]):
    meta.append(f"**HQ:** {prof['headquarters']}")
if pd.notna(prof["top_event_type"]):
    meta.append(f"**Most common event:** {prof['top_event_type']}")
if meta:
    st.caption("  ·  ".join(meta))

st.divider()

if sig.empty or sig["close"].notna().sum() == 0:
    st.info("No price history for this company in `gold.daily_signals`.")
    st.stop()

sig["date"] = pd.to_datetime(sig["date"])

# --- Candlestick + volume + sentiment overlay ------------------------------
fig = make_subplots(
    rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.04,
    row_heights=[0.72, 0.28], specs=[[{"secondary_y": True}], [{"secondary_y": False}]],
)

has_ohlc = sig[["open", "high", "low", "close"]].notna().all(axis=1).any()
if has_ohlc:
    fig.add_trace(
        go.Candlestick(
            x=sig["date"], open=sig["open"], high=sig["high"], low=sig["low"], close=sig["close"],
            name="Price", increasing_line_color=PALETTE["positive"],
            decreasing_line_color=PALETTE["negative"], showlegend=False,
        ),
        row=1, col=1, secondary_y=False,
    )
else:
    fig.add_trace(
        go.Scatter(x=sig["date"], y=sig["close"], name="Close",
                   line=dict(color=PALETTE["ink"], width=1.8)),
        row=1, col=1, secondary_y=False,
    )

# 7-day rolling sentiment on secondary axis
if sig["avg_sentiment_r7d"].notna().any():
    fig.add_trace(
        go.Scatter(
            x=sig["date"], y=sig["avg_sentiment_r7d"], name="Sentiment (7d)",
            line=dict(color=PALETTE["primary"], width=2, dash="dot"),
        ),
        row=1, col=1, secondary_y=True,
    )

# Event markers along the top of the price pane
if not evts.empty:
    evts["event_date"] = pd.to_datetime(evts["event_date"])
    evts = evts[(evts["event_date"] >= sig["date"].min()) & (evts["event_date"] <= sig["date"].max())]
    if not evts.empty:
        ev_types = sorted(evts["event_type"].dropna().unique())
        cmap = {t: COLORWAY[i % len(COLORWAY)] for i, t in enumerate(ev_types)}
        y_top = sig["high"].max() if has_ohlc else sig["close"].max()
        for t in ev_types:
            sub = evts[evts["event_type"] == t]
            fig.add_trace(
                go.Scatter(
                    x=sub["event_date"], y=[y_top] * len(sub), mode="markers",
                    name=t, marker=dict(symbol="triangle-down", size=10, color=cmap[t]),
                    hovertemplate=f"{t}<br>%{{x|%Y-%m-%d}}<extra></extra>",
                ),
                row=1, col=1, secondary_y=False,
            )

# Volume pane
if sig["volume"].notna().any():
    fig.add_trace(
        go.Bar(x=sig["date"], y=sig["volume"], name="Volume",
               marker_color=PALETTE["neutral"], opacity=0.6, showlegend=False),
        row=2, col=1,
    )

fig.update_yaxes(title_text="Price", row=1, col=1, secondary_y=False)
fig.update_yaxes(title_text="Sentiment", row=1, col=1, secondary_y=True, showgrid=False)
fig.update_yaxes(title_text="Volume", row=2, col=1)
fig.update_xaxes(rangeslider_visible=False)
fig.update_layout(hovermode="x unified")
st.plotly_chart(style_fig(fig, 560, f"{ticker} — price, sentiment & events"), use_container_width=True)

st.divider()

# --- Rolling news & sentiment trend ----------------------------------------
c1, c2 = st.columns(2)
with c1:
    st.markdown("##### News flow")
    if sig["news_count"].notna().any():
        fig = go.Figure(go.Bar(
            x=sig["date"], y=sig["news_count"], marker_color=PALETTE["primary"],
            hovertemplate="%{x|%Y-%m-%d}<br>%{y} articles<extra></extra>",
        ))
        fig.update_layout(yaxis_title="articles/day")
        st.plotly_chart(style_fig(fig, 300), use_container_width=True)
    else:
        st.caption("No news counts available.")

with c2:
    st.markdown("##### Positive vs negative ratio (7d)")
    if sig[["positive_ratio_r7d", "negative_ratio_r7d"]].notna().any().any():
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=sig["date"], y=sig["positive_ratio_r7d"], name="Positive",
            mode="lines", line=dict(color=PALETTE["positive"], width=2),
            stackgroup="one", fillcolor="rgba(22,163,74,0.15)",
        ))
        fig.add_trace(go.Scatter(
            x=sig["date"], y=sig["negative_ratio_r7d"], name="Negative",
            mode="lines", line=dict(color=PALETTE["negative"], width=2),
            stackgroup="one", fillcolor="rgba(220,38,38,0.15)",
        ))
        fig.update_layout(yaxis_title="ratio")
        st.plotly_chart(style_fig(fig, 300), use_container_width=True)
    else:
        st.caption("No sentiment-ratio data available.")

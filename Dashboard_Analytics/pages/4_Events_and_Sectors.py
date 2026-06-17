import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import db
from theme import COLORWAY, LLM_LABELS, PALETTE, SENTIMENT_SCALE, page_header, style_fig

st.set_page_config(page_title="Events & Sectors", page_icon="🗓️", layout="wide")
page_header(
    "Events & Sectors",
    "Detected events over time by type, the most active companies, and sector activity.",
    icon="🗓️",
)

freq = st.sidebar.radio("Time granularity", ["month", "week"], index=0)
by_time = db.events_by_type_over_time(freq)
active = db.most_active_companies(15)
sectors = db.sector_sentiment(120)

# --- Events over time by type ----------------------------------------------
st.markdown("#### Event flow by type")
if by_time.empty:
    st.info("No events in `gold.event_facts` yet. Run the gold pipeline first.")
else:
    by_time["period"] = pd.to_datetime(by_time["period"])
    by_time["label"] = by_time["event_type"].map(lambda t: LLM_LABELS.get(t, t))
    pivot = by_time.pivot_table(index="period", columns="label", values="n", aggfunc="sum", fill_value=0)
    fig = go.Figure()
    for i, col in enumerate(pivot.columns):
        fig.add_trace(go.Bar(
            x=pivot.index, y=pivot[col], name=col, marker_color=COLORWAY[i % len(COLORWAY)],
        ))
    fig.update_layout(barmode="stack", yaxis_title="events", xaxis_title=None)
    st.plotly_chart(style_fig(fig, 420, f"Events per {freq}"), use_container_width=True)

st.divider()

c1, c2 = st.columns([4, 5])

# --- Most active companies --------------------------------------------------
with c1:
    st.markdown("#### Most active companies")
    if active.empty:
        st.caption("No event-company links available.")
    else:
        a = active.sort_values("event_count")
        fig = go.Figure(go.Bar(
            x=a["event_count"], y=a["ticker"], orientation="h", marker_color=PALETTE["primary"],
            hovertext=a["company_name"], hovertemplate="%{hovertext}<br>%{x} events<extra></extra>",
        ))
        fig.update_layout(xaxis_title="events", yaxis_title=None)
        st.plotly_chart(style_fig(fig, 460), use_container_width=True)

# --- Sector × event-type matrix --------------------------------------------
with c2:
    st.markdown("#### Sector × event-type intensity")
    matrix = db.events(limit=5000)
    comps = db.companies()
    if matrix.empty or comps.empty:
        st.caption("Not enough data for a sector/event matrix.")
    else:
        # explode tickers -> sector
        matrix = matrix.dropna(subset=["tickers", "event_type"]).copy()
        matrix["ticker"] = matrix["tickers"].str.split(", ")
        ex = matrix.explode("ticker")
        ex = ex.merge(comps[["ticker", "sector"]], on="ticker", how="left").dropna(subset=["sector"])
        ex["label"] = ex["event_type"].map(lambda t: LLM_LABELS.get(t, t))
        if ex.empty:
            st.caption("No sector-mapped events available.")
        else:
            heat = ex.pivot_table(index="sector", columns="label", values="fingerprint",
                                  aggfunc="nunique", fill_value=0)
            fig = px.imshow(
                heat, color_continuous_scale="Blues", aspect="auto",
                labels=dict(color="events"),
            )
            fig.update_xaxes(tickangle=-40)
            st.plotly_chart(style_fig(fig, 460), use_container_width=True)

st.divider()

# --- Sector sentiment snapshot ---------------------------------------------
st.markdown("#### Sector sentiment vs activity (last 120d)")
if sectors.empty:
    st.caption("No sector sentiment data available.")
else:
    s = sectors.dropna(subset=["avg_sentiment", "news_count"])
    fig = go.Figure(go.Scatter(
        x=s["news_count"], y=s["avg_sentiment"], mode="markers+text",
        text=s["sector"], textposition="top center",
        marker=dict(
            size=(s["companies"].fillna(1) * 6 + 8),
            color=s["avg_sentiment"], colorscale=SENTIMENT_SCALE, cmid=0,
            showscale=True, colorbar=dict(title="sentiment"),
            line=dict(width=1, color="white"),
        ),
        hovertemplate="%{text}<br>news %{x}<br>sentiment %{y:+.3f}<extra></extra>",
    ))
    fig.add_hline(y=0, line_dash="dot", line_color=PALETTE["neutral"])
    fig.update_layout(xaxis_title="news volume", yaxis_title="avg sentiment")
    st.plotly_chart(style_fig(fig, 440), use_container_width=True)

st.divider()

# --- Recent events table ----------------------------------------------------
st.markdown("#### Recent events")
recent = db.events(limit=200)
if recent.empty:
    st.caption("No events to show.")
else:
    types = ["All"] + sorted(recent["event_type"].dropna().unique().tolist())
    pick = st.selectbox("Filter by type", types)
    show = recent if pick == "All" else recent[recent["event_type"] == pick]
    show = show[["event_date", "event_type", "tickers", "article_count"]].copy()
    show["event_type"] = show["event_type"].map(lambda t: LLM_LABELS.get(t, t))
    st.dataframe(
        show.rename(columns={"event_date": "date", "event_type": "type",
                             "article_count": "articles"}),
        hide_index=True, use_container_width=True,
    )

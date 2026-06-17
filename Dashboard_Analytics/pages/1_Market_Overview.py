import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import db
from theme import COLORWAY, PALETTE, page_header, sentiment_color, style_fig

st.set_page_config(page_title="Market Overview", page_icon="🌍", layout="wide")
page_header(
    "Market Overview",
    "Cross-company landscape: movers, sector sentiment, news flow and commodity context.",
    icon="🌍",
)

window = st.sidebar.slider("Lookback (days)", 14, 180, 90, step=7)

latest = db.market_latest()
vol = db.market_news_volume(window)
sectors = db.sector_sentiment(window)
comm = db.commodities()

if latest.empty:
    st.info("No data in `gold.daily_signals` yet. Run the gold pipeline to populate it.")
    st.stop()

# --- Top movers -------------------------------------------------------------
st.markdown("#### Top movers")
movers = latest.dropna(subset=["change_pct"]).copy()
if movers.empty:
    st.info("No price-change data available.")
else:
    left, right = st.columns(2)
    gainers = movers.nlargest(10, "change_pct").sort_values("change_pct")
    losers = movers.nsmallest(10, "change_pct").sort_values("change_pct")

    with left:
        fig = go.Figure(go.Bar(
            x=gainers["change_pct"], y=gainers["ticker"], orientation="h",
            marker_color=PALETTE["positive"],
            hovertext=gainers["company_name"], hovertemplate="%{hovertext}<br>%{x:+.2f}%<extra></extra>",
        ))
        fig.update_layout(yaxis_title=None, xaxis_title="Δ %")
        st.plotly_chart(style_fig(fig, 340, "Top gainers"), use_container_width=True)
    with right:
        fig = go.Figure(go.Bar(
            x=losers["change_pct"], y=losers["ticker"], orientation="h",
            marker_color=PALETTE["negative"],
            hovertext=losers["company_name"], hovertemplate="%{hovertext}<br>%{x:+.2f}%<extra></extra>",
        ))
        fig.update_layout(yaxis_title=None, xaxis_title="Δ %")
        st.plotly_chart(style_fig(fig, 340, "Top losers"), use_container_width=True)

st.divider()

# --- Sector sentiment + news volume ----------------------------------------
c1, c2 = st.columns([5, 4])

with c1:
    st.markdown("#### Sector sentiment")
    if sectors.empty:
        st.info("No sector-level data available.")
    else:
        s = sectors.dropna(subset=["avg_sentiment"]).sort_values("avg_sentiment")
        colors = [sentiment_color(v) for v in s["avg_sentiment"]]
        fig = go.Figure(go.Bar(
            x=s["avg_sentiment"], y=s["sector"], orientation="h", marker_color=colors,
            hovertemplate="%{y}<br>sentiment %{x:+.3f}<extra></extra>",
        ))
        fig.update_layout(xaxis_title="avg sentiment", yaxis_title=None)
        st.plotly_chart(style_fig(fig, 420, f"Avg sentiment by sector ({window}d)"), use_container_width=True)

with c2:
    st.markdown("#### News volume by sector")
    if sectors.empty:
        st.info("No sector-level data available.")
    else:
        s = sectors.dropna(subset=["news_count"]).sort_values("news_count", ascending=False)
        fig = go.Figure(go.Bar(
            x=s["sector"], y=s["news_count"], marker_color=PALETTE["primary"],
            hovertemplate="%{x}<br>%{y} articles<extra></extra>",
        ))
        fig.update_layout(xaxis_title=None, yaxis_title="articles")
        fig.update_xaxes(tickangle=-40)
        st.plotly_chart(style_fig(fig, 420, f"Articles by sector ({window}d)"), use_container_width=True)

st.divider()

# --- Market news flow + sentiment over time --------------------------------
st.markdown("#### Market news flow & sentiment")
if vol.empty:
    st.info("No news-volume history available.")
else:
    vol["date"] = pd.to_datetime(vol["date"])
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=vol["date"], y=vol["news_count"], name="Articles",
        marker_color=PALETTE["neutral"], opacity=0.55, yaxis="y",
    ))
    fig.add_trace(go.Scatter(
        x=vol["date"], y=vol["avg_sentiment"], name="Avg sentiment",
        mode="lines", line=dict(color=PALETTE["primary"], width=2.5), yaxis="y2",
    ))
    fig.update_layout(
        yaxis=dict(title="articles"),
        yaxis2=dict(title="sentiment", overlaying="y", side="right", showgrid=False),
        hovermode="x unified",
    )
    st.plotly_chart(style_fig(fig, 380), use_container_width=True)

st.divider()

# --- Commodities / forex ----------------------------------------------------
st.markdown("#### Commodity & forex context")
if comm.empty:
    st.caption("No commodity data in `gold.commodity_daily`.")
else:
    comm = comm.sort_values("category")
    cols = st.columns(min(5, len(comm)))
    for i, (_, row) in enumerate(comm.head(10).iterrows()):
        col = cols[i % len(cols)]
        ret = row.get("daily_return")
        trend = "📈" if (ret or 0) > 0 else "📉" if (ret or 0) < 0 else "▪️"
        delta = f"{ret:+.2%}" if pd.notna(ret) else "—"
        col.metric(
            label=f"{trend} {row['name'] or row['asset_key']}",
            value=f"{row['close']:.4g}" if pd.notna(row["close"]) else "—",
            delta=delta if delta != "—" else None,
        )

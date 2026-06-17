import pandas as pd
import streamlit as st

import db
from theme import hero, inject_css, kpi_row, PALETTE

st.set_page_config(
    page_title="BVC Market Intelligence",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

inject_css()
hero(
    "BVC Market Intelligence",
    "News-driven analytics for the Casablanca Stock Exchange — sentiment, events and price signals from the gold layer.",
)


@st.cache_data(ttl=300)
def _load():
    return db.companies(), db.market_latest(), db.market_news_volume(7)


companies, latest, vol7 = _load()

if companies.empty and latest.empty:
    st.warning(
        "No gold-layer data found. Make sure Postgres is reachable and the "
        "`gold_layer` pipeline has run at least once."
    )
    err = db.last_error()
    if err:
        with st.expander("Connection details"):
            st.code(err)
    st.stop()

# --- Headline KPIs ----------------------------------------------------------
n_companies = int(companies["ticker"].nunique()) if not companies.empty else 0
n_sectors = int(companies["sector"].nunique()) if not companies.empty else 0

avg_sent = latest["avg_sentiment_r7d"].dropna().mean() if not latest.empty else None
advancing = (
    (latest["change_pct"] > 0).mean() * 100
    if not latest.empty and latest["change_pct"].notna().any()
    else None
)
news_7d = int(vol7["news_count"].sum()) if not vol7.empty else 0

kpi_row([
    {"label": "Companies tracked", "value": f"{n_companies}"},
    {"label": "Sectors", "value": f"{n_sectors}"},
    {
        "label": "Market sentiment (7d)",
        "value": f"{avg_sent:+.2f}" if avg_sent is not None else "—",
        "delta": "bullish" if (avg_sent or 0) > 0 else "bearish" if avg_sent else None,
        "trend": "up" if (avg_sent or 0) > 0 else "down" if (avg_sent or 0) < 0 else "flat",
    },
    {
        "label": "Advancing",
        "value": f"{advancing:.0f}%" if advancing is not None else "—",
        "trend": "up" if (advancing or 0) >= 50 else "down",
    },
    {"label": "Articles (7d)", "value": f"{news_7d:,}"},
])

st.divider()

# --- Navigation cards -------------------------------------------------------
st.markdown("#### Explore")
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown("##### 🌍 Market Overview")
    st.caption("Top movers, sector sentiment, news volume and commodity context.")
    st.page_link("pages/1_Market_Overview.py", label="Open", icon="➡️")
with c2:
    st.markdown("##### 🔎 Company Deep-Dive")
    st.caption("Price candlesticks with sentiment overlay and event markers.")
    st.page_link("pages/2_Company_Deep_Dive.py", label="Open", icon="➡️")
with c3:
    st.markdown("##### ⚖️ Sentiment ↔ Price")
    st.caption("Does news sentiment relate to next-day returns?")
    st.page_link("pages/3_Sentiment_vs_Price.py", label="Open", icon="➡️")
with c4:
    st.markdown("##### 🗓️ Events & Sectors")
    st.caption("Event timeline by type and sector activity heatmaps.")
    st.page_link("pages/4_Events_and_Sectors.py", label="Open", icon="➡️")

st.divider()

# --- Movers preview ---------------------------------------------------------
if not latest.empty and latest["change_pct"].notna().any():
    movers = latest.dropna(subset=["change_pct"]).copy()
    left, right = st.columns(2)
    with left:
        st.markdown("##### Top gainers")
        top = movers.nlargest(5, "change_pct")[["ticker", "company_name", "change_pct", "avg_sentiment_r7d"]]
        st.dataframe(
            top.rename(columns={"change_pct": "Δ %", "avg_sentiment_r7d": "sent (7d)", "company_name": "name"}),
            hide_index=True, use_container_width=True,
        )
    with right:
        st.markdown("##### Top losers")
        bot = movers.nsmallest(5, "change_pct")[["ticker", "company_name", "change_pct", "avg_sentiment_r7d"]]
        st.dataframe(
            bot.rename(columns={"change_pct": "Δ %", "avg_sentiment_r7d": "sent (7d)", "company_name": "name"}),
            hide_index=True, use_container_width=True,
        )
    asof = pd.to_datetime(latest["date"]).max()
    st.caption(f"Snapshot as of {asof.date()}.")

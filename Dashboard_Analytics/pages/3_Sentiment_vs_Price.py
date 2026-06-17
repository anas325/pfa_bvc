import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import db
from theme import LLM_LABELS, PALETTE, page_header, sentiment_color, style_fig

st.set_page_config(page_title="Sentiment vs Price", page_icon="⚖️", layout="wide")
page_header(
    "Sentiment ↔ Price",
    "Does news sentiment relate to returns? Exploratory, correlational — not a trading signal.",
    icon="⚖️",
)

st.info(
    "These views are **correlational**. They show co-movement between news sentiment and "
    "returns, not a causal or predictive relationship.",
    icon="ℹ️",
)

days = st.sidebar.slider("Lookback (days)", 90, 730, 365, step=30)
pairs = db.sentiment_return_pairs(days)

if pairs.empty:
    st.info("No paired sentiment/return data available. Run the gold pipeline first.")
    st.stop()

pairs = pairs.dropna(subset=["avg_sentiment"])
pairs["next_day_return"] = pd.to_numeric(pairs["next_day_return"], errors="coerce")
pairs["daily_return"] = pd.to_numeric(pairs["daily_return"], errors="coerce")
pairs["avg_sentiment"] = pd.to_numeric(pairs["avg_sentiment"], errors="coerce")

# --- Scatter: sentiment vs next-day return ---------------------------------
st.markdown("#### Sentiment vs next-day return")
scatter = pairs.dropna(subset=["next_day_return"]).copy()
# clip extreme returns for a readable axis
if not scatter.empty:
    lo, hi = scatter["next_day_return"].quantile([0.01, 0.99])
    scatter = scatter[(scatter["next_day_return"] >= lo) & (scatter["next_day_return"] <= hi)]

if scatter.empty or len(scatter) < 5:
    st.caption("Not enough paired observations to plot.")
else:
    colors = [sentiment_color(v) for v in scatter["avg_sentiment"]]
    fig = go.Figure(go.Scatter(
        x=scatter["avg_sentiment"], y=scatter["next_day_return"], mode="markers",
        marker=dict(color=colors, size=6, opacity=0.5),
        text=scatter["ticker"],
        hovertemplate="%{text}<br>sent %{x:+.2f}<br>next ret %{y:+.2%}<extra></extra>",
        name="obs",
    ))
    # OLS trend line
    x = scatter["avg_sentiment"].to_numpy()
    y = scatter["next_day_return"].to_numpy()
    if np.ptp(x) > 0:
        b, a = np.polyfit(x, y, 1)
        xs = np.linspace(x.min(), x.max(), 50)
        fig.add_trace(go.Scatter(
            x=xs, y=a + b * xs, mode="lines",
            line=dict(color=PALETTE["ink"], width=2), name="trend",
        ))
        r = np.corrcoef(x, y)[0, 1]
        st.caption(f"Pearson correlation r = **{r:+.3f}** over {len(scatter):,} observations.")
    fig.update_layout(xaxis_title="avg sentiment (news day)", yaxis_title="next-day return")
    fig.update_yaxes(tickformat=".1%")
    fig.update_layout(hovermode="closest")
    st.plotly_chart(style_fig(fig, 440), use_container_width=True)

st.divider()

# --- Per-ticker lead-lag correlation table ---------------------------------
st.markdown("#### Lead-lag correlation by company")
st.caption("Correlation of news-day sentiment with same-day and next-day return (min 10 news days).")

rows = []
for tk, g in pairs.groupby("ticker"):
    g_same = g.dropna(subset=["daily_return"])
    g_next = g.dropna(subset=["next_day_return"])
    if len(g_next) < 10:
        continue
    same = (
        np.corrcoef(g_same["avg_sentiment"], g_same["daily_return"])[0, 1]
        if len(g_same) >= 10 and g_same["avg_sentiment"].nunique() > 1 else np.nan
    )
    nxt = (
        np.corrcoef(g_next["avg_sentiment"], g_next["next_day_return"])[0, 1]
        if g_next["avg_sentiment"].nunique() > 1 else np.nan
    )
    rows.append({
        "ticker": tk, "sector": g["sector"].iloc[0], "news_days": len(g_next),
        "corr_same_day": same, "corr_next_day": nxt,
    })

corr_df = pd.DataFrame(rows)
if corr_df.empty:
    st.caption("Not enough per-company history to compute correlations.")
else:
    corr_df = corr_df.sort_values("corr_next_day", ascending=False)
    st.dataframe(
        corr_df.style.format({"corr_same_day": "{:+.3f}", "corr_next_day": "{:+.3f}"})
        .background_gradient(cmap="RdYlGn", subset=["corr_same_day", "corr_next_day"], vmin=-0.5, vmax=0.5),
        hide_index=True, use_container_width=True,
    )

st.divider()

# --- Event-type impact on next-day return ----------------------------------
st.markdown("#### Average next-day return by event type")
st.caption("Mean next-day return on days an event type appears vs days it doesn't.")

impact = db.event_impact()
if impact.empty:
    st.caption("No event-impact data available.")
else:
    recs = []
    for ev in db.LLM_EVENT_TYPES:
        present = impact.get(f"{ev}__present", pd.Series([np.nan])).iloc[0]
        absent = impact.get(f"{ev}__absent", pd.Series([np.nan])).iloc[0]
        n = impact.get(f"{ev}__n", pd.Series([0])).iloc[0]
        if pd.notna(present) and (n or 0) > 0:
            recs.append({
                "event_type": LLM_LABELS.get(ev, ev), "n_days": int(n),
                "present": float(present), "absent": float(absent) if pd.notna(absent) else np.nan,
                "lift": float(present) - (float(absent) if pd.notna(absent) else 0.0),
            })
    imp_df = pd.DataFrame(recs)
    if imp_df.empty:
        st.caption("No event days with next-day return data.")
    else:
        imp_df = imp_df.sort_values("lift")
        colors = [PALETTE["positive"] if v >= 0 else PALETTE["negative"] for v in imp_df["lift"]]
        fig = go.Figure(go.Bar(
            x=imp_df["lift"], y=imp_df["event_type"], orientation="h", marker_color=colors,
            customdata=imp_df[["n_days", "present"]],
            hovertemplate="%{y}<br>lift %{x:+.2%}<br>n=%{customdata[0]} days<extra></extra>",
        ))
        fig.update_layout(xaxis_title="next-day return lift (present − absent)", yaxis_title=None)
        fig.update_xaxes(tickformat=".1%")
        st.plotly_chart(style_fig(fig, 440), use_container_width=True)

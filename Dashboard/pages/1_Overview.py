import pandas as pd
import streamlit as st

from db import articles_total, query_df, stock_prices_today

st.set_page_config(page_title="Overview", page_icon=":bar_chart:", layout="wide")
st.title("Overview (last 24h)")


@st.cache_data(ttl=5)
def runs_24h() -> pd.DataFrame:
    return query_df(
        """
        SELECT pipeline_name, status, rows_processed, rows_failed,
               started_at, ended_at
        FROM pipeline_runs
        WHERE started_at >= datetime('now', '-1 day')
        ORDER BY started_at DESC
        """
    )


@st.cache_data(ttl=30)
def runs_7d_trend() -> pd.DataFrame:
    return query_df(
        """
        SELECT date(started_at) AS day, pipeline_name, status,
               COUNT(*) AS runs,
               COALESCE(SUM(rows_processed), 0) AS rows_processed
        FROM pipeline_runs
        WHERE started_at >= datetime('now', '-7 days')
        GROUP BY day, pipeline_name, status
        ORDER BY day
        """
    )


@st.cache_data(ttl=5)
def error_events_24h() -> pd.DataFrame:
    return query_df(
        """
        SELECT ts, level, stage, message, item_key
        FROM pipeline_events
        WHERE level IN ('warning', 'error')
          AND ts >= datetime('now', '-1 day')
        ORDER BY ts DESC
        LIMIT 50
        """
    )


df = runs_24h()

total_runs = len(df)
active = int((df["status"] == "running").sum()) if total_runs else 0
succeeded = int((df["status"] == "success").sum()) if total_runs else 0
failed = int((df["status"] == "failed").sum()) if total_runs else 0
rows_processed = int(df["rows_processed"].sum()) if total_runs else 0
rows_failed = int(df["rows_failed"].sum()) if total_runs else 0
completed = succeeded + failed
success_rate = (succeeded / completed * 100) if completed else 0.0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Runs (24h)", total_runs)
c2.metric("Active", active)
c3.metric("Success rate", f"{success_rate:.0f}%" if completed else "—")
c4.metric("Rows processed", rows_processed)
c5.metric("Rows failed", rows_failed, delta=None if not rows_failed else f"-{rows_failed}")

st.divider()

left, right = st.columns(2)

with left:
    st.subheader("Runs per pipeline")
    if not df.empty:
        by_pipe = df.groupby(["pipeline_name", "status"]).size().unstack(fill_value=0)
        st.bar_chart(by_pipe)
    else:
        st.info("No runs in the last 24 hours.")

with right:
    st.subheader("Live Postgres row counts")
    sp_today = stock_prices_today()
    arts = articles_total()
    pc1, pc2 = st.columns(2)
    pc1.metric("stock_prices_daily today", sp_today if sp_today is not None else "n/a")
    pc2.metric("articles (total)", arts if arts is not None else "n/a")

st.divider()

trend = runs_7d_trend()
tl, tr = st.columns(2)

with tl:
    st.subheader("Run volume (7 days)")
    if not trend.empty:
        vol = trend.pivot_table(index="day", columns="status", values="runs", aggfunc="sum", fill_value=0)
        st.bar_chart(vol)
    else:
        st.info("No data yet.")

with tr:
    st.subheader("Rows processed (7 days)")
    if not trend.empty:
        thru = trend.pivot_table(index="day", columns="pipeline_name", values="rows_processed", aggfunc="sum", fill_value=0)
        st.line_chart(thru)
    else:
        st.info("No data yet.")

st.divider()

st.subheader("Recent warnings & errors")
errors = error_events_24h()
if errors.empty:
    st.success("No warnings or errors in the last 24 hours.")
else:
    st.dataframe(errors, use_container_width=True, hide_index=True)

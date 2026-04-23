import pandas as pd
import streamlit as st

from db import query_df

st.set_page_config(page_title="Runs", page_icon=":clipboard:", layout="wide")
st.title("Pipeline runs")


@st.cache_data(ttl=5)
def load_runs(limit: int = 500) -> pd.DataFrame:
    return query_df(
        """
        SELECT run_id, pipeline_name, status, started_at, ended_at,
               rows_processed, rows_failed, dag_run_id, task_id, error_message
        FROM pipeline_runs
        ORDER BY started_at DESC
        LIMIT ?
        """,
        (limit,),
    )


@st.cache_data(ttl=5)
def load_events(run_id: str) -> pd.DataFrame:
    return query_df(
        """
        SELECT ts, level, stage, message, item_key, metric_name, metric_value
        FROM pipeline_events
        WHERE run_id = ?
        ORDER BY id ASC
        """,
        (run_id,),
    )


runs = load_runs()

if runs.empty:
    st.info("No runs recorded yet.")
    st.stop()

pipelines = ["(all)"] + sorted(runs["pipeline_name"].unique().tolist())
statuses = ["(all)"] + sorted(runs["status"].unique().tolist())

f1, f2 = st.columns(2)
pipeline_filter = f1.selectbox("Pipeline", pipelines)
status_filter = f2.selectbox("Status", statuses)

filtered = runs.copy()
if pipeline_filter != "(all)":
    filtered = filtered[filtered["pipeline_name"] == pipeline_filter]
if status_filter != "(all)":
    filtered = filtered[filtered["status"] == status_filter]

st.caption(f"{len(filtered)} runs")
st.dataframe(
    filtered.drop(columns=["error_message"]),
    use_container_width=True,
    hide_index=True,
)

st.divider()
st.subheader("Drill into a run")

if not filtered.empty:
    run_id = st.selectbox(
        "run_id",
        filtered["run_id"].tolist(),
        format_func=lambda rid: f"{rid[:8]} — {filtered.loc[filtered.run_id == rid, 'pipeline_name'].iloc[0]}",
    )

    run_row = filtered[filtered["run_id"] == run_id].iloc[0]
    if run_row["error_message"]:
        st.error(run_row["error_message"])

    events = load_events(run_id)
    if events.empty:
        st.info("No events logged for this run.")
    else:
        metrics = events[events["metric_name"].notna()].copy()
        if not metrics.empty:
            st.subheader("Metrics")
            chart_data = metrics.set_index("metric_name")["metric_value"]
            st.bar_chart(chart_data)

        level_order = ["error", "warning", "info"]
        level_counts = (
            events["level"]
            .value_counts()
            .reindex(level_order)
            .dropna()
            .astype(int)
        )
        if len(level_counts) > 1:
            st.subheader("Event levels")
            st.bar_chart(level_counts)

        st.subheader("Events")
        st.dataframe(events, use_container_width=True, hide_index=True)

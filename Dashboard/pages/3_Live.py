import time

import pandas as pd
import streamlit as st

from db import query_df

st.set_page_config(page_title="Live", page_icon=":satellite:", layout="wide")
st.title("Live progress")

REFRESH_SECONDS = 5


def active_runs() -> pd.DataFrame:
    return query_df(
        """
        SELECT run_id, pipeline_name, started_at,
               rows_processed, rows_failed, dag_run_id, task_id
        FROM pipeline_runs
        WHERE status = 'running'
        ORDER BY started_at DESC
        """
    )


def recent_events(limit: int = 50) -> pd.DataFrame:
    return query_df(
        """
        SELECT ts, level, stage, message, item_key, metric_name, metric_value
        FROM pipeline_events
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    )


placeholder = st.empty()

with placeholder.container():
    active = active_runs()
    st.subheader(f"Active runs ({len(active)})")
    if active.empty:
        st.info("Nothing running right now.")
    else:
        st.dataframe(active, use_container_width=True, hide_index=True)

    st.subheader("Latest events")
    events = recent_events()
    if events.empty:
        st.caption("No events logged yet.")
    else:
        st.dataframe(events, use_container_width=True, hide_index=True)

    st.caption(f"Auto-refreshing every {REFRESH_SECONDS}s.")

time.sleep(REFRESH_SECONDS)
st.rerun()

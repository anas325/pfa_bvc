import streamlit as st

from db import SQLITE_PATH, sqlite_available

st.set_page_config(
    page_title="BVC Pipeline Monitor",
    page_icon=":bar_chart:",
    layout="wide",
)

st.title("BVC Pipeline Monitor")
st.caption("Structured logs from Airflow-orchestrated pipelines (RSS, stock prices, agent).")

if not sqlite_available():
    st.warning(
        f"No log database found at `{SQLITE_PATH}`. "
        "The dashboard will populate once a pipeline run writes its first event."
    )
else:
    st.success(f"Reading logs from `{SQLITE_PATH}`.")

st.markdown(
    """
    Use the sidebar to navigate:

    - **Overview** — KPIs for the last 24 hours + live Postgres row counts.
    - **Runs** — browse past runs and drill into their events.
    - **Live** — auto-refreshing view of currently-running pipelines.
    """
)

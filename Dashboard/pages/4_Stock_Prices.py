import pandas as pd
import streamlit as st

from db import daily_scrape_coverage, stock_prices_history

st.set_page_config(page_title="Stock Prices", page_icon=":chart_with_upwards_trend:", layout="wide")
st.title("Stock prices")


@st.cache_data(ttl=60)
def load_history(days: int) -> pd.DataFrame:
    df = stock_prices_history(days)
    if df.empty:
        return df
    df["scraped_at"] = pd.to_datetime(df["scraped_at"])
    df["cours_num"] = (
        df["cours"]
        .astype(str)
        .str.replace(r"\s", "", regex=True)
        .str.replace(",", ".")
        .pipe(pd.to_numeric, errors="coerce")
    )
    df["variation_num"] = (
        df["variation"]
        .astype(str)
        .str.replace(r"\s", "", regex=True)
        .str.replace(",", ".")
        .str.replace("%", "")
        .pipe(pd.to_numeric, errors="coerce")
    )
    return df


@st.cache_data(ttl=60)
def load_coverage() -> pd.DataFrame:
    df = daily_scrape_coverage()
    if not df.empty:
        df["scraped_at"] = pd.to_datetime(df["scraped_at"])
    return df


days = st.sidebar.slider("Lookback (days)", min_value=7, max_value=90, value=30, step=7)
df = load_history(days)

if df.empty:
    st.warning("No stock price data available. Check that Postgres is reachable and the stock_prices_daily table has data.")
    st.stop()

all_tickers = sorted(df["ticker"].unique().tolist())
selected = st.multiselect(
    "Select tickers",
    all_tickers,
    default=all_tickers[:5] if len(all_tickers) >= 5 else all_tickers,
)

st.divider()

if selected:
    filtered = df[df["ticker"].isin(selected)]
    valid = filtered.dropna(subset=["cours_num"])

    left, right = st.columns(2)

    with left:
        st.subheader("Price history (cours)")
        if not valid.empty:
            pivot = valid.pivot_table(index="scraped_at", columns="ticker", values="cours_num")
            st.line_chart(pivot)
        else:
            st.info("Could not parse price values.")

    with right:
        st.subheader("Daily variation (%)")
        var_valid = filtered.dropna(subset=["variation_num"])
        if not var_valid.empty:
            var_pivot = var_valid.pivot_table(index="scraped_at", columns="ticker", values="variation_num")
            st.line_chart(var_pivot)
        else:
            st.info("Could not parse variation values.")

    st.divider()
    st.subheader("Latest snapshot")
    latest_date = df["scraped_at"].max()
    latest = df[df["scraped_at"] == latest_date][["ticker", "libelle", "cours", "variation", "scraped_at"]]
    st.caption(f"Scraped on {latest_date.date()}")
    st.dataframe(latest.sort_values("ticker"), use_container_width=True, hide_index=True)

st.divider()

coverage = load_coverage()
st.subheader("Daily scrape coverage (tickers / day)")
if not coverage.empty:
    st.bar_chart(coverage.set_index("scraped_at")["tickers_scraped"])
else:
    st.info("No coverage data available.")

import re

import numpy as np
import pandas as pd
from sqlalchemy.engine import Engine

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EVENT_PATTERNS: dict[str, list[re.Pattern]] = {
    "ma": [
        re.compile(p, re.IGNORECASE)
        for p in [
            r"\bacquisition\b", r"\bfusion\b", r"\brachat\b",
            r"\bOPA\b", r"\bcession\b", r"\boffre publique\b",
            r"\bprise de participation\b", r"\bmerger\b",
            r"\btakeover\b", r"\bconsortium\b",
        ]
    ],
    "earnings": [
        re.compile(p, re.IGNORECASE)
        for p in [
            r"\brésultats?\b", r"\bbénéfice[s]?\b", r"\bEBIT\b",
            r"\bEBITDA\b", r"\bchiffre d.affaires\b", r"\brevenus?\b",
            r"\bdividende[s]?\b", r"\bbénéfice net\b", r"\bmarge\b",
            r"\bperte[s]?\b", r"\bprofit[s]?\b", r"\bearnings\b",
        ]
    ],
    "management": [
        re.compile(p, re.IGNORECASE)
        for p in [
            r"\bPDG\b", r"\bdirecteur.général\b", r"\bnomination\b",
            r"\bdémission\b", r"\bconseil d.administration\b",
            r"\bprésidence\b", r"\bnommé\b", r"\bCEO\b",
            r"\bretraite\b", r"\bcomité exécutif\b",
        ]
    ],
    "legal": [
        re.compile(p, re.IGNORECASE)
        for p in [
            r"\bprocès\b", r"\blitige\b", r"\bsanction\b",
            r"\bamende\b", r"\btribunal\b", r"\brégulateur\b",
            r"\bpoursuite[s]?\b", r"\bplainte\b", r"\benquête\b",
            r"\bfraud[e]?\b", r"\bcorruption\b",
        ]
    ],
}

NEGATION_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\bne\b.{0,25}\bpas\b", r"\baucun\b", r"\bpas de\b",
        r"\bn.a pas\b", r"\bn.ont pas\b", r"\bjamais\b", r"\bsans\b",
    ]
]

SPECULATION_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\bpourrait\b", r"\benvisage\b", r"\bprévoit\b",
        r"\bselon des sources\b", r"\brumeur\b", r"\bdevrait\b",
        r"\bserait\b", r"\bnégociation[s]?\b", r"\bdiscussion[s]?\b",
        r"\bà l.étude\b", r"\bselon nos informations\b",
    ]
]

POSITIVE_STEMS = [
    "croissance", "hausse", "progression", "bénéfice", "profit",
    "succès", "record", "expansion", "rebond", "gain", "dynamique",
    "excellent", "performant", "amélioration", "solide", "fort",
    "optimiste", "positif", "accroissement", "développement",
]

NEGATIVE_STEMS = [
    "baisse", "chute", "perte", "recul", "déficit", "déclin",
    "difficile", "crise", "risque", "incertitude", "ralentissement",
    "préoccupation", "tension", "négatif", "détérioration",
    "pessimiste", "fragilité", "dégradation",
]

# All possible LLM event type values (from EventType enum in models.py)
LLM_EVENT_TYPES = [
    "capital_operation", "debt_issuance", "dividend_announcement",
    "earnings_release", "economic_indicator", "ipo_listing",
    "leadership_change", "ma_deal", "market_data", "other",
    "project_contract", "regulatory_action", "strategic_plan",
]

BASE_ROLL_COLS = [
    "news_count", "avg_sentiment", "std_sentiment",
    "positive_ratio", "negative_ratio",
    "event_ma", "event_earnings", "event_management", "event_legal",
]

LLM_EVT_COLS = [f"llm_evt_{t}" for t in LLM_EVENT_TYPES]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _any_match(text: str, patterns: list[re.Pattern]) -> bool:
    return any(p.search(text) for p in patterns)


def _extract_event_flags(text: str) -> dict:
    row: dict = {}
    for event_type, patterns in EVENT_PATTERNS.items():
        row[f"event_{event_type}"] = int(_any_match(text, patterns))
    row["is_negated"] = int(_any_match(text, NEGATION_PATTERNS))
    row["is_speculative"] = int(_any_match(text, SPECULATION_PATTERNS))
    return row


def _lexicon_sentiment(text: str) -> dict:
    words = text.lower().split()
    pos = sum(1 for w in words if any(s in w for s in POSITIVE_STEMS))
    neg = sum(1 for w in words if any(s in w for s in NEGATIVE_STEMS))
    total = pos + neg
    if total == 0:
        return {"sentiment": "neutral", "score": 0.0, "confidence": 0.0}
    score = (pos - neg) / total
    label = "positive" if score > 0.1 else "negative" if score < -0.1 else "neutral"
    conf = round(min(total / max(len(words), 1) * 5, 1.0), 3)
    return {"sentiment": label, "score": round(score, 3), "confidence": conf}


# ---------------------------------------------------------------------------
# Public transformation functions
# ---------------------------------------------------------------------------

def map_entities(
    articles_df: pd.DataFrame,
    companies_df: pd.DataFrame,
    mentions_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Returns one row per (article, ticker) pair.
    DB-extracted mentions take priority; articles without mentions get
    keyword-matched against company names.
    """
    # Articles that already have DB-level mentions
    with_mentions = articles_df.merge(
        mentions_df.rename(columns={"article_url": "url"}),
        on="url",
        how="inner",
    )

    # Articles that need keyword matching
    mentioned_urls = set(mentions_df["article_url"])
    unmapped = articles_df[~articles_df["url"].isin(mentioned_urls)].copy()

    keyword_rows = []
    search_text = (
        unmapped["title"].fillna("") + " " + unmapped["full_text"].fillna("").str[:500]
    )
    for _, company in companies_df.iterrows():
        pattern = re.compile(re.escape(company["company_name"]), re.IGNORECASE)
        matched = unmapped[search_text.str.contains(pattern, regex=True, na=False)]
        if not matched.empty:
            chunk = matched.copy()
            chunk["ticker"] = company["ticker"]
            keyword_rows.append(chunk)

    keyword_mapped = pd.concat(keyword_rows, ignore_index=True) if keyword_rows else pd.DataFrame(columns=with_mentions.columns)

    mapped = pd.concat([with_mentions, keyword_mapped], ignore_index=True)
    # published_at is tz-aware (TIMESTAMPTZ -> UTC); drop the tz so the derived
    # date matches the naive stock_prices.date on merge and the Postgres DATE columns.
    published = mapped["published_at"]
    if published.dt.tz is not None:
        published = published.dt.tz_localize(None)
    mapped["date"] = published.dt.normalize()
    return mapped


def enrich_articles(
    mapped_df: pd.DataFrame,
    article_events_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Adds rule-based event flags, lexicon sentiment fallback, and
    one-hot LLM event type columns to the mapped article-ticker rows.
    """
    df = mapped_df.copy()

    # Rule-based event flags from text
    search_col = df["title"].fillna("") + " " + df["full_text"].fillna("")
    event_flags = search_col.apply(_extract_event_flags)
    df = pd.concat([df, pd.DataFrame(list(event_flags))], axis=1)

    # Lexicon sentiment fallback for rows without LLM sentiment
    needs_lexicon = df["sentiment"].isna()
    if needs_lexicon.any():
        lex = search_col[needs_lexicon].apply(_lexicon_sentiment)
        lex_df = pd.DataFrame(list(lex), index=df[needs_lexicon].index)
        df.loc[needs_lexicon, "sentiment"] = lex_df["sentiment"]
        df.loc[needs_lexicon, "score"] = lex_df["score"]
        df.loc[needs_lexicon, "confidence"] = lex_df["confidence"]

    df["score"] = df["score"].fillna(0.0)

    # LLM event type one-hot columns
    # article_events_df has (article_url, event_type)
    if not article_events_df.empty:
        # Keep one event type per article (take first if multiple)
        evt_map = (
            article_events_df
            .drop_duplicates("article_url")
            .set_index("article_url")["event_type"]
        )
        df["llm_event_type"] = df["url"].map(evt_map)
    else:
        df["llm_event_type"] = None

    # One-hot encode with all possible types as explicit columns
    for evt_type in LLM_EVENT_TYPES:
        col = f"llm_evt_{evt_type}"
        df[col] = (df["llm_event_type"] == evt_type).astype(int)

    return df


def build_features(enriched_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates enriched article rows to daily (ticker, date) features,
    then adds 3-day and 7-day rolling means.
    """
    df = enriched_df.copy()

    # Sentiment label as binary counts
    df["is_positive"] = (df["sentiment"] == "positive").astype(int)
    df["is_negative"] = (df["sentiment"] == "negative").astype(int)

    agg: dict = {
        "url": "nunique",
        "score": ["mean", "std"],
        "is_positive": "sum",
        "is_negative": "sum",
        "event_ma": "sum",
        "event_earnings": "sum",
        "event_management": "sum",
        "event_legal": "sum",
        "is_negated": "sum",
        "is_speculative": "sum",
    }
    agg.update({col: "sum" for col in LLM_EVT_COLS})

    daily = df.groupby(["ticker", "date"]).agg(agg)
    daily.columns = [
        "news_count", "avg_sentiment", "std_sentiment",
        "positive_count", "negative_count",
        "event_ma", "event_earnings", "event_management", "event_legal",
        "negated_count", "speculative_count",
        *LLM_EVT_COLS,
    ]
    daily = daily.reset_index()

    daily["std_sentiment"] = daily["std_sentiment"].fillna(0.0)
    daily["positive_ratio"] = daily["positive_count"] / daily["news_count"].replace(0, np.nan)
    daily["negative_ratio"] = daily["negative_count"] / daily["news_count"].replace(0, np.nan)
    daily[["positive_ratio", "negative_ratio"]] = daily[["positive_ratio", "negative_ratio"]].fillna(0.0)

    daily = daily.sort_values(["ticker", "date"])

    # Rolling windows applied per ticker
    roll_targets = BASE_ROLL_COLS + LLM_EVT_COLS
    for window, suffix in [(3, "r3d"), (7, "r7d")]:
        rolled = (
            daily.groupby("ticker")[roll_targets]
            .transform(lambda x: x.rolling(window, min_periods=1).mean())
        )
        rolled.columns = [f"{c}_{suffix}" for c in roll_targets]
        daily = pd.concat([daily, rolled], axis=1)

    return daily


def build_daily_signals(
    features_df: pd.DataFrame,
    stocks_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Joins features onto the full stock price history (all trading days).
    News features are zero-filled on no-news days.
    Adds daily_return, next_day_return, direction, and has_news.
    """
    stocks = stocks_df.sort_values(["ticker", "date"]).copy()

    stocks["daily_return"] = stocks.groupby("ticker")["close"].transform("pct_change")
    stocks["next_day_return"] = stocks.groupby("ticker")["daily_return"].transform(
        lambda x: x.shift(-1)
    )
    stocks["direction"] = (stocks["next_day_return"] > 0).astype("Int8")

    feature_cols = [c for c in features_df.columns if c not in ("ticker", "date")]

    merged = stocks.merge(features_df, on=["ticker", "date"], how="left")
    merged[feature_cols] = merged[feature_cols].fillna(0)
    merged["has_news"] = merged["news_count"].astype(bool)

    return merged


def build_company_dim(
    companies_df: pd.DataFrame,
    enriched_df: pd.DataFrame,
) -> pd.DataFrame:
    """Enriches company master list with aggregated news/sentiment stats."""
    stats = (
        enriched_df
        .groupby("ticker")
        .agg(
            total_news_count=("url", "nunique"),
            avg_sentiment_all_time=("score", "mean"),
            first_article_date=("date", "min"),
            last_article_date=("date", "max"),
        )
        .reset_index()
    )

    # Top event type per company (based on LLM event columns)
    evt_counts = (
        enriched_df[enriched_df["llm_event_type"].notna()]
        .groupby(["ticker", "llm_event_type"])
        .size()
        .reset_index(name="cnt")
    )
    if not evt_counts.empty:
        top_evt = evt_counts.loc[evt_counts.groupby("ticker")["cnt"].idxmax()][["ticker", "llm_event_type"]]
        top_evt = top_evt.rename(columns={"llm_event_type": "top_event_type"})
    else:
        top_evt = pd.DataFrame(columns=["ticker", "top_event_type"])

    dim = companies_df.merge(stats, on="ticker", how="left")
    dim = dim.merge(top_evt, on="ticker", how="left")
    dim["total_news_count"] = dim["total_news_count"].fillna(0).astype(int)
    return dim


def build_bi_aggregations(
    features_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Resamples daily features to weekly and monthly aggregations."""
    agg_cols = {
        "news_count": "sum",
        "avg_sentiment": "mean",
        "std_sentiment": "mean",
        "positive_ratio": "mean",
        "negative_ratio": "mean",
        "event_ma": "sum",
        "event_earnings": "sum",
        "event_management": "sum",
        "event_legal": "sum",
        **{col: "sum" for col in LLM_EVT_COLS},
    }

    def _resample(freq: str, period_col: str) -> pd.DataFrame:
        frames = []
        for ticker, grp in features_df.groupby("ticker"):
            grp = grp.set_index("date").sort_index()
            resampled = grp.resample(freq).agg(agg_cols).reset_index()
            resampled.rename(columns={"date": period_col}, inplace=True)
            resampled.insert(0, "ticker", ticker)
            frames.append(resampled)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    weekly = _resample("W-MON", "week_start")
    monthly = _resample("MS", "month_start")
    return weekly, monthly


def build_commodity_features(engine: Engine) -> pd.DataFrame:
    """
    Reads public.commodities and computes rolling return/price windows
    so commodity context can be joined alongside gold.daily_signals.
    """
    df = pd.read_sql(
        "SELECT asset_key, date, name, category, close FROM commodities ORDER BY asset_key, date",
        engine,
        parse_dates=["date"],
    )
    if df.empty:
        return df

    df = df.sort_values(["asset_key", "date"])
    df["daily_return"] = df.groupby("asset_key")["close"].transform("pct_change")

    for window, suffix in [(3, "r3d"), (7, "r7d")]:
        df[f"close_{suffix}"] = (
            df.groupby("asset_key")["close"]
            .transform(lambda x: x.rolling(window, min_periods=1).mean())
        )
        df[f"return_{suffix}"] = (
            df.groupby("asset_key")["daily_return"]
            .transform(lambda x: x.rolling(window, min_periods=1).mean())
        )

    df["volatility_r7d"] = (
        df.groupby("asset_key")["daily_return"]
        .transform(lambda x: x.rolling(7, min_periods=1).std())
    )

    return df[["asset_key", "date", "name", "category", "close", "daily_return",
               "close_r3d", "close_r7d", "return_r3d", "return_r7d", "volatility_r7d"]]


def build_event_tables(
    events_raw_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Splits the raw events+bridge DataFrame into:
      - event_facts: one row per fingerprint
      - event_company_bridge: one row per (fingerprint, ticker)
    """
    event_facts = (
        events_raw_df[["fingerprint", "event_type", "event_date", "first_seen", "article_count"]]
        .drop_duplicates("fingerprint")
        .dropna(subset=["fingerprint"])
    )
    event_bridge = (
        events_raw_df[["fingerprint", "ticker"]]
        .dropna()
        .drop_duplicates()
    )
    return event_facts, event_bridge

# News-to-Price Correlation Plan

## Objective

Correlate CDX-harvested news articles with non-trivial stock price variations on the BVC (Bourse des Valeurs de Casablanca).

---

## What exists

| Layer | Location |
|---|---|
| Historical HTML archives | `backfill/data/{domain}/index.parquet` + `.html.gz` |
| Stock prices | `Data/stock_history_clean/` CSVs (OHLCV + `Variation %`) |
| Sentiment analyzer | `Pipelines/rss/analyzer.py` — `LLMArticleAnalyzer` |
| Entity extraction | outputs `mentioned_tickers` per article |
| PostgreSQL schema | `stock_prices`, `articles`, `sentiment_scores`, `article_company_mentions` |

---

## Correlation strategy: Event Study

"Non-trivial variation" = **Cumulative Abnormal Return (CAR)**, not raw price change. Removes market-wide noise.

```
Abnormal Return (AR) = actual daily return - expected return
  where expected = median return of that stock over trailing 60-120 days

CAR[-1, +3] = sum of AR for trading days t-1 through t+3
  where t = first trading day on/after article publication
```

A variation is "non-trivial" when `|CAR| > 1.5σ` of that stock's historical AR distribution.

---

## Pipeline to build

```
index.parquet
    ↓  read rows where status=200
.html.gz  ←  raw_html_path
    ↓  decompress + domain scraper (reuse rss/rss_fetcher.py)
Article text + original_published_date
    ↓  LLMArticleAnalyzer (existing)
ArticleSentiment { score, mentioned_tickers }
    ↓  map published_at → next BVC trading day
stock_history_clean/{ticker}.csv
    ↓  compute AR, CAR[-1,+3]
event_dataset.csv: (article_id, ticker, sentiment_score, confidence, CAR)
    ↓
Notebook: Pearson/Spearman correlation + significance + directional accuracy
```

### Steps

1. **Inventory harvested data**
   ```bash
   cd backfill && uv run python -m cdx_harvester.run stats
   ```

2. **Build backfill ingestion script** (`backfill/ingest.py`)
   - Read `index.parquet` with pandas/pyarrow
   - Decompress `.html.gz`, extract text with existing domain scrapers
   - Batch-feed to `LLMArticleAnalyzer`
   - Write to PostgreSQL: `articles`, `sentiment_scores`, `article_company_mentions`

3. **Build event dataset** (`Notebooks/event_study.ipynb`)
   - Join `article_company_mentions` × `sentiment_scores` × `stock_prices` on (ticker, event_date)
   - Compute AR and CAR per event
   - Output `event_dataset.csv`

4. **Correlation analysis**
   ```python
   from scipy import stats
   r, p = stats.spearmanr(df['sentiment_score'], df['CAR_3d'])
   accuracy = (np.sign(df['sentiment_score']) == np.sign(df['CAR_3d'])).mean()
   ```

---

## BVC-specific subtleties

- **Trading day mapping** — BVC hours ~9:30–15:30 Casablanca time (UTC+1). Articles after 15:30 → event day is T+1. Ramadan schedule shifts hours. Need a BVC trading calendar.
- **Liquidity gaps** — many tickers go days without a trade. Filter events where volume > 0 across the entire event window.
- **Article timestamp reliability** — `original_published_date` (from HTML meta tags) is the ground truth. CDX `capture_timestamp` is when Wayback crawled, not when published — can be hours/days late.
- **Ticker normalization** — LLM extracts company names, not always tickers. Use fuzzy-match logic from `neo4j_loader.py` to canonicalize mentions.
